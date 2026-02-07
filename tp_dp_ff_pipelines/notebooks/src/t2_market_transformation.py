from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from tp_utils.common import get_logger, get_dbutils, read_run_params, load_cntrt_lkp, read_from_postgres, dynamic_expression, column_complementer, assign_skid, semaphore_acquisition, get_database_config,materialise_path, release_semaphore, semaphore_generate_path,safe_merge_with_retry,safe_write_with_retry
import argparse
import os

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark)
    logger = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url = db_config['ref_db_jdbc_url']
    ref_db_name = db_config['ref_db_name']
    ref_db_user = db_config['ref_db_user']
    ref_db_pwd = db_config['ref_db_pwd']

    # Get the job parameters
    args = read_run_params()
    FILE_NAME = args.FILE_NAME
    CNTRT_ID = args.CNTRT_ID
    RUN_ID = args.RUN_ID


    notebook_name = 'market_transformation'

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']

    # Filter the DataFrame to get the 'Source System ID' and TIME_PERD_TYPE_CODE for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(CNTRT_ID, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    SRCE_SYS_ID = df_cntrt_lkp.collect()[0].srce_sys_id
    time_perd_type_code = df_cntrt_lkp.collect()[0].time_perd_type_code
    CNTRY_NAME = df_cntrt_lkp.collect()[0].cntry_name
    logger.info(f"Source System ID: {SRCE_SYS_ID} Cntry_name:{CNTRY_NAME} Time Period Type: {time_perd_type_code}")
    t2_mkt_trans_mat_path = materialise_path(spark)

    # Read the Product parquet file into DataFrame
    df_mkt_extrn=spark.read.parquet(f"{t2_mkt_trans_mat_path}/{RUN_ID}/load_market_df_mkt_extrn")
    logger.info("df_mkt_extrn")
    df_mkt_extrn.show(5)
    
    # Create a temporary view from the DataFrame df_mkt_extrn
    df_mkt_extrn.createOrReplaceTempView('df_mkt_extrn')

    logger.info("Started Market Transformation.")

    # Select distinct rows from df_mkt_extrn where extrn_mkt_id is not null
    df_mkt_stgng_vw=spark.sql('SELECT DISTINCT * from df_mkt_extrn WHERE extrn_mkt_id IS NOT NULL')
    df_mkt_stgng_vw.createOrReplaceTempView('df_mkt_stgng_vw')

    # Define the object name for the expression lookup table
    object_name=f'{postgres_schema}.mm_exprn_lkp'

    # Read the lookup table from PostgreSQL
    df_exprn_lkp=read_from_postgres(object_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_exprn_lkp")
    df_exprn_lkp.show()

    # Define operation type and dimension type for expression lookup
    exprn_name='Addition of Key Columns'
    dmnsn_name='MKT'

    # Generate a dynamic SQL query for "addition of key columns" from mm_exprn_lkp
    query=dynamic_expression(df_exprn_lkp,CNTRT_ID,exprn_name,dmnsn_name,spark)

    e=eval
    # Evaluate the generated query
    final_query=e(query)
    logger.info(f"Dynamic SQL query for Market Transformation: {final_query}")

    # Execute the final query and create a temporary view from the result
    df_mkt_stgng_vw=spark.sql(final_query)
    logger.info("df_mkt_stgng_vw")
    df_mkt_stgng_vw.show(5)

    df_sch_sdim=spark.sql(f'SELECT * FROM {catalog_name}.internal_tp.tp_mkt_sdim LIMIT 0')

    # Complement the columns of df_prod_stgng_vw with those in tp_mkt_sdim
    df_mkt_stgng_vw=column_complementer(df_mkt_stgng_vw,df_sch_sdim)

    # Create a temporary view from the complemented DataFrame
    df_mkt_stgng_vw.createOrReplaceTempView('df_mkt_stgng_vw')
    logger.info('df_mkt_stgng_vw')
    df_mkt_stgng_vw.show(5)

    # Define operation type and dimension type for expression lookup
    oprtn_type='Fill Missing Key Attributes'
    dim_type='MKT'

    # Generate a dynamic SQL query for "Fill Missing Key Attributes" 
    query=dynamic_expression(df_exprn_lkp,CNTRT_ID,oprtn_type,dim_type,spark)

    e=eval
    # Evaluate the generated query
    final_query=e(query)
    logger.info(f"Dynamic SQL query for Market Transformation: {final_query}")
    df_mkt_stgng_vw=spark.sql(final_query)
    logger.info('df_mkt_stgng_vw')
    df_mkt_stgng_vw.show(5)   

    # Filter the dataframe identifying rows with missing prod_skid values 
    df_mkt_isNull = df_mkt_stgng_vw.filter(df_mkt_stgng_vw['mkt_skid'].isNull())
    df_mkt_isNotNull = df_mkt_stgng_vw.filter(df_mkt_stgng_vw['mkt_skid'].isNotNull())
    logger.info(f"df_mkt_isNull count: {df_mkt_isNull.count()}")
    df_mkt_isNull.show(5)

    #Passing the dataframe with Null mkt_skid's to the assign skid definition
    df_skid_assigned=assign_skid(df_mkt_isNull,RUN_ID,dim_type,catalog_name,spark,SRCE_SYS_ID, CNTRT_ID)
    logger.info("Market Skid assigned for nulls")
    df_skid_assigned.count()

    # Merging both the dataframe 
    df_union=df_skid_assigned.unionByName(df_mkt_isNotNull)
    logger.info("df_union")
    df_union.show(5)

    #Create a temporary view from df_mkt_stgng_vw to use for merging with the mkt_sdim
    df_union.createOrReplaceTempView("markets_from_raw")

    # Define the path to the MKT_SDIM data based on source system and contract ID
    dim_type='TP_MKT_SDIM'
    mkt_path = semaphore_generate_path(dim_type, SRCE_SYS_ID, CNTRT_ID)

    # Print the constructed path for verification
    print(mkt_path)

    # Acquire semaphore to ensure safe access to the MKT_SDIM path for the given run ID
    mkt_check_path = semaphore_acquisition(RUN_ID, mkt_path,catalog_name,spark)

    try:
        print('Started writing to MKT_SDIM')
        safe_merge_with_retry(spark,df_union, f"{catalog_name}.internal_tp.tp_mkt_sdim", f"src.mkt_skid = tgt.mkt_skid AND tgt.part_srce_sys_id={SRCE_SYS_ID}")
        # Print the completion of the write operation to MKT_SDIM
        print('Completed writing data to MKT_SDIM')

    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,RUN_ID,mkt_check_path, spark)
        logger.info(f"Semaphore released for run_id={RUN_ID} and paths={mkt_check_path}")

    dim_type='TP_RUN_MKT_PLC'
    run_mkt_plc_path = semaphore_generate_path(dim_type, SRCE_SYS_ID, CNTRT_ID)
    # Print the constructed path for verification
    logger.info(f"run_mkt_plc_path: {run_mkt_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_RUN_MKT_PLC")
    run_mkt_plc_check_path = semaphore_acquisition(RUN_ID, run_mkt_plc_path,catalog_name, spark)
    try:
        logger.info(f"Started Writing to tp_run_mkt_plc")
        # Write selected columns from the dataFrame to the Delta table
        df_union_plc = df_union.select("run_id", "srce_sys_id",lit(CNTRT_ID).cast("long").alias("cntrt_id"), "mkt_skid","extrn_mkt_id",col("mkt_name").alias("extrn_mkt_name"))
        safe_write_with_retry(df_union_plc, f"{catalog_name}.internal_tp.tp_run_mkt_plc", "append", partition_by='cntrt_id', options=None)
        logger.info("Write to MKT_PLC Completed Successfully")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,RUN_ID,run_mkt_plc_check_path , spark)
        logger.info("TP_RUN_MKT_PLC Semaphore released")

    df_union.write.mode('overwrite').format('parquet').save(f'''{t2_mkt_trans_mat_path}/{RUN_ID}/{notebook_name}_df_mkt_stgng_vw''')
    logger.info(f"Market Transformation Materialized Path: {t2_mkt_trans_mat_path}/{RUN_ID}/{notebook_name}_df_mkt_stgng_vw")
