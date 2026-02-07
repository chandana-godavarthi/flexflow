from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from tp_utils.common import (get_dbutils, read_run_params,load_cntrt_lkp,read_from_postgres,dynamic_expression,column_complementer,assign_skid,semaphore_acquisition,get_logger, get_database_config,materialise_path, release_semaphore, semaphore_generate_path,safe_write_with_retry,retry_with_backoff)
from pyspark.sql.types import IntegerType
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

    notebook_name = 'product_transformation'

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']

    # Filter the DataFrame to get the 'Source System ID' and TIME_PERD_TYPE_CODE for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(CNTRT_ID, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    SRCE_SYS_ID = df_cntrt_lkp.collect()[0].srce_sys_id
    time_perd_type_code = df_cntrt_lkp.collect()[0].time_perd_type_code
    cntry_name=df_cntrt_lkp.collect()[0].cntry_name
    logger.info(f"Source System ID: {SRCE_SYS_ID} Cntry_name:{cntry_name} Time Period Type: {time_perd_type_code}")
    
    # Define the light refined path
    LIGHT_REFINED_PATH='tp-publish-data/'
    t2_prod_trans_mat_path = materialise_path(spark)

    # Read the Product parquet file into DataFrame
    df_prod_extrn=spark.read.parquet(f"{t2_prod_trans_mat_path}/{RUN_ID}/load_product_df_prod_extrn")
    logger.info("df_prod_extrn")

    logger.info("Product Transformation started")

    # Create a temporary view from the DataFrame df_prod_extrn
    df_prod_extrn.createOrReplaceTempView('df_prod_extrn')

    # Select distinct rows from df_prod_extrn where extrn_prod_id is not null
    df_prod_stgng_vw = spark.sql('SELECT DISTINCT * from df_prod_extrn WHERE extrn_prod_id IS NOT NULL')
    df_prod_stgng_vw.createOrReplaceTempView('df_prod_stgng_vw')

    # Define the object name for the expression lookup table
    object_name = f'{postgres_schema}.mm_exprn_lkp'

    # Read the lookup table from PostgreSQL
    df_exprn_lkp = read_from_postgres(object_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_exprn_lkp")

    # Define operation type and dimension type for expression lookup
    exprn_name = 'Addition of Key Columns'
    dmnsn_name = 'PROD'

    # Generate a dynamic SQL query for "addition of key columns" from mm_exprn_lkp
    query = dynamic_expression(df_exprn_lkp, CNTRT_ID, exprn_name, dmnsn_name,spark)

    e=eval
    # Evaluate the generated query
    final_query = e(query)
    logger.info(f"Dynamic SQL query for Product Transformation: {final_query}")
    # Execute the final query and create a temporary view from the result
    df_prod_stgng_vw = spark.sql(final_query)
    logger.info("df_prod_stgng_vw")

    # DBTITLE 1,Read PROD_SDIM schema from Delta Table
    df_sch_sdim=spark.sql(f'SELECT * FROM {catalog_name}.internal_tp.tp_prod_sdim LIMIT 0')

    # Complement the columns of df_prod_stgng_vw with those in tp_prod_sdim
    df_prod_stgng_vw=column_complementer(df_prod_stgng_vw,df_sch_sdim)
    logger.info('df_prod_stgng_vw')

    # Create a temporary view from the complemented DataFrame
    df_prod_stgng_vw.createOrReplaceTempView('df_prod_stgng_vw')

    # DBTITLE 1,Fill Missing Key Attributes from DIM
    # Define operation type and dimension type for expression lookup
    oprtn_type='Fill Missing Key Attributes'
    dim_type='PROD'

    # Generate a dynamic SQL query for "Fill Missing Key Attributes" from {catalog_name}.mm_exprn_lkp
    query=dynamic_expression(df_exprn_lkp,CNTRT_ID,oprtn_type,dim_type,spark)

    e=eval
    # Evaluate the generated query
    final_query=e(query)
    logger.info(f"Dynamic SQL query for Product Transformation: {final_query}")

    # Execute the final query 
    df_prod_stgng_vw=spark.sql(final_query) 
    logger.info('df_prod_stgng_vw')

    # Filter the dataframe identifying rows with missing prod_skid values 

    df_prod_isNull = df_prod_stgng_vw.filter(df_prod_stgng_vw['prod_skid'].isNull())
    df_prod_isNotNull = df_prod_stgng_vw.filter(df_prod_stgng_vw['prod_skid'].isNotNull())

    logger.info(f"df_prod_stgng_vw Row count: {df_prod_stgng_vw.count()}")
    logger.info("New products Count:")
    df_prod_isNull.count()

    #Passing the dataframe with Null prod_skid's to the assign skid definition 
    df_skid_assigned=assign_skid(df_prod_isNull,RUN_ID,dim_type,catalog_name,spark,SRCE_SYS_ID, CNTRT_ID)
    logger.info("Product skid assigned for new products: df_skid_assigned")
    df_skid_assigned.count()

    # Merging both the dataframe  
    df_union=df_skid_assigned.unionByName(df_prod_isNotNull)
    logger.info(f"Row count after merge : {df_union.count()}")

    #Create a temporary view from df_prod_stgng_vw to use for merging with the prod_sdim
    df_union.createOrReplaceTempView("products_from_raw")


    # Define the path to the PROD_SDIM data based on source system and contract ID
    dim_type='TP_PROD_SDIM'
    prod_path = semaphore_generate_path(dim_type, SRCE_SYS_ID, CNTRT_ID)

    print(prod_path)

    # Acquire semaphore to ensure safe access to the PROD_SDIM path for the given run ID
    prod_check_path = semaphore_acquisition(RUN_ID, prod_path,catalog_name,spark)
    try:
        print('Started writing to PROD_SDIM')

        # DBTITLE 1,Update PROD_SDIM with New Product Data using prod_skid
        # Define operation type and dimension type for expression lookup
        oprtn_type='Merge Products to PROD_SDIM'
        dim_type='PROD'
        part_cntrt_id=0
        # Generate a dynamic SQL query for "Fill Missing Key Attributes" from {catalog_name}.mm_exprn_lkp
        query=dynamic_expression(df_exprn_lkp,CNTRT_ID,oprtn_type,dim_type,spark)

        e=eval
        # Evaluate the generated query
        final_query=e(query)
        logger.info(f"Dynamic SQL query for Product Transformation: {final_query}")

        # Execute the final query 
        @retry_with_backoff()
        def execute_dynamic_merge():
            df_merge_output=spark.sql(final_query) 
            logger.info('Merge operation completed on PROD_SDIM')
            df_merge_output.show(5)
        
        execute_dynamic_merge()
        # Print the completion of the write operation to PROD_SDIM
        print('Completed writing data to PROD_SDIM')

    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,RUN_ID,prod_check_path, spark)
        logger.info(f"Semaphore released for run_id={RUN_ID} and paths={prod_check_path}")

    # DBTITLE 1,Update run_prod_plc (Logging table)
    dim_type='TP_RUN_PROD_PLC'
    run_prod_plc_path = semaphore_generate_path(dim_type, SRCE_SYS_ID, CNTRT_ID)
    # Print the constructed path for verification
    logger.info(f"run_prod_plc_path: {run_prod_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_RUN_PROD_PLC")
    run_prod_plc_check_path = semaphore_acquisition(RUN_ID, run_prod_plc_path,catalog_name, spark)
    try:
        logger.info(f"Started Writing to tp_run_prod_plc")
        # Write selected columns from the DataFrame to the Delta table
        df_union_plc = df_union.select("run_id", "srce_sys_id", lit(CNTRT_ID).cast("long").alias("cntrt_id"), "prod_skid","extrn_prod_id",col("prod_name").alias("extrn_prod_name"),"extrn_prod_attr_val_list")
        safe_write_with_retry(df_union_plc, f"{catalog_name}.internal_tp.tp_run_prod_plc", "append", partition_by='cntrt_id', options=None)
        logger.info(" Write to TP_PROD_PLC completed successfully")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,RUN_ID,run_prod_plc_check_path , spark)
        logger.info("TP_RUN_PROD_PLC Semaphore released")

    # DBTITLE 1,Publish Product Dataframe
    df_union.write.mode("overwrite").format('parquet').save(f'''{t2_prod_trans_mat_path}/{RUN_ID}/{notebook_name}_df_prod_stgng_vw''')
    logger.info(f"Materialized Path: {t2_prod_trans_mat_path}/{RUN_ID}/{notebook_name}_df_prod_stgng_vw")