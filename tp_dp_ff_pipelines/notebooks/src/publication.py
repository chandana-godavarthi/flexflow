from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tp_utils.common import (get_dbutils, read_run_params, time_perd_class_codes, work_to_arch,semaphore_acquisition, add_secure_group_key, load_cntrt_lkp,column_complementer, release_semaphore, get_database_config,match_time_perd_class, materialise_path, update_to_postgres,semaphore_generate_path, dynamic_expression, read_from_postgres,safe_write_with_retry, safe_merge_with_retry, retry_with_backoff,get_logger,publish_delta_logs,calculate_retention_date,match_time_perd_class)
import argparse

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
    ref_db_hostname = db_config['ref_db_hostname']
  
    # Get the job parameters
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID
    job_run_id = args.job_run_id

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']
    # Define the light refined path
    LIGHT_REFINED_PATH='tp-publish-data/'

    # Read the table to get the 'Source System ID' for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
    retention_period = df_cntrt_lkp.collect()[0].retn_perd 
    logger.info(f"Source System ID: {srce_sys_id},retention_period: {retention_period}")

    time_perd_class_code=time_perd_class_codes(cntrt_id,catalog_name, postgres_schema,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info(f"Time Period Class Code: {time_perd_class_code}")
    t2_pub_mat_path = materialise_path(spark)

    min_retention_date = calculate_retention_date(run_id,cntrt_id, srce_sys_id,retention_period,catalog_name,postgres_schema, time_perd_class_code,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info(f"min_retention_date: {min_retention_date}")

    # Read the lookup table from PostgreSQL
    # Define the object name for the expression lookup table
    object_name = f'{postgres_schema}.mm_exprn_lkp'
    df_exprn_lkp = read_from_postgres(object_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_exprn_lkp")

    if srce_sys_id != 3:
        df_mkt_stgng_vw=spark.read.parquet(f'''{t2_pub_mat_path}/{run_id}/market_transformation_df_mkt_stgng_vw''')
        df_mkt_dim = spark.sql(f"select * from {catalog_name}.gold_tp.tp_mkt_dim limit 0")
        df_mkt_stgng_vw = column_complementer(df_mkt_stgng_vw, df_mkt_dim)
        df_mkt_stgng_vw=add_secure_group_key(df_mkt_stgng_vw,cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        df_mkt_stgng_vw.createOrReplaceTempView('markets_from_raw')
        logger.info("df_mkt_stgng_vw")

    if srce_sys_id != 3:
        # Define the path to the market dimension data based on source system and contract ID
        
        dim_type='TP_MKT_DIM'
        mkt_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)

        # Print the constructed path for verification
        logger.info(f"mkt_path: {mkt_path}")
    
        # Acquire semaphore for writing to the specified path
        logger.info("Acquiring semaphore for Market dimension.")
        mkt_check_path = semaphore_acquisition(run_id, mkt_path,catalog_name, spark)

        try:
            logger.info("Writing to TP_MKT_DIM.")
            safe_merge_with_retry(spark,df_mkt_stgng_vw, f"{catalog_name}.gold_tp.tp_mkt_dim", f"src.mkt_skid = tgt.mkt_skid AND tgt.part_srce_sys_id={srce_sys_id}")
            logger.info("Completed writing data to MKT_DIM")

        finally:
            # Release the semaphore for the current run
            release_semaphore(catalog_name,run_id,mkt_check_path , spark)
            logger.info("MKT_DIM Semaphore released")
    
    # Load product and fact staging tables
    df_prod_stgng_vw=spark.read.parquet(f'''{t2_pub_mat_path}/{run_id}/product_transformation_df_prod_stgng_vw''')
    df_prod_dim = spark.sql(f"select * from {catalog_name}.gold_tp.tp_prod_dim limit 0")
    df_prod_stgng_vw = column_complementer(df_prod_stgng_vw, df_prod_dim)

    #Adding secure group key
    df_prod_stgng_vw=add_secure_group_key(df_prod_stgng_vw,cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_prod_stgng_vw")
    
    if srce_sys_id==3:
        part_cntrt_id=cntrt_id
    else:
        part_cntrt_id=0

    #Publish product
    # Define the path to the product dimension data based on source system and contract ID
    dim_type='TP_PROD_DIM'
    prod_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)

    # Print the constructed path for verification
    logger.info(f"prod_path: {prod_path}")

    logger.info("Acquiring semaphore for Product dimension.")
        # Acquire semaphore for writing to the specified product path
    prod_check_path = semaphore_acquisition(run_id, prod_path,catalog_name, spark)
    
    df_prod_stgng_vw.createOrReplaceTempView("prod_input")
    
    try:
        logger.info("Writing to TP_PROD_DIM.")
        # Define operation type and dimension type for expression lookup
        oprtn_type='Merge Products to PROD_DIM'
        dim_type='PROD'

        # Generate a dynamic SQL query for "Merge Products to PROD_DIM" from {catalog_name}.mm_exprn_lkp
        query=dynamic_expression(df_exprn_lkp,cntrt_id,oprtn_type,dim_type,spark)

        e=eval
        # Evaluate the generated query
        final_query=e(query)
        logger.info(f"Dynamic SQL query for Product Publication: {final_query}")
        
        # Execute the final query 
        @retry_with_backoff()
        def execute_dynamic_merge():
            df_merge_output=spark.sql(final_query) 
            logger.info('Merge operation completed on PROD_DIM')
            df_merge_output.show(5)
            logger.info('Completed writing data to PROD_DIM')
        execute_dynamic_merge()

    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,run_id,prod_check_path , spark)
        logger.info("PROD_DIM Semaphore released")
    
    df_fact_stgng_vw=spark.read.parquet(f'''{t2_pub_mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw''')

    class_code = match_time_perd_class(time_perd_class_code.lower())
    df_fct_tgt = spark.sql(f"select * from {catalog_name}.gold_tp.TP_{class_code}_FCT limit 0")
    df_fact_stgng_vw = column_complementer(df_fact_stgng_vw, df_fct_tgt)

    df_fact_stgng_vw=add_secure_group_key(df_fact_stgng_vw,cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_fact_stgng_vw")

    df_fact_stgng_vw.createOrReplaceTempView('facts_from_raw')

    # Fetch distinct partitions from tp_run_prttn_plc
    affected_partitions = spark.sql(f"""
            SELECT DISTINCT  
                mm_time_perd_end_date AS part_mm_time_perd_end_date 
            FROM {catalog_name}.gold_tp.tp_run_prttn_plc 
            WHERE run_id = :run_id and cntrt_id= :cntrt_id
        """,{"run_id":run_id,"cntrt_id":cntrt_id}).collect()
    # print(affected_partitions)
    # Generate paths for each affected partition
    paths = [
            f"/mnt/tp-publish-data/TP_{time_perd_class_code}_FCT/part_srce_sys_id={srce_sys_id}/part_cntrt_id={cntrt_id}/part_time_perd_end_date={row.part_mm_time_perd_end_date}"
            for row in affected_partitions
        ]
    logger.info(f"Fact_path: {paths}")
    #Define fact table name
    table = f"{catalog_name}.gold_tp.TP_{time_perd_class_code}_FCT"
    print(table)

    class_code = match_time_perd_class(time_perd_class_code.lower())
    table_name = f"{catalog_name}.gold_tp.tp_{class_code}_fct"
    
    # Acquire the semaphore for the current run
    logger.info(f"Acquiring semaphore for {table} table.")
    check_path = semaphore_acquisition(run_id, paths,catalog_name, spark)

    try:
        df_fact_stgng_vw=df_fact_stgng_vw.withColumn('part_srce_sys_id',col("part_srce_sys_id").cast("short"))
        logger.info(f'Started writing data to {table}')
        safe_write_with_retry(df_fact_stgng_vw, table, "overwrite",["part_srce_sys_id","part_cntrt_id","part_mm_time_perd_end_date"],options={"partitionOverwriteMode": "dynamic"})

        logger.info(f'Completed writing data to {table}')
        
        fct_avail_ind = df_cntrt_lkp.collect()[0].fct_avail_ind
        # Update fct_avail_ind 
        if fct_avail_ind=='N':
            if (df_fact_stgng_vw.count() and srce_sys_id!=3):
                query=f"UPDATE {postgres_schema}.mm_cntrt_lkp SET fct_avail_ind='Y' WHERE cntrt_id=%s"
                params=(cntrt_id,)
                update_to_postgres(query,params, ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)
                logger.info('Updated fct_avail_ind in table mm_cntrt_lkp to Y')

        # Deleting below retention period data
        if min_retention_date:
            spark.sql(f"""DELETE FROM {table_name} 
                        WHERE part_cntrt_id = :cntrt_id
                        AND part_mm_time_perd_end_date < :min_retention_date """,{'cntrt_id':cntrt_id,'min_retention_date':min_retention_date})
            logger.info(f" Retention data cleanup completed successfully for {table}")

    finally:
        release_semaphore(catalog_name,run_id,check_path , spark)
        logger.info(f"{table} Semaphore released")

    # Call the publish_delta_logs function to log the delta changes
    df_fact_stgng_vw.createOrReplaceTempView("df_fact_stgng_vw")
    df_partition_list = spark.sql(f""" select distinct {job_run_id} as run_id, current_timestamp as update_timestamp, part_srce_sys_id, part_cntrt_id, part_mm_time_perd_end_date from df_fact_stgng_vw""")
    logger.info(f"No.Of partitions to be published: {df_partition_list.count()}")
    physical_table_name=f"TP_{time_perd_class_code}_FCT"  

    publish_delta_logs(spark,physical_table_name.lower(),catalog_name,df_partition_list)

    # Archive the processed file by moving it from the working directory to the archive directory
    work_to_arch(file_name,dbutils)
    logger.info("Archived the processed file from working directory to the archive directory.")