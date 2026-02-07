from pyspark.sql import SparkSession
from datetime import datetime, timezone
from tp_utils.common import (
    get_dbutils, get_logger, get_database_config, read_from_postgres,
    semaphore_acquisition, release_semaphore, write_to_postgres, update_to_postgres, read_query_from_postgres,read_run_params
)
import pytz
import argparse

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()

    # Get Databricks utilities and logger
    dbutils = get_dbutils(spark)
    logger = get_logger()

    # Retrieve database and catalog configuration from secrets
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url = db_config['ref_db_jdbc_url']
    ref_db_name = db_config['ref_db_name']
    ref_db_user = db_config['ref_db_user']
    ref_db_pwd = db_config['ref_db_pwd']
    ref_db_hostname = db_config['ref_db_hostname']
    catalog_name = db_config['catalog_name']
    postgres_schema = dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
   
    
    # Get the job parameters
    args = read_run_params()
    run_id=int(args.RUN_ID)
    logger.info(f"[INFO] run_id: {run_id}")

    # Define the view name to read cleanup metadata
    vw_name = f'{postgres_schema}.mm_ref_data_clnup_vw'

    # Read cleanup metadata from Postgres
    df_data_cleanup_vw = read_from_postgres(vw_name, spark,
                                            ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_data_cleanup_vw.show()

    # Abort if no records found for cleanup
    if df_data_cleanup_vw.count() == 0:
        raise Exception('No successful runs executed in the current time frame')


    queries = []

    try:
        # Iterate over each row in the cleanup metadata
        for row in df_data_cleanup_vw.collect():
            time_code = row.time_perd_class_code
            affected_partitions = row.srce_sys_id.split(',')

            logger.info(f"{time_code}, {affected_partitions}")

            # Build query to fetch relevant skids from fact table
            queries.append(
                f'''SELECT prod_skid, mkt_skid FROM {catalog_name}.gold_tp.TP_{time_code}_FCT 
                WHERE part_srce_sys_id IN ({row.srce_sys_id})'''
            )

            # Build paths for market and product partitions
            path_mkt_dim = [f"/mnt/tp-publish-data/TP_MKT_DIM/part_srce_sys_id={i}" for i in affected_partitions]
            path_prod_dim = [f"/mnt/tp-publish-data/TP_PROD_DIM/part_srce_sys_id={i}" for i in affected_partitions]

            path_mkt_sdim = [f"/mnt/tp-publish-data/TP_MKT_SDIM/part_srce_sys_id={i}" for i in affected_partitions]
            path_prod_sdim = [f"/mnt/tp-publish-data/TP_PROD_SDIM/part_srce_sys_id={i}" for i in affected_partitions]            

            logger.info(f"Market_Dim_Path: {path_mkt_dim}")
            logger.info(f"Product_Dim_Path: {path_prod_dim}")

            logger.info(f"Market_Sdim_Path: {path_mkt_sdim}")
            logger.info(f"Product_Sdim_Path: {path_prod_sdim}")

            # Acquire semaphore lock for both paths DIM and SDIM 
            semaphore_acquisition(run_id, path_mkt_dim, catalog_name, spark)
            semaphore_acquisition(run_id, path_prod_dim, catalog_name, spark)

            semaphore_acquisition(run_id, path_mkt_sdim, catalog_name, spark)
            semaphore_acquisition(run_id, path_prod_sdim, catalog_name, spark)

        # Combine all queries using UNION
        final_query = "\nUNION\n".join(queries)
        logger.info(f"[DEBUG] Query: {final_query}")

        # Execute the combined query
        df_fct = spark.sql(final_query)

        # Extract distinct product and market skids
        df_prod_skid_in_fct = df_fct.select("prod_skid").distinct()
        df_mkt_skid_in_fct = df_fct.select("mkt_skid").distinct()

        print('COUNT OF DISTINCT PROD_SKIDS IN FACT', df_prod_skid_in_fct.count())
        print('COUNT OF DISTINCT MKT_SKIDS IN FACT', df_mkt_skid_in_fct.count())

        # Create temp views for comparison
        df_prod_skid_in_fct.createOrReplaceTempView('mm_prod_skid_in_fct')
        df_mkt_skid_in_fct.createOrReplaceTempView('mm_mkt_skid_in_fct')

        # Identify product skids to delete
        query_prod = f'''
            SELECT prod_skid FROM {catalog_name}.internal_tp.tp_prod_sdim 
            EXCEPT
            SELECT prod_skid FROM mm_prod_skid_in_fct
        '''
        df_prod_del = spark.sql(query_prod)
        df_prod_del.createOrReplaceTempView('mm_prod_del')

        # Identify market skids to delete
        query_mkt = f'''
            SELECT mkt_skid FROM {catalog_name}.internal_tp.tp_mkt_sdim
            EXCEPT
            SELECT mkt_skid FROM mm_mkt_skid_in_fct
        '''
        df_mkt_del = spark.sql(query_mkt)
        df_mkt_del.createOrReplaceTempView('mm_mkt_del')

        # Delete stale records from test tables
        logger.info(f"[INFO] DELETING THE DATA FROM PROD_SDIM")
        spark.sql(f'DELETE FROM {catalog_name}.internal_tp.tp_prod_sdim WHERE prod_skid IN(SELECT prod_skid FROM mm_prod_del)')

        logger.info(f"[INFO] DELETING THE DATA FROM MKT_SDIM")
        spark.sql(f'DELETE FROM {catalog_name}.internal_tp.tp_mkt_sdim WHERE mkt_skid IN(SELECT mkt_skid FROM mm_mkt_del)')

        logger.info(f"[INFO] DELETING THE DATA FROM PROD_DIM")
        spark.sql(f'DELETE FROM {catalog_name}.gold_tp.tp_prod_dim WHERE prod_skid IN(SELECT prod_skid FROM mm_prod_del)')

        logger.info(f"[INFO] DELETING THE DATA FROM MKT_DIM")
        spark.sql(f'DELETE FROM {catalog_name}.gold_tp.tp_mkt_dim WHERE mkt_skid IN(SELECT mkt_skid FROM mm_mkt_del)')


    finally:
        # Always release semaphore lock
        logger.info(f"[INFO] RELEASING THE SEMAPHORE")
        spark.sql(f"DELETE FROM {catalog_name}.internal_tp.tp_run_lock_plc WHERE run_id = {run_id}")