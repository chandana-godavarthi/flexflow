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

   # Fetching wkf_job_id from mm_wkf_lkp
    query=f"SELECT * FROM {postgres_schema}.mm_wkf_lkp WHERE wkf_desc='Tier2_Reference_Data_Cleanup'"
    wkf_job_id=read_query_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd).first()['wkf_job_id']

    logger.info(f"[INFO] wkf_job_id: {wkf_job_id}")
    
    cntrt_id=0

    object_name = f'{postgres_schema}.mm_run_plc'

    # Prepare data for insertion into mm_run_plc
    cet_tz = pytz.timezone('CET')
    data = [(cntrt_id, 'WAITING', wkf_job_id, "Reference_Data_Cleanup", datetime.now(cet_tz), False)]
    columns = ['cntrt_id', 'run_sttus_name', 'wkf_job_id', 'file_name', 'rgstr_file_time', 'user_cancl_or_timedout']

    df = spark.createDataFrame(data, columns)
    logger.info(f"[INFO] Inserting run metadata into: {object_name}")
    df.show()
    write_to_postgres(df, object_name, ref_db_jdbc_url, ref_db_name, ref_db_user,ref_db_pwd)
    
    # Retrieve the run_id of the newly inserted record
    query = f"""
        SELECT * 
        FROM {object_name} 
        WHERE cntrt_id = '{cntrt_id}' AND run_sttus_name = 'WAITING' 
        ORDER BY rgstr_file_time DESC 
        LIMIT 1
    """
    logger.info(f"[DEBUG] Fetching run_id with query: {query}")
    run_id = read_query_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd).first()['run_id']
    
    logger.info(f"[INFO] run_id generated is: {run_id}")