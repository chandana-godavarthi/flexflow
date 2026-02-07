from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils,read_run_params,cdl_publishing,publish_delta_logs,time_perd_class_codes,get_database_config,materialise_path
from pyspark.sql.functions import current_timestamp
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient
import argparse
from tp_utils.common import get_logger

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

    time_perd_class_code=time_perd_class_codes(cntrt_id,catalog_name, postgres_schema,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info(f"Time Period Class Code: {time_perd_class_code}")

    t2_pub_mat_path = materialise_path(spark)
    df_fact_stgng_vw=spark.read.parquet(f'''{t2_pub_mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw''')

    # Define logical, physical, and Unity Catalog table names
    logical_table_name="TP_PROD_DIM"     #Eg: TP_WK_FCT
    physical_table_name="TP_PROD_DIM"    #Eg: TP_WK_FCT
    unity_catalog_table_name="TP_PROD_DIM"

    # Define partition columns separated by '/'
    partition_definition_value="part_srce_sys_id/part_cntrt_id" #Partition columns separated by /.

    # Call the cdl_publishing function with the defined parameters
    cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils,Configuration,MetaPSClient)

    # Define logical, physical, and Unity Catalog table names 
    logical_table_name="TP_MKT_DIM"     #Eg: TP_WK_FCT
    physical_table_name="TP_MKT_DIM"    #Eg: TP_WK_FCT
    unity_catalog_table_name="TP_MKT_DIM"

    # Define partition columns separated by '/'
    partition_definition_value="part_srce_sys_id" #Partition columns separated by /.

    # Call the cdl_publishing function with the defined parameters
    cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils,Configuration,MetaPSClient)

    # Define logical, physical, and Unity Catalog table names based on the frequency time period class code
    logical_table_name=f"TP_{time_perd_class_code}_FCT"     #Eg: TP_WK_FCT
    physical_table_name=f"TP_{time_perd_class_code}_FCT"    #Eg: TP_WK_FCT
    unity_catalog_table_name=f"TP_{time_perd_class_code}_FCT"

    # Define partition columns separated by '/'
    partition_definition_value="part_srce_sys_id/part_cntrt_id/part_mm_time_perd_end_date" #Partition columns separated by /.

    # Call the cdl_publishing function with the defined parameters
    cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils,Configuration,MetaPSClient)