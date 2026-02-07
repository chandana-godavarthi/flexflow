from pyspark.sql import SparkSession
from tp_utils.common import load_file,get_dbutils,read_run_params,load_cntrt_lkp,load_cntrt_file_lkp,load_cntrt_dlmtr_lkp, get_database_config, t2_load_file
from tp_utils.common import get_logger

if __name__ == "__main__":
    spark_single_file_mkt = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark_single_file_mkt)

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url_single_file_mkt = db_config['ref_db_jdbc_url']
    ref_db_name_single_file_mkt = db_config['ref_db_name']
    ref_db_user_single_file_mkt = db_config['ref_db_user']
    ref_db_pwd_single_file_mkt = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_single_file_mkt = args.CNTRT_ID
    run_id_single_file_mkt = args.RUN_ID

    # Get Schema names from keyvault 
    postgres_schema_single_file_mkt= db_config['postgres_schema']
    # Define the file type as 'mkt'
    file_type_single_file_mkt='mkt'
    dmnsn_name_single_file_mkt='COMMON'
    notebook_name_single_file_mkt = 'load_market'

    t2_load_file(cntrt_id_single_file_mkt, dmnsn_name_single_file_mkt, file_type_single_file_mkt, run_id_single_file_mkt, notebook_name_single_file_mkt, postgres_schema_single_file_mkt, spark_single_file_mkt, ref_db_jdbc_url_single_file_mkt, ref_db_name_single_file_mkt, ref_db_user_single_file_mkt, ref_db_pwd_single_file_mkt)
