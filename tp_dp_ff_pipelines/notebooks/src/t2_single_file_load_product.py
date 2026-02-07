from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,load_file,load_cntrt_lkp,load_cntrt_file_lkp,load_cntrt_dlmtr_lkp, get_database_config, t2_load_file
from tp_utils.common import get_logger

if __name__ == "__main__":
    spark_single_file_prod = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark_single_file_prod)

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url_single_file_prod = db_config['ref_db_jdbc_url']
    ref_db_name_single_file_prod = db_config['ref_db_name']
    ref_db_user_single_file_prod = db_config['ref_db_user']
    ref_db_pwd_single_file_prod = db_config['ref_db_pwd']
    # Get the job parameters
    args = read_run_params()
    cntrt_id_single_file_prod = args.CNTRT_ID
    run_id_single_file_prod = args.RUN_ID

    # Define the file type as 'fact'
    file_type_single_file_prod='prod'
    dmnsn_name_single_file_prod='COMMON'
    notebook_name_single_file_prod = 'load_product'
    postgres_schema_single_file_prod= db_config['postgres_schema']

    t2_load_file(cntrt_id_single_file_prod, dmnsn_name_single_file_prod, file_type_single_file_prod, run_id_single_file_prod, notebook_name_single_file_prod, postgres_schema_single_file_prod, spark_single_file_prod, ref_db_jdbc_url_single_file_prod, ref_db_name_single_file_prod, ref_db_user_single_file_prod, ref_db_pwd_single_file_prod)