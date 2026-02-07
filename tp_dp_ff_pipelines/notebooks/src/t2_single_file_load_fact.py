from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,load_file,load_cntrt_lkp,load_cntrt_file_lkp,load_cntrt_dlmtr_lkp,fact_multipliers_trans, get_database_config
from tp_utils.common import get_logger, t2_load_file

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
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    # Define the file type as 'fact'
    file_type='fact'
    dmnsn_name='COMMON'
    notebook_name = 'load_fact'
    postgres_schema= db_config['postgres_schema']

    t2_load_file(cntrt_id, dmnsn_name, file_type, run_id, notebook_name, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    logger.info("Started Fact Multipliers Transformation")
    fact_multipliers_trans(run_id,cntrt_id,spark)