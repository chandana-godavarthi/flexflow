from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,load_file,load_cntrt_lkp,load_cntrt_file_lkp,load_cntrt_dlmtr_lkp,fact_multipliers_trans, get_database_config
from tp_utils.common import get_logger, t2_load_file

if __name__ == "__main__":
    spark_four_file_fact = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark_four_file_fact)
    logger = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url_four_file_fact = db_config['ref_db_jdbc_url']
    ref_db_name_four_file_fact = db_config['ref_db_name']
    ref_db_user_four_file_fact = db_config['ref_db_user']
    ref_db_pwd_four_file_fact = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_four_file_fact = args.CNTRT_ID
    run_id_four_file_fact = args.RUN_ID

    # Define the file type as 'fact'
    file_type_four_file_fact='fact'
    dmnsn_name_four_file_fact='FCT'
    notebook_name_four_file_fact = 'load_fact'
    postgres_schema_four_file_fact= db_config['postgres_schema']

    t2_load_file(cntrt_id_four_file_fact, dmnsn_name_four_file_fact, file_type_four_file_fact, run_id_four_file_fact, notebook_name_four_file_fact, postgres_schema_four_file_fact, spark_four_file_fact, ref_db_jdbc_url_four_file_fact, ref_db_name_four_file_fact, ref_db_user_four_file_fact, ref_db_pwd_four_file_fact)

    logger.info("Started Fact Multipliers Transformation.")
    fact_multipliers_trans(run_id_four_file_fact,cntrt_id_four_file_fact,spark_four_file_fact)