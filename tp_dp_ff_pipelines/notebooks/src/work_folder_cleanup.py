from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, get_database_config,get_logger,derive_base_path,read_query_from_postgres
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    logger = get_logger()
    dbutils = get_dbutils(spark)

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url = db_config['ref_db_jdbc_url']
    ref_db_name = db_config['ref_db_name']
    ref_db_user = db_config['ref_db_user']
    ref_db_pwd = db_config['ref_db_pwd']
    postgres_schema= db_config['postgres_schema']

    # Define paths
    base_path= derive_base_path(spark)
    work_path = base_path + "/WORK/"
    reject_path = base_path + "/REJECT/"

    # Load run_plc table
    run_plc_df = read_query_from_postgres(f"SELECT file_name FROM {postgres_schema}.mm_run_plc where run_sttus_name in ('FAILED','TIMEDOUT','CANCELED','REMOVED') ", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    # List files in WORK
    files = dbutils.fs.ls(work_path)

    for f in files:
        file_name = f.name.rstrip("/")
        if run_plc_df.filter(col("file_name").contains(file_name)).count() > 0:
            if file_name.lower().endswith(".zip"):
                dbutils.fs.mv(work_path + file_name, reject_path + file_name)
                logger.info(f"File moved from WORK to REJECT: {file_name}")
            else:
                dbutils.fs.rm(work_path + file_name, recurse=True)
            