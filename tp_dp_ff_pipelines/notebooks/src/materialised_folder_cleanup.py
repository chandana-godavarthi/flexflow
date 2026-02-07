from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, get_database_config,get_logger,materialise_path,read_query_from_postgres,read_run_params
from pyspark.sql.functions import col
from datetime import datetime, timedelta

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

    # Get the job parameters
    args = read_run_params()
    retention_days = args.retention_days

    if retention_days:
        # Define base path
        materialised_path = materialise_path(spark)

        # Load run_plc table
        run_plc_df = read_query_from_postgres(f"SELECT run_id FROM {postgres_schema}.mm_run_plc where end_time < NOW() - INTERVAL '{int(retention_days)} DAYS' ", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

        files = dbutils.fs.ls(materialised_path)

        for f in files:
            file_name = f.name.rstrip("/")
            if run_plc_df.filter(col("run_id") == file_name).count() > 0:
                dbutils.fs.rm(f.path, recurse=True)
                logger.info(f"Deleted folder: {file_name}")

    else:
        logger.info("No Retention Days Provided")
            