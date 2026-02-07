from pyspark.sql import SparkSession
from tp_utils.common import read_from_postgres
from tp_utils.common import get_dbutils, read_run_params, load_cntrt_col_assign, get_database_config,materialise_path
import argparse
from tp_utils.common import get_logger

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
    
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    postgres_schema= db_config['postgres_schema']

    # Read the data from PostgreSQL into a DataFrame
    df_dpf_col_asign_vw=load_cntrt_col_assign(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    df_dpf_col_asign_vw.show(100)

    # Write the filtered DataFrame to a Parquet file in the specified directory
    base_path = materialise_path(spark)
    df_dpf_col_asign_vw.write.mode("overwrite").format('parquet').save(f'''{base_path}/{run_id}/column_mappings_df_dpf_col_asign_vw''')
    logger.info(f"Materialized path: {base_path}/{run_id}/column_mappings_df_dpf_col_asign_vw")