from pyspark.sql.functions import col, lit , upper
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import re

from tp_utils.common import get_dbutils, get_logger, get_database_config, read_run_params, read_from_postgres, update_to_postgres, trunc_and_write_to_postgres, cdl_publishing, sanitize_variable,column_complementer
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient


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
    refresh_type = args.refresh_type
    logger.info(f'REFRESH TYPE: {refresh_type}')

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']


    # Load and filter table configuration
    table_config = spark.sql(f"""
        SELECT * 
        FROM {catalog_name}.internal_tp.tp_refresh_tbl_lkp
        WHERE refresh_type = :refresh_type AND flag="Y"
        """,{'refresh_type':refresh_type})
    e=eval
   
    #Process each config row
    for config in table_config.collect():
        src_table = config["tgt_table"]
        tgt_table = config["src_table"]
        state = config["state"]
        part_keys = config["part_key_name"]
        part_value = e(config["part_key_value"])
        key_cols = config["key_cols"]

        logger.info(f'SOURCE TABLE: {src_table}')
        logger.info(f'TARGET TABLE: {tgt_table}')

        #Read source and target tables
        df_src = spark.table(f"{catalog_name}.{src_table}") 
        df_src=df_src.drop("part_srce_sys_id", "part_cntrt_id", "secure_group_key")    
        df_tgt = read_from_postgres(f"{postgres_schema}.{tgt_table}", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
        logger.info('LOADED SOURCE TABLE DATA INTO DATAFRAME')
        df_src = column_complementer(df_src,df_tgt)

        cdl_table = tgt_table.split(".", 2)[-1].upper()
        logical_table_name = physical_table_name = unity_catalog_table_name = cdl_table

        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', postgres_schema) or not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', tgt_table):
            raise ValueError("Unsafe schema or table name")
        
        tbl_name=f'{postgres_schema}.{tgt_table}'
        tbl_name=sanitize_variable(tbl_name)
        params=()

        logger.info("Writing data to target postgres table")
        trunc_and_write_to_postgres(df_src, tbl_name,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)