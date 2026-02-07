from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils,read_run_params, cdl_publishing
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient
from pg_composite_pipelines_cdl.rest.azure_token_provider import SPAuthClient
from tp_utils.common import get_logger

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    logger = get_logger()
    dbutils = get_dbutils(spark)

    # Get the job parameters
    args = read_run_params()

    # Define logical, physical, and Unity Catalog table names
    logical_table_name="TP_PROD_DIM"
    physical_table_name="TP_PROD_DIM"
    unity_catalog_table_name="TP_PROD_DIM"

    partition_definition_value = "part_srce_sys_id/part_cntrt_id"

    logger.info("Started refreshing Metadata")
    cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value,dbutils,Configuration,MetaPSClient)
    logger.info("Metadata Refreshed")