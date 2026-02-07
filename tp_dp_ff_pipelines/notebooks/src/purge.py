from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params
from pyspark.sql.functions import lower
import argparse
from tp_utils.common import time_perd_class_codes, get_database_config, load_cntrt_lkp, recursive_delete, cdl_publishing,derive_publish_path, update_to_postgres
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient
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
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID
    publish_path = derive_publish_path(spark)
    logger.info(f"Contract id that being purged: {cntrt_id}")

    postgres_schema= db_config['postgres_schema']
    # Retrieving the catalog name securely from Databricks secrets
    catalog_name = db_config['catalog_name']

    df = load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    
    # Assuming df is your DataFrame and it has only one row
    df.show()

    if df.count() >0:
        srce_sys_id = df.select("srce_sys_id").first()["srce_sys_id"]

        time_perd_class_code=time_perd_class_codes(cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        # Dynamically constructing the table name using catalog and time period class code
        table_name = f"{catalog_name}.gold_tp.tp_{time_perd_class_code.lower()}_fct"
        logger.info(f"table name: {table_name}")

        # Define the target directory path
        target_path = f"{publish_path}/TP_{time_perd_class_code}_FCT/part_srce_sys_id={srce_sys_id}/part_cntrt_id={cntrt_id}"

        logger.info(f"BLOB path that will be removed: {target_path}")

        # Executing a SQL DELETE query to remove records with the specified contract ID
        delete_query = f"""
        DELETE FROM {table_name}
        WHERE part_cntrt_id = :cntrt_id
        """
        query = spark.sql(delete_query,{"cntrt_id":cntrt_id})
        
        # Update fct_avail_ind to 'N' in mm_cntrt_lkp 
        query=f"UPDATE {postgres_schema}.mm_cntrt_lkp SET fct_avail_ind='N' WHERE cntrt_id=%s"
        params=(cntrt_id,)
        
        update_to_postgres(query, params, ref_db_name, ref_db_user, ref_db_pwd, ref_db_hostname)
        logger.info('Updated fct_avail_ind in table mm_cntrt_lkp to N')

        # Define logical, physical, and Unity Catalog table names based on the frequency time period class code
        logical_table_name=f"TP_{time_perd_class_code}_FCT"
        physical_table_name=f"TP_{time_perd_class_code}_FCT"
        unity_catalog_table_name=f"TP_{time_perd_class_code}_FCT"

        # Define partition columns separated by '/'
        partition_definition_value="part_srce_sys_id/part_cntrt_id/part_mm_time_perd_end_date" #Partition columns separated by /.

        # Call the cdl_publishing function with the defined parameters
        cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils,Configuration,MetaPSClient)

        logger.info(f"Purge process completed for the contract: {cntrt_id}")
