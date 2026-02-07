from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tp_utils.common import get_dbutils, read_run_params,materialize,get_logger,get_database_config,column_complementer,read_from_postgres,derive_product_assoc,cdl_publishing
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_prod_assoc = get_dbutils(spark)
    logger_prod_assoc = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_prod_assoc)
    ref_db_jdbc_url_prod_assoc = db_config['ref_db_jdbc_url']
    ref_db_name_prod_assoc = db_config['ref_db_name']
    ref_db_user_prod_assoc = db_config['ref_db_user']
    ref_db_pwd_prod_assoc = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_prod_assoc = args.CNTRT_ID
    run_id_prod_assoc = args.RUN_ID

    logger_prod_assoc.info(f"[Params] CNTRT_ID: {cntrt_id_prod_assoc} | RUN_ID: {run_id_prod_assoc}")
    postgres_schema_prod_assoc= dbutils_prod_assoc.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name_prod_assoc = dbutils_prod_assoc.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')

    # Load product dimension data from catalog and structure level lookup data from PostgreSQL
    df_prod_dim = spark.sql(f"SELECT * FROM {catalog_name_prod_assoc}.gold_tp.tp_prod_dim where part_srce_sys_id=3")
    logger_prod_assoc.info("Displaying df_prod_dim")
    df_prod_dim.show()
    df_strct_lkp = read_from_postgres(f'{postgres_schema_prod_assoc}.mm_strct_lvl_lkp',spark, ref_db_jdbc_url_prod_assoc, ref_db_name_prod_assoc, ref_db_user_prod_assoc, ref_db_pwd_prod_assoc )
    logger_prod_assoc.info("Displaying df_strct_lkp")
    df_strct_lkp.show()

    # Derive product association using product dimension and structure lookup data
    df_prod_assoc = derive_product_assoc(df_prod_dim, df_strct_lkp,spark)
    logger_prod_assoc.info("Displaying df_prod_assoc")
    df_prod_assoc.show()

    # Create a temporary view for Merge operations
    df_prod_assoc.createOrReplaceTempView('prod_assoc')

    # Materialize the derived DataFrame to the raw path
    materialize(df_prod_assoc,'Product_Assoc_Derivation_df_prod_assoc',run_id_prod_assoc)

    # Publish derived Prod_Assoc
    merge_df = spark.sql(f"""
            MERGE INTO {catalog_name_prod_assoc}.gold_tp.tp_prod_assoc target
        USING prod_assoc source
        ON source.child_prod_match_attr_list = target.child_prod_match_attr_list
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
            """)
    merge_df.show()

    # # Define logical, physical, and Unity Catalog table names
    logical_table_name_prod_assoc="TP_PROD_ASSOC"     #Eg: TP_WK_FCT
    physical_table_name_prod_assoc="TP_PROD_ASSOC"    #Eg: TP_WK_FCT
    unity_catalog_table_name_prod_assoc="TP_PROD_ASSOC"

    # Define partition columns separated by '/'
    partition_definition_value="" #Partition columns separated by /.

    # Call the cdl_publishing function with the defined parameters
    cdl_publishing(logical_table_name_prod_assoc,physical_table_name_prod_assoc,unity_catalog_table_name_prod_assoc,partition_definition_value, dbutils_prod_assoc,Configuration,MetaPSClient)