from pyspark.sql.functions import col, lit , upper
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, get_logger, get_database_config, read_run_params,add_secure_group_key, read_from_postgres, cdl_publishing, merge_tbl,column_complementer
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
    consol_postgres_schema= db_config['consol_postgres_schema']


    # Load and filter table configuration
    table_config = spark.sql(f"""
        SELECT * 
        FROM {catalog_name}.internal_tp.tp_refresh_tbl_lkp
        WHERE refresh_type = :refresh_type AND flag="Y"
        """,{'refresh_type':refresh_type})

    e=eval
    
    #Process each config row
    for config in table_config.collect():
        src_table = config["src_table"]
        tgt_table = config["tgt_table"]
        state = config["state"]
        part_keys = config["part_key_name"]
        part_value = e(config["part_key_value"])
        key_cols = config["key_cols"]
        
        logger.info(f'SOURCE TABLE: {src_table}')
        logger.info(f'TARGET TABLE: {tgt_table}')

        #  Read source and target tables
        if src_table in ["mmc_valdn_deflt_strct_lvl_prc_vw", "mmc_valdn_cntrt_strct_lvl_prc_vw"]:
            df_src = read_from_postgres(f"{consol_postgres_schema}.{src_table}",spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        else:
            df_src = read_from_postgres(f"{postgres_schema}.{src_table}",spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        
        df_src=df_src.withColumn('secure_group_key',lit(0))
        df_src = add_secure_group_key(df_src, 0, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)                
        logger.info(f"Partition Key: {part_keys}")
        df_src = df_src.withColumn(part_keys,part_value)   

        logger.info('LOADED SOURCE TABLE DATA INTO DATAFRAME')

        df_tgt = spark.table(f"{catalog_name}.{tgt_table}")

        logger.info('LOADED TARGET TABLE DATA INTO DATAFRAME')

        # Complement the columns
        df_src = column_complementer(df_src,df_tgt)

        # Extract table name for publishing
        cdl_table = tgt_table.split(".", 2)[-1].upper()
        logical_table_name = physical_table_name = unity_catalog_table_name = cdl_table

        if state == "overwrite":
            #  Overwrite logic
            logger.info(f'OVERWRITING THE TABLE: {tgt_table}')
            df_src.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").saveAsTable(f"{catalog_name}.{tgt_table}")
        else:
            #  Merge logic using function         
            logger.info(f'MERGING THE TABLE: {tgt_table}')           
            merge_expr = f"tgt.{key_cols} = src.{key_cols}"
            merge_tbl(df_src, catalog_name, tgt_table, merge_expr, spark)


        #  Call publishing function (commented out)
        logger.info(f"Started Publishing to CDL")
        partition_definition_value = part_keys
        cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils,Configuration,MetaPSClient)
