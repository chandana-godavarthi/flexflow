from pyspark.sql import SparkSession
from tp_utils.common import get_database_config,get_logger,get_dbutils, read_run_params,lst_database_tables

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark)
    logger = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)

    # Get the job parameters
    args = read_run_params()
    frequency = args.frequency
    mode = args.mode

    postgres_schema= db_config['postgres_schema']
    # Retrieving the catalog name securely from Databricks secrets
    catalog_name = db_config['catalog_name']
    lst_tables = []
    df_tables = lst_database_tables(spark, catalog_name)
    [lst_tables.append(i[0]) for i in  df_tables.select('table_name').collect()]
    
    df = spark.sql(f"select * from {catalog_name}.internal_tp.tp_vacuum_tbl_lkp").filter(f"frequency = '{frequency}' and excl_ind = 'N'")
    lst = df.collect()
    
    for i in lst:
        table_name = i['table_name']
        if table_name and table_name in lst_tables and mode in ['DRY RUN','FULL','LITE']:
            spark.sql(f"vacuum {catalog_name}.{table_name} {mode}")
            logger.info(f"Vacuumed table {i['table_name']} with mode: {mode} successfully")