from pyspark.sql import SparkSession
import argparse
import os
from tp_utils.common import get_dbutils,get_database_config
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel_DDL_Extractor").getOrCreate()
    dbutils = get_dbutils(spark)
    db_config = get_database_config(dbutils)
    # Configuration
    catalog_name = db_config['catalog_name']
    storage_name = dbutils.secrets.get('tp_dpf2cdl', 'storageName')
    parser = argparse.ArgumentParser()
    parser.add_argument("--TABLES",type=str)
    args = parser.parse_args()
    tables_to_migrate = args.TABLES.split(",")
    # tables_to_migrate = ["gold_tp.tp_time_perd_type_test1"]
    os.chdir("../../../") 
    os.chdir("resources/sql/")
    current_directory = os.getcwd()
    
    for script_path in tables_to_migrate:
        with open(f"{script_path}.sql", 'r') as file:
            sql_content = file.read()
            # Replace variables
            sql_content = sql_content.replace("{catalog_name}", catalog_name)
            sql_content = sql_content.replace("{storage_name}", storage_name)
            drop_squery = f""" drop table if exists {catalog_name}.{script_path};"""
            create_query = sql_content[:sql_content.find("TBLPROPERTIES")]
            table_prop = sql_content[sql_content.find("TBLPROPERTIES"):]
            
            table_prop_query = f"""alter table {catalog_name}.{script_path} set {table_prop};"""
            print(drop_squery)
            print(create_query)
            print(table_prop_query)
            spark.sql(drop_squery)
            spark.sql(create_query)
            spark.sql(table_prop_query)