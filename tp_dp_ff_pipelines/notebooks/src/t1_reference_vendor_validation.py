from pyspark.sql import SparkSession,Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from tp_utils.common import read_run_params,get_dbutils,get_logger,read_from_postgres,tier1_reference_vendor_validation
from pyspark.sql.functions import col,lit
from pyspark.sql.types import IntegerType
import argparse
import os

#variable in main notebook
if __name__ == '__main__':

    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark)
    logger = get_logger()
    validation_name = 'Reference Data Vendors Validations'
    catalog_name= dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')

    # Retrieve secrets from Databricks
    ref_db_jdbc_url = dbutils.secrets.get('tp_dpf2cdl', 'refDBjdbcURL')
    ref_db_name = dbutils.secrets.get('tp_dpf2cdl', 'refDBname')
    ref_db_user = dbutils.secrets.get('tp_dpf2cdl', 'refDBuser')
    ref_db_pwd = dbutils.secrets.get('tp_dpf2cdl', 'refDBpwd')

    # Get the job parameters
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID
    
    logger.info(f"---FILE_NAME-- , {file_name}")
    logger.info(f"---CNTRT_ID-- , {cntrt_id}")
    logger.info(f"---RUN_ID-- , {run_id}")
    
    # Define the table name for data quality validations
    table_name = f'{postgres_schema}.mm_run_valdn_plc'

    # Read the table from PostgreSQL into a DataFrame
    df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_valdn=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 9))

    if (df_valdn.count()==0 or df_valdn.collect()[0]['aprv_ind']!='Y'):
        print('Data Quality in progress')
        tier1_reference_vendor_validation(validation_name,file_name,cntrt_id,run_id,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd,postgres_schema,catalog_name)
        
    df_valdn.show()

    # Filter the DataFrame for the specific run ID and validation name, and collect the data quality check result
    fail_ind = df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 9 )).collect()[0].fail_ind
    aprv_ind=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 9 )).collect()[0].aprv_ind

    print(fail_ind)
    print(aprv_ind)

    # Check the data quality result and take appropriate action
    if fail_ind == 'N':
        print("Data Quality Passed")

    elif aprv_ind=='Y':
        print("DQ Approved")

    else:
        raise Exception("Data Quality Issue: Terminating the workflow")