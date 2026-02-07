from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils,get_logger,read_run_params, file_structure_validation, read_from_postgres, get_database_config
import argparse

#variable in main notebook
if __name__ == '__main__':

    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils = get_dbutils(spark)
    logger = get_logger()
    notebook_name = 'File_Structure_Validation'
    validation_name = 'File Structure Validation'
  

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils)
    ref_db_jdbc_url = db_config['ref_db_jdbc_url']
    ref_db_name = db_config['ref_db_name']
    ref_db_user = db_config['ref_db_user']
    ref_db_pwd = db_config['ref_db_pwd']

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']

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
    df_valdn=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 1))

    if (df_valdn.count()==0 or df_valdn.collect()[0]['aprv_ind']!='N'):
        logger.info('Data Quality in progress')
        file_structure_validation(notebook_name,validation_name,file_name,cntrt_id,run_id,spark, ref_db_jdbc_url,ref_db_name, ref_db_user, ref_db_pwd,postgres_schema,catalog_name)
    else:
        logger.info('Validation Passed')

    df_valdn.show()

    # Filter the DataFrame for the specific run ID and validation name, and collect the data quality check result
    fail_ind = df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 1 )).collect()[0].fail_ind
    aprv_ind=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 1 )).collect()[0].aprv_ind

    # Check the data quality result and take appropriate action
    if fail_ind == 'N':
        logger.info("Data Quality Passed")

    elif aprv_ind=='Y':
        logger.info("DQ Approved")

    else:
        raise RuntimeError("Data Quality Issue: Terminating the workflow")
