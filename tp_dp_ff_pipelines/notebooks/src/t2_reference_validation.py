from pyspark.sql import SparkSession
from tp_utils.common import get_logger, get_dbutils, read_run_params, read_from_postgres, reference_validation, get_database_config,materialise_path
import argparse

# variable in main notebook
if __name__ == '__main__':

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
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    notebook_name = 'Reference_Validation'
    materialised_path = materialise_path(spark)
    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']
    validation_name = 'Reference Data Validations'
    light_refined_path = "tp-publish-data"
    raw_path = "tp-source-data/WORK/"
    # Define the table name for data quality validations
    table_name = f'{postgres_schema}.mm_run_valdn_plc'

    # Read the table from PostgreSQL into a DataFrame
    df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    df_valdn=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 2))
    if (df_valdn.count()==0 or df_valdn.collect()[0]['aprv_ind']=='N'):
        logger.info('Data Quality in progress')

        reference_validation(materialised_path,validation_name,file_name,cntrt_id,run_id,spark, ref_db_jdbc_url,ref_db_name, ref_db_user, ref_db_pwd,postgres_schema,catalog_name)
    else:
        logger.info('Data Quality Passed')

    # Define the table name for data quality validations
    table_name = f'{postgres_schema}.mm_run_valdn_plc'

    # Read the table from PostgreSQL into a DataFrame
    df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_table.show()

    # Filter the DataFrame for the specific run ID and validation name, and collect the data quality check result
    fail_ind = df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 2 )).collect()[0].fail_ind
    aprv_ind=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 2 )).collect()[0].aprv_ind

    # Check the data quality result and take appropriate action
    if fail_ind == 'N':
        print("Data Quality Passed")
    elif aprv_ind=='Y':
        print("DQ Approved")
    else:
        raise RuntimeError("Data Quality Issue: Terminating the workflow")