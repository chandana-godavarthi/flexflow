from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params, your_information_validations,read_from_postgres,load_cntrt_lkp, get_database_config
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

    # Get the job parameters
    args = read_run_params()
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID
    file_name = args.FILE_NAME

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']
    validation_name = 'For Your Information Validations'

    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    srce_sys_id=df_cntrt_lkp.select('srce_sys_id').collect()[0].srce_sys_id
    logger.info(f"Source System ID: {srce_sys_id}")

    # Define the table name for data quality validations
    table_name = f'{postgres_schema}.mm_run_valdn_plc'

    # Read the table from PostgreSQL into a DataFrame
    df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_valdn=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 3))
    logger.info("df_valdn")
    df_valdn.show()

    if (df_valdn.count()==0 or df_valdn.collect()[0]['aprv_ind']=='N'):
        logger.info(" Started FYI validations")
        your_information_validations(run_id,cntrt_id,srce_sys_id,file_name,validation_name,catalog_name,postgres_schema,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    else:
        logger.info("Validation already approved. Skipping FYI validations.")

    # Define the table name for data quality validations
    table_name = f'{postgres_schema}.mm_run_valdn_plc'

    # Read the table from PostgreSQL into a DataFrame
    df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    # Filter the DataFrame for the specific run ID and validation name, and collect the data quality check result
    fail_ind = df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 3 )).collect()[0].fail_ind
    aprv_ind=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 3 )).collect()[0].aprv_ind

    logger.info(f"Validation status - fail_ind: {fail_ind}, aprv_ind: {aprv_ind}")

    # Check the data quality result and take appropriate action
    if fail_ind == 'N':
        logger.info("Data Quality Passed.")

    elif aprv_ind=='Y':
        logger.info("Data Quality Approved.")