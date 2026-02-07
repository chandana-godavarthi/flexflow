from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,load_file,load_cntrt_lkp,load_cntrt_file_lkp,load_cntrt_dlmtr_lkp, get_database_config
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

    # Define the file type as 'fact'
    file_type='time'
    dmnsn_name='TIME'
    notebook_name = 'load_time'
    postgres_schema= db_config['postgres_schema']

    # Filter the DataFrame to get the vendor file pattern and step file pattern for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    vendor_file_pattern=df_cntrt_lkp.select('file_patrn').collect()[0].file_patrn
    logger.info(f"Vendor File Pattern: {vendor_file_pattern}")

    df_cntrt_file_lkp=load_cntrt_file_lkp(cntrt_id, dmnsn_name, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    step_file_pattern=df_cntrt_file_lkp.select('file_patrn').collect()[0].file_patrn
    logger.info(f"Step File Pattern: {step_file_pattern}")

    # Define the query to get the file delimiter from the PostgreSQL database
    delimiter=load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd).collect()[0].dlmtr_val
    logger.info(f"File Delimiter: {delimiter}")

    # Use the UDF to Load the file using the specified parameters
    load_file(file_type,run_id,cntrt_id,step_file_pattern,notebook_name,delimiter, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)