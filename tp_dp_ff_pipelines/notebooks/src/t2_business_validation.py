from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tp_utils.common import get_dbutils, read_run_params,load_cntrt_lkp,read_from_postgres,business_validation, get_database_config
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
    
    validation_name = 'Business Validation'

    # Filter the DataFrame to get the 'Source System ID' and TIME_PERD_TYPE_CODE for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
    mastr_valdn_ind = df_cntrt_lkp.collect()[0].mastr_valdn_ind
    logger.info(f"Source System ID: {srce_sys_id}")
    logger.info(f"Master Validation Indicator: {mastr_valdn_ind}")
    
    
    query = f"{postgres_schema}.mm_cntrt_valdn_assoc"
    mm_cntrt_checks_assoc = read_from_postgres(query, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd).filter(f"cntrt_id = {cntrt_id} and use_ind = 'Y'")
    checks = mm_cntrt_checks_assoc.select('valdn_id').collect()

    lst_of_checks=[]
    for i in checks:
        lst_of_checks.append(i[0].upper())
    
    if any(check in lst_of_checks for check in ['CHK_DQ1', 'CHK_DQ2', 'CHK_DQ3']):
        bv_checks_enabled = 'Y'
    else:
        bv_checks_enabled = 'N'

    #Execute the Business Validation only if the mastr_valdn_ind is active and atleast one of the Check is enabled
    if mastr_valdn_ind=='Y' and bv_checks_enabled=='Y':
        # Define the table name for data quality validations
        table_name = f'{postgres_schema}.mm_run_valdn_plc'

        # Read the table from PostgreSQL into a DataFrame
        df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        df_valdn=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 4))

        if (df_valdn.count()==0 or df_valdn.collect()[0]['aprv_ind']!='Y' ):
            logger.info(f'Data Quality in progress for Contract ID: {cntrt_id}, Run ID: {run_id}, File Name: {file_name}')
            business_validation(run_id,cntrt_id,srce_sys_id,file_name,validation_name,postgres_schema,catalog_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
        else:
            logger.info("Data Quality already approved. Skipping validation step.")

        logger.info("df_valdn")
        df_valdn.show()

        # Define the table name for data quality validations
        table_name = f'{postgres_schema}.mm_run_valdn_plc'

        # Read the table from PostgreSQL into a DataFrame
        df_table = read_from_postgres(table_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

        df_table.where(col('run_id')==run_id).show()

        # Filter the DataFrame for the specific run ID and validation name, and collect the data quality check result
        fail_ind = df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 4 )).collect()[0].fail_ind
        aprv_ind=df_table.filter((df_table['run_id'] == run_id) & (df_table['valdn_grp_id'] == 4 )).collect()[0].aprv_ind

        # Filter the DataFrame to get the vendor file pattern and step file pattern for the specified contract ID
        auto_apprv_ind=df_cntrt_lkp.collect()[0].auto_apprv_ind
        
        logger.info(f"Auto Approval Indicator: {auto_apprv_ind}")
        logger.info(f"Validation Failure Indicator: {fail_ind}")
        logger.info(f"Approval Indicator: {aprv_ind}")

        # Check the data quality result and take appropriate action
        if fail_ind == 'N' and aprv_ind!='Y':
            if auto_apprv_ind == 'Y':
                logger.info("Data Quality Passed and Auto Approved.")
            elif auto_apprv_ind == 'N':
                raise RuntimeError("Auto Approve turned off for the Business Validation: Terminating the workflow")

        elif aprv_ind=='Y':
            logger.info("Data Quality Approved")
        else:
            raise RuntimeError("Data Quality Issue: Terminating the workflow")
