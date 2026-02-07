from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,t1_load_file,load_fact_sfffile,materialize,extract_fact_ffs,extract_fact_sff,extract_fact_tape,add_row_count, materialise_path
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc

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
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID
    raw_path = materialise_path(spark)

    logger.info(f"[LoadFile] FILE_NAME: {file_name} | CNTRT_ID: {cntrt_id} | RUN_ID: {run_id}")

    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')

    file_type='fact_data'
    file_type2='aggregated_data'

    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_categ_assoc_lkp = load_cntrt_categ_cntry_assoc(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    categ_id = df_cntrt_categ_assoc_lkp.categ_id
    srce_sys_id = df_cntrt_categ_assoc_lkp.srce_sys_id
    fileformat = df_cntrt_categ_assoc_lkp.file_formt
    cntrt_code = df_cntrt_categ_assoc_lkp.cntrt_code

    logger.info(f"[Contract Lookup] Category ID: {categ_id} | Source System ID: {srce_sys_id} | File Format: {fileformat} | Contract Code: {cntrt_code}")

    # Load the file using the specified parameters
    if fileformat in ('FFS','FFS2',"Tape2","Tape3"):
        df_raw1 = t1_load_file(file_type,run_id,file_type2,fileformat,spark)
    elif fileformat in ('SFF3','SFF2','SFF'):
        df_raw1= load_fact_sfffile(file_type,run_id,file_type2,fileformat,spark)

    logger.info(f"[t1_load_file] df_raw1 count: {df_raw1.count()}")
    materialize(df_raw1,'Load_Fact_df_raw1',run_id)

    # Transform raw market data based on file format
    df_raw1=spark.read.parquet(f"{raw_path}/{run_id}/Load_Fact_df_raw1")

    logger.info("[LoadFile] Started Extracting fact file")

    if fileformat in ('FFS','FFS2'): 
        df_srce_mfact = extract_fact_ffs(df_raw1,run_id,cntrt_id,dbutils,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd,postgres_schema)
    elif fileformat in ('SFF2','SFF','SFF3'):
        df_srce_mfact = extract_fact_sff(df_raw1,run_id,cntrt_id,srce_sys_id,spark)
    elif fileformat in ('Tape2', 'Tape3'):
        df_srce_mfact = extract_fact_tape(df_raw1,run_id,cntrt_id,dbutils,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd,postgres_schema)

    count_df_srce_mfact=df_srce_mfact.count()
    logger.info(f"Count of df_srce_mfact: {count_df_srce_mfact}")

    # Save the processed DataFrame to parquet file
    materialize(df_srce_mfact,'Load_Fact_df_srce_mfact',run_id)

    # Add row_count to mm_run_detl_plc table
    
    add_row_count(count_df_srce_mfact,postgres_schema,run_id,cntrt_id, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    logger.info("[LoadFile] Fact File loaded succesfully")
    