from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,t1_load_file,materialize,extract_time_tape,extract_time_sff,extract_time_sff3,extract_time_ffs, materialise_path
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc,tier1_time_map

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_time = get_dbutils(spark)
    logger_time = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_time)
    ref_db_jdbc_url_time = db_config['ref_db_jdbc_url']
    ref_db_name_time = db_config['ref_db_name']
    ref_db_user_time = db_config['ref_db_user']
    ref_db_pwd_time = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    file_name_time = args.FILE_NAME
    cntrt_id_time = args.CNTRT_ID
    run_id_time = args.RUN_ID

    logger_time.info(f"[Params] FILE_NAME: {file_name_time} | CNTRT_ID: {cntrt_id_time} | RUN_ID: {run_id_time}")

    postgres_schema_time= dbutils_time.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name_time = dbutils_time.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    raw_path=materialise_path(spark)
    file_type_time='period'
    file_type2_time='PER'
    strct_code_time = 'TP_H1'

    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_categ_assoc_lkp = load_cntrt_categ_cntry_assoc(cntrt_id_time, postgres_schema_time, spark, ref_db_jdbc_url_time, ref_db_name_time, ref_db_user_time, ref_db_pwd_time)
    fileformat = df_cntrt_categ_assoc_lkp.file_formt
    vendr_id_time=df_cntrt_categ_assoc_lkp.vendr_id
    logger_time.info(f"[Contract Lookup] File Format: {fileformat} | Vendor ID: {vendr_id_time}")

    # Load the file using the specified parameters
    df_raw = t1_load_file(file_type_time,run_id_time,file_type2_time,fileformat,spark)

    logger_time.info(f"[LoadFile] Count of df_raw: {df_raw.count()}")
    
    materialize(df_raw,'Load_Time_df_raw',run_id_time)

    df_raw=spark.read.parquet(f"{raw_path}/{run_id_time}/Load_Time_df_raw")

    logger_time.info("[LoadFile] Started Extracting time file")

    # Transform raw market data based on file format
    if fileformat in ('FFS','FFS2'):
        df_srce_mtime = extract_time_ffs(df_raw)
    elif fileformat == 'SFF3':
        df_srce_mtime = extract_time_sff3(df_raw)
    elif fileformat =='SFF2' or fileformat =='SFF':
        df_srce_mtime = extract_time_sff(df_raw)
    elif fileformat in ('Tape2', 'Tape3'):
        df_srce_mtime = extract_time_tape(df_raw)

    logger_time.info("[LoadFile] Time File loaded succesfully")
    logger_time.info(f"[LoadFile] Count of df_srce_mtime:{df_srce_mtime.count()}")
    # Save the processed DataFrame to parquet file
    materialize(df_srce_mtime,'Load_Time_df_srce_mtime',run_id_time)
    
    tier1_time_map(vendr_id_time,catalog_name_time,postgres_schema_time,spark, ref_db_jdbc_url_time, ref_db_name_time, ref_db_user_time, ref_db_pwd_time,run_id_time)