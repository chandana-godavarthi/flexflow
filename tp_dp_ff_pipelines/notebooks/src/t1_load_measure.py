from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,t1_load_file,materialize,extract_measure_ffs,extract_measure_sff3,extract_measure_sff,extract_measure_tape, materialise_path
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_measure = get_dbutils(spark)
    logger_measure = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_measure)
    ref_db_jdbc_url_measure = db_config['ref_db_jdbc_url']
    ref_db_name_measure = db_config['ref_db_name']
    ref_db_user_measure = db_config['ref_db_user']
    ref_db_pwd_measure = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    file_name_measure = args.FILE_NAME
    cntrt_id_measure = args.CNTRT_ID
    run_id_measure = args.RUN_ID

    logger_measure.info(f"[Params] FILE_NAME: {file_name_measure} | CNTRT_ID: {cntrt_id_measure} | RUN_ID: {run_id_measure}")

    postgres_schema= dbutils_measure.secrets.get('tp_dpf2cdl', 'database-postgres-schema')

    raw_path=materialise_path(spark)
    file_type2_measure='FCT'
    file_type_measure= 'facts'

    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_categ_assoc_lkp = load_cntrt_categ_cntry_assoc(cntrt_id_measure, postgres_schema, spark, ref_db_jdbc_url_measure, ref_db_name_measure, ref_db_user_measure, ref_db_pwd_measure)
    fileformat = df_cntrt_categ_assoc_lkp.file_formt
    logger_measure.info(f"[Contract Lookup] File Format: {fileformat}")

    # Load the file using the specified parameters
    df_raw = t1_load_file(file_type_measure,run_id_measure,file_type2_measure,fileformat,spark)

    logger_measure.info(f"[LoadFile] Count of df_raw: {df_raw.count()}") 
    materialize(df_raw,'Load_Measure_df_raw',run_id_measure)
    logger_measure.info("[LoadFile] Started Extracting fact file")

    df_raw=spark.read.parquet(f"{raw_path}/{run_id_measure}/Load_Measure_df_raw")

    if fileformat == 'FFS' or fileformat=='FFS2':
        df_srce_mmeasr = extract_measure_ffs(df_raw)
    elif fileformat == 'SFF3':
        df_srce_mmeasr = extract_measure_sff3(df_raw)
    elif fileformat =='SFF2' or fileformat =='SFF':
        df_srce_mmeasr = extract_measure_sff(df_raw)
    elif fileformat in ('Tape2', 'Tape3'):
        df_srce_mmeasr = extract_measure_tape(df_raw)

    logger_measure.info("[LoadFile] Measure File loading completed succesfully")
    logger_measure.info(f"[LoadFile] Count of df_srce_mmeasr:{df_srce_mmeasr.count()}")
    
    # Save the processed DataFrame to parquet file
    materialize(df_srce_mmeasr,'Load_Measure_df_srce_mmeasr',run_id_measure)