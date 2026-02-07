from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,t1_load_file,materialize,extract_market_ffs,extract_market_ffs2,extract_market_sff3,extract_market_sff,extract_market_tape, materialise_path
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_market = get_dbutils(spark)
    logger_market = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_market)
    ref_db_jdbc_url_market = db_config['ref_db_jdbc_url']
    ref_db_name_market = db_config['ref_db_name']
    ref_db_user_market = db_config['ref_db_user']
    ref_db_pwd_market = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id_market = args.CNTRT_ID
    run_id_market = args.RUN_ID

    logger_market.info(f"[Params] FILE_NAME: {file_name} | CNTRT_ID: {cntrt_id_market} | RUN_ID: {run_id_market}")

    postgres_schema_market= dbutils_market.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    raw_path=materialise_path(spark)
    file_type_market='market'
    file_type2_market='MKT'
    strct_code_market = 'TP_H1'

    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_categ_assoc_lkp = load_cntrt_categ_cntry_assoc(cntrt_id_market, postgres_schema_market, spark, ref_db_jdbc_url_market, ref_db_name_market, ref_db_user_market, ref_db_pwd_market)
    categ_id_market = df_cntrt_categ_assoc_lkp.categ_id
    srce_sys_id_market = df_cntrt_categ_assoc_lkp.srce_sys_id
    fileformat_market = df_cntrt_categ_assoc_lkp.file_formt
    cntrt_code_market = df_cntrt_categ_assoc_lkp.cntrt_code
    logger_market.info(f"[Contract Lookup] Category ID: {categ_id_market} | Source System ID: {srce_sys_id_market} | File Format: {fileformat_market} | Contract Code: {cntrt_code_market}")

    # Load the file using the specified parameters
    df_raw = t1_load_file(file_type_market,run_id_market,file_type2_market,fileformat_market,spark)

    logger_market.info(f"[LoadFile] Count of df_raw: {df_raw.count()}")
    materialize(df_raw,'Load_Market_df_raw',run_id_market)
    logger_market.info("[LoadFile] Started Extracting market file")
    
    df_raw=spark.read.parquet(f"{raw_path}/{run_id_market}/Load_Market_df_raw")
    # Transform raw market data based on file format
    if fileformat_market == 'FFS':
        df_srce_mmkt = extract_market_ffs(df_raw,run_id_market,cntrt_id_market,categ_id_market,srce_sys_id_market,strct_code_market)
    elif fileformat_market == 'FFS2':
        df_srce_mmkt = extract_market_ffs2(df_raw,run_id_market,cntrt_id_market,categ_id_market,srce_sys_id_market,strct_code_market)
    elif fileformat_market == 'SFF3':
        df_srce_mmkt = extract_market_sff3(df_raw,run_id_market,cntrt_id_market,categ_id_market,srce_sys_id_market,strct_code_market,spark)
    elif fileformat_market in('SFF2','SFF'):
        df_srce_mmkt = extract_market_sff(df_raw,run_id_market,cntrt_id_market,categ_id_market,srce_sys_id_market,strct_code_market)
    elif fileformat_market in ('Tape2', 'Tape3'):
        df_srce_mmkt = extract_market_tape(df_raw,run_id_market,cntrt_id_market,categ_id_market,srce_sys_id_market,strct_code_market)

    logger_market.info("[LoadFile] Market File loaded succesfully")
    logger_market.info(f"[LoadFile] Count of df_srce_mmkt :{df_srce_mmkt.count()}")
    
    # Save the processed DataFrame to parquet file
    materialize(df_srce_mmkt,'Load_Market_df_srce_mmkt',run_id_market)