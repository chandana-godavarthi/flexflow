from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from tp_utils.common import get_dbutils, read_run_params,load_cntrt_lkp,load_time_exprn_id, get_database_config,materialise_path,semaphore_generate_path,semaphore_acquisition,release_semaphore,column_complementer,safe_write_with_retry
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

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']
    logger.info("Started Time Transformation")
    time_trans_mat_path = materialise_path(spark)

    df_time_extrn = spark.read.parquet(f"{time_trans_mat_path}/{run_id}/load_file_df_fact_extrn")
    df_time_extrn.createOrReplaceTempView("df_time_extrn")
    logger.info("df_time_extrn Loaded")
    df_time_extrn.show()
    
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    time_exprn_id = df_cntrt_lkp.collect()[0].time_exprn_id
    srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
    logger.info(f"Retrieved Time Expression ID: {time_exprn_id}")

    df_time_exprn = load_time_exprn_id(time_exprn_id,postgres_schema,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("df_time_exprn Loaded")
    df_time_exprn.show()

    # Extract column values dynamically from the first row of the DataFrame
    query_parts = []
    for col, value in zip(df_time_exprn.columns, df_time_exprn.first()):
        query_parts.append(f"{value} as {col}")


    # Construct the SQL query to add the extracted column values to the existing table
    table_name = "df_time_extrn"
    query = f"SELECT *, {', '.join(query_parts)} FROM {table_name}"
    logger.info(f"SQL query to add the extracted columns from df_time_exprn to df_time_extrn: {query}")
    # Execute the constructed SQL query and create a DataFrame from the result
    df_time_extrn = spark.sql(query)
    df_time_extrn.createOrReplaceTempView("mm_time_extrn")
    df_time_extrn.show()

    df_fdim=spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.tp_time_perd_fdim')
    df_fdim.createOrReplaceTempView("df_fdim")
    df_fdim.show(10)

    # Define the SQL query to join mm_time_extrn and df_fdim on end_date and time_pd_type_cd
    query = '''
    SELECT t.extrn_time_perd_id AS extrn_perd_id, fdim.time_perd_id, t.end_date_val AS mm_time_perd_end_date, fdim.time_perd_type_code 
    FROM mm_time_extrn t 
    LEFT OUTER JOIN df_fdim fdim 
    ON t.end_date_val = fdim.time_perd_end_date AND t.time_perd_type_val = fdim.time_perd_type_code
    '''

    # Execute the SQL query and create a DataFrame from the result
    df_time_stgng_vw=spark.sql(query)
    logger.info("df_time_stgng_vw after join")
    df_time_stgng_vw = df_time_stgng_vw.distinct()
    df_time_stgng_vw.show(10)

    df_run_time_perd_plc = spark.sql(f"select * from {catalog_name}.internal_tp.tp_run_time_perd_plc limit 0")

    # Need to log run info in the logging table 
    df_time_log = df_time_stgng_vw.selectExpr(
        "time_perd_id",
        "extrn_perd_id as extrn_time_perd_id"
        ).withColumn("run_id",lit(run_id).cast("long")) \
        .withColumn("srce_sys_id", lit(srce_sys_id).cast("short")) \
        .withColumn("cntrt_id",  lit(cntrt_id).cast("long"))
    logger.info("Added run_id,srce_sys_id,cntrt_id to df_time_log")
    df_time_log= column_complementer(df_time_log,df_run_time_perd_plc)
    logger.info("df_time_log after column complimenting")
    df_time_log.show()

    dim_type='TP_RUN_TIME_PERD_PLC'
    run_time_perd_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)
    # Print the constructed path for verification
    logger.info(f"run_time_perd_plc_path: {run_time_perd_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_RUN_TIME_PERD_PLC")
    run_time_perd_plc_check_path = semaphore_acquisition(run_id, run_time_perd_plc_path,catalog_name, spark)
    try:
        logger.info("Started writing data to tp_run_time_perd_plc")
        safe_write_with_retry(df_time_log, f"{catalog_name}.internal_tp.tp_run_time_perd_plc", "append", partition_by="cntrt_id", options=None)
        logger.info(" Write completed to tp_run_time_perd_plc")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,run_id,run_time_perd_plc_check_path , spark)
        logger.info("TP_RUN_TIME_PERD_PLC Semaphore released")

    # Publish Time Dataframe
    df_time_stgng_vw.write.mode("overwrite").format('parquet').save(f'''{time_trans_mat_path}/{run_id}/time_transformation_df_time_stgng_vw''')
    logger.info(f"Time transformation Materialized to Path: {time_trans_mat_path}/{run_id}/time_transformation_df_time_stgng_vw.parquet")