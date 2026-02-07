from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,lit
from tp_utils.common import get_dbutils, read_run_params,materialize,get_logger,get_database_config,time_perd_class_codes,column_complementer,add_secure_group_key,load_cntrt_categ_cntry_assoc,read_from_postgres,cdl_publishing,materialise_path,semaphore_generate_path,semaphore_acquisition,release_semaphore,safe_write_with_retry
from pg_composite_pipelines_configuration.configuration import Configuration
from pg_composite_pipelines_cdl.main_meta_client import MetaPSClient
from pg_composite_pipelines_cdl.rest.azure_token_provider import SPAuthClient

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_publogs = get_dbutils(spark)
    logger_publogs = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_publogs)
    ref_db_jdbc_url_publogs = db_config['ref_db_jdbc_url']
    ref_db_name_publogs = db_config['ref_db_name']
    ref_db_user_publogs = db_config['ref_db_user']
    ref_db_pwd_publogs = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_publogs = args.CNTRT_ID
    run_id_publogs = args.RUN_ID

    logger_publogs.info(f"[Params] CNTRT_ID: {cntrt_id_publogs} | RUN_ID: {run_id_publogs}")
    postgres_schema_publogs= dbutils_publogs.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name_publogs = dbutils_publogs.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    raw_path_publogs = materialise_path(spark)

    # Fetch time period classification code
    time_perd_class_code=time_perd_class_codes(cntrt_id_publogs,catalog_name_publogs, postgres_schema_publogs,spark, ref_db_jdbc_url_publogs, ref_db_name_publogs, ref_db_user_publogs, ref_db_pwd_publogs)
    logger_publogs.info(f"time_perd_class_code: {time_perd_class_code}")
    # Fetch contract metadata from Postgres
    mm_cntrt_categ_assoc = load_cntrt_categ_cntry_assoc(cntrt_id_publogs, postgres_schema_publogs, spark, ref_db_jdbc_url_publogs, ref_db_name_publogs, ref_db_user_publogs, ref_db_pwd_publogs)
    categ_id = mm_cntrt_categ_assoc.categ_id
    srce_sys_id = mm_cntrt_categ_assoc.srce_sys_id
    logger_publogs.info(f"[Params] categ_id: {categ_id} | srce_sys_id: {srce_sys_id} | time_perd_class_code: {time_perd_class_code}")

    logger_publogs.info("[MEASURE_PLC] Started Publish RUN_MEASURE_PLC")
    # Load measr_map and df_fct_crbm2
    df_measr_map =spark.read.parquet(f"{raw_path_publogs}/{run_id_publogs}/Fact_Derivation_df_measr_map")
    df_measr_map.createOrReplaceTempView('MLKP')
    logger_publogs.info("[MEASURE_PLC] Displaying df_measr_map :")
    df_measr_map.show()
    df_fct_crbm2 = spark.read.parquet(f"{raw_path_publogs}/{run_id_publogs}/Atomic_Measure_Calculations_df_fct_crbm2")
    df_fct_crbm2.createOrReplaceTempView('TAB_COLS')
    logger_publogs.info("[MEASURE_PLC] Displaying df_fct_crbm2 :")
    df_fct_crbm2.show()

    # Load measure lookup table 
    mm_measr_lkp=read_from_postgres(f"{postgres_schema_publogs}.mm_measr_lkp",spark, ref_db_jdbc_url_publogs, ref_db_name_publogs, ref_db_user_publogs, ref_db_pwd_publogs)
    mm_measr_lkp=mm_measr_lkp.withColumn("measr_phys_name",lower(mm_measr_lkp.measr_phys_name))
    mm_measr_lkp.createOrReplaceTempView('input')
    logger_publogs.info("[MEASURE_PLC] Displaying mm_measr_lkp :")
    mm_measr_lkp.show()

    # Get column names from TAB_COLS
    df=spark.table("TAB_COLS")
    col_names=df.columns
    in_clause= ", ".join("'"+ item + "'" for item in col_names)
    # SQL query to identify new measures
    logger_publogs.info("[MEASURE_PLC] Executing measure lookup join query")
    df_measr_plc=spark.sql(f"""
    WITH temp1 AS(
        SELECT  input.* ,
        :run_id as run_id,
        :srce_sys_id as srce_sys_id, 
        :cntrt_id as cntrt_id,
        CASE 
            WHEN mlkp.measr_id is not null THEN 'n' 
            ELSE 'y'
        END calc_ind,
        mlkp.extrn_code extrn_measr_id,mlkp.extrn_name extrn_measr_name 
        FROM input 
        LEFT OUTER JOIN MLKP 
            ON input.measr_id = MLKP.measr_id
        WHERE input.measr_phys_name IN ({in_clause})
        )
        SELECT * FROM temp1 
        """,{'run_id':run_id_publogs,'srce_sys_id':srce_sys_id,'cntrt_id':cntrt_id_publogs})
    logger_publogs.info("[MEASURE_PLC] Displaying df_measr_plc :")
    df_measr_plc.show()
    materialize(df_measr_plc,'Publish_Run_Logs_df_measr_plc',run_id_publogs)

    df_measr_plc.createOrReplaceTempView('df_measr_plc')
    # Identify new records in df_measr_plc not already present in tp_run_measr_plc
    df_measr_plc_1 = spark.sql(f"""
            SELECT * FROM df_measr_plc AS measr_plc
            LEFT ANTI JOIN (select * from {catalog_name_publogs}.gold_tp.tp_run_measr_plc where cntrt_id=:cntrt_id_publogs) AS ref 
            ON ref.run_id=measr_plc.run_id
            """,{"cntrt_id_publogs":cntrt_id_publogs})
    logger_publogs.info(f"[MEASURE_PLC] df_measr_plc result count: {df_measr_plc_1.count()}")
    df_measr_plc_1.show()
    
    df_mm_run_measr_plc = spark.sql(f"select * from {catalog_name_publogs}.gold_tp.tp_run_measr_plc limit 0")
    df_measr_plc_1 = column_complementer(df_measr_plc_1,df_mm_run_measr_plc)
    logger_publogs.info("[MEASURE_PLC] Displaying df_measr_plc_1 after column complimenting with tp_run_measr_plc:")
    df_measr_plc_1.show()
    df_measr_plc_1 = add_secure_group_key(df_measr_plc_1,cntrt_id_publogs, postgres_schema_publogs, spark, ref_db_jdbc_url_publogs, ref_db_name_publogs, ref_db_user_publogs, ref_db_pwd_publogs)
    logger_publogs.info("[MEASURE_PLC] Displaying df_measr_plc_1 after adding secure group key:")
    df_measr_plc_1.show()
    
    dim_type='TP_RUN_MEASR_PLC'
    run_measr_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id_publogs)
    # Print the constructed path for verification
    logger_publogs.info(f"run_measr_plc_path: {run_measr_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger_publogs.info("Acquiring semaphore for TP_RUN_MEASR_PLC")
    run_measr_plc_check_path = semaphore_acquisition(run_id_publogs, run_measr_plc_path,catalog_name_publogs, spark)
    try:
        logger_publogs.info("[MEASURE_PLC] Started writing data to TP_RUN_MEASR_PLC:")
        safe_write_with_retry(df_measr_plc_1, f"{catalog_name_publogs}.gold_tp.tp_run_measr_plc", 'append', partition_by='cntrt_id', options=None)
        logger_publogs.info("Completed writing data to TP_RUN_MEASR_PLC")

    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name_publogs,run_id_publogs,run_measr_plc_check_path , spark)
        logger_publogs.info("TP_RUN_MEASR_PLC Semaphore released")

    logger_publogs.info("[MEASURE_PLC] Publish RUN_MEASURE_PLC Completed")

    logger_publogs.info("[TIME_PERD_PLC] Starting Publish RUN_TIME_PERD_PLC")
    # Load time_map
    df_time_map = spark.read.parquet(f"{raw_path_publogs}/{run_id_publogs}/Load_Time_df_time_map")
    logger_publogs.info("[TIME_PERD_PLC] Loaded df_time_map")
    df_time_map.show()
    df_time_plc = (
        df_time_map.withColumn("run_id", lit(run_id_publogs))
        .withColumn("extrn_time_perd_id",col("extrn_code"))
    )

    # Identify new records in df_time_plc not already present in df_run_time_perd_plc
    df_time_plc.createOrReplaceTempView("df_time_plc")
    df_time_plc_1 = spark.sql(f"""
            SELECT  * FROM df_time_plc AS time_plc 
            LEFT ANTI JOIN (select * from {catalog_name_publogs}.internal_tp.tp_run_time_perd_plc where cntrt_id=:cntrt_id_publogs) AS ref 
            ON ref.run_id=time_plc.run_id
        """,{"cntrt_id_publogs":cntrt_id_publogs})
    logger_publogs.info("[TIME_PERD_PLC] df_time_plc_1 after joining with tp_run_time_perd_plc")
    df_time_plc_1.show()

    materialize(df_time_plc_1,'Publish_Run_Logs_df_time_plc',run_id_publogs)
    logger_publogs.info("[TIME_PERD_PLC] complementing input with run_time_perd_plc")
    df_run_time_perd_plc = spark.sql(f"select * from {catalog_name_publogs}.internal_tp.tp_run_time_perd_plc limit 0") 
    df_time_plc_1 = column_complementer(df_time_plc_1, df_run_time_perd_plc)
    logger_publogs.info("[TIME_PERD_PLC] df_time_plc_1 after compliment:")
    df_time_plc_1.show()

    dim_type='TP_RUN_TIME_PERD_PLC'
    run_time_perd_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id_publogs)
    # Print the constructed path for verification
    logger_publogs.info(f"run_time_perd_plc_path: {run_time_perd_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger_publogs.info("Acquiring semaphore for TP_RUN_TIME_PERD_PLC")
    run_time_perd_plc_check_path = semaphore_acquisition(run_id_publogs, run_time_perd_plc_path,catalog_name_publogs, spark)
    try:
        logger_publogs.info(f"[TIME_PERD_PLC] Started writing data to tp_run_time_perd_plc with count: {df_time_plc_1.count()}")
        safe_write_with_retry(df_time_plc_1, f"{catalog_name_publogs}.internal_tp.tp_run_time_perd_plc", "append", partition_by="cntrt_id", options=None)
        logger_publogs.info("[TIME_PERD_PLC] Data written to tp_run_time_perd_plc")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name_publogs,run_id_publogs,run_time_perd_plc_check_path , spark)
        logger_publogs.info("TP_RUN_TIME_PERD_PLC Semaphore released")
        
    logger_publogs.info("[TIME_PERD_PLC] Publish RUN_TIME_PERD_PLC Completed")

    logger_publogs.info("Starting Publish RUN_PRTTN_PLC")

    df_prttn_plc = (
        df_time_map.withColumn("run_id", lit(run_id_publogs))
        .withColumn("time_perd_class_code", lit(time_perd_class_code))
        .withColumn("srce_sys_id", lit(srce_sys_id))
        .withColumn("cntrt_id", lit(cntrt_id_publogs))
    )
    logger_publogs.info("Displaying df_prttn_plc:")
    df_prttn_plc.show()

    # Identify new records in df_prttn_plc not already present in df_run_prttn_plc
    df_prttn_plc.createOrReplaceTempView('df_prttn_plc')
    df_prttn_plc_1 = spark.sql(f"""
                            SELECT  * FROM df_prttn_plc AS prttn_plc 
                            LEFT ANTI JOIN (select * from {catalog_name_publogs}.gold_tp.tp_run_prttn_plc where cntrt_id=:cntrt_id_publogs) AS ref 
                            ON ref.run_id=prttn_plc.run_id
                            """,{"cntrt_id_publogs":cntrt_id_publogs})
    logger_publogs.info(f"[PRTTN_PLC] df_prttn_plc anti-joined with tp_run_prttn_plc, new rows: {df_prttn_plc_1.count()}")
    df_prttn_plc_1.show()

    materialize(df_prttn_plc_1,'Publish_Run_Logs_df_prttn_plc',run_id_publogs)
    logger_publogs.info("[PRTTN_PLC] Starting Column Compliment for df_prttn_plc_1 with tp_run_prttn_plc")
    df_run_prttn_plc = spark.sql(f"select * from {catalog_name_publogs}.gold_tp.tp_run_prttn_plc limit 0")
    df_prttn_plc_1 = column_complementer(df_prttn_plc_1, df_run_prttn_plc)
    logger_publogs.info("[PRTTN_PLC] Column Compliment completed for df_prttn_plc_1")
    df_prttn_plc_1.show()
    logger_publogs.info("[PRTTN_PLC] Started adding Secure group key for df_prttn_plc_1")
    df_prttn_plc_1 = add_secure_group_key(df_prttn_plc_1, cntrt_id_publogs, postgres_schema_publogs, spark, ref_db_jdbc_url_publogs, ref_db_name_publogs, ref_db_user_publogs, ref_db_pwd_publogs)
    logger_publogs.info("[PRTTN_PLC] Secure group key added for df_prttn_plc_1")
    df_prttn_plc_1.show()

    dim_type='TP_RUN_PRTTN_PLC'
    run_prttn_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id_publogs)
    # Print the constructed path for verification
    logger_publogs.info(f"run_prttn_plc_path: {run_prttn_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger_publogs.info("Acquiring semaphore for TP_RUN_PRTTN_PLC")
    run_prttn_plc_check_path = semaphore_acquisition(run_id_publogs, run_prttn_plc_path,catalog_name_publogs, spark)
    try:
        logger_publogs.info(f"[PRTTN_PLC] Writing {df_prttn_plc_1.count()} rows to tp_run_prttn_plc")
        safe_write_with_retry(df_prttn_plc_1, f"{catalog_name_publogs}.gold_tp.tp_run_prttn_plc", "append", partition_by="cntrt_id", options=None)
        logger_publogs.info("[PRTTN_PLC] Publish PRTTN_PLC Completed Successfully")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name_publogs,run_id_publogs,run_prttn_plc_check_path , spark)
        logger_publogs.info("TP_RUN_PRTTN_PLC Semaphore released")

    logger_publogs.info("[MKT_PLC] Started publish for MKT_PLC")
    # Load Market data
    df_mkt_as = spark.read.parquet(f"{raw_path_publogs}/{run_id_publogs}/Market_Derivation_df_mkt_as")
    df_mkt_as.createOrReplaceTempView('df_mkt_as')
    logger_publogs.info("[MKT_PLC] Loaded df_mkt_as")
    df_mkt_as.show()
    # Identify new records not already present in run_mkt_plc table
    logger_publogs.info("[MKT_PLC] Started join with df_mkt_as and tp_run_mkt_plc")
    df_mkt_as_1 = spark.sql(f"""
            SELECT  * FROM df_mkt_as AS mkt 
            LEFT ANTI JOIN (select * from {catalog_name_publogs}.internal_tp.tp_run_mkt_plc where cntrt_id=:cntrt_id_publogs) AS ref 
            ON  ref.run_id = mkt.run_id and ref.mkt_skid = mkt.mkt_skid
            """,{"cntrt_id_publogs":cntrt_id_publogs})
    logger_publogs.info(f"[MKT_PLC] df_mkt_as anti-joined with tp_run_mkt_plc, new rows: {df_mkt_as_1.count()}")
    df_mkt_as_1.show()
    logger_publogs.info("[MKT_PLC] Starting Column Compliment for df_mkt_as_1 with tp_run_mkt_plc")
    df_run_mkt_plc = spark.sql(f"select * from {catalog_name_publogs}.internal_tp.tp_run_mkt_plc limit 0")
    df_mkt_as_1 = column_complementer(df_mkt_as_1, df_run_mkt_plc)
    logger_publogs.info("[MKT_PLC] Column Compliment completed for df_mkt_as_1")
    df_mkt_as_1.show()

    dim_type='TP_RUN_MKT_PLC'
    run_mkt_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id_publogs)
    # Print the constructed path for verification
    logger_publogs.info(f"run_mkt_plc_path: {run_mkt_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger_publogs.info("Acquiring semaphore for TP_RUN_MKT_PLC")
    run_mkt_plc_check_path = semaphore_acquisition(run_id_publogs, run_mkt_plc_path,catalog_name_publogs, spark)
    try:
        logger_publogs.info(f"[MKT_PLC] Writing {df_mkt_as_1.count()} rows to tp_run_mkt_plc")
        safe_write_with_retry(df_mkt_as_1, f"{catalog_name_publogs}.internal_tp.tp_run_mkt_plc", "append", partition_by="cntrt_id", options=None)
        logger_publogs.info("[MKT_PLC] Publish MKT_PLC Completed Successfully")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name_publogs,run_id_publogs,run_mkt_plc_check_path , spark)
        logger_publogs.info("TP_RUN_MKT_PLC Semaphore released")

    logger_publogs.info("[PROD_PLC] Started publish for PROD_PLC")
    # Load product data
    df_prod_as = spark.read.parquet(f"{raw_path_publogs}/{run_id_publogs}/Product_Derivation_df_prod_as")
    df_prod_as.createOrReplaceTempView('df_prod_as')
    logger_publogs.info("[PROD_PLC] Loaded df_prod_as")
    logger_publogs.info("[PROD_PLC] Started join with df_prod_as and tp_run_prod_plc")
    df_prod_as.show()
    # Identify new records not already present in run_prod_plc table
    df_prod_as_1 = spark.sql(f"""
            SELECT  * FROM df_prod_as AS prod
            LEFT ANTI JOIN (select * from {catalog_name_publogs}.internal_tp.tp_run_prod_plc where cntrt_id=:cntrt_id_publogs) AS ref 
            ON ref.run_id=prod.run_id and ref.prod_skid=prod.prod_skid
            """,{'cntrt_id_publogs':cntrt_id_publogs})
    logger_publogs.info(f"[PROD_PLC] df_prod_as anti-joined with tp_run_prod_plc, new rows: {df_prod_as_1.count()}")
    df_prod_as_1.show()
    logger_publogs.info("[PROD_PLC] Starting Column Compliment for df_prod_as_1 with tp_run_prod_plc")
    df_run_prod_plc = spark.sql(f"select * from {catalog_name_publogs}.internal_tp.tp_run_prod_plc limit 0")
    df_prod_as_1 = column_complementer(df_prod_as_1, df_run_prod_plc)
    logger_publogs.info("[PROD_PLC] Column Compliment completed for df_prod_as_1")
    df_prod_as_1.show()

    dim_type='TP_RUN_PROD_PLC'
    run_prod_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id_publogs)
    # Print the constructed path for verification
    logger_publogs.info(f"run_prod_plc_path: {run_prod_plc_path}")
    # Acquire semaphore for writing to the specified path
    logger_publogs.info("Acquiring semaphore for TP_RUN_PROD_PLC")
    run_prod_plc_check_path = semaphore_acquisition(run_id_publogs, run_prod_plc_path,catalog_name_publogs, spark)
    try:
        logger_publogs.info(f"[PROD_PLC] Writing {df_prod_as_1.count()} rows to tp_run_prod_plc")
        safe_write_with_retry(df_prod_as_1, f"{catalog_name_publogs}.internal_tp.tp_run_prod_plc", "append", partition_by="cntrt_id", options=None)
        logger_publogs.info("[PROD_PLC] Publish  PROD_PLC completed successfully")
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name_publogs,run_id_publogs,run_prod_plc_check_path , spark)
        logger_publogs.info("TP_RUN_PROD_PLC Semaphore released")

    # Define logical, physical, and Unity Catalog table names
    logical_table_name="TP_RUN_MEASR_PLC"     
    physical_table_name="TP_RUN_MEASR_PLC"    
    unity_catalog_table_name="TP_RUN_MEASR_PLC"

    # Define partition columns separated by '/'
    partition_definition_value="cntrt_id"  
    logger_publogs.info(f"[CDL] Publishing to CDL with logical_table_name={logical_table_name}, partition={partition_definition_value}")
    # Call the cdl_publishing function with the defined parameters
    cdl_publishing(logical_table_name,physical_table_name,unity_catalog_table_name,partition_definition_value, dbutils_publogs,Configuration,MetaPSClient)
