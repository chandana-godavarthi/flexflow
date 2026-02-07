from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,materialize
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc,load_mkt_skid_lkp,materialise_path

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

    logger.info(f"[Market_Derivation] FILE_NAME: {file_name} | CNTRT_ID: {cntrt_id} | RUN_ID: {run_id}")

    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')

    # Fetch data from PostgreSQL
    mm_cntrt_categ_cntry_lkp = load_cntrt_categ_cntry_assoc(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    vendr_id = mm_cntrt_categ_cntry_lkp.vendr_id
    srce_sys_id = mm_cntrt_categ_cntry_lkp.srce_sys_id
    cntry_id = mm_cntrt_categ_cntry_lkp.cntry_id
    raw_path = materialise_path(spark)

    logger.info(f"[Contract Lookup] Source System ID: {srce_sys_id} | Vendor ID: {vendr_id} | Country ID: {cntry_id}")
    logger.info("Starting derive_mkt_df function")
    df_srce_mmkt = spark.read.parquet(f"{raw_path}/{run_id}/Load_Market_df_srce_mmkt")
    logger.info("Loaded source market data from parquet. Preview:")
    df_srce_mmkt.show()

    df_mkt_skid_lkp = load_mkt_skid_lkp(cntry_id,vendr_id,srce_sys_id,postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_mkt_skid_lkp.createOrReplaceTempView("df_mkt_skid_lkp")
    df_srce_mmkt.createOrReplaceTempView("df_srce_mmkt")
    logger.info("[derive_mkt_df] Loaded df_mkt_skid_lkp")
    df_mkt_skid_lkp.show()

    logger.info("[derive_mkt_df] Executing market derivation SQL query:")
    df_mkt_as = spark.sql("""
    WITH df_mkt_skid_lkp_1 AS (
        SELECT * FROM df_mkt_skid_lkp WHERE cntrt_id IS NULL
        ),
        df_mkt_skid_lkp_2 AS (
            SELECT * FROM df_mkt_skid_lkp WHERE cntrt_id=:cntrt_id AND extrn_mkt_id IS NULL
            )
        SELECT COALESCE(mkt_cntrt.mkt_skid, mkt.mkt_skid) AS mkt_skid, 
        input.* ,
        mkt.mkt_skid as mkt_skid_1,
        mkt_cntrt.mkt_skid as mkt_skid_2
        FROM df_srce_mmkt AS input
        LEFT OUTER JOIN df_mkt_skid_lkp_1 AS mkt ON input.extrn_mkt_id = mkt.extrn_mkt_id
        LEFT OUTER JOIN df_mkt_skid_lkp_2 AS mkt_cntrt ON input.extrn_mkt_id = mkt_cntrt.extrn_mkt_id
    """,{'cntrt_id':cntrt_id})
    logger.info("[Market_Derivation] Market DataFrame derived successfully. Displaying df_mkt_as:")
    df_mkt_as.show()

    materialize(df_mkt_as,'Market_Derivation_df_mkt_as',run_id)

    logger.info("[Market_Derivation] Market derivation process completed successfully.")