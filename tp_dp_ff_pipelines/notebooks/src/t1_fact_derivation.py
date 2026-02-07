from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit
from tp_utils.common import get_dbutils, read_run_params,materialize,time_perd_class_codes,derive_fact_sff,derive_fact_non_sff,load_measr_id_lkp,materialise_path
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
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    logger.info(f"[Fact_Derivation] Starting job for run_id={run_id}, cntrt_id={cntrt_id}")

    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    logger.info("[Fact_Derivation] Starting fact derivation process")
    # Fetch data from PostgreSQL
    mm_cntrt_categ_cntry_lkp = load_cntrt_categ_cntry_assoc(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    vendr_id = mm_cntrt_categ_cntry_lkp.vendr_id
    srce_sys_id = mm_cntrt_categ_cntry_lkp.srce_sys_id
    cntry_id = mm_cntrt_categ_cntry_lkp.cntry_id
    file_formt = mm_cntrt_categ_cntry_lkp.file_formt
    raw_path = materialise_path(spark)
    logger.info(f"[Fact_Derivation] Contract metadata: vendr_id={vendr_id}, srce_sys_id={srce_sys_id}, cntry_id={cntry_id}, file_formt={file_formt}")

    # Read data
    df_srce_mfct = spark.read.parquet(f"{raw_path}/{run_id}/Load_Fact_df_srce_mfact")
    logger.info("[Fact_Derivation] Preview of source fact data df_srce_mfct:")
    df_srce_mfct.show(5)
    df_srce_mmeasr = spark.read.parquet(f"{raw_path}/{run_id}/Load_Measure_df_srce_mmeasr")
    logger.info("[Fact_Derivation] Preview of source measure data df_srce_mmeasr:")
    df_srce_mmeasr.show(5)

    if file_formt in ('SFF', 'SFF2', 'SFF3'):
        df_fct_cfl = derive_fact_sff(df_srce_mfct,spark)
    else:
        df_fct_cfl = derive_fact_non_sff(df_srce_mfct,df_srce_mmeasr,run_id,cntrt_id,srce_sys_id,spark)

    logger.info("[Fact_Derivation] Derived fact DataFrame df_fct_cfl") 
    logger.info("[Fact_Derivation] Preview of df_fct_cfl")
    df_fct_cfl.show(5)
    materialize(df_fct_cfl,'Fact_Derivation_df_fct_cfl',run_id)

    logger.info("[Fact_Derivation] Starting null handling")

    fact_columns = [f"fact_amt_{i}" for i in range(1, 100)]
   
    for column in fact_columns:
        df_fct_dvm_100_measr = df_fct_cfl.withColumn(column, coalesce(col(column), lit(None).cast("integer")))

    logger.info("[Fact_Derivation] Null handling completed. Preview of df_fct_dvm_100_measr:")
    df_fct_dvm_100_measr.show(5)
    materialize(df_fct_dvm_100_measr,'Fact_Derivation_df_fct_dvm_100_measr',run_id)

    logger.info("[Measr_Mapping] Starting Measure Mapping process.")
    df_measr_id_lkp = load_measr_id_lkp(vendr_id,postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_measr_id_lkp.createOrReplaceTempView("measr_id_lkp")
    df_srce_mmeasr.createOrReplaceTempView("srce_mmeasr")
    logger.info("[Measr_Mapping] Lookup table loaded successfully. Preview of df_measr_id_lkp")
    df_measr_id_lkp.show()

    logger.info("[Measr_Mapping] Executing Fact Mapping SQL query:")
    df_measr_map = spark.sql("""
        SELECT input.*, 
        measr_deflt.measr_id as deflt_measr_id, 
        measr_cntry.measr_id as cntry_measr_id,
        measr_cntrt.measr_id as cntrt_measr_id 
        FROM srce_mmeasr AS input 
        LEFT OUTER JOIN (SELECT * FROM measr_id_lkp WHERE cntry_id IS NULL AND cntrt_id IS NULL) AS measr_deflt 
            ON measr_deflt.extrn_measr_id=input.extrn_code 
        LEFT OUTER JOIN (SELECT * FROM measr_id_lkp WHERE cntry_id=:cntry_id) AS measr_cntry 
            ON measr_cntry.extrn_measr_id=input.extrn_code 
        LEFT OUTER JOIN (SELECT * FROM measr_id_lkp WHERE cntrt_id=:cntrt_id) AS measr_cntrt 
            ON measr_cntrt.extrn_measr_id=input.extrn_code""",{'cntry_id':cntry_id,'cntrt_id':cntrt_id})

    df_measr_map.createOrReplaceTempView('measr_map')
    df_measr_map = spark.sql("""SELECT *,COALESCE(cntrt_measr_id, cntry_measr_id, deflt_measr_id) as measr_id from measr_map""")
    logger.info("[Measr_Mapping] Measure Mapping completed successfully. Preview of df_measr_map")
    df_measr_map.show()
    materialize(df_measr_map,'Fact_Derivation_df_measr_map',run_id)
    
    logger.info("[Fact_Derivation] Fact derivation process completed successfully")