from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tp_utils.common import get_dbutils, read_run_params,materialize,get_logger,get_database_config,time_perd_class_codes,column_complementer, match_time_perd_class,materialise_path

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_factimg = get_dbutils(spark)
    logger_factimg = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_factimg)
    ref_db_jdbc_url_factimg = db_config['ref_db_jdbc_url']
    ref_db_name_factimg = db_config['ref_db_name']
    ref_db_user_factimg = db_config['ref_db_user']
    ref_db_pwd_factimg = db_config['ref_db_pwd']
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_factimg = args.CNTRT_ID
    run_id_factimg = args.RUN_ID

    logger_factimg.info(f"[Params] CNTRT_ID: {cntrt_id_factimg} | RUN_ID: {run_id_factimg}")
    postgres_schema_factimg= dbutils_factimg.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name_factimg = dbutils_factimg.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    raw_path_factimg = materialise_path(spark)

    time_perd_class_code=time_perd_class_codes(cntrt_id_factimg,catalog_name_factimg, postgres_schema_factimg,spark, ref_db_jdbc_url_factimg, ref_db_name_factimg, ref_db_user_factimg, ref_db_pwd_factimg)
    logger_factimg.info(f"time_perd_class_code: {time_perd_class_code}")

    # Load df_time_map,df_mkt_as,df_fct_crbm2,prod_sdim 
    class_code = match_time_perd_class(time_perd_class_code.lower())
    df_fct_schema = spark.sql(f"select * from {catalog_name_factimg}.gold_tp.tp_{class_code}_fct limit 1")
    logger_factimg.info("Displaying df_fct_schema:")
    df_fct_schema.show()
    df_time_map = spark.read.parquet(f"{raw_path_factimg}/{run_id_factimg}/Load_Time_df_time_map")
    df_time_map.createOrReplaceTempView('df_time_map')
    logger_factimg.info("Displaying df_time_map:")
    df_time_map.show()
    df_mkt_as = spark.read.parquet(f"{raw_path_factimg}/{run_id_factimg}/Market_Derivation_df_mkt_as")
    df_mkt_as.createOrReplaceTempView('df_mkt_as')
    logger_factimg.info("Displaying df_mkt_as:")
    df_mkt_as.show()
    df_fct_crbm2 = spark.read.parquet(f"{raw_path_factimg}/{run_id_factimg}/Atomic_Measure_Calculations_df_fct_crbm2")
    df_fct_crbm2.createOrReplaceTempView('df_fct_crbm2')
    logger_factimg.info("Displaying df_fct_crbm2:")
    df_fct_crbm2.show()
    logger_factimg.info("Started generating fact image")
    df_fct_img = spark.sql(f"""
        SELECT  input.*, mkt_map.mkt_skid, prod_map.prod_skid 
        FROM df_fct_crbm2 AS input 
        LEFT OUTER JOIN df_time_map AS time_map 
        ON time_map.extrn_code=input.time_extrn_code 
        LEFT OUTER JOIN df_mkt_as AS mkt_map 
        ON mkt_map.srce_sys_id = input.srce_sys_id and  input.mkt_extrn_code = mkt_map.extrn_mkt_id 
        LEFT OUTER JOIN (SELECT * FROM {catalog_name_factimg}.internal_tp.tp_prod_sdim WHERE part_cntrt_id=:cntrt_id and part_srce_sys_id = 3 ) AS prod_map 
        ON prod_map.srce_sys_id = input.srce_sys_id and  input.prod_extrn_code = prod_map.extrn_prod_id
        """,{'cntrt_id':cntrt_id_factimg})
    
    df_fct_img.show()
    logger_factimg.info("Displaying df_fct_img:")
    materialize(df_fct_img,'Generate_Fact_Image_df_fct_skid',run_id_factimg)

    df_fct_materl_img = column_complementer(df_fct_img,df_fct_schema)
    logger_factimg.info("Displaying df_fct_img after column complimenting with  fact schema:")
    df_fct_materl_img.show()

    # Add and typecast partition columns to the DataFrame
    df_fct_materl_img = df_fct_materl_img \
        .withColumn("part_srce_sys_id", df_fct_materl_img["srce_sys_id"]) \
        .withColumn("part_cntrt_id", df_fct_materl_img["cntrt_id"]) \
        .withColumn("part_mm_time_perd_end_date", df_fct_materl_img["mm_time_perd_end_date"])
    logger_factimg.info("Displaying df_fct_img after adding partition columns:")
    df_fct_materl_img.show()
    materialize(df_fct_materl_img,'fact_transformation_df_fact_stgng_vw',run_id_factimg)

    df_fct=spark.sql(f"SELECT * FROM {catalog_name_factimg}.gold_tp.tp_{class_code}_fct WHERE cntrt_id=:cntrt_id",{'cntrt_id':cntrt_id_factimg})
    df_fct_img=df_fct_materl_img.unionByName(df_fct)
    
    logger_factimg.info("df_fct_img union with previous fact data")
    df_fct_img.show()

    materialize(df_fct_img,'Generate_Fact_Image_df_fct_img',run_id_factimg)
    
    logger_factimg.info("Fact Materialization completed Successfully")