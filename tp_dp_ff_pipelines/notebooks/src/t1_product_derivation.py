from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tp_utils.common import get_dbutils, read_run_params,materialize,get_logger,get_database_config
from tp_utils.common import convert_cols_to_lower,column_complementer,t1_get_attribute_codes,t1_get_attribute_values,t1_add_row_change_description,skid_service,t1_normalise_product_attributes,t1_generate_product_description,t1_product_publish,load_cntrt_categ_cntry_assoc,materialise_path
from dnalib.skidv2 import SkidClientv2

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    dbutils_proddrvtn = get_dbutils(spark)
    logger_proddrvtn = get_logger()

    # Retrieve secrets from Databricks
    db_config = get_database_config(dbutils_proddrvtn)
    ref_db_jdbc_url_proddrvtn = db_config['ref_db_jdbc_url']
    ref_db_name_proddrvtn = db_config['ref_db_name']
    ref_db_user_proddrvtn = db_config['ref_db_user']
    ref_db_pwd_proddrvtn = db_config['ref_db_pwd']

    tenant_id = dbutils.secrets.get('tp_dpf2cdl', 'pg-azure-tenant-id')
    publisher_name= 'TRADEPANEL'
    application_key= '11'
    
    # Get the job parameters
    args = read_run_params()
    cntrt_id_proddrvtn = args.CNTRT_ID
    run_id_proddrvtn = args.RUN_ID
    env = args.ENV

    logger_proddrvtn.info(f"[Params] CNTRT_ID: {cntrt_id_proddrvtn} | RUN_ID: {run_id_proddrvtn}")
    postgres_schema_proddrvtn= dbutils_proddrvtn.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name_proddrvtn = dbutils_proddrvtn.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')

    raw_path_proddrvtn = materialise_path(spark)

    # Fetch data from PostgreSQL
    mm_cntrt_categ_lkp = load_cntrt_categ_cntry_assoc(cntrt_id_proddrvtn, postgres_schema_proddrvtn, spark, ref_db_jdbc_url_proddrvtn, ref_db_name_proddrvtn, ref_db_user_proddrvtn, ref_db_pwd_proddrvtn)
    categ_id_proddrvtn = mm_cntrt_categ_lkp.categ_id
    srce_sys_id_proddrvtn = mm_cntrt_categ_lkp.srce_sys_id
    fileformat_proddrvtn = mm_cntrt_categ_lkp.file_formt
    cntrt_code_proddrvtn = mm_cntrt_categ_lkp.cntrt_code
    logger_proddrvtn.info(f"[Contract Lookup] Source System ID: {srce_sys_id_proddrvtn} | categ_id: {categ_id_proddrvtn} | fileformat: {fileformat_proddrvtn} | cntrt_code: {cntrt_code_proddrvtn}")

    # Load product SDIM and DIM data
    logger_proddrvtn.info("Loading Product SDIM and DIM tables")
    df_mm_prod_csdim =spark.sql(f"SELECT * FROM {catalog_name_proddrvtn}.internal_tp.tp_prod_sdim WHERE srce_sys_id=:srce_sys_id and cntrt_id=:cntrt_id",{'srce_sys_id':srce_sys_id_proddrvtn,'cntrt_id':cntrt_id_proddrvtn})
    logger_proddrvtn.info("Preview of df_mm_prod_csdim")
    df_mm_prod_csdim.show()
    df_srce_mprod = spark.read.parquet(f"{raw_path_proddrvtn}/{run_id_proddrvtn}/Load_Product_df_srce_mprod")
    logger_proddrvtn.info("Preview of df_srce_mprod")
    df_srce_mprod.show()
    
    logger_proddrvtn.info("[Product_Derivation] Get product attribute codes and values")
    df_prod_gan,df_prod_nactc = t1_get_attribute_codes(df_srce_mprod,postgres_schema_proddrvtn,spark, ref_db_jdbc_url_proddrvtn, ref_db_name_proddrvtn, ref_db_user_proddrvtn, ref_db_pwd_proddrvtn)
    materialize(df_prod_nactc,'Product_Derivation_pre_df_prod_nactc',run_id_proddrvtn)
    df_prod_gav = t1_get_attribute_values(df_mm_prod_csdim,df_prod_gan,postgres_schema_proddrvtn,spark, ref_db_jdbc_url_proddrvtn, ref_db_name_proddrvtn, ref_db_user_proddrvtn, ref_db_pwd_proddrvtn,fileformat_proddrvtn,run_id_proddrvtn)
    logger_proddrvtn.info(f"Count of df_prod_nactc before adding row_change_description: {df_prod_nactc.count()}")
    df_prod_nactc = t1_add_row_change_description(df_prod_nactc,df_mm_prod_csdim,spark)
    materialize(df_prod_nactc,'Product_Derivation_df_prod_nactc',run_id_proddrvtn)

    df_prod_isnull = df_prod_nactc.filter(col('row_chng_desc') == 'NEW PRODUCT')
    df_prod_isnotnull = df_prod_nactc.filter(col('row_chng_desc') != 'NEW PRODUCT')

    new_product_count = df_prod_isnull.count()
    existing_product_count = df_prod_isnotnull.count()

    logger_proddrvtn.info(f"New products count: {new_product_count} | Existing products count: {existing_product_count}")

    if new_product_count > 0:
        data_provider_code= f"TP_EU_{categ_id_proddrvtn}"
        logger_proddrvtn.info(f"[Product_Derivation] Assigning SKIDs for new products | data_provider_code={data_provider_code}")
        # Generate skids for NEW PRODUCTS
        df=skid_service(df_prod_isnull,'extrn_prod_id,cntrt_id','prod_dim',data_provider_code,spark,dbutils,SkidClientv2,tenant_id,env,publisher_name,application_key)
        df_prod_isnull = df.withColumn("prod_skid", col("surrogate_key")).drop("surrogate_key")
    
    df_prod_nactc = df_prod_isnotnull.unionByName(df_prod_isnull, True)
    null_prodskid_count = df_prod_nactc.filter(col("prod_skid").isNull()).count()
    product_count = df_prod_nactc.count()
    logger_proddrvtn.info(f"Null Prod_skid Count: {null_prodskid_count} | Products count after assigning skid: {product_count}")
    # df_prod_nactc = df_prod_nactc.filter(col('row_chng_desc') != 'FULL MATCH')
    logger_proddrvtn.info(f"Modified products count : {df_prod_nactc.count()}")
    materialize(df_prod_nactc,'Product_Derivation_df_prod_nactc',run_id_proddrvtn)

    df_prod_nactc.createOrReplaceTempView("df_prod_nactc")
    df_mm_prod_csdim.createOrReplaceTempView("df_mm_prod_csdim")

    # Join Input data with csdim to assign product skid to input data
    df_prod_as = spark.sql(""" SELECT  
        input.* EXCEPT (prod_skid,prod_name), 
        input.prod_name as prod_name_1, 
        coalesce(csdim.prod_skid,input.prod_skid ) as prod_skid, 
        coalesce(input.extrn_prod_name,csdim.prod_name) as prod_name 
        FROM df_prod_nactc AS input 
        LEFT OUTER JOIN df_mm_prod_csdim AS csdim 
        ON input.extrn_prod_id = csdim.extrn_prod_id 
        and input.prod_match_attr_list = csdim.prod_match_attr_list 
        """)
    logger_proddrvtn.info("Preview of df_prod_as")
    df_prod_as.show()
    materialize(df_prod_as,'Product_Derivation_df_prod_as',run_id_proddrvtn)
    df_prod_dsdim=df_prod_as
    # df_prod_dsdim=df_prod_as.filter(col('row_chng_desc') != 'FULL MATCH')
    df_prod_dsdim_cols=df_prod_dsdim
    logger_proddrvtn.info("Preview of df_prod_dsdim_cols")
    df_prod_dsdim_cols.show()

    df_prod_dsdim = convert_cols_to_lower(df_prod_dsdim)
    logger_proddrvtn.info("Column compliment completed succesfully for df_prod_dsdim")
    df_prod_dsdim = column_complementer(df_prod_dsdim, df_mm_prod_csdim)
    logger_proddrvtn.info("Preview of df_prod_dsdim")
    df_prod_dsdim.show()
    materialize(df_prod_dsdim,'Product_Derivation_df_prod_dsdim',run_id_proddrvtn)

    t1_normalise_product_attributes(df_prod_dsdim,df_prod_dsdim_cols,postgres_schema_proddrvtn,run_id_proddrvtn,spark, ref_db_jdbc_url_proddrvtn, ref_db_name_proddrvtn, ref_db_user_proddrvtn, ref_db_pwd_proddrvtn)

    t1_generate_product_description(df_mm_prod_csdim,postgres_schema_proddrvtn,spark, ref_db_jdbc_url_proddrvtn, ref_db_name_proddrvtn, ref_db_user_proddrvtn, ref_db_pwd_proddrvtn,run_id_proddrvtn)

    t1_product_publish(cntrt_id_proddrvtn, run_id_proddrvtn, srce_sys_id_proddrvtn, spark, catalog_name_proddrvtn)
    logger_proddrvtn.info("Product Derivation Completed Succesfully")