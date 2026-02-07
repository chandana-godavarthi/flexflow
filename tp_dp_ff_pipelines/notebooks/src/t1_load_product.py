from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,t1_load_file,materialize,extract_product_tape,extract_product_sff3,extract_product_sff2,extract_product_sff,extract_product_ffs2,extract_product_ffs,materialise_path
from tp_utils.common import get_logger,get_database_config,load_cntrt_categ_cntry_assoc,load_max_lvl

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

    logger.info(f"[Params] FILE_NAME: {file_name} | CNTRT_ID: {cntrt_id} | RUN_ID: {run_id}")

    catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')

    file_type='product'
    file_type2='PROD'

    # Read the data from PostgreSQL into a DataFrame
    df_cntrt_categ_assoc_lkp = load_cntrt_categ_cntry_assoc(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    categ_id = df_cntrt_categ_assoc_lkp.categ_id
    srce_sys_id = df_cntrt_categ_assoc_lkp.srce_sys_id
    fileformat = df_cntrt_categ_assoc_lkp.file_formt
    cntrt_code = df_cntrt_categ_assoc_lkp.cntrt_code
    logger.info(f"[Contract Lookup] Category ID: {categ_id} | Source System ID: {srce_sys_id} | File Format: {fileformat} | Contract Code: {cntrt_code}")

    df_max_lvl = load_max_lvl(categ_id,postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("[LoadFile] df_max_lvl preview:")

    raw_path = materialise_path(spark)
    # Load the file using the specified parameters
    df_raw = t1_load_file(file_type,run_id,file_type2,fileformat,spark)
    materialize(df_raw,'load_product_df_raw',run_id)
    
    logger.info("[LoadFile] Started Extracting product file")
    df_raw = spark.read.parquet(f"{raw_path}/{run_id}/load_product_df_raw")
    # Transform raw market data based on file format
    if fileformat == 'FFS':
        df_srce_mprod = extract_product_ffs(df_raw,run_id,cntrt_id,categ_id,srce_sys_id,df_max_lvl)
    elif fileformat == 'FFS2':
        df_srce_mprod = extract_product_ffs2(df_raw,run_id,cntrt_id,categ_id,srce_sys_id,df_max_lvl)
    elif fileformat == 'SFF3':
        df_srce_mprod,df_invld_hier_prod = extract_product_sff3(df_raw, run_id, cntrt_id, categ_id, srce_sys_id, df_max_lvl,spark)
        materialize(df_invld_hier_prod,'Load_Product_df_invld_hier_prod',run_id)
    elif fileformat == 'SFF2':
        df_srce_mprod = extract_product_sff2(df_raw, run_id, cntrt_id, categ_id, srce_sys_id, df_max_lvl)
        df_invld_hier_prod = df_srce_mprod.select('extrn_prod_id').limit(0)
        materialize(df_invld_hier_prod,'Load_Product_df_invld_hier_prod',run_id)
    elif fileformat == 'SFF':
        df_srce_mprod = extract_product_sff(df_raw, run_id, cntrt_id, categ_id, srce_sys_id, df_max_lvl)
        df_invld_hier_prod = df_srce_mprod.select('extrn_prod_id').limit(0)
        materialize(df_invld_hier_prod,'Load_Product_df_invld_hier_prod',run_id)
    elif fileformat in ('Tape2', 'Tape3'):
        df_srce_mprod = extract_product_tape(df_raw,run_id,cntrt_id,categ_id,srce_sys_id,df_max_lvl)

    logger.info("[LoadFile] Product File loading completed sucessfully")
    logger.info("[LoadFile] df_srce_mprod preview:")

    # Save the processed DataFrame to parquet file
    materialize(df_srce_mprod,'Load_Product_df_srce_pre_mprod',run_id)
    df_srce_mprod = spark.read.parquet(f"{raw_path}/{run_id}/Load_Product_df_srce_pre_mprod")

    df_pre_prod_sdim=spark.sql(f"SELECT * FROM {catalog_name}.internal_tp.tp_prod_sdim lkp WHERE part_srce_sys_id=:srce_sys_id and part_cntrt_id=:cntrt_id",{'srce_sys_id':srce_sys_id,'cntrt_id':cntrt_id})

    df_srce_mprod.createOrReplaceTempView('srce_mprod')
    df_pre_prod_sdim.createOrReplaceTempView('sdim')
    df_srce_mprod=spark.sql(f"""with multiple_prods as 
    (select extrn_prod_id,
    prod_match_attr_list|| ' ('|| extrn_prod_id ||')' prod_match_attr_list,
    extrn_prod_name || ' ('|| extrn_prod_id ||')' extrn_prod_name,
    extrn_name || ' ('|| extrn_code ||')' extrn_name
    from
    (select T.prod_match_attr_list,T.extrn_prod_id,T.extrn_code,T.extrn_name,T.extrn_prod_name,T.extrn_prod_attr_val_list,COUNT(1) OVER (PARTITION BY T.extrn_prod_name, T.extrn_prod_attr_val_list) cnt from srce_mprod T)
    where cnt > 1),
    sdim_compare_prods as
    (select p.extrn_prod_id,
    p.prod_match_attr_list|| ' ('|| p.extrn_prod_id ||')' prod_match_attr_list,
    p.extrn_prod_name || ' ('|| p.extrn_prod_id ||')' extrn_prod_name,
    p.extrn_name || ' ('|| p.extrn_code ||')' extrn_name
                                            from sdim x
                                            join srce_mprod p
                                            where x.srce_sys_id = '{srce_sys_id}' and x.cntrt_id = '{cntrt_id}' and p.extrn_prod_id = x.extrn_prod_id and p.extrn_prod_attr_val_list = x.extrn_prod_attr_val_list and x.prod_name = p.extrn_prod_name|| ' ('|| p.extrn_prod_id ||')')
    select m.extrn_code,
    coalesce(xp.extrn_name,mp.extrn_name,m.extrn_name) extrn_name,
    m.attr_code_list,
    m.line_num,
    m.lvl_num,
    m.extrn_prod_id,
    m.extrn_prod_attr_val_list,
    coalesce(xp.extrn_prod_name,mp.extrn_prod_name,m.extrn_prod_name) extrn_prod_name,
    m.prod_name,
    m.prod_desc,
    m.attr_code_0,
    m.attr_code_1,
    m.attr_code_2,
    m.attr_code_3,
    m.attr_code_4,
    m.attr_code_5,
    m.attr_code_6,
    m.attr_code_7,
    m.attr_code_8,
    m.attr_code_9,
    m.attr_code_10,
    m.categ_id,
    m.cntrt_id,
    m.run_id,
    coalesce(xp.prod_match_attr_list,mp.prod_match_attr_list,m.prod_match_attr_list) prod_match_attr_list,
    m.srce_sys_id
    from srce_mprod  m
    left join multiple_prods mp
    on m.extrn_prod_id = mp.extrn_prod_id
    left join sdim_compare_prods xp
    on m.extrn_prod_id = xp.extrn_prod_id""")

    logger.info(f"No.Of Products loaded:{df_srce_mprod.count()}")
    materialize(df_srce_mprod,'Load_Product_df_srce_mprod',run_id)