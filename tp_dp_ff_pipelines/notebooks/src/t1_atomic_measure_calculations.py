from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when, max, lit
from tp_utils.common import get_dbutils, read_run_params,materialize,get_logger,get_database_config,write_to_postgres,read_from_postgres,dynamic_expression,load_measr_vendr_factr_lkp,load_measr_cntrt_factr_lkp,load_cntrt_categ_cntry_assoc,materialise_path,add_latest_time_perd

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
    ref_db_hostname = db_config['ref_db_hostname']
    
    # Get the job parameters
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    logger.info(f"[Params] CNTRT_ID: {cntrt_id} | RUN_ID: {run_id}")

    postgres_schema= dbutils.secrets.get('tp_dpf2cdl', 'database-postgres-schema')
    catalog_name = dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
   
    raw_path = materialise_path(spark)

    logger.info("Reading source parquet files.")
    # Load source measure data
    df_measr_map = spark.read.parquet(f"{raw_path}/{run_id}/Fact_Derivation_df_measr_map")
    logger.info("Preview of df_measr_map:")
    df_measr_map.show()
    df_fct_cfl = spark.read.parquet(f"{raw_path}/{run_id}/Fact_Derivation_df_fct_cfl")
    logger.info("Preview of df_fct_cfl:")
    df_fct_cfl.show()
    df_srce_mmeasr = spark.read.parquet(f"{raw_path}/{run_id}/Load_Measure_df_srce_mmeasr")
    logger.info("Preview of df_srce_mmeasr:")
    df_srce_mmeasr.show()
    df_time_map = spark.read.parquet(f"{raw_path}/{run_id}/Load_Time_df_time_map")
    logger.info("Preview of df_time_map:")
    df_time_map.show()
    df_fct_schema = spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.tp_wk_fct limit 1")
    logger.info("Preview of df_fct_schema:")
    df_fct_schema.show()

    # Fetch contract metadata
    mm_cntrt_categ_assoc = load_cntrt_categ_cntry_assoc(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    categ_id = mm_cntrt_categ_assoc.categ_id
    srce_sys_id = mm_cntrt_categ_assoc.srce_sys_id
    fileformat = mm_cntrt_categ_assoc.file_formt
    cntrt_code = mm_cntrt_categ_assoc.cntrt_code
    vendor_id=mm_cntrt_categ_assoc.vendr_id
    iso_crncy_code=mm_cntrt_categ_assoc.crncy_id
    logger.info(f"[Contract Metadata] srce_sys_id: {srce_sys_id} | File Format: {fileformat} | Vendor ID: {vendor_id} | Category ID: {categ_id} | cntrt_code: {cntrt_code} | iso_crncy_code: {iso_crncy_code}")

    logger.info("Started Atomic Measure calculations")

    # Loading srce measure and reading lookup tables from postgres
    df_measr_map.createOrReplaceTempView('measr_map')    
    df_srce_mmeasr.createOrReplaceTempView('srce_mmeasr') 
    df_measr_vendr_factr_lkp= load_measr_vendr_factr_lkp(postgres_schema,spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("Preview of df_measr_vendr_factr_lkp:")
    df_measr_vendr_factr_lkp.show()
    df_measr_vendr_factr_lkp.createOrReplaceTempView('MM_MEASR_VENDR_FACTR_LKP')
    df_measr_cntrt_factr_lkp = load_measr_cntrt_factr_lkp(postgres_schema,spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info("Preview of df_measr_cntrt_factr_lkp:")
    df_measr_cntrt_factr_lkp.show()    
    df_measr_cntrt_factr_lkp.createOrReplaceTempView('MM_MEASR_CNTRT_FACTR_LKP')
    logger.info("Mapping source measures with precision and factor integration.")
    mm_measr_lkp=read_from_postgres(f'{postgres_schema}.mm_measr_lkp', spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    mm_measr_lkp.createOrReplaceTempView('mm_measr_lkp')
    # Mapping source measures with Precision and Factor Integration
    if fileformat in ('SFF','SFF2','SFF3'):

        df_srce_fct=spark.read.parquet(f"{raw_path}/{run_id}/Load_Fact_df_srce_fct")
        df_srce_fct.createOrReplaceTempView('srce_pre_mfct')
        logger.info("Preview of df_srce_fct:")
        df_srce_fct.show() 
        cols = ",".join([f"COL{i}" for i in range(1, 104)])
        logger.info("[SFF] Started Executing Sql query for mapping source measures with precision and factor integration")
        df_measr_factr_cntrt_mapped=spark.sql(f"""
                WITH input as (
                    SELECT MIDL.EXTRN_CODE,ML.MEASR_PHYS_NAME,MC.FACTR c_factr,MV.FACTR v_factr,ML.USE_IND,MIDL.NR
                    FROM (SELECT SRC.*,row_number() OVER(PARTITION BY 1 ORDER BY SRC.LINE_NUM) NR FROM measr_map SRC) MIDL
                    JOIN mm_measr_lkp ML on MIDL.MEASR_ID=ML.MEASR_ID
                    LEFT JOIN MM_MEASR_VENDR_FACTR_LKP MV on MIDL.MEASR_ID=MV.MEASR_ID and MV.VENDR_ID=1
                    LEFT JOIN MM_MEASR_CNTRT_FACTR_LKP MC on MIDL.MEASR_ID=MC.MEASR_ID and MC.CNTRT_ID= :cntrt_id
                    ),
                unpivoted as (
                    SELECT * FROM (select * FROM srce_pre_mfct WHERE col1='MKT_TAG' and col2='PROD_TAG' and col3='PER_TAG') unpivot(measr_code for col_name in ({cols}))
                    ),
                mapng_reslt as (
                    SELECT measr_code,CASE WHEN cast(substr(col_name,4) as int)>3 then 'FACT_AMT_'||replace(cast(substr(col_name,4)-3 as string),'.0','') else null end fact_amt_col_name
                    FROM unpivoted
                    ),
                fct_col_precision_mapped as (
                    SELECT r.*,m.precision_val FROM mapng_reslt r JOIN srce_mmeasr m on r.measr_code=m.extrn_code
                    )
                SELECT i.MEASR_PHYS_NAME measr_phys_name,i.C_FACTR c_factr,i.V_FACTR v_factr,i.USE_IND use_ind,i.NR nr,f.fact_amt_col_name,f.precision_val
                FROM input i JOIN fct_col_precision_mapped f on i.extrn_code=f.measr_code
                ORDER BY cast(substr(f.fact_amt_col_name,10) as int)
                """,{'cntrt_id':cntrt_id})
    else:
        logger.info("[FFS] Sql query for mapping source measures with precision and factor integration:")
        df_measr_factr_cntrt_mapped=spark.sql(f"""
                SELECT ML.MEASR_PHYS_NAME measr_phys_name,MC.FACTR c_factr,MV.FACTR v_factr,ML.USE_IND use_ind,MIDL.NR nr,
                'fact_amt_'||replace(cast(MIDL.NR as string),'.0','') fact_amt_col_name
                FROM (SELECT SRC.*,row_number() OVER(PARTITION BY 1 ORDER BY SRC.LINE_NUM) NR FROM measr_map SRC) MIDL
                JOIN mm_measr_lkp ML on MIDL.MEASR_ID=ML.MEASR_ID
                LEFT JOIN MM_MEASR_VENDR_FACTR_LKP MV on MIDL.MEASR_ID=MV.MEASR_ID and MV.VENDR_ID= :vendor_id
                LEFT JOIN MM_MEASR_CNTRT_FACTR_LKP MC on MIDL.MEASR_ID=MC.MEASR_ID and MC.CNTRT_ID= :cntrt_id
                ORDER BY MIDL.NR
                """,{'vendor_id':vendor_id,'cntrt_id':cntrt_id})
        
    logger.info("Preview of df_measr_factr_cntrt_mapped:")
    df_measr_factr_cntrt_mapped.show() 
    materialize(df_measr_factr_cntrt_mapped,'Atomic_Measure_Calculations_df_measr_factr_cntrt_mapped',run_id)

    df_measr_factr_cntrt_mapped=df_measr_factr_cntrt_mapped.withColumn("c_factr_count",length(col("c_factr")))
    df_measr_factr_cntrt_mapped=df_measr_factr_cntrt_mapped.withColumn("c_factr",when (col("c_factr_count")==0,None).otherwise(col("c_factr"))).drop("c_factr_count")

    if fileformat in ('SFF','SFF2','SFF3'):
        df_fct_cfl.createOrReplaceTempView('fct_cfl')
        df_measr_factr_cntrt_mapped.createOrReplaceTempView('measr_factr_cntrt_mapped')
        lst_key_cols = ['line_num','cntrt_id','srce_sys_id','run_id','mkt_extrn_code','prod_extrn_code','time_extrn_code']

        # Generate SQL expressions to apply precision scaling to each fact amount column
        df_measr_mapped  = spark.sql("""
            SELECT fact_amt_col_name || "/power ( 10," || cast(precision_val as decimal) || ") as " || fact_amt_col_name as fact_amt_col_name FROM measr_factr_cntrt_mapped  
            """)
        
        # Collect the transformed column expressions into a list
        lst_measr_trans = [row['fact_amt_col_name'] for row in df_measr_mapped.select('fact_amt_col_name').collect()]

        # Combine non measure columns with transformed measure columns
        lst_all_cols = lst_key_cols + lst_measr_trans

        # Build the final SELECT clause
        str_select_cols = ','.join(lst_all_cols)
        logger.info(f"str_select_cols: {str_select_cols}")
        # Execute the final query to produce the transformed fact dataset
        df_fct_cfl = spark.sql(f""" SELECT {str_select_cols} FROM fct_cfl """)
        logger.info("Preview of df_fct_cfl:")
        df_fct_cfl.show()
    
    materialize(df_fct_cfl,'Atomic_Measure_Calculations_df_fct_cfl',run_id)

    df_fct_cfl.createOrReplaceTempView('fct_cfl')
    df_measr_factr_cntrt_mapped.createOrReplaceTempView('measr_factr_cntrt_mapped')

    if fileformat in ('SFF','SFF2','SFF3'):
        lst_key_cols = ['line_num','cntrt_id','srce_sys_id','run_id','mkt_extrn_code','prod_extrn_code','time_extrn_code']
    else:
        lst_key_cols = ['cntrt_id','srce_sys_id','run_id','mkt_extrn_code','other_fct_data','line_num','dummy_separator_1','prod_extrn_code','time_extrn_code','dummy_separator_2','mkt_extrn_code2','dummy_separator_3']

    # applying factor to fact cols
    df_measr_mapped  = spark.sql("""
            SELECT 
            CASE WHEN nvl(c_factr,v_factr) is NOT NULL THEN '(' || nvl(c_factr,v_factr) || ' * ' || fact_amt_col_name || ') as ' || lower(measr_phys_name) 
            ELSE fact_amt_col_name || ' as ' || lower(measr_phys_name) END measr_transf 
            FROM measr_factr_cntrt_mapped WHERE use_ind = 'Y'
            """)

    # Collect the transformed measure expressions into a list
    lst_measr_trans = [row['measr_transf'] for row in df_measr_mapped.select('measr_transf').collect()]

    # Combine non fact columns and transformed measure columns
    lst_all_cols = lst_key_cols + lst_measr_trans

    # Build the final SELECT query
    str_select_cols = ','.join(lst_all_cols)

    # Execute the final query to produce the transformed fact dataset
    query=f"""SELECT {str_select_cols} FROM fct_cfl"""
    df_fct_smn_m = spark.sql(query)

    # joining input with time_map for time_perd_id and mm_time_perd_end_date
    df_time_map.createOrReplaceTempView('time_map')
    df_fct_smn_m.createOrReplaceTempView('fct_smn_m')
    df_fct_smn_m = spark.sql("""
                            SELECT input.* ,time_map.time_perd_id,time_map.mm_time_perd_end_date, :iso_crncy_code AS iso_crncy_code
                            FROM fct_smn_m input
                            JOIN time_map time_map ON input.time_extrn_code = time_map.extrn_code
                            """,{'iso_crncy_code':iso_crncy_code})
    logger.info("Preview of df_fct_smn_m:")
    df_fct_smn_m.show()

    # Add latest_time_perd to mm_run_detl_plc table
    add_latest_time_perd(df_fct_smn_m,postgres_schema,run_id,ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)

    logger.info("Started column complimenting df_fct_smn_m with fact schema")
    lkp_cols = df_fct_smn_m.columns
    sdim_cols = df_fct_schema.columns

    add_cols = list(set(sdim_cols)-set(lkp_cols))

    for col_name in add_cols:
        data_type = dict(df_fct_schema.dtypes)[col_name]
        df_fct_smn_m = df_fct_smn_m.withColumn(col_name, lit(None).cast(data_type))

    logger.info("Preview of df_fct_smn_m after complimenting with fact schema:")
    df_fct_smn_m.show()
    materialize(df_fct_smn_m,'Atomic_Measure_Calculations_df_fct_smn_m',run_id)
    
    logger.info("Started CRBM1 transformation:")
    df_fct_smn_m.createOrReplaceTempView('crncy')
    crncy_columns = df_fct_smn_m.columns

    # list of CRBM1-specific columns that will be excluded from the dynamic expression logic
    crbm1cols=['basln_mu_qty','basln_mu_non_prmtn_qty','basln_mu_any_prmtn_qty','basln_msu_qty','basln_msu_non_prmtn_qty','basln_msu_any_prmtn_qty','basln_mpu_qty','basln_mpu_non_prmtn_qty','basln_mpu_any_prmtn_qty','basln_mlc_amt','basln_mlc_non_prmtn_amt','basln_mlc_any_prmtn_amt','numrc_dist_stock_pct','numrc_dist_oos_pct','wgt_dist_stock_pct','wgt_dist_oos_pct','sales_mu_qty','sales_mu_non_prmtn_qty','sales_mu_any_prmtn_qty','sales_msu_qty','sales_msu_non_prmtn_qty','sales_msu_any_prmtn_qty','sales_mpu_qty','sales_mpu_non_prmtn_qty','sales_mpu_any_prmtn_qty','sales_mlc_amt','sales_mlc_non_prmtn_amt','sales_mlc_any_prmtn_amt','numrc_dist_pct','wgt_dist_pct','num_store_unvrs_u_qty','num_store_unvrs_sellg_u_qty']

    # excluding CRBM1-specific
    for i in crbm1cols:
        crncy_columns.remove(i)

    # Build a comma-separated string of the non CRBM1 specific cols
    s = ''
    for c in crncy_columns:
        s = s+str(c)+', '

    object_name = f'{postgres_schema}.mm_exprn_lkp'

    # Read the lookup table from PostgreSQL
    df_exprn_lkp = read_from_postgres(object_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    # Define operation type and dimension type for expression lookup
    oprtn_type='CRBM1'
    dim_type='FCT'

    # Generate a dynamic SQL query for "CRBM1" 
    query=dynamic_expression(df_exprn_lkp,cntrt_id,oprtn_type,dim_type,spark)

    # Evaluate the generated query
    e=eval
    final_query=e(query)
    logger.info(f"CRBM Query:\n{final_query}")

    # Execute the final query 
    df_fct_crbm=spark.sql(final_query) 
    logger.info("CRBM1 transformation completed")
    logger.info("Preview of df_fct_crbm:")
    df_fct_crbm.show()
    materialize(df_fct_crbm,'Atomic_Measure_Calculations_df_fct_crbm',run_id)
    logger.info("Started CRBM2 transformation:")
    crbm1output_columns = df_fct_crbm.columns

    # list of CRBM2-specific columns that will be excluded from the dynamic expression logic
    crbm2cols=['sales_per_lngth_msu_qty', 'sales_per_lngth_mlc_amt', 'sales_per_lngth_musd_amt','sales_per_lngth_mcus_amt', 'sales_per_lngth_meur_amt', 'stock_per_dspt_msu_qty','frwrd_stock_per_dspt_msu_qty', 'basln_musd_amt', 'basln_musd_any_prmtn_amt',
    'basln_mcus_amt', 'basln_mcus_any_prmtn_amt', 'sku_per_dspt_u_qty','prmtn_itsty_su_pct', 'dist_effcy_u_qty', 'price_per_u_lc_amt',
    'price_per_u_lc_non_prmtn_amt', 'price_per_u_lc_any_prmtn_amt','price_per_u_usd_amt', 'price_per_u_usd_non_prmtn_amt', 'price_per_u_usd_any_prmtn_amt','price_per_u_cus_amt', 'price_per_u_cus_non_prmtn_amt', 'price_per_u_cus_any_prmtn_amt','price_per_u_eur_amt', 'price_per_su_lc_amt', 'price_per_su_lc_non_prmtn_amt','price_per_su_lc_any_prmtn_amt', 'price_per_su_usd_amt', 'price_per_su_usd_any_prmtn_amt','price_per_su_cus_amt', 'price_per_su_cus_any_prmtn_amt', 'price_per_su_eur_amt','price_per_p_lc_amt', 'price_per_p_usd_amt', 'price_per_p_cus_amt','sales_musd_amt', 'sales_musd_non_prmtn_amt', 'sales_musd_any_prmtn_amt','sales_mcus_amt', 'sales_mcus_non_prmtn_amt', 'sales_mcus_any_prmtn_amt','sales_meur_amt', 'sales_meur_non_prmtn_amt', 'sales_per_dspt_mu_qty','sales_per_dspt_msu_qty', 'sales_per_dspt_mlc_amt', 'sales_per_dspt_musd_amt','sales_per_dspt_mcus_amt', 'sales_per_dspt_meur_amt']

    # exclude CRBM2-specific columns from the full list of columns
    for i in crbm2cols:
        crbm1output_columns.remove(i)

    # Build a comma-separated string of the non CRBM2-specific columns
    s1 = ''
    for c in crbm1output_columns:
        s1 = s1+str(c)+', '

    df_fct_crbm.createOrReplaceTempView("df_fct_crncy")

    # Define operation type and dimension type for expression lookup
    oprtn_type='CRBM2'
    dim_type='FCT'

    # Generate a dynamic SQL query for "CRBM2" 
    query=dynamic_expression(df_exprn_lkp,cntrt_id,oprtn_type,dim_type,spark)

    # Evaluate the generated query
    e=eval
    final_query=e(query)
    logger.info(f"CRBM2 Query:\n{final_query}")

    # Execute the final query 
    df_fct_crbm2=spark.sql(final_query) 
    df_fct_crbm2=df_fct_crbm2.drop("mkt_skid","prod_skid")
    logger.info("Preview of df_fct_crbm2:")
    df_fct_crbm2.show()
    materialize(df_fct_crbm2,'Atomic_Measure_Calculations_df_fct_crbm2',run_id)

    logger.info("Atomic_Measure_Calculations execution completed successfully.")