from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params, load_cntrt_lkp, run_prttn_dtls, publish_valdn_run_strct_lvl, time_perd_class_codes, publish_valdn_run_strct, publish_valdn_agg_fct, column_complementer, add_secure_group_key,read_from_postgres, get_database_config, match_time_perd_class,materialise_path,semaphore_generate_path,semaphore_acquisition,release_semaphore,safe_write_with_retry,safe_delete_with_retry
import argparse
from tp_utils.common import get_logger
from pyspark.sql.functions import lit, col, count, max, when, expr

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
    logger = get_logger()
    dbutils = get_dbutils(spark)

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

    postgres_schema = db_config['postgres_schema']
    catalog_name = db_config['catalog_name']

    # Filter the DataFrame to get the vendor file pattern and step file pattern for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )

    # Fetch source system ID and time period type code for the given contract ID
    srce_sys_id=df_cntrt_lkp.select('srce_sys_id').collect()[0].srce_sys_id
    time_perd_type_code=df_cntrt_lkp.select('time_perd_type_code').collect()[0].time_perd_type_code


    # Determine the time period class code based on the type code
    time_perd_class_code=time_perd_class_codes(cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)


    # Load materialized product dimension data for the given run
    pre_bq_mat_path = materialise_path(spark)
    tier2_prod_mtrlz_tbl = spark.read.parquet(
        f'{pre_bq_mat_path}/{run_id}/product_transformation_df_prod_stgng_vw'
    )


    # Load product promotion dimension from gold layer for the given source system
    df_prod_promo_vw = spark.sql(
        f"SELECT * FROM {catalog_name}.internal_tp.tp_prod_sdim WHERE srce_sys_id = :srce_sys_id",{"srce_sys_id":srce_sys_id}
    )


    # Load fact data for promotions
    df_fact_promo_img = spark.read.parquet(
        f"{pre_bq_mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw"
    )

    # Load external fact data
    df_mmc_fact = spark.read.parquet(
        f'{pre_bq_mat_path}/{run_id}/load_fact_df_fact_extrn'
    )

    # Load market dimension data
    tier2_mkt_mtrlz_tbl = spark.read.parquet(
        f'{pre_bq_mat_path}/{run_id}/market_transformation_df_mkt_stgng_vw'
    )




    # Create MM_TIER2_TP_VALDN_PROD_DIM_VW

    # Load product dimension data for the given source system ID
    df_mm_prod_dim = spark.sql(
        f"SELECT * FROM {catalog_name}.gold_tp.tp_prod_dim WHERE srce_sys_id = :srce_sys_id",{"srce_sys_id":srce_sys_id}
    )

    # Register as temporary view for SQL operations
    df_mm_prod_dim.createOrReplaceTempView('MM_PROD_DIM')

    # Load contract lookup data for the given contract ID
    df_mm_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    df_mm_cntrt_lkp.createOrReplaceTempView('mm_cntrt_lkp')


    # Load contract-category association data
    object_name=f'{postgres_schema}.mm_cntrt_categ_assoc'
    df_mm_cntrt_categ_assoc=read_from_postgres(object_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)    
    df_mm_cntrt_categ_assoc=df_mm_cntrt_categ_assoc.filter(df_mm_cntrt_categ_assoc['cntrt_id']==cntrt_id)
    df_mm_cntrt_categ_assoc.createOrReplaceTempView('mm_cntrt_categ_assoc')


    # Load run data for the contract
    object_name=f'{postgres_schema}.mm_run_plc'
    df_dpf_run=read_from_postgres(object_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)    
    df_dpf_run=df_dpf_run.filter(df_dpf_run['cntrt_id']==cntrt_id)
    df_dpf_run.createOrReplaceTempView('dpf_run')

    # SQL query to transform and decode product hierarchy levels
    query = """
    WITH product_levels AS (
        SELECT /*+ MATERIALIZE */
            prod_skid, run_id, srce_sys_id,
            CASE 
                WHEN prod_lvl_name IS NULL AND UPPER(priv_label_txt) <> 'YES' THEN 'NoLevelName'
                WHEN prod_lvl_name IS NULL AND UPPER(priv_label_txt) = 'YES' THEN MFGR_NAME_TXT 
                WHEN model_num_txt IS NOT NULL AND prod_lvl_id IS NOT NULL THEN 
                    'Hier no. ' || CAST(prod_lvl_id AS LONG) || ', level no. ' || model_num_txt || ' - ' || prod_lvl_name
                ELSE prod_lvl_name 
            END AS prod_lvl_name,
            pg_categ_txt, categ_txt, long_prod_desc_txt, short_prod_desc_txt,
            CASE 
                WHEN prod_lvl_name LIKE '%!%' THEN LENGTH(prod_lvl_name) - LENGTH(REPLACE(prod_lvl_name,'!')) + 1 
                WHEN prod_lvl_name LIKE '0%' THEN CAST(SUBSTR(prod_lvl_name, 2, 1) AS INTEGER)
                WHEN model_num_txt IS NOT NULL AND prod_lvl_id IS NOT NULL THEN 
                    CAST(CAST(prod_lvl_id AS LONG) || model_num_txt AS INTEGER)
                ELSE 1 -- Top level
            END AS prod_lvl
        FROM MM_PROD_DIM
        WHERE srce_sys_id NOT IN (3, 100)
        AND UPPER(prod_lvl_name) NOT IN ('LEVEL')
        AND UPPER(prod_lvl_name) NOT LIKE ('%DRILL%')
    ),
    decoded_names AS (
        SELECT /*+ MATERIALIZE */
            prod_skid, run_id, srce_sys_id, prod_lvl_name,
            CASE 
                WHEN prod_lvl = 1 THEN NVL(pg_categ_txt, NVL(categ_txt, NVL(long_prod_desc_txt, NVL(short_prod_desc_txt, 'NoProductName'))))
                ELSE NVL(long_prod_desc_txt, NVL(short_prod_desc_txt, 'NoProductName'))
            END AS prod_desc,
            prod_lvl
        FROM product_levels
    )
    SELECT /*+ PARALLEL(8) */
        a.prod_skid, a.run_id, a.srce_sys_id, a.prod_lvl_name, a.prod_desc,
        -- Handle Japan-specific product level naming
        CASE 
            WHEN cntrt_name LIKE 'JAPAN%' THEN
                CASE 
                    WHEN PROD_LVL_NAME = 'CATEGORY' THEN 1 
                    WHEN UPPER(PROD_LVL_NAME) = 'SUB-CATEGORY' THEN 3
                    WHEN UPPER(PROD_LVL_NAME) = 'SEGMENT' THEN 5
                    WHEN UPPER(PROD_LVL_NAME) = 'GENDER' THEN 7
                    WHEN UPPER(PROD_LVL_NAME) = 'MAKER' THEN 9
                    WHEN UPPER(PROD_LVL_NAME) = 'MAKER GROUP' THEN 11
                    WHEN UPPER(PROD_LVL_NAME) = 'MAIN BRAND TOTAL' THEN 13
                    WHEN UPPER(PROD_LVL_NAME) = 'MAIN BRAND TTL' THEN 15
                    WHEN UPPER(PROD_LVL_NAME) = 'BRAND' THEN 17
                    WHEN UPPER(PROD_LVL_NAME) = 'SUB BRAND' THEN 19
                    WHEN UPPER(PROD_LVL_NAME) = 'FORM' THEN 21
                    WHEN UPPER(PROD_LVL_NAME) = 'TYPE' THEN 23
                    WHEN UPPER(PROD_LVL_NAME) = 'SEGMENT+DISPOSABLE TYPE' THEN 25
                    WHEN UPPER(PROD_LVL_NAME) = 'USER' THEN 27
                    WHEN UPPER(PROD_LVL_NAME) = 'TARGET USER' THEN 29
                    WHEN UPPER(PROD_LVL_NAME) = 'CONCEPT' THEN 31
                    WHEN UPPER(PROD_LVL_NAME) = 'HDD_LDD' THEN 33
                    WHEN UPPER(PROD_LVL_NAME) = 'VARIANT' THEN 35
                    WHEN UPPER(PROD_LVL_NAME) = 'FLAVOR' THEN 37
                    WHEN UPPER(PROD_LVL_NAME) = 'COMPACT' THEN 39
                    WHEN UPPER(PROD_LVL_NAME) = 'KOGEN VARIANT' THEN 41
                    WHEN UPPER(PROD_LVL_NAME) = 'MATERIAL' THEN 43
                    WHEN UPPER(PROD_LVL_NAME) = 'PACKAGE TYPE' THEN 45
                    WHEN UPPER(PROD_LVL_NAME) = 'LINE UP' THEN 47
                    WHEN UPPER(PROD_LVL_NAME) = 'PRODUCT TYPE' THEN 49
                    WHEN UPPER(PROD_LVL_NAME) = 'PURPOSE' THEN 51
                    WHEN UPPER(PROD_LVL_NAME) = 'CONCENTRATION' THEN 53
                    WHEN UPPER(PROD_LVL_NAME) = 'BENEFIT DIRECTION' THEN 55
                    WHEN UPPER(PROD_LVL_NAME) = 'PACKAGE' THEN 57
                    WHEN UPPER(PROD_LVL_NAME) = 'PACK SIZE' OR UPPER(PROD_LVL_NAME) = 'PACKSIZE' THEN 59
                    WHEN UPPER(PROD_LVL_NAME) = 'LIQUID SIZE' THEN 61
                    WHEN UPPER(PROD_LVL_NAME) = 'SIZE' THEN 63
                    WHEN UPPER(PROD_LVL_NAME) = 'BASIC SIZE' THEN 65
                    WHEN UPPER(PROD_LVL_NAME) = 'SKU' THEN 67
                    ELSE 99 
                END
            ELSE a.prod_lvl 
        END AS prod_lvl,
        CASE 
            WHEN a.run_id = last_run.run_id THEN 'Y' 
            ELSE 'N' 
        END AS last_run_ind,
        b.cntrt_id, b.cntrt_code
    FROM decoded_names a,
        mm_cntrt_lkp b,
        mm_cntrt_categ_assoc c,
        (SELECT MAX(run_id) run_id, cntrt_id FROM dpf_run GROUP BY cntrt_id) last_run
    WHERE last_run.cntrt_id = b.cntrt_id
        AND a.srce_sys_id = b.srce_sys_id
        AND b.cntrt_id = c.cntrt_id
    """

    # Execute the final query and create the product dimension validation view
    df_TP_VALDN_PROD_DIM_VW = spark.sql(query)

    logger.info("TP_VALDN_PROD_DIM_VW")

    # Display the result
    df_TP_VALDN_PROD_DIM_VW.show()




    # Load selected columns from product data for the given source system ID
    df_xref = spark.sql(f"""
        SELECT 
            extrn_prod_id,
            extrn_prod_attr_val_list,
            prod_match_attr_list,
            prod_skid,
            srce_sys_id,
            cntrt_id,
            part_srce_sys_id
        FROM {catalog_name}.internal_tp.tp_prod_sdim 
        WHERE srce_sys_id = :srce_sys_id""",{"srce_sys_id":srce_sys_id}
        )


    # Load run partition policy data for the given contract ID
    df_run_prttn_plc = spark.sql(f"""
        SELECT * 
        FROM {catalog_name}.gold_tp.tp_run_prttn_plc 
        WHERE cntrt_id = :cntrt_id""",{"cntrt_id":cntrt_id}
    )


    # Set New Time Period Flag
    # Step 1

    # Utility function to convert all column names to lowercase
    def convert_cols_to_lower(df):
        cols = df.columns
        cols_lower = [col.lower() for col in cols]
        df = df.select(*cols_lower)
        return df
    
    # Get Product Structure Setup of Business Checks

    publish_valdn_run_strct(run_id, cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)


    # Get Product Structure Level Setup of Checks

    df4=publish_valdn_run_strct_lvl(cntrt_id, run_id, srce_sys_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    
    # Get Products for Business Checks

    df_xref = convert_cols_to_lower(df_xref)
    df_xref.createOrReplaceTempView('df_xref')

    strct_lvl_id_list = [row.strct_lvl_id for row in df4.select('strct_lvl_id').collect()]

    df_prod_promo = df_prod_promo_vw
    df_prod_promo.createOrReplaceTempView('df_prod_promo')

    df_tier2_vldn = df_TP_VALDN_PROD_DIM_VW
    df_tier2_vldn.createOrReplaceTempView('df_tier2_vldn')

    query = '''
        SELECT 
            df_prod_promo.*, 
            df_tier2_vldn.prod_lvl, 
            COALESCE(df_prod_promo.short_prod_desc_txt, COALESCE(df_prod_promo.long_prod_desc_txt, df_xref.extrn_prod_attr_val_list)) AS prod_attr_val_name
        FROM df_prod_promo 
        INNER JOIN df_tier2_vldn 
            ON df_prod_promo.srce_sys_id = df_tier2_vldn.srce_sys_id 
            AND df_prod_promo.prod_skid = df_tier2_vldn.prod_skid
        INNER JOIN df_xref 
            ON df_prod_promo.prod_skid = df_xref.prod_skid
        '''

    df_dqm_prod = spark.sql(query)
    ###################################################################################################################
    df_dqm_prod = df_dqm_prod.filter(col('prod_lvl').isin(strct_lvl_id_list))
    
    df_dqm_prod = df_dqm_prod.withColumn('secure_group_key',lit(0))
    # Prepare Validation Products

    df_dqm_prod = add_secure_group_key(df_dqm_prod, cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    df_tp_valdn_prod_dim_schema=spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.TP_VALDN_PROD_DIM LIMIT 0")

    df6 = column_complementer(df_dqm_prod,df_tp_valdn_prod_dim_schema)


    dim_type='TP_VALDN_PROD_DIM'
    tp_valdn_prod_dim_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)
    # Print the constructed path for verification
    logger.info(f"tp_valdn_prod_dim_path: {tp_valdn_prod_dim_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_VALDN_PROD_DIM")
    tp_valdn_prod_dim_check_path = semaphore_acquisition(run_id, tp_valdn_prod_dim_path,catalog_name, spark)
    try:
        # Publish Validation Reports
        safe_delete_with_retry(spark,f"{catalog_name}.gold_tp.tp_valdn_prod_dim",run_id,cntrt_id,srce_sys_id)
        
        logger.info(f"Started Writing to tp_valdn_prod_dim")
        safe_write_with_retry(df6, f'{catalog_name}.gold_tp.tp_valdn_prod_dim', 'append', partition_by=['srce_sys_id','cntrt_id'])
        
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,run_id,tp_valdn_prod_dim_check_path , spark)
        logger.info("TP_VALDN_PROD_DIM Semaphore released")

    logger.info("TP_VALDN_PROD_DIM")

    df6.show()

    # ------------------------------------------------------------
    # Get Market Top Nodes
    df_DQM_CNTRT_MKT=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    df_DQM_CNTRT_MKT=df_DQM_CNTRT_MKT.select('cntrt_id','mkt_top_node_skid')
    logger.info("The top market node skid is")
    df_DQM_CNTRT_MKT.show()

    df_DQM_CNTRT_MKT = convert_cols_to_lower(df_DQM_CNTRT_MKT)

    # ------------------------------------------------------------
    # Get All the Monthly Time Periods

    df_mm_run_prttn_run_sttus_vw=run_prttn_dtls(cntrt_id, catalog_name, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    df7 = df_mm_run_prttn_run_sttus_vw.filter(
        f"cntrt_id = cntrt_id AND (run_id = {run_id} OR (run_id <> {run_id} AND run_sttus_name = 'SUCCESS'))"
    )
    df7 = df7.withColumn('run_id', col('run_id').cast('int'))

    df8 = df7.groupBy(
        'mm_time_perd_end_date', 'cntrt_id', 'time_perd_class_code', 'srce_sys_id'
    ).agg(
        max('run_id').alias('run_id'),
        count('run_id').alias('cnt')
    )


    
    # Set New Time Period Flag

    df_new_time_pds = df8.withColumn('new_time_perd_ind', when(col('cnt') == 1, lit('Y')).otherwise(lit('N'))) \
                     .withColumn('pd_time_perd_ind', when(col('cnt') > 1, lit('Y')).otherwise(lit('N'))) \
                     .withColumn('time_factr', lit(1))
    
    logger.info("New Time Periods")

    df_new_time_pds.show()

    # ------------------------------------------------------------
    # Get Fact Data for Previously Delivered Time Periods

    df_new_pds = df_new_time_pds.filter("new_time_perd_ind = 'N'")


    # Register DataFrames as temporary views for SQL queries
    df_dqm_prod.createOrReplaceTempView('df_dqm_prod')
    df_fact_promo_img.createOrReplaceTempView('df_fact_promo_img')
    df_new_pds.createOrReplaceTempView('df_new_pds')

    # Load monthly fact data for the given contract ID
    df_mth_fct = spark.sql(f"""
    SELECT * 
    FROM {catalog_name}.gold_tp.tp_mth_fct 
    WHERE cntrt_id = :cntrt_id """,{"cntrt_id":cntrt_id}
    )
    df_mth_fct.createOrReplaceTempView('df_mth_fct')

    # Register the updated contract-market mapping as a view
    df_DQM_CNTRT_MKT.createOrReplaceTempView('df_DQM_CNTRT_MKT')


    # Get fact data previously delivered Time periods
    # Step 2

    # Load monthly fact data based on time period class code
    class_code = match_time_perd_class(time_perd_class_code.lower())
    df_mth_fct = spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.tp_{class_code}_fct")

    # Convert column names to lowercase
    df_mth_fct = convert_cols_to_lower(df_mth_fct)
    df_new_pds = convert_cols_to_lower(df_new_pds)
    df_DQM_CNTRT_MKT = convert_cols_to_lower(df_DQM_CNTRT_MKT)

    # Register DataFrames as temporary views for SQL operations
    df_dqm_prod.createOrReplaceTempView('df_dqm_prod')
    df_fact_promo_img.createOrReplaceTempView('df_fact_promo_img')
    df_new_pds.createOrReplaceTempView('df_new_pds')
    df_mth_fct.createOrReplaceTempView('df_mth_fct')
    df_DQM_CNTRT_MKT.createOrReplaceTempView('df_DQM_CNTRT_MKT')

    # SQL query to join product, fact, and time period data
    query = '''
        SELECT 
            a.* 
            EXCEPT (
                srce_sys_id, cntrt_id, extrn_prod_id, prod_10_txt, prod_11_txt, prod_12_txt, 
                prod_13_txt, prod_14_txt, prod_15_txt, prod_16_txt, task_txt, last_sellg_txt, 
                pg_last_sellg_txt, face_appl_txt, pg_face_appl_txt, part_srce_sys_id, 
                part_cntrt_id, extrn_prod_attr_val_list, prod_match_attr_list, prod_attr_val_name
            ),
            b.sales_msu_qty, b.sales_mlc_amt, b.sales_musd_amt, b.mkt_skid, b.time_perd_id, 
            b.mm_time_perd_end_date, b.srce_sys_id, b.cntrt_id,
            c.new_time_perd_ind, c.time_factr,
            'Y' AS pd_time_perd_ind,
            CASE WHEN 'Y' = 'Y' THEN d.sales_msu_qty END AS sales_msu_qty_pd,
            CASE WHEN 'Y' = 'Y' THEN d.sales_mlc_amt END AS sales_mlc_amt_pd,
            CASE WHEN 'Y' = 'Y' THEN d.sales_musd_amt END AS sales_musd_amt_pd
        FROM df_dqm_prod AS a
        JOIN df_fact_promo_img AS b
            ON a.prod_skid = b.prod_skid
        JOIN df_new_pds AS c
            ON b.mm_time_perd_end_date = c.mm_time_perd_end_date
            AND b.srce_sys_id = c.srce_sys_id
            AND b.cntrt_id = c.cntrt_id
        JOIN df_mth_fct AS d
            ON c.mm_time_perd_end_date = d.mm_time_perd_end_date
            AND c.srce_sys_id = d.srce_sys_id
            AND c.cntrt_id = d.cntrt_id
            AND a.prod_skid = d.prod_skid
        JOIN df_DQM_CNTRT_MKT AS e
            ON b.mkt_skid = e.mkt_top_node_skid
            AND d.mkt_skid = e.mkt_top_node_skid
    '''

    # Execute the query and display the result
    df_curr_fct = spark.sql(query)

    logger.info("Fetched fact data for previously delivered time periods")

    # Get fact data for newly delivered time periods
    # Step 3:

    # Convert column names to lowercase for consistency
    df_new_time_pds = convert_cols_to_lower(df_new_time_pds)
    df_dqm_prod = convert_cols_to_lower(df_dqm_prod)
    df_DQM_CNTRT_MKT = convert_cols_to_lower(df_DQM_CNTRT_MKT)

    # Separate new and previously delivered time periods
    df_TIME_PERD_NEW = df_new_time_pds.filter("new_time_perd_ind = 'Y'")
    df_TIME_PERD_PD = df_new_time_pds.filter("new_time_perd_ind = 'N'")

    # Register DataFrames as temporary views for SQL operations
    df_fact_promo_img.createOrReplaceTempView('df_fact_promo_img')
    df_dqm_prod.createOrReplaceTempView('df_dqm_prod')
    df_new_time_pds.createOrReplaceTempView('df_new_time_pds')
    df_DQM_CNTRT_MKT.createOrReplaceTempView('df_DQM_CNTRT_MKT')
    df_TIME_PERD_NEW.createOrReplaceTempView('df_TIME_PERD_NEW')
    df_TIME_PERD_PD.createOrReplaceTempView('df_TIME_PERD_PD')

    # -------------------------------------------------------
    # SQL query to join product and fact data with time period flags

    query = """ 
        SELECT
            a.* 
            EXCEPT (
                extrn_prod_id, 
                --match_lvl_code, extrn_prod_seq_txt, pg_sub_sectr_txt, parnt_prod_skid, 
                prod_10_txt, prod_11_txt, prod_12_txt, prod_13_txt, 
                prod_14_txt, prod_15_txt, prod_16_txt, task_txt, last_sellg_txt, 
                pg_last_sellg_txt, face_appl_txt, pg_face_appl_txt, part_srce_sys_id, 
                part_cntrt_id, extrn_prod_attr_val_list, prod_match_attr_list, prod_attr_val_name
            ),

            b.sales_msu_qty, b.sales_mlc_amt, b.sales_musd_amt, b.mkt_skid, 
            b.time_perd_id, b.mm_time_perd_end_date,

            COALESCE(c.new_time_perd_ind, 'N') AS new_time_perd_ind,
            c.time_factr,

            d.time_factr AS time_factr_pd,
            d.srce_sys_id AS srce_sys_id_pd

        FROM df_dqm_prod a
        JOIN df_fact_promo_img b
            ON a.prod_skid = b.prod_skid  

        LEFT JOIN df_TIME_PERD_NEW c
            ON b.mm_time_perd_end_date = c.mm_time_perd_end_date
            AND b.srce_sys_id = c.srce_sys_id
            AND b.cntrt_id = c.cntrt_id

        LEFT JOIN df_TIME_PERD_PD d
            ON b.mm_time_perd_end_date = d.mm_time_perd_end_date
            AND b.srce_sys_id = d.srce_sys_id
            AND b.cntrt_id = d.cntrt_id

        JOIN df_DQM_CNTRT_MKT e
            ON b.mkt_skid = e.mkt_top_node_skid
    """

    logger.info("Fetched fact data for newly delivered time periods")

    # Execute query and apply final transformations
    df_new_del_time_pd = spark.sql(query) \
        .withColumn('TIME_FACTR', expr("nvl(TIME_FACTR, time_factr_pd)")) \
        .withColumn('pd_time_perd_ind', when(col('srce_sys_id_pd').isNotNull(), lit('Y')).otherwise(lit('N'))) \
        .filter("pd_time_perd_ind = 'N'") \
        .drop('time_factr_pd', 'srce_sys_id_pd')

    # Display the final DataFrame
    # display(df_new_del_time_pd)



    # Union current fact data with newly delivered time period data
    df_dqm_fct = df_curr_fct.unionByName(df_new_del_time_pd, True)  # DPF_00150502_DQM_FCT

  
    # Calculate aggregated metrics at the market and time period level

    df_dqm_fct.createOrReplaceTempView('DQM_FCT')

    sql1 = """
        SELECT  
            MKT_SKID,
            TIME_PERD_ID,
            SUM(SALES_MSU_QTY) AS SALES_MSU_QTY,
            SUM(SALES_MLC_AMT) AS SALES_MLC_AMT,
            SUM(SALES_MSU_QTY_PD) AS SALES_MSU_QTY_PD,
            SUM(SALES_MLC_AMT_PD) AS SALES_MLC_AMT_PD
        FROM DQM_FCT
        GROUP BY MKT_SKID, TIME_PERD_ID
    """

    # Execute the aggregation query
    df_top_node = spark.sql(sql1)

    # Display the aggregated result
    # display(df_top_node)



    # Load the time period dimension data 
    df_time_pd=spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.tp_time_perd_fdim")


    # Calculate YA - Year Ago - PP - Previous Period metrics
    # Step 5:

    # Convert column names to lowercase for consistency
    df_dqm_fct = convert_cols_to_lower(df_dqm_fct)
    df_top_node = convert_cols_to_lower(df_top_node)

    # Create YA (Year Ago) and PP (Previous Period) datasets by renaming relevant columns
    df_fct_ya = df_dqm_fct \
        .withColumnRenamed('mkt_skid', 'mkt_skid_ya') \
        .withColumnRenamed('time_perd_id', 'time_perd_id_ya1') \
        .withColumnRenamed('prod_skid', 'prod_skid_ya') \
        .withColumnRenamed('sales_msu_qty', 'sales_msu_qty_ya') \
        .withColumnRenamed('sales_mlc_amt', 'sales_mlc_amt_ya') \
        .withColumnRenamed('sales_musd_amt', 'sales_musd_amt_ya')

    df_fct_pp = df_dqm_fct.filter("1=1") \
        .withColumnRenamed('mkt_skid', 'mkt_skid_pp') \
        .withColumnRenamed('time_perd_id', 'time_perd_id_pp1') \
        .withColumnRenamed('prod_skid', 'prod_skid_pp') \
        .withColumnRenamed('sales_msu_qty', 'sales_msu_qty_pp') \
        .withColumnRenamed('sales_mlc_amt', 'sales_mlc_amt_pp') \
        .withColumnRenamed('sales_musd_amt', 'sales_musd_amt_pp') \
        .select('mkt_skid_pp', 'time_perd_id_pp1', 'prod_skid_pp', 'sales_msu_qty_pp', 'sales_mlc_amt_pp', 'sales_musd_amt_pp')

    # Register all DataFrames as temporary views
    df_dqm_fct.createOrReplaceTempView('df_dqm_fct')
    df_top_node.createOrReplaceTempView('df_top_node')
    df_time_pd.createOrReplaceTempView('df_time_pd')
    df_fct_ya.createOrReplaceTempView('df_fct_ya')
    df_fct_pp.createOrReplaceTempView('df_fct_pp')

    # SQL query to calculate share percentages and join with YA and PP data
    query = '''
    SELECT
        CASE 
        WHEN b.sales_msu_qty != 0 THEN a.sales_msu_qty / b.sales_msu_qty * 100
        END AS share_pu_pct,

        CASE 
        WHEN b.sales_mlc_amt != 0 THEN a.sales_mlc_amt / b.sales_mlc_amt * 100
        END AS share_lc_pct,

        CASE 
        WHEN b.sales_msu_qty_pd != 0 THEN a.sales_msu_qty_pd / b.sales_msu_qty_pd * 100
        END AS share_su_pct_pd,

        CASE 
        WHEN b.sales_mlc_amt_pd != 0 THEN a.sales_mlc_amt_pd / b.sales_mlc_amt_pd * 100
        END AS share_lc_pct_pd,

        a.*,
        y.sales_msu_qty_ya, y.sales_mlc_amt_ya, y.sales_musd_amt_ya,
        p.sales_msu_qty_pp, p.sales_mlc_amt_pp, p.sales_musd_amt_pp

    FROM df_dqm_fct a
    LEFT OUTER JOIN df_top_node b
        ON a.mkt_skid = b.mkt_skid AND a.time_perd_id = b.time_perd_id
    LEFT OUTER JOIN df_time_pd t
        ON a.time_perd_id = t.time_perd_id
    LEFT OUTER JOIN df_fct_ya y
        ON t.time_perd_id_ya = y.time_perd_id_ya1
        AND a.prod_skid = y.prod_skid_ya
        AND a.mkt_skid = y.mkt_skid_ya
    LEFT OUTER JOIN df_fct_pp p
        ON t.time_perd_id_prev = p.time_perd_id_pp1
        AND a.prod_skid = p.prod_skid_pp
        AND a.mkt_skid = p.mkt_skid_pp
    '''

    # Execute the query and convert column names to lowercase
    df_calc_ya_pp = spark.sql(query)
    df_calc_ya_pp = convert_cols_to_lower(df_calc_ya_pp)

    logger.info("Calculated share percentages ")
    # Display the final result
    df_calc_ya_pp.show()



    # Calculate Indexes (IPP, IYA, IPD) for Sales Metrics
    # Step 6:

    # Calculate index metrics for sales quantity and amount
    df_calc_index = df_calc_ya_pp \
        .withColumn('IPP_SU_PCT', when(
            col('SALES_MSU_QTY_PP') != 0,
            (col('SALES_MSU_QTY') * col('TIME_FACTR') - col('SALES_MSU_QTY_PP')) / col('SALES_MSU_QTY_PP') * 100
        )) \
        .withColumn('IYA_SU_PCT', when(
            col('SALES_MSU_QTY_YA') != 0,
            (col('SALES_MSU_QTY') * col('TIME_FACTR') - col('SALES_MSU_QTY_YA')) / col('SALES_MSU_QTY_YA') * 100
        )) \
        .withColumn('IPD_SU_PCT', when(
            col('SALES_MSU_QTY_PD') != 0,
            (col('SALES_MSU_QTY') - col('SALES_MSU_QTY_PD')) / col('SALES_MSU_QTY_PD') * 100
        )) \
        .withColumn('IPP_LC_PCT', when(
            col('SALES_MLC_AMT_PP') != 0,
            (col('SALES_MLC_AMT') * col('TIME_FACTR') - col('SALES_MLC_AMT_PP')) / col('SALES_MLC_AMT_PP') * 100
        )) \
        .withColumn('IYA_LC_PCT', when(
            col('SALES_MLC_AMT_YA') != 0,
            (col('SALES_MLC_AMT') * col('TIME_FACTR') - col('SALES_MLC_AMT_YA')) / col('SALES_MLC_AMT_YA') * 100
        )) \
        .withColumn('IPD_LC_PCT', when(
            col('SALES_MLC_AMT_PD') != 0,
            (col('SALES_MLC_AMT') - col('SALES_MLC_AMT_PD')) / col('SALES_MLC_AMT_PD') * 100
        ))

    logger.info("Calculate Indexes (IPP, IYA, IPD)")
    df_calc_index.show()

    dim_type='TP_VALDN_PROD_DIM'
    tp_valdn_prod_dim_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)
    # Print the constructed path for verification
    logger.info(f"tp_valdn_prod_dim_path: {tp_valdn_prod_dim_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_VALDN_PROD_DIM")
    tp_valdn_prod_dim_check_path = semaphore_acquisition(run_id, tp_valdn_prod_dim_path,catalog_name, spark)
    try:
        # Publish Validation Reports
        safe_delete_with_retry(spark,f"{catalog_name}.gold_tp.tp_valdn_prod_dim",run_id,cntrt_id,srce_sys_id)
        
    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,run_id,tp_valdn_prod_dim_check_path , spark)
        logger.info("TP_VALDN_PROD_DIM Semaphore released")

    # Convert column names to lowercase for consistency
    df_calc_index = convert_cols_to_lower(df_calc_index)
    df_calc_index.write.mode('overwrite').format('parquet').save(f'{pre_bq_mat_path}/{run_id}/DQ_Pre_Business_Validation_df_calc_index')



    #Standardize MM_TP_VALDN_AGG_FCT
    publish_valdn_agg_fct(df_calc_index,cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd,srce_sys_id,run_id)
