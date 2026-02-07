from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params, load_cntrt_lkp, run_prttn_dtls, publish_valdn_run_strct_lvl, time_perd_class_codes, publish_valdn_run_strct, publish_valdn_agg_fct, column_complementer, add_secure_group_key, get_database_config,sanitize_variable,match_time_perd_class,materialise_path,semaphore_generate_path,semaphore_acquisition,release_semaphore,safe_write_with_retry
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

    cntrt_id = sanitize_variable(cntrt_id)
    run_id = sanitize_variable(run_id)
    postgres_schema = sanitize_variable(postgres_schema)
    catalog_name = sanitize_variable(catalog_name)
    # Fetch source system ID and time period type code for the given contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )

    # Fetch source system ID and time period type code for the given contract ID
    srce_sys_id=df_cntrt_lkp.select('srce_sys_id').collect()[0].srce_sys_id
    time_perd_type_code=df_cntrt_lkp.select('time_perd_type_code').collect()[0].time_perd_type_code

    # Determine the time period class code based on the type code
    time_perd_class_code=time_perd_class_codes(cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)


    mat_path = materialise_path(spark)
    class_code = match_time_perd_class(time_perd_class_code.lower())

    df_fct_schema =spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.tp_{class_code}_fct LIMIT 0')


    df_fct_MTH_img=spark.read.parquet(f"{mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw")
    df_fct_MTH_img.createOrReplaceTempView('df_fct_MTH_img')


    df_prod_nav = spark.read.parquet(f"{mat_path}/{run_id}/Product_Derivation_df_prod_nav")
    df_prod_nav.createOrReplaceTempView('mm_prod_nav')

    
    df_dqm_cntrt_mkt=df_cntrt_lkp.select(col('mkt_top_node_skid'))
    df_dqm_cntrt_mkt.createOrReplaceTempView('df_dqm_cntrt_mkt')

    publish_valdn_run_strct(run_id, cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)


    df_dqm_dvm_rslp=publish_valdn_run_strct_lvl(cntrt_id, run_id, srce_sys_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)


    df_dqm_dvm_rslp2=df_dqm_dvm_rslp.filter(col('top_lvl_ind')=='Y')
    df_dqm_dvm_rslp2.createOrReplaceTempView('mm_dqm_dvm_rslp2')
    
    df_dqm_dvm_rslp2.show(5)

    df_mm_prod_cxref=spark.sql(f'SELECT extrn_prod_id,  extrn_prod_attr_val_list, prod_match_attr_list, prod_skid, srce_sys_id, cntrt_id, part_srce_sys_id FROM {catalog_name}.internal_tp.tp_prod_sdim WHERE part_cntrt_id=:cntrt_id',{'cntrt_id':cntrt_id})

    df_mm_prod_cxref.createOrReplaceTempView('mm_prod_cxref')


    query='''
    SELECT  
    inp.* except(extrn_prod_attr_val_list), 
    lvl.top_lvl_ind as top_lvl_ind, lvl.lvl_num as lvl_num, lvl.parnt_lvl_num as parnt_lvl_num, 
    top_lvl.lvl_num as lvl_num_2, 
    xref.extrn_prod_attr_val_list as extrn_prod_attr_val_list 
    FROM mm_prod_nav AS inp 
    INNER JOIN mm_dqm_dvm_rslp AS lvl ON inp.prod_lvl_id = lvl.strct_lvl_id 
    INNER JOIN mm_dqm_dvm_rslp2 AS top_lvl ON top_lvl.cntrt_id = inp.cntrt_id 
    INNER JOIN mm_prod_cxref AS xref ON xref.prod_skid=inp.prod_skid
    '''

    df_dqm_prod=spark.sql(query)
    df_dqm_prod.show(5)
    df_dqm_prod.createOrReplaceTempView('mm_dqm_prod')


    query='''
    SELECT 
    case when top_lvl_ind = 'N' then substring_index(extrn_prod_attr_val_list, ' ',  ((lvl_num+1)-(lvl_num - lvl_num_2))  ) else extrn_prod_attr_val_list end as top_lvl_prod_attr_val_list,
    row_number()over(partition by extrn_prod_attr_val_list order by run_id desc) as rn,
    substring_index(extrn_prod_attr_val_list, ' ', ((lvl_num+1) - (lvl_num - parnt_lvl_num))) as parnt_extrn_prod_attr_val_list,
    *
    FROM mm_dqm_prod 
    QUALIFY rn=1
    '''
    df_dqm_prod=spark.sql(query)
    df_dqm_prod.show(5)

    df_dqm_prod.createOrReplaceTempView('mm_dqm_prod')

    query='''
    SELECT  inp.*, ref.prod_skid as parnt_prod_skid FROM mm_dqm_prod inp LEFT OUTER JOIN mm_dqm_prod ref ON ref.parnt_extrn_prod_attr_val_list = inp.extrn_prod_attr_val_list
    '''
    df_dqm_prod_assoc=spark.sql(query)
    df_dqm_prod_assoc.createOrReplaceTempView('mm_dqm_prod_assoc')


    query='''
    SELECT nvl(parnt_prod_skid,prod_skid) as parnt_prod_skid, attr_val as prod_attr_val_name, * EXCEPT(parnt_prod_skid) FROM mm_dqm_prod_assoc
    '''
    df_dqm_pre_prod_pub=spark.sql(query)
    df_dqm_pre_prod_pub.createOrReplaceTempView('df_dqm_pre_prod_pub')

    
    df_dqm_pre_prod_pub.show(5)


    df_tp_valdn_prod_dim=spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.TP_VALDN_PROD_DIM where srce_sys_id=:srce_sys_id and cntrt_id=:cntrt_id',{"srce_sys_id":srce_sys_id,"cntrt_id":cntrt_id})
    df_tp_valdn_prod_dim.createOrReplaceTempView('df_tp_valdn_prod_dim')

    query=f'''
        SELECT * 
        FROM df_dqm_pre_prod_pub AS inp 
        LEFT ANTI JOIN (select * from df_tp_valdn_prod_dim) AS ref ON ref.run_id = inp.run_id
    '''
    df_dqm_pre_prod_pub_1=spark.sql(query)
    df_dqm_pre_prod_pub_1.show(5)
    df_dqm_pre_prod_pub_1=df_dqm_pre_prod_pub_1.withColumn('secure_group_key',lit(0))
    df_dqm_pre_prod_pub_1 = add_secure_group_key(df_dqm_pre_prod_pub_1, cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_dqm_pre_prod_pub_1=column_complementer(df_dqm_pre_prod_pub_1,df_tp_valdn_prod_dim)
    df_dqm_pre_prod_pub_1.show(5)

    dim_type='TP_VALDN_PROD_DIM'
    tp_valdn_prod_dim_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)
    # Print the constructed path for verification
    logger.info(f"tp_valdn_prod_dim_path: {tp_valdn_prod_dim_path}")
    # Acquire semaphore for writing to the specified path
    logger.info("Acquiring semaphore for TP_VALDN_PROD_DIM")
    tp_valdn_prod_dim_check_path = semaphore_acquisition(run_id, tp_valdn_prod_dim_path,catalog_name, spark)
    try:
        logger.info(f"Started Writing to tp_valdn_prod_dim")
        safe_write_with_retry(df_dqm_pre_prod_pub_1, f'{catalog_name}.gold_tp.tp_valdn_prod_dim', 'append', partition_by=['srce_sys_id','cntrt_id'])

    finally:
        # Release the semaphore for the current run
        release_semaphore(catalog_name,run_id,tp_valdn_prod_dim_check_path , spark)
        logger.info("TP_VALDN_PROD_DIM Semaphore released")

    df_run_prttn_run_sttus_vw=run_prttn_dtls(cntrt_id, catalog_name, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    df_run_prttn_run_sttus_vw = df_run_prttn_run_sttus_vw.filter((col('cntrt_id') == cntrt_id) &(col('run_id') == run_id) &(col('run_sttus_name') == 'SUCCESS') &(col('time_perd_class_code') == time_perd_class_code))
    df_run_prttn_run_sttus_vw.show(5)

    df_mm_tp_mth_fct=spark.sql(f'''SELECT * FROM {catalog_name}.gold_tp.tp_{class_code}_fct WHERE part_cntrt_id=:cntrt_id''',{"cntrt_id":cntrt_id})
    
    df_mm_tp_mth_fct.createOrReplaceTempView('df_mm_tp_mth_fct')

    df_mm_tp_mth_fct2=df_mm_tp_mth_fct.select('srce_sys_id', 'cntrt_id', 'mm_time_perd_end_date', 'mkt_skid', 'run_id').distinct() 
    

    df_fct_MTH_img1=df_fct_MTH_img.select('srce_sys_id','cntrt_id','mm_time_perd_end_date','mkt_skid','run_id')
    

    df_all_fct_mkt_list=df_fct_MTH_img1.unionByName(df_mm_tp_mth_fct2)
    df_all_fct_mkt_list.createOrReplaceTempView('df_all_fct_mkt_list')
    


    df_mm_run_prttn_plc=spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.tp_run_prttn_plc where cntrt_id= :cntrt_id',{"cntrt_id":cntrt_id})
    df_mm_run_prttn_plc.createOrReplaceTempView('df_mm_run_prttn_plc')


    query='''
    SELECT DISTINCT(*) 
        FROM (
        SELECT 
        inp.mm_time_perd_end_date, inp.cntrt_id, inp.time_perd_class_code, inp.srce_sys_id, inp.run_id, 
        ref.mkt_skid 
        FROM df_mm_run_prttn_plc  AS inp 
        INNER JOIN df_all_fct_mkt_list AS ref 
        ON ref.mm_time_perd_end_date = inp.mm_time_perd_end_date and ref.cntrt_id = inp.cntrt_id)
        WHERE mm_time_perd_end_date >= ADD_MONTHS(TRUNC(current_date(),'MM'), -62)
        '''
    df_dqm_all_time_perd=spark.sql(query)
    df_dqm_all_time_perd.createOrReplaceTempView('df_dqm_all_time_perd')
    df_dqm_all_time_perd.show(5)


    # Calculate rmax run_id and time_perd
    query='''
        SELECT 
            mm_time_perd_end_date, cntrt_id, time_perd_class_code, srce_sys_id, mkt_skid, 
            max(run_id) AS run_id, count(*) AS cnt 
        FROM df_dqm_all_time_perd
        GROUP BY 
            mm_time_perd_end_date, cntrt_id, time_perd_class_code, srce_sys_id, mkt_skid
        '''
    df_dqm_time_perd_cnt=spark.sql(query)
    df_dqm_time_perd_cnt.show(5)


    time_perd_assoc=spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.tp_time_perd_assoc')
    time_perd_fdim=spark.sql(f'SELECT time_perd_id_ya,time_perd_id_prev,* FROM {catalog_name}.gold_tp.tp_time_perd_fdim')
    time_perd_fdim.show(5)


    df_mm_run_prttn_plc.createOrReplaceTempView('run_prttn_plc')

    time_perd_assoc.createOrReplaceTempView('MM_TIME_PERD_ASSOC_VW')

    time_perd_fdim.createOrReplaceTempView('MM_TIME_PERD_FDIM_VW')

    df_dqm_time_perd_cnt.createOrReplaceTempView('TIME_PERD_CNT')

    query = f""" select
    (case when run_id = :run_id and cnt=1   then 'Y' else 'N' end ) new_time_perd_ind,
    (case when run_id = :run_id and cnt>1   then 'Y' else 'N' end ) pd_time_perd_ind,

    nvl((case when (select count(*) from run_prttn_plc where run_id = :run_id and lower(time_perd_class_code) = 'wk') > 0
    then (case when (select count (*)
    from (select a.time_perd_id time_perd_id_a, a.TIME_PERD_END_DATE TIME_PERD_END_DATE_A, b.time_perd_id time_perd_id_b from
    MM_TIME_PERD_ASSOC_VW t,
    MM_TIME_PERD_FDIM_VW a, 
    MM_TIME_PERD_FDIM_VW b
    where t.time_perd_id_a = a.time_perd_id
    and t.time_perd_id_b = b.time_perd_id
    and a.time_perd_type_code = 'MH'
    and b.time_perd_type_code = 'WKMS')

    where TIME_PERD_END_DATE_A = mm_time_perd_end_date) = 5 then 4/5 else 1 end)
    else 1 end ),(1)) time_factr,
    mm_time_perd_end_date,cntrt_id,time_perd_class_code,srce_sys_id,mkt_skid,run_id,cnt
    from TIME_PERD_CNT"""

    df_dqm_new_time_perd = spark.sql(query,{"run_id":run_id})

    df_dqm_new_time_perd.show(5)


    # filter time_perd
    df_dqm_new_time_perd_n=df_dqm_new_time_perd.filter(col('new_time_perd_ind')=='N')
    df_dqm_new_time_perd_n.show(5)
    df_dqm_new_time_perd_n.createOrReplaceTempView('df_dqm_new_time_perd_n')


    # get fact data for previously delivered time periods
    query='''
    SELECT 
    inp.* ,
    curr_fct.sales_msu_qty, curr_fct.sales_mlc_amt, curr_fct.sales_musd_amt, curr_fct.mkt_skid, curr_fct.time_perd_id, curr_fct.prod_skid AS prod_skid_2, curr_fct.srce_sys_id AS srce_sys_id_2, curr_fct.cntrt_id AS cntrt_id_2,
    time_perd.new_time_perd_ind, time_perd.mm_time_perd_end_date AS mm_time_perd_end_date_3, time_perd.srce_sys_id AS srce_sys_id_3, time_perd.cntrt_id AS cntrt_id_3, time_perd.mkt_skid AS mkt_skid_3, time_perd.time_factr,
    cntrt.mkt_top_node_skid,
    prev_fct.mm_time_perd_end_date AS mm_time_perd_end_date_4, prev_fct.sales_msu_qty AS sales_msu_qty_1, prev_fct.sales_mlc_amt AS sales_mlc_amt_1, prev_fct.sales_musd_amt AS sales_musd_amt_1, prev_fct.mkt_skid AS mkt_skid_4, prev_fct.srce_sys_id AS srce_sys_id_4, prev_fct.cntrt_id AS cntrt_id_4, prev_fct.prod_skid AS prod_skid_4
    FROM mm_dqm_prod inp 
    INNER JOIN df_fct_MTH_img AS curr_fct ON inp.prod_skid = curr_fct.prod_skid
    INNER JOIN df_dqm_new_time_perd_n AS time_perd ON curr_fct.mm_time_perd_end_date = time_perd.mm_time_perd_end_date and curr_fct.srce_sys_id = time_perd.srce_sys_id and curr_fct.cntrt_id = time_perd.cntrt_id 
    INNER JOIN df_dqm_cntrt_mkt AS cntrt ON curr_fct.mkt_skid = cntrt.mkt_top_node_skid and time_perd.mkt_skid = cntrt.mkt_top_node_skid 
    LEFT OUTER JOIN df_mm_tp_mth_fct AS prev_fct ON cntrt.mkt_top_node_skid = prev_fct.mkt_skid and time_perd.mm_time_perd_end_date = prev_fct.mm_time_perd_end_date and time_perd.srce_sys_id = prev_fct.srce_sys_id and time_perd.cntrt_id = prev_fct.cntrt_id and inp.prod_skid = prev_fct.prod_skid
    '''

    df_dqm_fct=spark.sql(query)
    df_dqm_fct.show(5)


    df_dqm_fct=df_dqm_fct.withColumn('pd_time_perd_ind',lit('Y'))
    df_dqm_fct.show(5)
    df_dqm_fct.createOrReplaceTempView('df_dqm_fct')


    query='''
        SELECT 
            *, 
            CASE WHEN pd_time_perd_ind = 'Y' THEN sales_msu_qty_1 END AS sales_msu_qty_pd,
            CASE WHEN pd_time_perd_ind = 'Y' THEN sales_mlc_amt_1 END AS sales_mlc_amt_pd,
            CASE WHEN pd_time_perd_ind = 'Y' THEN sales_musd_amt_1 END AS sales_musd_amt_pd
        FROM df_dqm_fct
        '''
    df_dqm_fct=spark.sql(query)
    df_dqm_fct.show(5)
    df_dqm_fct.createOrReplaceTempView('mm_dqm_fct')


    df_dqm_new_time_perd_y=df_dqm_new_time_perd.filter(col('new_time_perd_ind')=='Y')
    df_dqm_new_time_perd_y.show(5)
    df_dqm_new_time_perd_y.createOrReplaceTempView('df_dqm_new_time_perd_y')
                                        
    # get fact data for new delivered time
    query='''
        SELECT 
            inp.*,
            fct.sales_msu_qty, fct.sales_mlc_amt, fct.sales_musd_amt, fct.mkt_skid AS mkt_skid_2, fct.time_perd_id, fct.prod_skid AS prod_skid_2, fct.mm_time_perd_end_date AS mm_time_perd_end_date_2, fct.srce_sys_id AS srce_sys_id_2, fct.cntrt_id AS cntrt_id_2,
            time_perd_new.mm_time_perd_end_date AS mm_time_perd_end_date_3, time_perd_new.new_time_perd_ind, time_perd_new.time_factr AS time_factr_3, time_perd_new.srce_sys_id AS srce_sys_id_3, time_perd_new.cntrt_id AS cntrt_id_3, time_perd_new.mkt_skid AS mkt_skid_3,
            time_perd_pd.srce_sys_id AS srce_sys_id_4, time_perd_pd.time_factr AS time_factr_4, time_perd_pd.cntrt_id AS cntrt_id_4, time_perd_pd.mm_time_perd_end_date AS mm_time_perd_end_date_4, 
            cntrt.mkt_top_node_skid
            FROM mm_dqm_prod AS inp
            INNER JOIN df_fct_MTH_img AS fct ON inp.prod_skid = fct.prod_skid 
            LEFT OUTER JOIN df_dqm_new_time_perd_y AS time_perd_new ON fct.mm_time_perd_end_date = time_perd_new.mm_time_perd_end_date and fct.srce_sys_id = time_perd_new.srce_sys_id and fct.cntrt_id = time_perd_new.cntrt_id
            LEFT OUTER JOIN df_dqm_new_time_perd_n AS time_perd_pd ON fct.mm_time_perd_end_date = time_perd_pd.mm_time_perd_end_date and fct.srce_sys_id = time_perd_pd.srce_sys_id and fct.cntrt_id = time_perd_pd.cntrt_id 
            INNER JOIN df_dqm_cntrt_mkt AS cntrt ON fct.mkt_skid = cntrt.mkt_top_node_skid and time_perd_new.mkt_skid = cntrt.mkt_top_node_skid
        '''
    df_dqm_fct2=spark.sql(query)
    df_dqm_fct2.show(5)
    df_dqm_fct2.createOrReplaceTempView('df_dqm_fct2')


    # get fact data for new delivered time
    query='''
        WITH CTE(
            SELECT 
            nvl(ddf.new_time_perd_ind,'N') as new_time_perd_ind1,
            case when ddf.srce_sys_id_4 is not null then 'Y' else 'N' end as pd_time_perd_ind,
            nvl(ddf.time_factr_3,time_factr_4) as time_factr, 
            ddf.* EXCEPT(new_time_perd_ind) 
            FROM df_dqm_fct2 ddf)
        SELECT new_time_perd_ind1 AS new_time_perd_ind, mkt_skid_2 AS mkt_skid, * EXCEPT(new_time_perd_ind1, mkt_skid_2) FROM CTE 
        WHERE pd_time_perd_ind='N'
        '''
    df_dqm_fct2=spark.sql(query)
    df_dqm_fct2.show(5)

    df_dqm_fct2.createOrReplaceTempView('mm_dqm_fct2')


    df_dqm_fct2=column_complementer(df_dqm_fct2,df_dqm_fct)
    df_dqm_fct2.show(5)


    df_dqm_fct=df_dqm_fct2.union(df_dqm_fct)
    df_dqm_fct.show(5)
    df_dqm_fct.createOrReplaceTempView('mm_dqm_fct')


    df_dqm_fct2=df_dqm_fct.filter(col('top_lvl_ind')=='Y')
    df_dqm_fct2.show(5)
    df_dqm_fct2.createOrReplaceTempView('mm_dqm_fct2')


    query=f'''
    SELECT 
    inp.*,
    fct_top_lvl.sales_msu_qty AS sales_msu_qty_top, fct_top_lvl.sales_mlc_amt AS sales_mlc_amt_top, fct_top_lvl.sales_msu_qty_pd AS sales_msu_qty_pd_top, fct_top_lvl.sales_mlc_amt_pd AS sales_mlc_amt_pd_top,
    time_perd.time_perd_id AS time_perd_id_3, time_perd.time_perd_id_ya, time_perd.time_perd_id_prev,  
    fct_ya.sales_msu_qty AS sales_msu_qty_ya, fct_ya.sales_mlc_amt AS sales_mlc_amt_ya, fct_ya.sales_musd_amt AS sales_musd_amt_ya,
    fct_pp.sales_msu_qty AS sales_msu_qty_pp, fct_pp.sales_mlc_amt AS sales_mlc_amt_pp, fct_pp.sales_musd_amt AS sales_musd_amt_pp, fct_pp.time_factr AS time_factr_pp
    FROM mm_dqm_fct AS inp
    LEFT OUTER JOIN mm_dqm_fct2 AS fct_top_lvl ON inp.mkt_skid = fct_top_lvl.mkt_skid 
    and inp.time_perd_id = fct_top_lvl.time_perd_id and inp.top_lvl_prod_attr_val_list = fct_top_lvl.extrn_prod_attr_val_list 
    LEFT OUTER JOIN {catalog_name}.gold_tp.tp_time_perd_fdim AS time_perd ON inp.time_perd_id = time_perd.time_perd_id 
    LEFT OUTER JOIN mm_dqm_fct AS fct_ya ON time_perd.time_perd_id_ya = fct_ya.time_perd_id and inp.prod_skid = fct_ya.prod_skid and inp.mkt_skid = fct_ya.mkt_skid 
    LEFT OUTER JOIN mm_dqm_fct AS fct_pp ON time_perd.time_perd_id_prev = fct_pp.time_perd_id and inp.prod_skid = fct_pp.prod_skid and inp.mkt_skid = fct_pp.mkt_skid
    '''
    df_dqm_calc_measr=spark.sql(query)
    df_dqm_calc_measr.show(5)
    df_dqm_calc_measr.createOrReplaceTempView('mm_dqm_calc_measr')


    query='''
        SELECT 
        CASE WHEN sales_msu_qty_top != 0 THEN sales_msu_qty/sales_msu_qty_top *100 end as share_su_pct,
        case when sales_mlc_amt_top != 0 then sales_mlc_amt/sales_mlc_amt_top*100 end as share_lc_pct,
        case when sales_msu_qty_pd_top != 0 then sales_msu_qty_pd/sales_msu_qty_pd_top*100 end as share_su_pct_pd,
        case when sales_mlc_amt_pd_top != 0 then sales_mlc_amt_pd/sales_mlc_amt_pd_top*100 end as share_lc_pct_pd, 
        * 
        FROM mm_dqm_calc_measr
    '''
    df_dqm_calc_measr=spark.sql(query)
    df_dqm_calc_measr.show(5)
    df_dqm_calc_measr.createOrReplaceTempView('mm_dqm_calc_measr')


    query='''
    SELECT 
        (case when sales_msu_qty_pp != 0 then (sales_msu_qty * time_factr-sales_msu_qty_pp*time_factr_pp)/sales_msu_qty_pp*100 end) as ipp_su_pct,
        (case when sales_msu_qty_ya !=0 then (sales_msu_qty - sales_msu_qty_ya)/sales_msu_qty_ya*100 end) as iya_su_pct,
        (case when sales_msu_qty_pd !=0 then (sales_msu_qty - sales_msu_qty_pd)/sales_msu_qty_pd*100 end) as ipd_su_pct,
        (case when sales_mlc_amt_pp !=0 then (sales_mlc_amt * time_factr-sales_mlc_amt_pp*time_factr_pp)/sales_mlc_amt_pp*100 end) as ipp_lc_pct,
        (case when sales_mlc_amt_ya !=0 then (sales_mlc_amt-sales_mlc_amt_ya)/sales_mlc_amt_ya*100 end) as iya_lc_pct,
        (case when sales_mlc_amt_pd !=0 then (sales_mlc_amt - sales_mlc_amt_pd)/sales_mlc_amt_pd*100 end) as ipd_lc_pct, *
    FROM 
    mm_dqm_calc_measr
    '''
    df_dqm_calc_index=spark.sql(query)
    df_dqm_calc_index.show(5)
    df_dqm_calc_index.createOrReplaceTempView('mm_dqm_calc_index')
    df_dqm_calc_index.write.format('parquet').mode('overwrite').save(f'{mat_path}/{run_id}/t1_dq_pre_business_validation_df_dqm_calc_index')
    # materialize(df_dqm_calc_index,'df_dqm_calc_index',NOTEBOOK_NAME,run_id,MAT_PATH)


    query='''
        SELECT 
            cast( 1 as integer) as parnt_strct_lvl_id,* 
        FROM mm_dqm_calc_index
    '''
    df_dqm_per_publ=spark.sql(query)
    df_dqm_per_publ.show(5)



    #Standardize MM_TP_VALDN_AGG_FCT
    publish_valdn_agg_fct(df_dqm_per_publ,cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd,srce_sys_id,run_id)
    