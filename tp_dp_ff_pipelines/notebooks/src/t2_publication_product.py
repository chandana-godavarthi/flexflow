from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils,read_run_params, add_secure_group_key,semaphore_acquisition, load_cntrt_lkp, release_semaphore, get_database_config, t2_publish_product,materialise_path,column_complementer, semaphore_generate_path, work_to_arch
import argparse
from tp_utils.common import get_logger

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
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    catalog_name= db_config['catalog_name']
    postgres_schema= db_config['postgres_schema']
    # Define the light refined path
    LIGHT_REFINED_PATH='tp-publish-data/'

    # Read the table to get the 'Source System ID' for the specified contract ID
    df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
    logger.info(f"Source System ID: {srce_sys_id}")
    t2_publish_mat_path = materialise_path(spark)

    # Load product and fact staging tables
    df_prod_stgng_vw=spark.read.parquet(f'''{t2_publish_mat_path}/{run_id}/product_transformation_df_prod_stgng_vw''')
    df_prod_dim = spark.sql(f"select * from {catalog_name}.gold_tp.tp_prod_dim limit 0")
    df_prod_stgng_vw = column_complementer(df_prod_stgng_vw, df_prod_dim)

    #Adding secure group key
    df_prod_stgng_vw=add_secure_group_key(df_prod_stgng_vw,cntrt_id, postgres_schema, spark,ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    logger.info(f"df_prod_stgng_vw row counts: {df_prod_stgng_vw.count()}")
    part_cntrt_id=0
    # Acquire the semaphore for the current run
    dim_type='TP_PROD_DIM'
    prod_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)

    try:
        logger.info(f"semaphore path to be aquired: {prod_path}")
        prod_check_path = semaphore_acquisition(run_id, prod_path, catalog_name, spark)
        logger.info('Started writing to PROD_DIM')
        t2_publish_product(df_prod_stgng_vw,catalog_name,"gold_tp", "tp_prod_dim", spark)
    finally:
        logger.info(f"Started releasing the semaphore: {prod_path}")
        release_semaphore(catalog_name,run_id,prod_check_path, spark )
        logger.info(f"semaphore released: {prod_check_path}")  

    # Archive the processed file by moving it from the working directory to the archive directory
    work_to_arch(file_name,dbutils)
    logger.info("Archived the processed file from working directory to the archive directory.")  