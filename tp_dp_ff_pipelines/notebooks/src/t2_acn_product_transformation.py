from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,assign_skid,load_cntrt_lkp,acn_prod_trans,acn_prod_trans_materialize, get_database_config
import argparse
from tp_utils.common import get_logger

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
  FILE_NAME = args.FILE_NAME
  CNTRT_ID = args.CNTRT_ID
  RUN_ID = args.RUN_ID

  catalog_name= db_config['catalog_name']
  postgres_schema=  db_config['postgres_schema']

  # Filter the DataFrame to get the 'Source System ID' and TIME_PERD_TYPE_CODE for the specified contract ID
  df_cntrt_lkp=load_cntrt_lkp(CNTRT_ID, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
  srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
  time_perd_type_code = df_cntrt_lkp.collect()[0].time_perd_type_code
  logger.info(f"TIME PERD TYPE CODE: {time_perd_type_code}")

  logger.info("Product Transformation started")
  df_prod_trans = acn_prod_trans(srce_sys_id, RUN_ID, catalog_name, spark )

  logger.info(f"Row Count: {df_prod_trans.count()}")
  new_products_cnt = df_prod_trans.filter("prod_skid is null").count()
  logger.info(f"New products Count: {new_products_cnt}")

  df_existing_prods = df_prod_trans.filter("prod_skid is not null")
  df_prod_trans = df_prod_trans.filter("prod_skid is null")

  # Assign a Product SKID
  df = assign_skid(df_prod_trans,RUN_ID,'prod', catalog_name, spark,srce_sys_id, CNTRT_ID)
  logger.info("Product skid assigned for new products")

  df = df.unionByName(df_existing_prods,True)

  acn_prod_trans_materialize(df,RUN_ID)
