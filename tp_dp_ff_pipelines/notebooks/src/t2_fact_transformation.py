from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from tp_utils.common import get_dbutils, read_run_params,add_secure_group_key,column_complementer,dynamic_expression,add_partition_columns,load_cntrt_lkp,read_from_postgres, get_database_config,materialise_path,add_latest_time_perd,semaphore_generate_path,semaphore_acquisition,release_semaphore,safe_write_with_retry
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
   ref_db_hostname = db_config['ref_db_hostname']
   # Get the job parameters
   args = read_run_params()
   cntrt_id = args.CNTRT_ID
   run_id = args.RUN_ID  

   catalog_name= db_config['catalog_name']
   postgres_schema= db_config['postgres_schema']
  
   # Filter the DataFrame to get the 'Source System ID' and TIME_PERD_TYPE_CODE for the specified contract ID
   df_cntrt_lkp=load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
   srce_sys_id = df_cntrt_lkp.collect()[0].srce_sys_id
   iso_crncy_code = df_cntrt_lkp.collect()[0].crncy_id
   time_perd_type_code = df_cntrt_lkp.collect()[0].time_perd_type_code

   logger.info(f"Source System ID: {srce_sys_id}, Crncy_id: {iso_crncy_code}, Time Period Type: {time_perd_type_code}")
   fact_derv_mat_path = materialise_path(spark)
   # Read all the Product, Market, Time and Fact parquet file into DataFrame
   df_prod_stgng_vw = spark.read.parquet(f'''{fact_derv_mat_path}/{run_id}/product_transformation_df_prod_stgng_vw''')
   df_prod_stgng_vw.createOrReplaceTempView('df_prod_stgng_vw')

   df_mkt_stgng_vw = spark.read.parquet(f'''{fact_derv_mat_path}/{run_id}/market_transformation_df_mkt_stgng_vw''')
   df_mkt_stgng_vw.createOrReplaceTempView('df_mkt_stgng_vw')

   df_time_stgng_vw = spark.read.parquet(f'''{fact_derv_mat_path}/{run_id}/time_transformation_df_time_stgng_vw''')
   df_time_stgng_vw.createOrReplaceTempView('df_time_stgng_vw')

   df_fact_extrn = spark.read.parquet(f'''{fact_derv_mat_path}/{run_id}/load_file_df_fact_extrn''')
   df_fact_extrn.createOrReplaceTempView('df_fact_extrn')

   logger.info("Started fact transformation.")
   
   # Define the object name for the expression lookup table
   object_name=f"{postgres_schema}.mm_exprn_lkp"
   
   # Read the lookup table from PostgreSQL
   df_exprn_lkp=read_from_postgres(object_name,spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )

   # df_exprn_lkp.createOrReplaceTempView('mm_exprn_lkp')
   df_exprn_lkp.show()

   # Define operation type and dimension type for expression lookup
   exprn_name='Addition of Key Columns'
   dmnsn_name='FCT'

   # Generate a dynamic SQL query for "addition of key columns" from mm_exprn_lkp
   query=dynamic_expression(df_exprn_lkp,cntrt_id,exprn_name,dmnsn_name,spark)

   format_query = eval
   final_query= format_query(query)
   logger.info(f"Dynamic SQL query for FACT transformation: {final_query}")

   df_fact_stgng_vw = spark.sql(final_query)
   df_fact_stgng_vw.createOrReplaceTempView('df_fact_stgng_vw')

   fact_cols = df_fact_stgng_vw.columns
   fact_exclude_cols = ["extrn_prod_id", "extrn_mkt_id", "extrn_time_perd_id", "mm_time_perd_end_date", "time_perd_id"]
   fact_exclude_cols = [col for col in fact_exclude_cols if col in fact_cols]
   exclude_str = ", ".join(fact_exclude_cols)

   # Define the SQL query to join df_fact_stgng_vw with df_prod_stgng_vw, df_mkt_stgng_vw, and df_time_stgng_vw
   query = f'''
   SELECT fct.* EXCEPT ({exclude_str}), 
       prod.prod_skid, 
       mkt.mkt_skid, 
       time.time_perd_id, 
       time.mm_time_perd_end_date 
   FROM df_fact_stgng_vw fct 
   LEFT OUTER JOIN df_prod_stgng_vw prod ON fct.extrn_prod_id = prod.extrn_prod_id 
   LEFT OUTER JOIN df_mkt_stgng_vw mkt ON fct.extrn_mkt_id = mkt.extrn_mkt_id 
   LEFT OUTER JOIN df_time_stgng_vw time ON fct.extrn_time_perd_id = time.extrn_perd_id
   '''

   # Execute the SQL query and create a DataFrame from the result
   df_fact_stgng_vw = spark.sql(query)
   logger.info(f"Row count after joining fact with prod,mkt and time: {df_fact_stgng_vw.count()}")

   #Add latest time_perd into mm_run_detl_plc table
   add_latest_time_perd(df_fact_stgng_vw,postgres_schema,run_id,ref_db_name, ref_db_user, ref_db_pwd,ref_db_hostname)

   # Read FACT schema from Delta Table
   df_sch_fact=spark.sql(f'SELECT * FROM {catalog_name}.gold_tp.tp_wk_fct LIMIT 0')

   #Complementing input data with FACT Schema
   df_fact_stgng_vw=column_complementer(df_fact_stgng_vw,df_sch_fact)

   #Populate and typecast partition columns to the Fact
   df_fact_stgng_vw = add_partition_columns(df_fact_stgng_vw, cntrt_id)
                                   
   df_fact_stgng_vw.createOrReplaceTempView('df_fact_stgng_vw')

   # Add secure group key column
   df_run_prttn_plc = add_secure_group_key(df_fact_stgng_vw,cntrt_id,postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
   
   # Add a new column 'time_perd_class_code' with a constant value and select specific columns
   df_run_prttn_plc = df_run_prttn_plc.withColumn("time_perd_class_code", lit(time_perd_type_code)) \
                                   .select("srce_sys_id", "cntrt_id",  "run_id", "mm_time_perd_end_date", "time_perd_class_code","secure_group_key").distinct()
   row_count= df_run_prttn_plc.count()
   
   dim_type='TP_RUN_PRTTN_PLC'
   run_prttn_plc_path = semaphore_generate_path(dim_type, srce_sys_id, cntrt_id)
   # Print the constructed path for verification
   logger.info(f"run_prttn_plc_path: {run_prttn_plc_path}")
   # Acquire semaphore for writing to the specified path
   logger.info("Acquiring semaphore for TP_RUN_PRTTN_PLC")
   run_prttn_plc_check_path = semaphore_acquisition(run_id, run_prttn_plc_path,catalog_name, spark)
   try:
      logger.info(f"Started Writing to tp_run_prttn_plc")
      # Write the DataFrame to a Delta table in append mode
      safe_write_with_retry(df_run_prttn_plc, f"{catalog_name}.gold_tp.TP_RUN_PRTTN_PLC", "append", partition_by='cntrt_id', options=None)
      logger.info(f"Fact data written successfully to {catalog_name}.gold_tp.TP_RUN_PRTTN_PLC with {row_count} rows")
   finally:
      # Release the semaphore for the current run
      release_semaphore(catalog_name,run_id,run_prttn_plc_check_path , spark)
      logger.info("TP_RUN_PRTTN_PLC Semaphore released")
   
   # Publish FACT Dataframe
   df_fact_stgng_vw.write.mode("overwrite").format('parquet').save(f'''{fact_derv_mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw''')
   logger.info(f"Materialized Path: {fact_derv_mat_path}/{run_id}/fact_transformation_df_fact_stgng_vw.parquet")