from pyspark.sql import SparkSession
from tp_utils.common import get_dbutils, read_run_params,load_cntrt_lkp,calculate_retention_date,semaphore_acquisition,release_semaphore,recursive_delete,time_perd_class_codes, get_database_config,match_time_perd_class,derive_publish_path,read_from_postgres,materialise_path,read_query_from_postgres
from pyspark.sql.functions import col, lit, lower,max,upper
import argparse

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
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
    publish_path = derive_publish_path(spark)
    postgres_schema= db_config['postgres_schema']
    catalog_name = db_config['catalog_name']

    # Step 2: Read contract retention info
    df_cntrt_lkp = load_cntrt_lkp(cntrt_id, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd )
    df_cntrt_lkp = df_cntrt_lkp.select("cntrt_id",col("retn_perd").alias("retention_period"),"srce_sys_id")
    SRCE_SYS_ID = df_cntrt_lkp.select("srce_sys_id").first()["srce_sys_id"]
    # Step 3: Add time period class code
    time_perd_class_code=time_perd_class_codes(cntrt_id, postgres_schema, catalog_name, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

    latst_time_perd_id_df = read_query_from_postgres(f"SELECT latst_time_perd_id from {postgres_schema}.mm_run_detl_plc where run_id={int(run_id)}", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
    latst_time_perd_id = latst_time_perd_id_df.first()[0]
    end_date_df = spark.sql(f"SELECT time_perd_end_date FROM {catalog_name}.gold_tp.tp_time_perd_fdim WHERE time_perd_id= {int(latst_time_perd_id)}")
    end_date = end_date_df.first()[0]
    df = df_cntrt_lkp.withColumn("time_perd_class_code", upper(lit(time_perd_class_code))).withColumn("end_date", lit(end_date))
    df.show()
    # Step 4: Calculate retention date
    df = calculate_retention_date(df)

    # Step 5: Filter expired contracts
    df = df.select("cntrt_id", "retention_date")
    df.createOrReplaceTempView("contract_to_delete")

    # Step 6: Define table name
    class_code = match_time_perd_class(time_perd_class_code.lower())
    table_name = f"{catalog_name}.gold_tp.tp_{class_code}_fct"

    # Step 8: Get mm_time_perd_end_date values to delete
    delete_df = spark.sql(f"""
    SELECT distinct part_srce_sys_id,part_cntrt_id,part_mm_time_perd_end_date
    FROM {table_name}
     WHERE part_cntrt_id IN (SELECT cntrt_id FROM contract_to_delete)
      AND part_mm_time_perd_end_date < (SELECT retention_date FROM contract_to_delete)
    """)
    dates_to_delete = [(row['part_mm_time_perd_end_date'], row['part_srce_sys_id'], row['part_cntrt_id']) for row in delete_df.collect()]

    # Define the path to the Fact data based on source system and contract ID
    fact_path = [
        f"/mnt/tp-publish-data/TP_{time_perd_class_code}_FCT/part_srce_sys_id={SRCE_SYS_ID}/part_cntrt_id={cntrt_id}"
        ]

    # Print the constructed path for verification
    print(fact_path)

    # Acquire semaphore to ensure safe access to the PROD_SDIM path for the given run ID
    fact_check_path = semaphore_acquisition(run_id, fact_path, catalog_name, spark)

    try:
      # Step 7: Delete from Delta table
      query = spark.sql(f"""
      DELETE FROM {table_name}
      WHERE part_cntrt_id IN (SELECT cntrt_id FROM contract_to_delete)
        AND part_mm_time_perd_end_date < (SELECT retention_date FROM contract_to_delete)
      """)
      
      # Print the completion of the write operation to MKT_SDIM
      print('Completed Remove data to Fact')
    
    finally:
      # Release the semaphore for the current run
      release_semaphore(catalog_name,run_id,fact_check_path, spark )

    for date, srce_sys_id, part_cntrt_id in dates_to_delete:
        target_path = f"{publish_path}/TP_{time_perd_class_code}_FCT/part_srce_sys_id={srce_sys_id}/part_cntrt_id={part_cntrt_id}/part_mm_time_perd_end_date={date}"
        recursive_delete(target_path,dbutils)
        print(f"Deleted: {target_path}")