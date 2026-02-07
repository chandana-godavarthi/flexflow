from pyspark.sql import SparkSession
import os

def extract_table_ddl(spark: SparkSession, catalog: str, table: str, file_name:str):
    """Extract table DDL information for Asset Bundle configuration."""
    
    # Get table description
    df_ddl = spark.sql(f"SHOW CREATE TABLE  {catalog}.{table}")
    ddl = df_ddl.collect()[0][0]
    ddl = ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
    ddl = ddl.replace("cdl_tp_dev", "{catalog_name}")
    ddl = ddl.replace("sa01flexflowtp01dev", "{storage_name}")
    print(ddl)
    with open(f"{file_name}.sql", "w") as file:
        file.write(ddl)
    return ddl

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Tradepanel_DDL_Extractor").getOrCreate()
    
    # Configuration
    source_catalog = "cdl_tp_dev"
    tables_to_migrate = ['gold_tp.tp_attr_lkp',	'gold_tp.tp_bimth_fct',	'gold_tp.tp_categ_lkp',	'gold_tp.tp_cntrt_categ_assoc',	'gold_tp.tp_cntrt_lkp',	'gold_tp.tp_cntry_regn',	'gold_tp.tp_custm_fct',	'gold_tp.tp_measr_lkp',	'gold_tp.tp_mkt_assoc',	'gold_tp.tp_mkt_dim',	'gold_tp.tp_mth_fct',	'gold_tp.tp_prod_assoc',	'gold_tp.tp_prod_attr_val_lkp',	'gold_tp.tp_prod_dim',	'gold_tp.tp_qtr_fct',	'gold_tp.tp_run_measr_plc',	'gold_tp.tp_run_plc',	'gold_tp.tp_run_prttn_plc',	'gold_tp.tp_run_task_plc',	'gold_tp.tp_strct_lkp',	'gold_tp.tp_strct_lvl_lkp',	'gold_tp.tp_time_perd_assoc',	'gold_tp.tp_time_perd_assoc_custm',	'gold_tp.tp_time_perd_assoc_type',	'gold_tp.tp_time_perd_fdim',	'gold_tp.tp_time_perd_type',	'gold_tp.tp_tot_cntry_cntrt_assoc',	'gold_tp.tp_valdn_agg_fct',	'gold_tp.tp_valdn_cntrt_strct_lvl_prc_consol_vw',	'gold_tp.tp_valdn_deflt_strct_lvl_prc_consol_vw',	'gold_tp.tp_valdn_prod_dim',	'gold_tp.tp_valdn_run_strct_lvl_plc',	'gold_tp.tp_valdn_run_strct_plc',	'gold_tp.tp_wk_fct',	'internal_tp.tp_data_vldtn_rprt',	'internal_tp.tp_mkt_sdim',	'internal_tp.tp_mkt_skid_seq',	'internal_tp.tp_prod_sdim',	'internal_tp.tp_prod_skid_seq',	'internal_tp.tp_refresh_tbl_lkp',	'internal_tp.tp_run_lock_plc',	'internal_tp.tp_run_mkt_plc',	'internal_tp.tp_run_prod_plc',	'internal_tp.tp_run_time_perd_plc',	'internal_tp.tp_strct_lkp_bkp',	'internal_tp.tp_tab_name',	'internal_tp.tp_valdn_run_strct_lvl_plc'] # Provide list of catalog tables to generate the query
    os.chdir("../../../") 
    os.chdir("resources/sql/")
    current_directory = os.getcwd()
    for table in tables_to_migrate:
        print(f"Extracting DDL for {table}...")
        print(f"Current working directory: {current_directory}")
        try:
            extract_table_ddl(spark, source_catalog, table, table)
            print(f"Successfully extracted DDL for {table}")
        except Exception as e:
            print(f"Failed to extract DDL for {table}: {e}")