CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_run_prod_plc (
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  prod_skid BIGINT,
  extrn_prod_id STRING,
  extrn_prod_name STRING,
  extrn_prod_attr_val_list STRING)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/INTERNAL/TP_RUN_PROD_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
