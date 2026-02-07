CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_run_mkt_plc (
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  mkt_skid BIGINT,
  extrn_mkt_id STRING,
  extrn_mkt_name STRING,
  extrn_mkt_attr_val_list STRING)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/INTERNAL/TP_RUN_MKT_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
