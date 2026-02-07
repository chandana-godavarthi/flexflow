CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_run_measr_plc (
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  measr_id SMALLINT,
  calc_ind CHAR(1),
  extrn_measr_id STRING,
  extrn_measr_name STRING,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_RUN_MEASR_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
