CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_run_time_perd_plc (
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  time_perd_id BIGINT,
  extrn_time_perd_id STRING)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/INTERNAL/TP_RUN_TIME_PERD_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
