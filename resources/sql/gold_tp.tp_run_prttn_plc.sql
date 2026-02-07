CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_run_prttn_plc (
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  mm_time_perd_end_date DATE,
  time_perd_class_code VARCHAR(10),
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_RUN_PRTTN_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
