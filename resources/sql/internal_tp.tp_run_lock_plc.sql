CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_run_lock_plc (
  run_id BIGINT,
  lock_sttus BOOLEAN,
  creat_date TIMESTAMP,
  lock_path STRING)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/INTERNAL/TP_RUN_LOCK_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
