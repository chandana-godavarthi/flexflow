CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_time_perd_assoc_type (
  time_perd_assoc_type_id SMALLINT,
  time_perd_assoc_type_name VARCHAR(200),
  time_perd_assoc_type_desc STRING,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_TIME_PERD_ASSOC_TYPE'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
