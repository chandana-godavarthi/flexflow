CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_time_perd_assoc (
  cal_type_id SMALLINT,
  time_perd_id_a BIGINT,
  time_perd_id_b BIGINT,
  time_perd_assoc_type_id SMALLINT,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_TIME_PERD_ASSOC'
WITH ROW FILTER `{catalog_name}`.`gold_tp`.`das_rls` ON (secure_group_key)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
