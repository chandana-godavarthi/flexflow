CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_cntrt_categ_assoc (
  cntrt_id BIGINT,
  categ_id VARCHAR(3),
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_CNTRT_CATEG_ASSOC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
