CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_prod_assoc (
  seq_num SMALLINT,
  child_prod_match_attr_list STRING,
  parnt_prod_match_attr_list STRING,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_PROD_ASSOC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
