CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_mkt_assoc (
  seq_num SMALLINT,
  child_mkt_skid BIGINT,
  child_strct_lvl_id SMALLINT,
  parnt_mkt_skid BIGINT,
  parnt_strct_lvl_id SMALLINT,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_MKT_ASSOC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
