CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_strct_lvl_lkp (
  strct_lvl_id SMALLINT,
  attr_id SMALLINT,
  data_std_sttus_id SMALLINT,
  lvl_num SMALLINT,
  strct_id SMALLINT,
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_STRCT_LVL_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
