CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_categ_lkp (
  categ_id VARCHAR(3),
  categ_name VARCHAR(200),
  data_std_ind CHAR(1),
  sub_gbu_id VARCHAR(15),
  use_ind CHAR(1),
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_CATEG_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
