CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_strct_lkp (
  strct_id SMALLINT,
  strct_code VARCHAR(10),
  strct_name VARCHAR(200),
  strct_desc STRING,
  dmnsn_name VARCHAR(30),
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_STRCT_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
