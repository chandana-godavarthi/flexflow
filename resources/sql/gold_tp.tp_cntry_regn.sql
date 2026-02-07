CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_cntry_regn (
  regn_id VARCHAR(6),
  regn_name VARCHAR(200),
  sub_regn_id VARCHAR(6),
  sub_regn_name VARCHAR(200),
  cntry_hub_id VARCHAR(10),
  cntry_hub_name VARCHAR(200),
  cntry_id VARCHAR(3),
  cntry_name VARCHAR(200),
  crncy_id VARCHAR(3),
  use_ind CHAR(1),
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_CNTRY_REGN'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
