CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_valdn_prod_dim (
  prod_skid BIGINT,
  parnt_prod_skid BIGINT,
  run_id BIGINT,
  srce_sys_id SMALLINT,
  cntrt_id BIGINT,
  prod_lvl_id SMALLINT,
  prod_attr_val_name VARCHAR(200),
  prod_lvl_name STRING,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_VALDN_PROD_DIM'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
