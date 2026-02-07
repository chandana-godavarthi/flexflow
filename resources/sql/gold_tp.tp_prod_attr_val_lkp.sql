CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_prod_attr_val_lkp (
  prod_attr_val_id BIGINT,
  attr_id SMALLINT,
  categ_id VARCHAR(3),
  data_std_sttus_id SMALLINT,
  prod_attr_val_code VARCHAR(15),
  prod_attr_val_name VARCHAR(200),
  prod_attr_val_desc STRING,
  use_ind CHAR(1),
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_PROD_ATTR_VAL_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
