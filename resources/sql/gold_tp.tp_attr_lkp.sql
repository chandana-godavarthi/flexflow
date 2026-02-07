CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_attr_lkp (
  attr_id SMALLINT,
  attr_name VARCHAR(200),
  attr_logcl_name VARCHAR(200),
  attr_desc VARCHAR(2000),
  attr_phys_name VARCHAR(30),
  attr_val_max_lngth_qty SMALLINT,
  dmnsn_name VARCHAR(30),
  virtl_ind CHAR(1),
  data_std_sttus_id SMALLINT,
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_ATTR_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
