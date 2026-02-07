CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_strct_lkp_bkp (
  strct_id SMALLINT,
  strct_code VARCHAR(10),
  strct_name VARCHAR(200),
  strct_desc STRING,
  dmnsn_name VARCHAR(30),
  secure_group_key BIGINT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
