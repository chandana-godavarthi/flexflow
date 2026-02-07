CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_refresh_tbl_lkp (
  id INT,
  src_table VARCHAR(225),
  tgt_table VARCHAR(225),
  key_cols VARCHAR(225),
  state VARCHAR(225),
  part_key_name VARCHAR(225),
  part_key_value VARCHAR(225),
  refresh_type CHAR(25),
  flag VARCHAR(20))
USING delta
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
