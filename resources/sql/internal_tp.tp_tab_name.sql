CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_tab_name (
  COL_NAME VARCHAR(225),
  TAB_NAME VARCHAR(225))
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
