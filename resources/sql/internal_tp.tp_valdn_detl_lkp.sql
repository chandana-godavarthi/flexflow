CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_valdn_detl_lkp (
  id INT NOT NULL,
  valdn_grp_name VARCHAR(200),
  valdn_type VARCHAR(50),
  valdn_name VARCHAR(200),
  qry_txt STRING,
  data_tier VARCHAR(6),
  hyperlink_tag VARCHAR(500))
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'false',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
