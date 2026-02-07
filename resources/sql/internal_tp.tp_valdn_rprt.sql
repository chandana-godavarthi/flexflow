CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_valdn_rprt (
  run_id BIGINT,
  valdn_name VARCHAR(200),
  reslt STRING,
  hyperlink_txt STRING,
  valdn_type STRING,
  valdn_path STRING,
  creat_time TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
