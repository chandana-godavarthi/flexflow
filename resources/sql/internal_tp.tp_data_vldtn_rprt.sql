CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_data_vldtn_rprt (
  run_id INT,
  validation STRING,
  result STRING,
  details STRING,
  validation_type STRING,
  validation_path STRING,
  created_at TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
