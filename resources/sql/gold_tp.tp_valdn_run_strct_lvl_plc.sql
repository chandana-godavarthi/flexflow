CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_valdn_run_strct_lvl_plc (
  run_id BIGINT,
  strct_lvl_id BIGINT,
  abslt_thshd_val DOUBLE,
  ipp_check_val DOUBLE,
  iya_check_val DOUBLE,
  ipd_check_val DOUBLE)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
