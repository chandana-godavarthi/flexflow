CREATE OR REPLACE TABLE {catalog_name}.internal_tp.tp_valdn_run_strct_lvl_plc (
  run_id BIGINT,
  strct_lvl_id BIGINT,
  abslt_thshd_val DOUBLE,
  ipp_check_val DOUBLE,
  iya_check_val DOUBLE,
  ipd_check_val DOUBLE)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_DVM_RUN_STRCT_LVL_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
