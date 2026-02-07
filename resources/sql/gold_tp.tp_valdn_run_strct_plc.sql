CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_valdn_run_strct_plc (
  run_id BIGINT,
  strct_id BIGINT,
  parnt_child_check_val DOUBLE,
  neg_check_val DOUBLE,
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_VALDN_RUN_STRCT_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
