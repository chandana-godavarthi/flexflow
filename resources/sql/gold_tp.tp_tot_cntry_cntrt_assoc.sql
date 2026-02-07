CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_tot_cntry_cntrt_assoc (
  tot_cntry_cntrt_assoc_id SMALLINT,
  tot_cntry_id SMALLINT,
  cntrt_id BIGINT,
  parnt_mkt_skid BIGINT,
  child_mkt_skid BIGINT,
  cnt_child_mkt SMALLINT,
  child_mkt_slct_flag SMALLINT,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_TOT_CNTRY_CNTRT_ASSOC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
