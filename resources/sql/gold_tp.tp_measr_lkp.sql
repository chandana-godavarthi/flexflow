CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_measr_lkp (
  measr_id SMALLINT,
  measr_name VARCHAR(200),
  measr_logcl_name VARCHAR(200),
  measr_desc VARCHAR(2000),
  measr_grp_id SMALLINT,
  measr_phys_name VARCHAR(120),
  uom_name VARCHAR(100),
  prmtn_type_name VARCHAR(200),
  mkt_agg_methd_code VARCHAR(30),
  prod_agg_methd_code VARCHAR(30),
  time_agg_methd_code VARCHAR(30),
  bimth_mth_alloc_factr SMALLINT,
  frml_txt VARCHAR(270),
  use_ind CHAR(1),
  secure_group_key BIGINT NOT NULL)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_MEASR_LKP'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
