CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_valdn_deflt_strct_lvl_prc_consol_vw (
  categ_id VARCHAR(3),
  time_perd_class_code VARCHAR(10),
  strct_lvl_id_list STRING,
  abslt_thshd_val DOUBLE,
  ipp_check_val DOUBLE,
  iya_check_val DOUBLE,
  ipd_check_val DOUBLE,
  lvl_num SMALLINT,
  lvl_name_list STRING,
  time_perd_type_code VARCHAR(10),
  id BIGINT,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_VALDN_DEFLT_STRCT_LVL_PRC_CONSOL_VW'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
