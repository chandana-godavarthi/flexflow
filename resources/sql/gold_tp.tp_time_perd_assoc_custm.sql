CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_time_perd_assoc_custm (
  mh_time_perd_id BIGINT,
  mh_time_perd_start_date DATE,
  mh_time_perd_end_date DATE,
  custm_time_perd_id BIGINT,
  custm_time_perd_id_ya BIGINT,
  custm_time_perd_type_code VARCHAR(10),
  custm_time_perd_name VARCHAR(200),
  custm_time_perd_long_name VARCHAR(200),
  custm_time_perd_abbr_name VARCHAR(200),
  custm_time_perd_start_date DATE,
  custm_time_perd_end_date DATE,
  custm_num_day_qty SMALLINT,
  custm_time_perd_desc STRING,
  pmh_time_perd_id BIGINT,
  pmh_time_perd_start_date DATE,
  pmh_time_perd_end_date DATE,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_TIME_PERD_ASSOC_CUSTM'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
