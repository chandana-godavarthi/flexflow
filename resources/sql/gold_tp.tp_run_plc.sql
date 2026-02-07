CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_run_plc (
  run_id BIGINT,
  wkf_job_run_id BIGINT,
  cntrt_id BIGINT,
  wkf_job_id BIGINT,
  lfcyl_state VARCHAR(255),
  run_sttus_name VARCHAR(255),
  file_name VARCHAR(255),
  rgstr_file_time TIMESTAMP,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  run_durtn BIGINT,
  run_name VARCHAR(255),
  run_page_url VARCHAR(255),
  state_msg STRING,
  user_cancl_or_timedout BOOLEAN,
  state VARCHAR(255),
  code VARCHAR(255),
  msg STRING,
  type VARCHAR(255),
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_RUN_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
