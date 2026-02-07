CREATE OR REPLACE TABLE {catalog_name}.gold_tp.tp_run_task_plc (
  task_run_id BIGINT,
  cluster_id VARCHAR(255),
  spark_context_id VARCHAR(255),
  end_time TIMESTAMP,
  execution_duration BIGINT,
  run_page_url VARCHAR(255),
  setup_duration BIGINT,
  start_time TIMESTAMP,
  lfcyl_state VARCHAR(255),
  run_sttus_name VARCHAR(255),
  state_msg STRING,
  user_cancl_or_timedout BOOLEAN,
  state VARCHAR(255),
  code VARCHAR(255),
  msg STRING,
  type VARCHAR(255),
  task_key VARCHAR(255),
  wkf_job_run_id BIGINT,
  secure_group_key BIGINT)
USING delta
LOCATION 'abfss://tp-publish-data@{storage_name}.dfs.core.windows.net/TP_RUN_TASK_PLC'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
