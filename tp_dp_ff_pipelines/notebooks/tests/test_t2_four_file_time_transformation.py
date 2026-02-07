import sys
import types
import pytest
from unittest.mock import MagicMock, patch

@pytest.fixture
def mock_modules():
    # Mock SparkSession
    mock_spark = MagicMock(name="MockSparkSession")
    mock_builder = MagicMock()
    mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

    spark_session_module = types.ModuleType("pyspark.sql.session")
    spark_session_module.SparkSession = MagicMock(builder=mock_builder)

    spark_sql_module = types.ModuleType("pyspark.sql")
    spark_sql_module.SparkSession = spark_session_module.SparkSession

    functions_module = types.ModuleType("pyspark.sql.functions")
    functions_module.lit = MagicMock()

    # Mock tp_utils.common
    mock_common = types.ModuleType("tp_utils.common")
    mock_logger = MagicMock()
    mock_safe_write_with_retry = MagicMock()
    mock_common.safe_write_with_retry = mock_safe_write_with_retry

    # Mock DataFrames
    mock_df_cntrt_lkp = MagicMock()
    mock_df_cntrt_lkp.collect.return_value = [MagicMock(srce_sys_id="SYS1", time_exprn_id="TEXP1")]

    mock_df_time_exprn = MagicMock()
    mock_df_time_exprn.columns = ['start_date_val', 'end_date_val', 'time_perd_type_val']
    mock_df_time_exprn.first.return_value = ['2023-01-01', '2023-12-31', 'MONTH']

    mock_df_time_extrn = MagicMock()
    mock_df_time_extrn.withColumnRenamed.return_value.drop.return_value = mock_df_time_extrn
    mock_df_time_extrn.selectExpr.return_value = mock_df_time_extrn
    mock_df_time_extrn.withColumn.return_value = mock_df_time_extrn
    mock_df_time_extrn.createOrReplaceTempView.return_value = None

    mock_df_fdim = MagicMock()
    mock_df_fdim.createOrReplaceTempView.return_value = None

    mock_df_stgng = MagicMock()
    mock_df_stgng.selectExpr.return_value = mock_df_stgng
    mock_df_stgng.withColumn.return_value = mock_df_stgng
    mock_df_stgng.createOrReplaceTempView.return_value = None
    mock_df_stgng.write.mode.return_value.format.return_value.save.return_value = None

    mock_df_run_time_perd_plc = MagicMock()

    def mock_sql(query):
        if "tp_time_perd_fdim" in query:
            return mock_df_fdim
        elif "tp_run_time_perd_plc" in query:
            return mock_df_run_time_perd_plc
        elif "SELECT t.extrn_perd_id" in query:
            return mock_df_stgng
        else:
            return MagicMock()

    mock_spark.sql.side_effect = mock_sql
    mock_spark.read.parquet.return_value = mock_df_time_extrn

    mock_common.__dict__.update({
        "get_logger": MagicMock(return_value=mock_logger),
        "get_dbutils": MagicMock(return_value=MagicMock()),
        "get_database_config": MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        }),
        "read_run_params": MagicMock(return_value=MagicMock(CNTRT_ID="C123", RUN_ID="R456")),
        "load_cntrt_lkp": MagicMock(return_value=mock_df_cntrt_lkp),
        "load_time_exprn_id": MagicMock(return_value=mock_df_time_exprn),
        "materialise_path": MagicMock(return_value="mock_path"),
        "column_complementer": MagicMock(return_value=mock_df_stgng),
        "semaphore_generate_path": MagicMock(return_value="mock_path"),
        "semaphore_acquisition": MagicMock(return_value="mock_path"),
        "release_semaphore": MagicMock(return_value="mock_path")
    })

    modules = {
        "pyspark": types.ModuleType("pyspark"),
        "pyspark.sql": spark_sql_module,
        "pyspark.sql.session": spark_session_module,
        "pyspark.sql.functions": functions_module,
        "tp_utils": types.ModuleType("tp_utils"),
        "tp_utils.common": mock_common,
        "src.tp_utils.common": mock_common
    }

    return modules, mock_common, mock_spark, mock_logger

def test_time_transformation_script_executes(mock_modules):
    modules, mock_common, mock_spark, mock_logger = mock_modules
    script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_time_transformation.py"

    with patch.dict(sys.modules, modules):
        with open(script_path) as f:
            code = compile(f.read(), script_path.split("/")[-1], 'exec')
            exec(code, {"__name__": "__main__", "spark": mock_spark})

    # ✅ Validate key logger calls
    expected_messages = [
        "Started Time Transformation",
        "df_time_extrn",
        "df_time_exprn",
        "df_time_stgng_vw after join",
        "Added run_id,srce_sys_id,cntrt_id to df_time_log",
        "df_time_log after column complimenting"
    ]

    for msg in expected_messages:
        mock_logger.info.assert_any_call(msg)

    # ✅ Flexible check for materialization log
    materialized_logs = [call.args[0] for call in mock_logger.info.call_args_list]
    assert any(
        "Time transformation Materialized to Path:" in log and "time_transformation_df_time_stgng_vw.parquet" in log
        for log in materialized_logs
    )
