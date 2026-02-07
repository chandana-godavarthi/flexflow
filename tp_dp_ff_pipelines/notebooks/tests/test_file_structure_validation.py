import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.tp_utils.common import file_structure_validation

@pytest.fixture(autouse=True)
def patch_dbutils(monkeypatch):
    monkeypatch.setattr("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.dq_query_retrieval")
@patch("src.tp_utils.common.validation")
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.write_to_postgres")
@patch("src.tp_utils.common.update_to_postgres")
@patch("src.tp_utils.common.generate_report")
@patch("src.tp_utils.common.generate_formatting_report")
@patch("src.tp_utils.common.row_number")
@patch("src.tp_utils.common.Window")
@patch("src.tp_utils.common.monotonically_increasing_id")
@patch("src.tp_utils.common.lit")
@patch("src.tp_utils.common.col")
@patch("src.tp_utils.common.lower")
@patch("src.tp_utils.common.upper")
@patch("src.tp_utils.common.trim")
@patch("src.tp_utils.common.expr")
@patch("src.tp_utils.common.when")
@patch("src.tp_utils.common.current_timestamp")
def test_file_structure_validation(
    mock_current_timestamp, mock_when, mock_expr, mock_trim, mock_upper, mock_lower,
    mock_col, mock_lit, mock_monotonically_increasing_id, mock_Window, mock_row_number,
    mock_generate_formatting_report, mock_generate_report, mock_update_to_postgres,
    mock_write_to_postgres, mock_read_from_postgres, mock_validation,
    mock_dq_query_retrieval, mock_load_cntrt_lkp, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_spark = MagicMock(spec=SparkSession)
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_df.orderBy.return_value = mock_df
    mock_df.filter.return_value.count.return_value = 0
    mock_df.show.return_value = None
    mock_df.write.mode.return_value.format.return_value.save.return_value = None
    mock_df.write.mode.return_value.saveAsTable.return_value = None
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.createDataFrame.return_value = mock_df

    mock_cntrt_df = MagicMock()
    mock_cntrt_df.collect.return_value = [MagicMock(time_perd_type_code="QTR", cntry_name="USA")]
    mock_load_cntrt_lkp.return_value = mock_cntrt_df

    mock_query_df = MagicMock()
    mock_query_df.collect.return_value = [{"qry_txt": "fact_bad_df = tier2_fact_mtrlz_tbl"}]
    mock_dq_query_retrieval.return_value = mock_query_df

    mock_validation.return_value = {
        "run_id": 456,
        "validation": "Test",
        "result": "PASSED",
        "details": "All good",
        "validation_type": "Type",
        "validation_path": "path"
    }

    mock_read_from_postgres.return_value = mock_df

    # Patch exec manually
    def fake_exec(query, env):
        env["fact_bad_df"] = MagicMock()
        env["df_val5"] = MagicMock()

    import builtins
    original_exec = builtins.exec
    builtins.exec = fake_exec

    try:
        file_structure_validation(
            notebook_name="test_notebook",
            validation_name="test_validation",
            file_name="test_file",
            cntrt_id="123",
            run_id="456",
            spark=mock_spark,
            ref_db_jdbc_url="jdbc:test",
            ref_db_name="test_db",
            ref_db_user="user",
            ref_db_pwd="pwd",
            postgres_schema="schema",
            catalog_name="catalog"
        )
    finally:
        builtins.exec = original_exec  # Restore original exec

    assert mock_validation.called
    assert mock_generate_report.called
    assert mock_generate_formatting_report.called
