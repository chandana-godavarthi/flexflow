import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import business_validation

@pytest.fixture
def mock_spark():
    spark = MagicMock()

    mock_df = MagicMock()
    mock_df.collect.return_value = [{"qry_txt": "SELECT * FROM dummy_table"}]
    mock_df.createOrReplaceTempView = MagicMock()
    mock_df.show = MagicMock()
    spark.sql.return_value = mock_df
    spark.table.return_value = mock_df
    spark.read.parquet.return_value = mock_df
    spark.read.format.return_value.load.return_value = mock_df

    return spark

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.filter.return_value = df
    df.select.return_value = df
    df.collect.return_value = [("CHK_DQ1",), ("CHK_DQ2",), ("CHK_DQ3",)]
    df.createOrReplaceTempView = MagicMock()
    df.show = MagicMock()
    return df

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.dq_query_retrieval")
@patch("src.tp_utils.common.validation")
@patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl")
@patch("src.tp_utils.common.generate_report")
@patch("src.tp_utils.common.generate_formatting_report")
@patch("src.tp_utils.common.get_dbutils")
def test_successful_execution(
    mock_get_dbutils,
    mock_generate_formatting_report,
    mock_generate_report,
    mock_merge_report,
    mock_validation,
    mock_dq_query_retrieval,
    mock_read_from_postgres,
    mock_get_logger,
    mock_spark,
    mock_df
):
    # Setup logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    # Setup read_from_postgres
    mock_read_from_postgres.return_value = mock_df

    # Setup dq_query_retrieval with correct key
    mock_query_df = MagicMock()
    mock_query_df.collect.return_value = [{"qry_txt": "SELECT * FROM dummy_table"}]
    mock_dq_query_retrieval.return_value = mock_query_df

    # Setup validation results
    mock_validation.side_effect = ["val1", "val2", "val3"]

    # Setup dbutils
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.return_value = "mocked_hostname"
    mock_get_dbutils.return_value = mock_dbutils

    # Patch exec to inject df_val3
    def fake_exec(code, globals_dict):
        globals_dict["df_val3"] = MagicMock()
        globals_dict["df_val3"].show = MagicMock()

    with patch("builtins.exec", side_effect=fake_exec):
        business_validation(
            run_id=1001,
            cntrt_id=2002,
            srce_sys_id="SYS001",
            file_name="report.xlsx",
            validation_name="Tier2_Validation",
            postgres_schema="tp_schema",
            catalog_name="tp_catalog",
            spark=mock_spark,
            ref_db_jdbc_url="jdbc:test",
            ref_db_name="ref_db",
            ref_db_user="user",
            ref_db_pwd="pwd"
        )

    # Assertions
    mock_read_from_postgres.assert_called_once()
    assert mock_validation.call_count == 3
    mock_merge_report.assert_called_once()
    mock_generate_report.assert_called_once()
    mock_generate_formatting_report.assert_called_once()
    mock_logger.info.assert_any_call("Creating summary report for validations.")
    mock_logger.info.assert_any_call("Starting report generation")
