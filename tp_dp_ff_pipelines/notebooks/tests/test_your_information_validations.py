import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import your_information_validations

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.parquet.return_value = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.sql.return_value.collect.return_value = [{"qry_txt": "SELECT * FROM dummy_table"}]
    return spark

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.filter.return_value = df
    df.select.return_value = df
    df.collect.return_value = [("CHK_DQ10",), ("CHK_DQ13",)]
    df.columns = ["some_column"]
    df.withColumn.return_value = df
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
def test_your_information_validations_all_checks_enabled(
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
    # Setup
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_read_from_postgres.return_value = mock_df
    mock_dq_query_retrieval.return_value = MagicMock()
    mock_dq_query_retrieval.return_value.collect.side_effect = [
        [{"qry_txt": "df_val10 = spark.sql('SELECT * FROM dummy_table')"}],
        [{"qry_txt": "df_val13 = spark.sql('SELECT * FROM dummy_table')"}]
    ]
    mock_validation.side_effect = ["val10", "val13"]

    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.return_value = "mocked_hostname"
    mock_get_dbutils.return_value = mock_dbutils

    def fake_exec(code, globals_dict):
        code_str = str(code)
        if "df_val10" in code_str:
            globals_dict["df_val10"] = MagicMock()
        if "df_val13" in code_str:
            globals_dict["df_val13"] = MagicMock()

    with patch("builtins.exec", side_effect=fake_exec):
        your_information_validations(
            run_id=1001,
            cntrt_id=2002,
            srce_sys_id=3,
            file_name="report.xlsx",
            validation_name="Tier2_Validation",
            catalog_name="tp_catalog",
            postgres_schema="tp_schema",
            spark=mock_spark,
            ref_db_jdbc_url="jdbc:test",
            ref_db_name="ref_db",
            ref_db_user="user",
            ref_db_pwd="pwd"
        )

    # Assertions
    mock_read_from_postgres.assert_called_once()
    assert mock_validation.call_count == 2
    mock_merge_report.assert_called_once()
    mock_generate_report.assert_called_once()
    mock_generate_formatting_report.assert_called_once()
    mock_logger.info.assert_any_call("Validation checks enabled: CHK_DQ10=True, CHK_DQ13=True")
    mock_logger.info.assert_any_call("Creating summary report for validations.")
    mock_logger.info.assert_any_call("Starting report generation")
