import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import fact_multipliers_trans

@pytest.fixture(autouse=True)
def mock_dbutils(monkeypatch):
    import pyspark.dbutils
    monkeypatch.setattr(pyspark.dbutils, 'DBUtils', lambda spark: MagicMock(name="DBUtils"))

@pytest.fixture
def spark_mock():
    return MagicMock()

@pytest.fixture
def logger_patch():
    with patch("src.tp_utils.common.get_logger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        yield mock_logger

@pytest.fixture(autouse=True)
def spark_functions_patch():
    with patch("src.tp_utils.common.col", MagicMock()), \
         patch("src.tp_utils.common.concat_ws", MagicMock()), \
         patch("src.tp_utils.common.nanvl", MagicMock()), \
         patch("src.tp_utils.common.lit", MagicMock()):
        yield

def assert_log_contains(mock_logger, text):
    calls = [str(call_arg[0][0]) for call_arg in mock_logger.info.call_args_list]
    assert any(text in msg for msg in calls), f"Expected log containing: '{text}'"

def test_valid_multipliers(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()
    df_transformed = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1", multr_val="2", fct_trans="col1*2"),
        MagicMock(db_col_name="col2", multr_val="3", fct_trans="col2*3")
    ]
    df_fact.columns = ["col1", "col2", "col3"]
    spark_mock.sql.return_value = df_transformed
    df_transformed.count.return_value = 2

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "df_fact_extrn")

def test_no_multipliers(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = []
    df_fact.columns = ["col1", "col2"]

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "Multiplier mappings found:")

def test_comma_separated_columns(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()
    df_transformed = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1,col2", multr_val="2", fct_trans="col1*2")
    ]
    df_fact.columns = ["col1", "col2"]
    spark_mock.sql.return_value = df_transformed
    df_transformed.count.return_value = 1

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "[test_run] SQL query for Fact Multipliers Transformation:")

def test_case_insensitive_column_match(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()
    df_transformed = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="Col1", multr_val="2", fct_trans="Col1*2")
    ]
    df_fact.columns = ["col1"]
    spark_mock.sql.return_value = df_transformed
    df_transformed.count.return_value = 1

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "df_fact_extrn")

def test_exception_during_read(spark_mock, logger_patch):
    spark_mock.read.parquet.side_effect = Exception("Read failed")

    with pytest.raises(Exception, match="Read failed"):
        fact_multipliers_trans("test_run", "C001", spark_mock)

def test_empty_input_columns(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1", multr_val="2", fct_trans="col1*2")
    ]
    df_fact.columns = []

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "Multiplier mappings found:")

def test_invalid_transformation_expression(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1", multr_val="2", fct_trans="INVALID_EXPR")
    ]
    df_fact.columns = ["col1"]
    spark_mock.sql.side_effect = Exception("SQL error")

    with pytest.raises(Exception, match="SQL error"):
        fact_multipliers_trans("test_run", "C001", spark_mock)

def test_partial_column_match(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1,colX", multr_val="2", fct_trans="col1*2")
    ]
    df_fact.columns = ["col1"]

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "Multiplier mappings found:")

def test_sql_query_logged(spark_mock, logger_patch):
    df_dpf = MagicMock()
    df_fact = MagicMock()
    df_transformed = MagicMock()

    spark_mock.read.parquet.side_effect = [df_dpf, df_fact]
    df_dpf.collect.return_value = [
        MagicMock(db_col_name="col1", multr_val="2", fct_trans="col1*2")
    ]
    df_fact.columns = ["col1"]
    spark_mock.sql.return_value = df_transformed
    df_transformed.count.return_value = 1

    fact_multipliers_trans("test_run", "C001", spark_mock)
    assert_log_contains(logger_patch, "SQL query for Fact Multipliers Transformation:")
