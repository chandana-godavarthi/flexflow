import pytest
from unittest.mock import patch, MagicMock
from datetime import date
from src.tp_utils.common import calculate_retention_date

@pytest.fixture
def dummy_args():
    spark_mock = MagicMock()
    return {
        "run_id": 123,
        "cntrt_id": 456,
        "srce_sys_id": 1,
        "retention_period": 2,
        "catalog_name": "test_catalog",
        "postgres_schema": "test_schema",
        "time_perd_class_code": "WK",  # Override in specific tests
        "spark": spark_mock,
        "ref_db_jdbc_url": "jdbc:test",
        "ref_db_name": "test_db",
        "ref_db_user": "user",
        "ref_db_pwd": "pwd"
    }

class MockRow:
    def __init__(self, retention_date):
        self.retention_date = retention_date

    def __getitem__(self, key):
        if key == "retention_date":
            return self.retention_date
        return self.retention_date

def setup_mock_spark(spark_mock, mock_date):
    # Mock for read_query_from_postgres
    mock_postgres_df = MagicMock()
    mock_postgres_df.first.return_value = [12345]  # latst_time_perd_id
    # Mock for spark.sql
    mock_sql_df = MagicMock()
    mock_sql_df.first.return_value = [mock_date]  # end_date
    # Mock for createDataFrame().withColumn().first()
    mock_retention_df = MagicMock()
    mock_retention_df.first.return_value = {"retention_date": mock_date}
    spark_mock.sql.side_effect = [mock_sql_df]
    spark_mock.createDataFrame.return_value.withColumn.return_value = mock_retention_df
    return mock_postgres_df

@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.when", return_value=MagicMock(name="WhenColumn"))
@patch("src.tp_utils.common.expr", return_value=MagicMock(name="ExprColumn"))
@patch("src.tp_utils.common.col", return_value=MagicMock(name="ColColumn"))
def test_retention_weekly(mock_col, mock_expr, mock_when, mock_read_query, dummy_args):
    mock_read_query.return_value = setup_mock_spark(dummy_args["spark"], date(2024, 7, 14))
    result = calculate_retention_date(**dummy_args)
    assert isinstance(result, date)
    assert result == date(2024, 7, 14)

@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.when", return_value=MagicMock(name="WhenColumn"))
@patch("src.tp_utils.common.expr", return_value=MagicMock(name="ExprColumn"))
@patch("src.tp_utils.common.col", return_value=MagicMock(name="ColColumn"))
def test_retention_monthly(mock_col, mock_expr, mock_when, mock_read_query, dummy_args):
    dummy_args["time_perd_class_code"] = "MTH"
    mock_read_query.return_value = setup_mock_spark(dummy_args["spark"], date(2024, 5, 1))
    result = calculate_retention_date(**dummy_args)
    assert isinstance(result, date)
    assert result == date(2024, 5, 1)

@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.when", return_value=MagicMock(name="WhenColumn"))
@patch("src.tp_utils.common.expr", return_value=MagicMock(name="ExprColumn"))
@patch("src.tp_utils.common.col", return_value=MagicMock(name="ColColumn"))
def test_retention_bimonthly(mock_col, mock_expr, mock_when, mock_read_query, dummy_args):
    dummy_args["time_perd_class_code"] = "BIMTH"
    mock_read_query.return_value = setup_mock_spark(dummy_args["spark"], date(2024, 4, 1))
    result = calculate_retention_date(**dummy_args)
    assert isinstance(result, date)
    assert result == date(2024, 4, 1)

@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.when", return_value=MagicMock(name="WhenColumn"))
@patch("src.tp_utils.common.expr", return_value=MagicMock(name="ExprColumn"))
@patch("src.tp_utils.common.col", return_value=MagicMock(name="ColColumn"))
def test_retention_quarterly(mock_col, mock_expr, mock_when, mock_read_query, dummy_args):
    dummy_args["time_perd_class_code"] = "QTR"
    mock_read_query.return_value = setup_mock_spark(dummy_args["spark"], date(2024, 2, 1))
    result = calculate_retention_date(**dummy_args)
    assert isinstance(result, date)
    assert result == date(2024, 2, 1)

@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.when", return_value=MagicMock(name="WhenColumn"))
@patch("src.tp_utils.common.expr", return_value=MagicMock(name="ExprColumn"))
@patch("src.tp_utils.common.col", return_value=MagicMock(name="ColColumn"))
def test_retention_other_code(mock_col, mock_expr, mock_when, mock_read_query, dummy_args):
    dummy_args["time_perd_class_code"] = "XYZ"
    mock_read_query.return_value = setup_mock_spark(dummy_args["spark"], date(2024, 7, 24))
    result = calculate_retention_date(**dummy_args)
    assert isinstance(result, date)
    assert result == date(2024, 7, 24)