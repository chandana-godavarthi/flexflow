import pytest
from unittest.mock import MagicMock
from src.tp_utils import common

# Scenario 1: Basic successful execution
def test_exec_missing_measures_success():
    mock_df = MagicMock(name="DataFrame")
    mock_spark = MagicMock(name="SparkSession")
    mock_spark.sql.return_value = mock_df

    result_df = common.exec_missing_measures(mock_spark, mock_df, "1,2,3")

    mock_df.createOrReplaceTempView.assert_called_once_with("srce_mmeasr")
    mock_spark.sql.assert_called_once_with("select line_num from srce_mmeasr where line_num not in (1,2,3)")
    assert result_df == mock_df

# Scenario 2: Empty str_line_num
def test_exec_missing_measures_empty_str_line_num():
    mock_df = MagicMock(name="DataFrame")
    mock_spark = MagicMock(name="SparkSession")
    mock_spark.sql.return_value = mock_df

    result_df = common.exec_missing_measures(mock_spark, mock_df, "")

    mock_spark.sql.assert_called_once_with("select line_num from srce_mmeasr where line_num not in ()")
    assert result_df == mock_df

# Scenario 3: str_line_num with spaces and formatting
def test_exec_missing_measures_str_line_num_with_spaces():
    mock_df = MagicMock(name="DataFrame")
    mock_spark = MagicMock(name="SparkSession")
    mock_spark.sql.return_value = mock_df

    result_df = common.exec_missing_measures(mock_spark, mock_df, " 4 , 5 , 6 ")

    mock_spark.sql.assert_called_once_with("select line_num from srce_mmeasr where line_num not in ( 4 , 5 , 6 )")
    assert result_df == mock_df

# Scenario 4: SQL execution raises exception
def test_exec_missing_measures_sql_exception():
    mock_df = MagicMock(name="DataFrame")
    mock_spark = MagicMock(name="SparkSession")
    mock_spark.sql.side_effect = Exception("SQL error")

    with pytest.raises(Exception, match="SQL error"):
        common.exec_missing_measures(mock_spark, mock_df, "1,2,3")