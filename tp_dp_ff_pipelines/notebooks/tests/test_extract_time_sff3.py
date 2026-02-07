import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from databricks.connect import DatabricksSession
from src.tp_utils import common
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from src.tp_utils.common import extract_time_sff3
@pytest.fixture
def spark():
    return DatabricksSession.builder.getOrCreate()

@patch("src.tp_utils.common.get_logger")
def test_extract_time_sff3_valid_input(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    data = [("202301", "January 2023"), ("202302", "February 2023")]
    columns = ["period_id", "period_long_description"]
    df_raw1 = spark.createDataFrame(data, columns)

    result_df = extract_time_sff3(df_raw1)

    assert result_df.count() == 2
    assert set(["extrn_code", "extrn_name", "line_num", "mm_time_perd_id"]).issubset(result_df.columns)
    mock_logger.info.assert_any_call("[extract_time_sff3] Function execution started.")

# Scenario 2: Empty DataFrame
@patch("src.tp_utils.common.get_logger")
def test_extract_time_sff3_empty_df(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    schema = "period_id STRING, period_long_description STRING"
    df_raw1 = spark.createDataFrame([], schema=schema)

    result_df = extract_time_sff3(df_raw1)

    assert result_df.count() == 0
    assert set(["extrn_code", "extrn_name", "line_num", "mm_time_perd_id"]).issubset(result_df.columns)

### Scenario 3: Missing required column
##@patch("src.tp_utils.common.get_logger")
##def test_extract_time_sff3_missing_column(mock_get_logger, spark):
##    mock_logger = MagicMock()
##    mock_get_logger.return_value = mock_logger
##
##    data = [("January 2023",)]
##    columns = ["period_long_description"]
##    df_raw1 = spark.createDataFrame(data, columns)
##
##    with pytest.raises(Exception):
##        extract_time_sff3(df_raw1)

# Scenario 4: Duplicate period_id values
@patch("src.tp_utils.common.get_logger")
def test_extract_time_sff3_duplicate_period_ids(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    data = [("202301", "January 2023"), ("202301", "January 2023")]
    columns = ["period_id", "period_long_description"]
    df_raw1 = spark.createDataFrame(data, columns)

    result_df = extract_time_sff3(df_raw1)

    assert result_df.count() == 2
    assert result_df.select("line_num").distinct().count() == 2  # row_number should assign unique line_num