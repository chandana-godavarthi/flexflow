import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from databricks.connect import DatabricksSession
from pyspark.sql import Row
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from src.tp_utils.common import extract_time_ffs

from pyspark.sql.functions import (
    col,
    substring,
    expr,
    row_number,
    lit,
    ltrim,
    rtrim,
    monotonically_increasing_id,ltrim,rtrim
)
@pytest.fixture
def spark():
    return DatabricksSession.builder.getOrCreate()


@patch("src.tp_utils.common.get_logger")
def test_extract_time_ffs_valid_input(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    data = [("R12345678SomeAttributeDataSomeDescriptionData",)]
    df_raw1 = spark.createDataFrame(data, ["value"])

    result_df = extract_time_ffs(df_raw1)

    assert result_df is not None
    assert "extrn_code" in result_df.columns
    assert "extrn_name" in result_df.columns
    assert "mm_time_perd_id" in result_df.columns
    mock_logger.info.assert_any_call("[extract_time_ffs] Final df_srce_mtime created")

@patch("src.tp_utils.common.get_logger")
def test_extract_time_ffs_missing_value_column(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_raw1 = spark.createDataFrame([("R12345678",)], ["vendor"])  # Missing 'value' column

    with pytest.raises(Exception):
        extract_time_ffs(df_raw1)

@patch("src.tp_utils.common.get_logger")
def test_extract_time_ffs_no_vendor_starting_with_R(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    data = [("X12345678SomeAttributeDataSomeDescriptionData",)]
    df_raw1 = spark.createDataFrame(data, ["value"])

    result_df = extract_time_ffs(df_raw1)

    assert result_df.count() == 0  # No rows should match vendor starting with 'R'

@patch("src.tp_utils.common.get_logger")
def test_extract_time_ffs_trimming_columns(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    data = [(" R12345678   SomeAttributeData   SomeDescriptionData   ",)]
    df_raw1 = spark.createDataFrame(data, ["value"])

    result_df = extract_time_ffs(df_raw1)

    # Check that trimming worked
    for row in result_df.collect():
        for value in row:
            assert isinstance(value, (str, int))
            if isinstance(value, str):
                assert value == value.strip()
