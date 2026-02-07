import pytest
from unittest.mock import MagicMock
from src.tp_utils import common

# Scenario 1: Basic column conversion
def test_convert_cols_to_upper_basic():
    mock_df = MagicMock(name="DataFrame")
    mock_df.columns = ["name", "age", "country"]
    mock_df.select.return_value = mock_df

    result_df = common.convert_cols_to_upper(mock_df)

    mock_df.select.assert_called_once_with("NAME", "AGE", "COUNTRY")
    assert result_df == mock_df

# Scenario 2: Already uppercase columns
def test_convert_cols_to_upper_already_uppercase():
    mock_df = MagicMock(name="DataFrame")
    mock_df.columns = ["NAME", "AGE", "COUNTRY"]
    mock_df.select.return_value = mock_df

    result_df = common.convert_cols_to_upper(mock_df)

    mock_df.select.assert_called_once_with("NAME", "AGE", "COUNTRY")
    assert result_df == mock_df

# Scenario 3: Mixed case and special characters
def test_convert_cols_to_upper_mixed_case_special_chars():
    mock_df = MagicMock(name="DataFrame")
    mock_df.columns = ["Name", "Age", "Country_Code"]
    mock_df.select.return_value = mock_df

    result_df = common.convert_cols_to_upper(mock_df)

    mock_df.select.assert_called_once_with("NAME", "AGE", "COUNTRY_CODE")
    assert result_df == mock_df

# Scenario 4: Empty column list
def test_convert_cols_to_upper_empty_columns():
    mock_df = MagicMock(name="DataFrame")
    mock_df.columns = []
    mock_df.select.return_value = mock_df

    result_df = common.convert_cols_to_upper(mock_df)

    mock_df.select.assert_called_once_with()
    assert result_df == mock_df