import pytest
from unittest.mock import MagicMock
from src.tp_utils import common

# Scenario 1: Ensure all expected column renames are applied
def test_extract_measure_sff_column_renaming_chain():
    mock_df = MagicMock(name="mock_df")
    mock_df.withColumnRenamed.side_effect = lambda col, new_col: mock_df

    result_df = common.extract_measure_sff(mock_df)

    expected_calls = [
        ('SHORT', 'extrn_name'),
        ('LONG', 'extrn_long_name'),
        ('DISPLAY_ORDER', 'line_num'),
        ('TAG', 'extrn_code'),
        ('PRECISION', 'precision_val'),
        ('DENOMINATOR', 'denominator_val')
    ]
    actual_calls = [call.args for call in mock_df.withColumnRenamed.call_args_list]
    assert actual_calls == expected_calls
    assert result_df is not None

# Scenario 2: Ensure function returns the same DataFrame object
def test_extract_measure_sff_returns_same_mock():
    mock_df = MagicMock(name="mock_df")
    mock_df.withColumnRenamed.return_value = mock_df

    result_df = common.extract_measure_sff(mock_df)

    assert result_df == mock_df

# Scenario 3: Empty DataFrame input should still return a DataFrame
def test_extract_measure_sff_empty_df():
    mock_df = MagicMock(name="EmptyDataFrame")
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.count.return_value = 0

    result_df = common.extract_measure_sff(mock_df)

    assert result_df is not None
    mock_df.withColumnRenamed.assert_called()

# Scenario 4: Ensure specific column renaming is applied correctly
def test_extract_measure_sff_specific_column_renaming():
    mock_df = MagicMock(name="mock_df")
    mock_df.withColumnRenamed.return_value = mock_df

    result_df = common.extract_measure_sff(mock_df)

    mock_df.withColumnRenamed.assert_any_call('SHORT', 'extrn_name')
    mock_df.withColumnRenamed.assert_any_call('LONG', 'extrn_long_name')
    mock_df.withColumnRenamed.assert_any_call('TAG', 'extrn_code')
