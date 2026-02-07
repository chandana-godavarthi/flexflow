import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_time_sff

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.lit")
def test_extract_time_sff_valid(mock_lit, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_df_raw = MagicMock()
    mock_df_renamed = MagicMock()
    mock_df_final = MagicMock()
    mock_lit_return = MagicMock()
    mock_casted_lit = MagicMock()

    mock_df_raw.withColumnRenamed.return_value = mock_df_renamed
    mock_df_renamed.withColumnRenamed.return_value = mock_df_renamed
    mock_df_renamed.withColumnRenamed.return_value = mock_df_renamed

    mock_lit.return_value = mock_lit_return
    mock_lit_return.cast.return_value = mock_casted_lit
    mock_df_renamed.withColumn.return_value = mock_df_final

    result = extract_time_sff(mock_df_raw)

    mock_df_raw.withColumnRenamed.assert_any_call('TAG', 'extrn_code')
    mock_df_renamed.withColumnRenamed.assert_any_call('LONG', 'extrn_name')
    mock_df_renamed.withColumnRenamed.assert_any_call('DISPLAY_ORDER', 'line_num')
    mock_lit.assert_called_once_with(0)
    mock_lit_return.cast.assert_called_once_with("integer")
    mock_df_renamed.withColumn.assert_called_once_with("mm_time_perd_id", mock_casted_lit)
    assert result == mock_df_final

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.lit")
def test_extract_time_sff_missing_columns(mock_lit, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_df_raw = MagicMock()
    mock_df_raw.withColumnRenamed.side_effect = Exception("Column not found")

    with pytest.raises(Exception, match="Column not found"):
        extract_time_sff(mock_df_raw)

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.lit")
def test_extract_time_sff_empty_dataframe(mock_lit, mock_get_logger):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_df_raw = MagicMock()
    mock_df_renamed = MagicMock()
    mock_df_final = MagicMock()
    mock_lit_return = MagicMock()
    mock_casted_lit = MagicMock()

    mock_df_raw.withColumnRenamed.return_value = mock_df_renamed
    mock_df_renamed.withColumnRenamed.return_value = mock_df_renamed
    mock_df_renamed.withColumnRenamed.return_value = mock_df_renamed

    mock_lit.return_value = mock_lit_return
    mock_lit_return.cast.return_value = mock_casted_lit
    mock_df_renamed.withColumn.return_value = mock_df_final

    mock_df_final.count.return_value = 0  # simulate empty DataFrame

    result = extract_time_sff(mock_df_raw)
    assert result.count() == 0
