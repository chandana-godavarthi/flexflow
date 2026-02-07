import pytest
from unittest.mock import MagicMock
from src.tp_utils.common import dynamic_expression

def test_expression_found_for_contract():
    df_exprn_lkp = MagicMock()
    spark = MagicMock()

    mock_df = MagicMock()
    mock_df.select.return_value.first.return_value = {'exprn_val': 'col1 + col2'}
    spark.sql.return_value = mock_df

    result = dynamic_expression(df_exprn_lkp, 100, 'expr1', 'dim1', spark)
    assert result == "f'''col1 + col2'''"

def test_fallback_to_default_contract():
    df_exprn_lkp = MagicMock()
    spark = MagicMock()

    mock_df_empty = MagicMock()
    mock_df_empty.select.return_value.first.return_value = None

    mock_df_default = MagicMock()
    mock_df_default.select.return_value.first.return_value = {'exprn_val': 'col3 * col4'}

    spark.sql.side_effect = [mock_df_empty, mock_df_default]

    result = dynamic_expression(df_exprn_lkp, 999, 'expr1', 'dim1', spark)
    assert result == "f'''col3 * col4'''"

def test_no_expression_found():
    df_exprn_lkp = MagicMock()
    spark = MagicMock()

    mock_df = MagicMock()
    mock_df.select.return_value.first.return_value = None

    spark.sql.side_effect = [mock_df, mock_df]

    with pytest.raises(TypeError, match=".*"):
        dynamic_expression(df_exprn_lkp, 100, 'expr1', 'dim1', spark)
