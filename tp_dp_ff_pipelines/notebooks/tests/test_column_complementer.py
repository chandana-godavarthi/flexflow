import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from src.tp_utils.common import  column_complementer

MODULE_PATH = "src.tp_utils.common"

@patch(f"{MODULE_PATH}.lit")
@patch(f"{MODULE_PATH}.col")
def test_add_missing_columns_and_cast(mock_col, mock_lit):
    mock_lit.return_value.cast.return_value = "mocked_column"
    mock_col.return_value.cast.return_value = "mocked_cast_column"

    df_input = MagicMock(spec=DataFrame)
    df_schema = MagicMock(spec=DataFrame)

    df_input.columns = ['id', 'name']
    df_schema.columns = ['id', 'name', 'department']

    df_input.dtypes = [('id', 'int'), ('name', 'string')]
    df_schema.dtypes = [('id', 'int'), ('name', 'string'), ('department', 'string')]

    df_input.withColumn = MagicMock(return_value=df_input)
    df_input.select = MagicMock(return_value=df_input)

    result = column_complementer(df_input, df_schema)

    df_input.withColumn.assert_any_call('department', "mocked_column")
    df_input.select.assert_called_once()
    assert result == df_input


@patch(f"{MODULE_PATH}.col")
def test_no_missing_columns_and_cast(mock_col):
    mock_col.return_value.cast.return_value = "mocked_cast_column"

    df_input = MagicMock(spec=DataFrame)
    df_schema = MagicMock(spec=DataFrame)

    df_input.columns = ['id', 'name', 'department']
    df_schema.columns = ['id', 'name', 'department']

    df_input.dtypes = [('id', 'int'), ('name', 'string'), ('department', 'string')]
    df_schema.dtypes = [('id', 'int'), ('name', 'string'), ('department', 'string')]

    df_input.select = MagicMock(return_value=df_input)
    df_input.withColumn = MagicMock(return_value=df_input)

    result = column_complementer(df_input, df_schema)

    df_input.select.assert_called_once()
    assert result == df_input
