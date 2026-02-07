import pytest
from unittest.mock import MagicMock, call
from src.tp_utils import common

@pytest.fixture(autouse=True)
def mock_pyspark_functions(monkeypatch):
    monkeypatch.setattr(common, 'row_number', MagicMock(name="row_number"))
    monkeypatch.setattr(common, 'Window', MagicMock(name="Window"))
    monkeypatch.setattr(common, 'lit', MagicMock(name="lit"))

# Scenario 1: Basic transformation pipeline
def test_extract_measure_sff3_pipeline_applied():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df

    common.Window.orderBy.return_value = MagicMock(name="WindowSpec")
    common.row_number.return_value.over.return_value = MagicMock(name="RowNumberColumn")

    result_df = common.extract_measure_sff3(mock_df)

    assert mock_df.withColumn.call_count >= 2
    assert mock_df.withColumnRenamed.call_count >= 4
    assert result_df is not None

# Scenario 2: Empty DataFrame input
def test_extract_measure_sff3_empty_df():
    mock_df = MagicMock(name="EmptyDataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.count.return_value = 0

    result_df = common.extract_measure_sff3(mock_df)

    assert result_df is not None
    mock_df.withColumn.assert_called()
    mock_df.withColumnRenamed.assert_called()

# Scenario 3: Ensure correct renaming of columns (based on actual function logic)
def test_extract_measure_sff3_column_renaming():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.side_effect = lambda col1, col2: mock_df

    result_df = common.extract_measure_sff3(mock_df)

    expected_calls = [
        call('fact_description', 'extrn_name'),
        call('fact_description', 'extrn_long_name'),
        call('DISPLAY_ORDER', 'line_num'),
        call('fact_column', 'TAG'),
        call('SHORT', 'extrn_name'),
        call('LONG', 'extrn_long_name'),
        call('DISPLAY_ORDER', 'line_num'),
        call('TAG', 'extrn_code')
    ]

    mock_df.withColumnRenamed.assert_has_calls(expected_calls, any_order=False)

# Scenario 4: Ensure precision_val column is added
def test_extract_measure_sff3_precision_column_added():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df

    result_df = common.extract_measure_sff3(mock_df)

