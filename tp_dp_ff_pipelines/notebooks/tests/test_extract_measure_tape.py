import pytest
from unittest.mock import MagicMock
from src.tp_utils import common

@pytest.fixture(autouse=True)
def mock_pyspark_functions(monkeypatch):
    monkeypatch.setattr(common, 'col', MagicMock(name="col"))
    monkeypatch.setattr(common, 'substring', MagicMock(name="substring"))
    monkeypatch.setattr(common, 'row_number', MagicMock(name="row_number"))
    monkeypatch.setattr(common, 'Window', MagicMock(name="Window"))
    monkeypatch.setattr(common, 'monotonically_increasing_id', MagicMock(name="monotonically_increasing_id"))
    monkeypatch.setattr(common, 'ltrim', MagicMock(name="ltrim"))
    monkeypatch.setattr(common, 'rtrim', MagicMock(name="rtrim"))

# Scenario 1: Basic transformation pipeline
def test_extract_measure_tape_pipeline_applied():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df

    common.Window.orderBy.return_value = MagicMock(name="WindowSpec")
    common.row_number.return_value.over.return_value = MagicMock(name="RowNumberColumn")

    result_df = common.extract_measure_tape(mock_df)

    assert mock_df.withColumn.call_count >= 5
    assert mock_df.filter.called
    assert mock_df.drop.call_count >= 2
    assert result_df is not None

# Scenario 2: Empty DataFrame input
def test_extract_measure_tape_empty_df():
    mock_df = MagicMock(name="EmptyDataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.count.return_value = 0

    result_df = common.extract_measure_tape(mock_df)

    assert result_df is not None
    mock_df.filter.assert_called()

# Scenario 3: Ensure correct column names are used
def test_extract_measure_tape_column_usage():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df

    result_df = common.extract_measure_tape(mock_df)

    common.col.assert_any_call("value")
    mock_df.withColumn.assert_any_call("extrn_code", common.substring(common.col("value"), 12, 37))
    mock_df.withColumn.assert_any_call("extrn_name", common.substring(common.col("value"), 49, 80))

# Scenario 4: Ensure trimming functions are applied
def test_extract_measure_tape_trimming_applied():
    mock_df = MagicMock(name="DataFrame")
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df

    result_df = common.extract_measure_tape(mock_df)

    mock_df.withColumn.assert_any_call("extrn_code", common.ltrim("extrn_code"))
    mock_df.withColumn.assert_any_call("extrn_code", common.rtrim("extrn_code"))
    mock_df.withColumn.assert_any_call("extrn_name", common.ltrim("extrn_name"))
    mock_df.withColumn.assert_any_call("extrn_name", common.rtrim("extrn_name"))