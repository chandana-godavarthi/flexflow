import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from src.tp_utils import common


@patch("src.tp_utils.common.safe_write_with_retry")
@patch("src.tp_utils.common.lit")
@patch("src.tp_utils.common.current_timestamp")
def test_semaphore_queue_success(mock_timestamp, mock_lit, mock_safe_write):
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.createOrReplaceTempView.return_value = None

    run_id = 1
    paths = ['path1', 'path2']
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    expected_rows = [Row(lock_path='path1'), Row(lock_path='path2')]
    mock_spark.createDataFrame.assert_called_once_with(expected_rows)
    assert mock_df.withColumn.call_count == 3
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")

    table_name = f"{catalog_name}.internal_tp.tp_run_lock_plc"
    mock_safe_write.assert_called_once_with(mock_df, table_name, "append", ["run_id", "lock_path"])

    expected_check_path = "'path1', 'path2'"
    assert result == expected_check_path


@patch("src.tp_utils.common.safe_write_with_retry")
@patch("src.tp_utils.common.lit")
@patch("src.tp_utils.common.current_timestamp")
def test_semaphore_queue_empty_paths(mock_timestamp, mock_lit, mock_safe_write):
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.createOrReplaceTempView.return_value = None

    run_id = 1
    paths = []
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    mock_spark.createDataFrame.assert_called_once_with([])
    assert mock_df.withColumn.call_count == 3
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")

    table_name = f"{catalog_name}.internal_tp.tp_run_lock_plc"
    mock_safe_write.assert_called_once_with(mock_df, table_name, "append", ["run_id", "lock_path"])

    assert result == ""


@patch("src.tp_utils.common.safe_write_with_retry", side_effect=Exception("write fail"))
@patch("src.tp_utils.common.lit")
@patch("src.tp_utils.common.current_timestamp")
def test_semaphore_queue_write_failure(mock_timestamp, mock_lit, mock_safe_write):
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.createOrReplaceTempView.return_value = None

    run_id = 1
    paths = ['path1']
    catalog_name = 'catalog_name'

    with pytest.raises(Exception, match="write fail"):
        common.semaphore_queue(run_id, paths, catalog_name, mock_spark)
