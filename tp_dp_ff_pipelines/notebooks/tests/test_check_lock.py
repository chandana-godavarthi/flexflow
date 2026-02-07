import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    return spark

@patch("src.tp_utils.common.time.sleep", return_value=None)
def test_check_lock_acquire(mock_sleep, mock_spark):
    # Mock the DataFrame returned by spark.sql
    mock_df = MagicMock()
    mock_df.count.return_value = 0
    mock_spark.sql.return_value = mock_df

    # Call the function
    result = common.check_lock(1, "'/some/path'", 'catalog_name', mock_spark)

    # Assert result and SQL call
    assert result == "'/some/path'"
    assert mock_spark.sql.call_count == 2  # one for SELECT, one for UPDATE
    # Check the UPDATE query got called
    update_query = """
            UPDATE catalog_name.internal_tp.tp_run_lock_plc 
            SET lock_sttus = true 
            WHERE run_id = 1 AND lock_path IN ('/some/path')
        """
    mock_spark.sql.assert_any_call(update_query)
    mock_sleep.assert_not_called()


@patch("src.tp_utils.common.time.sleep", return_value=None)
def test_check_lock_wait_and_retry(mock_sleep, mock_spark):
    # First call returns DataFrame with count 1, then 0
    mock_df1 = MagicMock()
    mock_df1.count.return_value = 1
    mock_df2 = MagicMock()
    mock_df2.count.return_value = 0

    mock_spark.sql.side_effect = [mock_df1, mock_df2, None]  # 2 SELECTs, 1 UPDATE

    # Call the function
    result = common.check_lock(2, "'/another/path'", 'catalog_name', mock_spark)

    # Assert result
    assert result == "'/another/path'"
    # Total calls: 
    # First SELECT → count=1
    # Second SELECT after recursion → count=0
    # Then UPDATE
    assert mock_spark.sql.call_count == 3
    mock_sleep.assert_called_once_with(30)


def test_check_lock_recursive_chain(mock_spark):
    # Setup the mock to always return count=0
    mock_df = MagicMock()
    mock_df.count.return_value = 0
    mock_spark.sql.return_value = mock_df

    with patch("src.tp_utils.common.time.sleep", return_value=None) as mock_sleep:
        result = common.check_lock(3, "'/final/path'", 'catalog_name', mock_spark)
        assert result == "'/final/path'"
        mock_sleep.assert_not_called()
        assert mock_spark.sql.call_count == 2


@patch("src.tp_utils.common.time.sleep", return_value=None)
def test_check_lock_multiple_waits(mock_sleep, mock_spark):
    # Simulate 2 consecutive waits before acquiring lock
    mock_df1 = MagicMock()
    mock_df1.count.return_value = 1
    mock_df2 = MagicMock()
    mock_df2.count.return_value = 1
    mock_df3 = MagicMock()
    mock_df3.count.return_value = 0

    mock_spark.sql.side_effect = [mock_df1, mock_df2, mock_df3, None]

    result = common.check_lock(4, "'/delayed/path'", 'catalog_name', mock_spark)

    assert result == "'/delayed/path'"
    assert mock_spark.sql.call_count == 4
    assert mock_sleep.call_count == 2
