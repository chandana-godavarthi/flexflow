import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils import common

def test_semaphore_acquisition_calls_and_return():
    # Mock spark session
    mock_spark = MagicMock()

    # Mock return values for semaphore_queue and check_lock
    with patch("src.tp_utils.common.semaphore_queue", return_value="'/mock/path'") as mock_queue, \
         patch("src.tp_utils.common.check_lock", return_value="'/final/path'") as mock_check:

        result = common.semaphore_acquisition(99, "'/test/path'", "catalog_name", mock_spark)

        # Assert result is from check_lock
        assert result == "'/final/path'"

        # Assert semaphore_queue called with correct arguments
        mock_queue.assert_called_once_with(99, "'/test/path'", "catalog_name", mock_spark)

        # Assert check_lock called with result from semaphore_queue
        mock_check.assert_called_once_with(99, "'/mock/path'", "catalog_name", mock_spark)
