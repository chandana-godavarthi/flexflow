import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import release_semaphore

class TestReleaseSemaphore(unittest.TestCase):

    def setUp(self):
        self.catalog_name = "mock_catalog"
        self.run_id = 123
        self.lock_path = "/path/to/lock"
        self.sanitized_lock_path = "'/path/to/lock'"
        self.expected_query = f"""
        DELETE FROM {self.catalog_name}.internal_tp.tp_run_lock_plc
        WHERE run_id = {self.run_id}
        AND lock_path IN ({self.sanitized_lock_path})
    """

    def test_release_semaphore_executes_sql_and_prints_query(self):
        # Create a mock Spark session
        mock_spark = MagicMock()

        with patch("src.tp_utils.common.sanitize_filepath", return_value=self.sanitized_lock_path) as mock_sanitize, \
             patch("builtins.print") as mock_print:

            release_semaphore(self.catalog_name, self.run_id, self.lock_path, mock_spark)

            mock_sanitize.assert_called_once_with(self.lock_path)
            mock_print.assert_called_once_with(self.expected_query)
            mock_spark.sql.assert_called_once_with(self.expected_query)
