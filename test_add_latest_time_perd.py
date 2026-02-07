import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import add_latest_time_perd

class TestAddLatestTimePerd(unittest.TestCase):
    def setUp(self):
        self.df = MagicMock()
        self.schema = "public"
        self.run_id = 123
        self.ref_db_name = "ref_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.ref_db_hostname = "localhost"

    @patch("src.tp_utils.common.update_to_postgres")
    def test_add_latest_time_perd_success(self, mock_update):
        # Mock max date
        max_date_row = MagicMock()
        max_date_row.__getitem__.return_value = "2025-12-31"
        self.df.select.return_value.first.return_value = max_date_row

        # Mock latest time_perd_id
        latest_perd_row = MagicMock()
        latest_perd_row.__getitem__.return_value = "TP456"
        self.df.filter.return_value.select.return_value.first.return_value = latest_perd_row

        add_latest_time_perd(self.df, self.schema, self.run_id,
                             self.ref_db_name, self.ref_db_user, self.ref_db_pwd, self.ref_db_hostname)

        expected_query = f"UPDATE {self.schema}.mm_run_detl_plc SET latst_time_perd_id = 'TP456' WHERE run_id = {self.run_id} "
        mock_update.assert_called_once_with(expected_query, (), self.ref_db_name, self.ref_db_user, self.ref_db_pwd, self.ref_db_hostname)

    @patch("src.tp_utils.common.update_to_postgres")
    def test_add_latest_time_perd_no_date_found(self, mock_update):
        # Simulate None for both max_date and latest_perd_id
        none_row = MagicMock()
        none_row.__getitem__.return_value = None
        self.df.select.return_value.first.return_value = none_row
        self.df.filter.return_value.select.return_value.first.return_value = none_row

        add_latest_time_perd(self.df, self.schema, self.run_id,
                             self.ref_db_name, self.ref_db_user, self.ref_db_pwd, self.ref_db_hostname)

        expected_query = f"UPDATE {self.schema}.mm_run_detl_plc SET latst_time_perd_id = 'None' WHERE run_id = {self.run_id} "
        mock_update.assert_called_once_with(expected_query, (), self.ref_db_name, self.ref_db_user, self.ref_db_pwd, self.ref_db_hostname)

    @patch("src.tp_utils.common.update_to_postgres", side_effect=Exception("Update failed"))
    def test_add_latest_time_perd_update_exception(self, mock_update):
        max_date_row = MagicMock()
        max_date_row.__getitem__.return_value = "2025-12-31"
        self.df.select.return_value.first.return_value = max_date_row

        latest_perd_row = MagicMock()
        latest_perd_row.__getitem__.return_value = "TP456"
        self.df.filter.return_value.select.return_value.first.return_value = latest_perd_row

        with self.assertRaises(Exception) as context:
            add_latest_time_perd(self.df, self.schema, self.run_id,
                                 self.ref_db_name, self.ref_db_user, self.ref_db_pwd, self.ref_db_hostname)
