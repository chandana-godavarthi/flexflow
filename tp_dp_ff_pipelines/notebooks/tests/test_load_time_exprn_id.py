import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_time_exprn_id

class TestLoadTimeExprnId(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.postgres_schema = "mock_schema"
        self.time_exprn_id = 789
        self.ref_db_jdbc_url = "jdbc:mock"
        self.ref_db_name = "mock_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.expected_columns = ['start_date_val', 'end_date_val', 'time_perd_type_val']

    @patch("src.tp_utils.common.read_from_postgres")
    def test_load_time_exprn_id_success(self, mock_read_from_postgres):
        # Arrange
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_selected_df = MagicMock()

        mock_read_from_postgres.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df

        # Act
        result = load_time_exprn_id(
            self.time_exprn_id,
            self.postgres_schema,
            self.spark,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        # Assert
        table_name = f"{self.postgres_schema}.mm_time_exprn_lkp"
        mock_read_from_postgres.assert_called_once_with(
            table_name, self.spark, self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd
        )
        mock_df.filter.assert_called_once_with(f"time_exprn_id= {self.time_exprn_id}")
        mock_filtered_df.select.assert_called_once_with(*self.expected_columns)
        self.assertEqual(result, mock_selected_df)

    @patch("src.tp_utils.common.read_from_postgres")
    def test_load_time_exprn_id_empty_result(self, mock_read_from_postgres):
        # Arrange
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_selected_df = MagicMock()
        mock_selected_df.count.return_value = 0

        mock_read_from_postgres.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df

        # Act
        result = load_time_exprn_id(
            self.time_exprn_id,
            self.postgres_schema,
            self.spark,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        # Assert
        self.assertEqual(result.count(), 0)
        mock_read_from_postgres.assert_called_once()
        mock_df.filter.assert_called_once()
        mock_filtered_df.select.assert_called_once_with(*self.expected_columns)
