import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_mm_cntrt_categ_assoc

class TestLoadMmCntrtCategAssoc(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.postgres_schema = "mock_schema"
        self.cntrt_id = 456
        self.ref_db_jdbc_url = "jdbc:mock"
        self.ref_db_name = "mock_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"

    @patch("src.tp_utils.common.read_from_postgres")
    def test_load_mm_cntrt_categ_assoc_success(self, mock_read_from_postgres):
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_selected_df = MagicMock()

        mock_read_from_postgres.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df

        result = load_mm_cntrt_categ_assoc(
            self.spark,
            self.postgres_schema,
            self.cntrt_id,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        query = f"{self.postgres_schema}.mm_cntrt_categ_assoc"
        mock_read_from_postgres.assert_called_once_with(
            query, self.spark, self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd
        )
        mock_df.filter.assert_called_once_with(f"cntrt_id = {self.cntrt_id}")
        mock_filtered_df.select.assert_called_once_with("categ_id")
        self.assertEqual(result, mock_selected_df)

    @patch("src.tp_utils.common.read_from_postgres")
    def test_load_mm_cntrt_categ_assoc_empty_result(self, mock_read_from_postgres):
        # Simulate empty DataFrame
        mock_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_selected_df = MagicMock()
        mock_selected_df.count.return_value = 0  # simulate empty result

        mock_read_from_postgres.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df

        result = load_mm_cntrt_categ_assoc(
            self.spark,
            self.postgres_schema,
            self.cntrt_id,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        self.assertEqual(result.count(), 0)
        mock_read_from_postgres.assert_called_once()
        mock_df.filter.assert_called_once()
        mock_filtered_df.select.assert_called_once_with("categ_id")