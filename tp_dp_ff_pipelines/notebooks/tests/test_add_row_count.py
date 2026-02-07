import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import add_row_count

class TestAddRowCount(unittest.TestCase):
    def setUp(self):
        self.df = MagicMock()
        self.spark = MagicMock()
        self.schema = "public"
        self.run_id = 123
        self.cntrt_id = 456
        self.jdbc_url = "jdbc_url"
        self.db_name = "ref_db"
        self.db_user = "user"
        self.db_pwd = "pwd"


    # @patch("src.tp_utils.common.write_to_postgres")
    # @patch("src.tp_utils.common.read_query_from_postgres")
    # def test_add_row_count_new_run_id(self, mock_read, mock_write):
    #     mock_df_run = MagicMock()
    #     mock_df_run.count.return_value = 0
    #     mock_read.return_value = mock_df_run

    #     df_selected = MagicMock()
    #     df_with_run_id = MagicMock()
    #     df_with_cntrt_id = MagicMock()

    #     self.df.select.return_value = df_selected
    #     df_selected.withColumn.return_value = df_with_run_id
    #     df_with_run_id.withColumn.return_value = df_with_cntrt_id

    #     add_row_count(1000, self.schema, self.run_id, self.cntrt_id,
    #                   self.spark, self.jdbc_url, self.db_name, self.db_user, self.db_pwd)
    
    #     mock_write.assert_called_once_with(1000, f"{self.schema}.mm_run_detl_plc",
    #                                        self.jdbc_url, self.db_name, self.db_user, self.db_pwd)

    @patch("src.tp_utils.common.write_to_postgres")
    @patch("src.tp_utils.common.read_query_from_postgres")
    def test_add_row_count_existing_run_id(self, mock_read, mock_write):
        mock_df_run = MagicMock()
        mock_df_run.count.return_value = 1
        mock_read.return_value = mock_df_run

        add_row_count(self.df, self.schema, self.run_id, self.cntrt_id,
                      self.spark, self.jdbc_url, self.db_name, self.db_user, self.db_pwd)

        mock_write.assert_not_called()

    @patch("src.tp_utils.common.write_to_postgres")
    @patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB read failed"))
    def test_add_row_count_read_exception(self, mock_read, mock_write):
        with self.assertRaises(Exception) as context:
            add_row_count(self.df, self.schema, self.run_id, self.cntrt_id,
                          self.spark, self.jdbc_url, self.db_name, self.db_user, self.db_pwd)
        self.assertIn("DB read failed", str(context.exception))
        mock_write.assert_not_called()

    @patch("src.tp_utils.common.write_to_postgres", side_effect=Exception("Write failed"))
    @patch("src.tp_utils.common.read_query_from_postgres")
    def test_add_row_count_write_exception(self, mock_read, mock_write):
        mock_df_run = MagicMock()
        mock_df_run.count.return_value = 0
        mock_read.return_value = mock_df_run

        df_selected = MagicMock()
        df_with_run_id = MagicMock()
        df_with_cntrt_id = MagicMock()

        self.df.select.return_value = df_selected
        df_selected.withColumn.return_value = df_with_run_id
        df_with_run_id.withColumn.return_value = df_with_cntrt_id

        with self.assertRaises(Exception) as context:
            add_row_count(self.df, self.schema, self.run_id, self.cntrt_id,
                          self.spark, self.jdbc_url, self.db_name, self.db_user, self.db_pwd)
        self.assertIn("Write failed", str(context.exception))