import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import bad_kpi_check  # 🔧 Adjust path as needed

class TestBadKpiCheck(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.dq = "DQ_TEST"
        self.kpi = "Product"

    def mock_row(self, data_dict):
        row = MagicMock()
        row.asDict.return_value = data_dict
        return row

    @patch("src.tp_utils.common.find_bad_ascii")
    def test_no_bad_ascii(self, mock_find_bad_ascii):
        row = self.mock_row({"col1": "Brand A", "col2": "Market X"})
        df = MagicMock()
        df.columns = ["col1", "col2"]
        df.collect.return_value = [row]
        mock_find_bad_ascii.return_value = df

        self.spark.sql.return_value = df
        self.spark.createDataFrame.return_value = MagicMock()
        result_df = bad_kpi_check(self.spark, self.dq, df, self.kpi)

        self.spark.createDataFrame.assert_called_once()
        self.assertEqual(result_df, self.spark.createDataFrame.return_value)

    @patch("src.tp_utils.common.find_bad_ascii")
    def test_with_bad_ascii(self, mock_find_bad_ascii):
        row = self.mock_row({"col1": "Bränd A", "col2": "Market X"})  # Non-ASCII in col1
        df = MagicMock()
        df.columns = ["col1", "col2"]
        df.collect.return_value = [row]
        mock_find_bad_ascii.return_value = df

        self.spark.sql.return_value = df
        self.spark.createDataFrame.return_value = MagicMock()
        result_df = bad_kpi_check(self.spark, self.dq, df, self.kpi)

        self.spark.createDataFrame.assert_called_once()
        self.assertEqual(result_df, self.spark.createDataFrame.return_value)

    @patch("src.tp_utils.common.find_bad_ascii")
    def test_empty_dataframe(self, mock_find_bad_ascii):
        df = MagicMock()
        df.columns = ["col1", "col2"]
        df.collect.return_value = []
        mock_find_bad_ascii.return_value = df

        self.spark.sql.return_value = df
        self.spark.createDataFrame.return_value = MagicMock()
        result_df = bad_kpi_check(self.spark, self.dq, df, self.kpi)

        self.spark.createDataFrame.assert_called_once_with([], unittest.mock.ANY)
        self.assertEqual(result_df, self.spark.createDataFrame.return_value)