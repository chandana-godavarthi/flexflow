import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from src.tp_utils.common import extract_market_ffs  # 🔧 Adjust import path as needed

class TestExtractcommonFFS(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        self.run_id = "RUN123"
        self.cntrt_id = "CNTRT456"
        self.categ_id = "CAT789"
        self.srce_sys_id = "SYS001"
        self.strct_code = "STRCT01"

    @patch("src.tp_utils.common.get_logger")
    def test_successful_execution(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create mock DataFrame with valid 'G' row
        data = [Row(value="G123456789SomeNameHereThatIsLongEnoughToPassSubstringChecks     Y1234567890")]
        df_raw1 = self.spark.createDataFrame(data)

        result_df = extract_market_ffs(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)


        self.assertGreater(result_df.count(), 0)
        self.assertIn("extrn_mkt_name", result_df.columns)
        self.assertEqual(result_df.select("run_id").first()["run_id"], self.run_id)

    @patch("src.tp_utils.common.get_logger")
    def test_missing_value_column(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create DataFrame without 'value' column
        data = [Row(dummy="G123456789")]
        df_raw1 = self.spark.createDataFrame(data)

        with self.assertRaises(Exception):
            extract_market_ffs(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

    @patch("src.tp_utils.common.get_logger")
    def test_malformed_value_string(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create DataFrame with too short 'value' string
        data = [Row(value="G1")]
        df_raw1 = self.spark.createDataFrame(data)

        result_df = extract_market_ffs(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

        # Should still return a DataFrame but with empty or default values
        self.assertIn("extrn_mkt_name", result_df.columns)
        self.assertEqual(result_df.select("run_id").first()["run_id"], self.run_id)

if __name__ == "__main__":
    unittest.main()