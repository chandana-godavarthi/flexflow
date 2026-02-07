import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.tp_utils.common import extract_market_tape  # 🔧 Adjust path as needed

class TestExtractMarketTape(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        self.run_id = "RUN123"
        self.cntrt_id = "CNTRT456"
        self.categ_id = "CAT789"
        self.srce_sys_id = "SRC001"
        self.strct_code = "STRCT002"


    @patch("src.tp_utils.common.get_logger")
    def test_malformed_value_column(self, mock_logger):
        mock_logger.return_value = MagicMock()
    
        # Simulate a row with a short 'value' string
        data = [Row(value="short")]
        df_raw1 = self.spark.createDataFrame(data)
    
        result_df = extract_market_tape(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)
    
        # Expecting no rows to be processed
        self.assertEqual(result_df.count(), 0)

    @patch("src.tp_utils.common.get_logger")
    def test_missing_value_column(self, mock_logger):
        mock_logger.return_value = MagicMock()

        # Simulate a DataFrame without 'value' column
        data = [Row(dummy="no_value_here")]
        df_raw1 = self.spark.createDataFrame(data)

        with self.assertRaises(Exception):
            extract_market_tape(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

if __name__ == "__main__":
    unittest.main()