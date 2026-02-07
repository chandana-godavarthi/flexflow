import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Assuming the function is in src.tp_utils.common
from src.tp_utils.common import extract_market_sff

class TestExtractMarketSFF(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        self.run_id = "RUN123"
        self.cntrt_id = "CNTRT456"
        self.categ_id = "CAT789"
        self.srce_sys_id = "SYS001"
        self.strct_code = "STRCT002"

    @patch("src.tp_utils.common.get_logger")
    def test_successful_execution(self, mock_logger):
        mock_logger.return_value = MagicMock()

        schema = StructType([
            StructField("TAG", StringType(), True),
            StructField("SHORT", StringType(), True),
            StructField("LONG", StringType(), True),
            StructField("DISPLAY_ORDER", IntegerType(), True)
        ])

        data = [("EXT001", "A B C D E F G H I J K", "Market Name", 1)]
        df_raw1 = self.spark.createDataFrame(data, schema)

        result_df = extract_market_sff(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

        self.assertEqual(result_df.count(), 1)
        self.assertIn("attr_code_0", result_df.columns)
        self.assertEqual(result_df.select("run_id").first()["run_id"], self.run_id)

    @patch("src.tp_utils.common.get_logger")
    def test_missing_column_handling(self, mock_logger):
        mock_logger.return_value = MagicMock()

        schema = StructType([
            StructField("TAG", StringType(), True),
            StructField("SHORT", StringType(), True),
            StructField("DISPLAY_ORDER", IntegerType(), True)
        ])

        data = [("EXT002", "X Y Z", 2)]
        df_raw1 = self.spark.createDataFrame(data, schema)

        with self.assertRaises(Exception):
            extract_market_sff(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

    @patch("src.tp_utils.common.get_logger")
    def test_short_attr_code_list(self, mock_logger):
        mock_logger.return_value = MagicMock()

        schema = StructType([
            StructField("TAG", StringType(), True),
            StructField("SHORT", StringType(), True),
            StructField("LONG", StringType(), True),
            StructField("DISPLAY_ORDER", IntegerType(), True)
        ])

        data = [("EXT003", "X Y", "Short Market", 3)]
        df_raw1 = self.spark.createDataFrame(data, schema)

        result_df = extract_market_sff(df_raw1, self.run_id, self.cntrt_id, self.categ_id, self.srce_sys_id, self.strct_code)

        self.assertEqual(result_df.select("attr_code_0").first()["attr_code_0"], "X")
        self.assertEqual(result_df.select("attr_code_1").first()["attr_code_1"], "Y")
        self.assertIsNone(result_df.select("attr_code_2").first()["attr_code_2"])

if __name__ == "__main__":
    unittest.main()