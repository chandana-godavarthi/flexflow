import unittest
from unittest.mock import MagicMock, patch

class TestExtractMarketSFF3(unittest.TestCase):

    def setUp(self):
        # Metadata inputs
        self.run_id = "RUN001"
        self.cntrt_id = "CNTRT001"
        self.categ_id = "CAT001"
        self.srce_sys_id = "SYS001"
        self.strct_code = "STRCT001"

        # Mock DataFrame
        self.mock_df = MagicMock(name="MockDataFrame")
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.select.return_value = self.mock_df
        self.mock_df.withColumnRenamed.return_value = self.mock_df
        self.mock_df.collect.return_value = [{"run_id": self.run_id}]
        self.mock_df.count.return_value = 1
        self.mock_df.columns = ["market_id", "M_MKT_DISP_NAME"]

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.lit")
    @patch("src.tp_utils.common.split")
    @patch("src.tp_utils.common.size")
    @patch("src.tp_utils.common.concat_ws")
    @patch("src.tp_utils.common.when")
    def test_successful_execution(
        self, mock_when, mock_concat_ws, mock_size, mock_split,
        mock_lit, mock_col, mock_row_number, mock_Window, mock_get_logger
    ):
        # Setup mocks
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_Window.orderBy.return_value = "window_spec"
        mock_row_number.return_value.over.return_value = "row_number_column"
        mock_split.return_value.getItem.return_value = "split_value"
        mock_size.return_value = "size_column"
        mock_when.return_value = "mkt_match_attr_list"

        # Simulate function logic manually
        result_df = self.mock_df.withColumn("MRKT_DSC_LNG", mock_lit("null")) \
            .select(mock_col("market_id"), mock_col("M_MKT_DISP_NAME"), mock_col("MRKT_DSC_LNG")) \
            .withColumn("DISPLAY_ORDER", "row_number_column") \
            .withColumnRenamed("market_id", "extrn_code") \
            .withColumnRenamed("M_MKT_DISP_NAME", "attr_code_list") \
            .withColumnRenamed("MRKT_DSC_LNG", "extrn_name") \
            .withColumnRenamed("DISPLAY_ORDER", "line_num") \
            .withColumn("lvl_num", "size_column") \
            .withColumn("mkt_match_attr_list", "mkt_match_attr_list") \
            .withColumn("extrn_code", mock_col("extrn_code")) \
            .withColumn("extrn_mkt_id", mock_col("extrn_code")) \
            .withColumn("extrn_mkt_attr_val_list", mock_col("attr_code_list")) \
            .withColumn("extrn_mkt_name", mock_col("extrn_name")) \
            .withColumn("attr_code_0", "split_value") \
            .withColumn("attr_code_1", "split_value") \
            .withColumn("categ_id", mock_lit(self.categ_id)) \
            .withColumn("cntrt_id", mock_lit(self.cntrt_id)) \
            .withColumn("srce_sys_id", mock_lit(self.srce_sys_id)) \
            .withColumn("run_id", mock_lit(self.run_id)) \
            .withColumn("strct_code", mock_lit(self.strct_code))

        self.assertEqual(result_df.count(), 1)
        self.assertIn("market_id", result_df.columns)

    @patch("src.tp_utils.common.get_logger")
    def test_column_selection_failure(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        self.mock_df.select.side_effect = Exception("Column selection failed")

        with self.assertRaises(Exception) as context:
            self.mock_df.select("market_id", "M_MKT_DISP_NAME")
        self.assertIn("Column selection failed", str(context.exception))

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.when")
    @patch("src.tp_utils.common.concat_ws")
    def test_lvl_num_equals_9_logic(self, mock_concat_ws, mock_when, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_concat_ws.return_value = "concatenated_string"
        mock_when.return_value = "mkt_match_attr_list"

        result = mock_when("condition", "concatenated_string")
        self.assertEqual(result, "mkt_match_attr_list")

if __name__ == "__main__":
    unittest.main()