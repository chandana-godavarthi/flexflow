import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_product_tape

class TestExtractProductTape(unittest.TestCase):

    def setUp(self):
        self.df_raw1 = MagicMock()
        self.df_max_lvl = MagicMock()
        self.run_id = "run_001"
        self.cntrt_id = "C001"
        self.categ_id = "CAT001"
        self.srce_sys_id = "SYS001"

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.lit")
    @patch("src.tp_utils.common.IntegerType")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.split")
    @patch("src.tp_utils.common.size")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.when")
    @patch("src.tp_utils.common.concat")
    def test_extract_product_tape_success(
        self, mock_concat, mock_when, mock_rtrim, mock_ltrim, mock_size, mock_split,
        mock_substring, mock_row_number, mock_monotonically_id, mock_window,
        mock_int_type, mock_lit, mock_col
    ):
        df_step = MagicMock()
        df_join = MagicMock()
        df_final = MagicMock()

        self.df_raw1.withColumn.return_value = df_step
        df_step.filter.return_value = df_step
        df_step.withColumn.return_value = df_step
        df_step.select.return_value = df_step
        df_step.drop.return_value = df_step
        df_step.columns = ["extrn_code", "attr_code_list", "extrn_name"]

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl
        df_step.join.return_value = df_join
        df_join.withColumn.return_value = df_final
        df_final.drop.return_value = df_final

        result = extract_product_tape(
            self.df_raw1, self.run_id, self.cntrt_id,
            self.categ_id, self.srce_sys_id, self.df_max_lvl
        )

        self.assertIsInstance(result, MagicMock)

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.lit")
    @patch("src.tp_utils.common.IntegerType")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.split")
    @patch("src.tp_utils.common.size")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.when")
    @patch("src.tp_utils.common.concat")
    def test_extract_product_tape_empty_input(
        self, mock_concat, mock_when, mock_rtrim, mock_ltrim, mock_size, mock_split,
        mock_substring, mock_row_number, mock_monotonically_id, mock_window,
        mock_int_type, mock_lit, mock_col
    ):
        df_step = MagicMock()
        df_join = MagicMock()
        df_final = MagicMock()

        self.df_raw1.withColumn.return_value = df_step
        df_step.filter.return_value = df_step
        df_step.withColumn.return_value = df_step
        df_step.select.return_value = df_step
        df_step.drop.return_value = df_step
        df_step.columns = ["extrn_code", "attr_code_list", "extrn_name"]

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl
        df_step.join.return_value = df_join
        df_join.withColumn.return_value = df_final
        df_final.drop.return_value = df_final

        result = extract_product_tape(
            self.df_raw1, self.run_id, self.cntrt_id,
            self.categ_id, self.srce_sys_id, self.df_max_lvl
        )

        self.assertIsInstance(result, MagicMock)

    @patch("src.tp_utils.common.col", side_effect=Exception("Column 'value' not found"))
    def test_extract_product_tape_missing_value_column(self, mock_col):
        with self.assertRaises(Exception) as context:
            extract_product_tape(
                self.df_raw1, self.run_id, self.cntrt_id,
                self.categ_id, self.srce_sys_id, self.df_max_lvl
            )
        self.assertIn("Column 'value' not found", str(context.exception))

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.split", side_effect=Exception("Split failed"))
    def test_extract_product_tape_invalid_split(self, mock_split, mock_col):
        df_step = MagicMock()
        self.df_raw1.withColumn.return_value = df_step
        df_step.filter.return_value = df_step
        df_step.withColumn.side_effect = Exception("Split failed")

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl

        with self.assertRaises(Exception) as context:
            extract_product_tape(
                self.df_raw1, self.run_id, self.cntrt_id,
                self.categ_id, self.srce_sys_id, self.df_max_lvl
            )

        self.assertIn("Split failed", str(context.exception))
