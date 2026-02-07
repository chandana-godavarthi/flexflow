import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_time_tape

class TestExtractTimeTape(unittest.TestCase):

    def setUp(self):
        self.df_raw1 = MagicMock()

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.lit")
    def test_extract_time_tape_success(
        self, mock_lit, mock_rtrim, mock_ltrim, mock_substring,
        mock_row_number, mock_monotonically_id, mock_window,
        mock_col, mock_get_logger
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df_step1 = MagicMock()
        df_step2 = MagicMock()
        df_step3 = MagicMock()
        df_step4 = MagicMock()
        df_step5 = MagicMock()

        self.df_raw1.withColumn.return_value = df_step1
        df_step1.filter.return_value = df_step2
        df_step2.withColumn.return_value = df_step3
        df_step3.withColumn.return_value = df_step3
        df_step3.withColumn.return_value = df_step3
        df_step3.drop.return_value = df_step4
        df_step4.drop.return_value = df_step5
        df_step5.select.return_value = df_step5
        df_step5.withColumn.return_value = df_step5
        df_step5.count.return_value = 10

        result = extract_time_tape(self.df_raw1)

        mock_get_logger.assert_called_once()
        mock_logger.info.assert_any_call("[extract_time_tape] Function execution started.")
        mock_logger.info.assert_any_call("[extract_time_tape] Filtered df_1 with 'ccc=01'")
        mock_logger.info.assert_any_call("[extract_time_tape] Extracted extrn_code and extrn_name")
        mock_logger.info.assert_any_call("[extract_time_tape] Trimmed all columns.")
        mock_logger.info.assert_any_call("[extract_time_tape] Final df_srce_mtime created")
        self.assertEqual(result.count(), 10)

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.lit")
    def test_extract_time_tape_empty_df(
        self, mock_lit, mock_rtrim, mock_ltrim, mock_substring,
        mock_row_number, mock_monotonically_id, mock_window,
        mock_col, mock_get_logger
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df_step = MagicMock()
        self.df_raw1.withColumn.return_value = df_step
        df_step.filter.return_value = df_step
        df_step.withColumn.return_value = df_step
        df_step.drop.return_value = df_step
        df_step.select.return_value = df_step
        df_step.count.return_value = 0

        result = extract_time_tape(self.df_raw1)

        mock_logger.info.assert_any_call("[extract_time_tape] Function execution started.")
        mock_logger.info.assert_any_call("[extract_time_tape] Final df_srce_mtime created")
        self.assertEqual(result.count(), 0)

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.col", side_effect=Exception("Column 'value' not found"))
    def test_extract_time_tape_malformed_value_column(self, mock_col, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df_raw1 = MagicMock()

        with self.assertRaises(Exception) as context:
            extract_time_tape(df_raw1)

        self.assertIn("Column 'value' not found", str(context.exception))
        mock_logger.info.assert_any_call("[extract_time_tape] Function execution started.")
