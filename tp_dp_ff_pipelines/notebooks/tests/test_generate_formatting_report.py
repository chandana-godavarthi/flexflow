import unittest
from unittest.mock import patch, MagicMock
import os
from src.tp_utils.common import generate_formatting_report
class TestGenerateFormattingReport(unittest.TestCase):

    @patch('src.tp_utils.common.get_logger')
    @patch('src.tp_utils.common.openpyxl.load_workbook')
    @patch('src.tp_utils.common.copyfile')
    @patch('src.tp_utils.common.os.remove')
    def test_successful_formatting(self, mock_remove, mock_copyfile, mock_load_workbook, mock_get_logger):
        # Setup
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_wb = MagicMock()
        mock_ws_summary = MagicMock()
        mock_ws_vd = MagicMock()
        mock_wb.__getitem__.side_effect = lambda name: {'SUMMARY': mock_ws_summary, 'VALIDATION_DETAILS': mock_ws_vd}[name]
        mock_wb.sheetnames = ['SUMMARY', 'VALIDATION_DETAILS']
        mock_load_workbook.return_value = mock_wb

        from src.tp_utils.common import generate_formatting_report
        generate_formatting_report('12345', 'test_catalog')

        mock_load_workbook.assert_called_once()
        mock_wb.save.assert_called_once()
        mock_copyfile.assert_called_once()
        mock_remove.assert_called_once()
        mock_logger.info.assert_any_call("Starting formatting of the report.")

    @patch('src.tp_utils.common.get_logger')
    @patch('src.tp_utils.common.openpyxl.load_workbook', side_effect=FileNotFoundError)
    def test_workbook_not_found(self, mock_load_workbook, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        from src.tp_utils.common import generate_formatting_report
        with self.assertRaises(FileNotFoundError):
            generate_formatting_report('12345', 'test_catalog')

    @patch('src.tp_utils.common.get_logger')
    @patch('src.tp_utils.common.openpyxl.load_workbook')
    @patch('src.tp_utils.common.copyfile')
    @patch('src.tp_utils.common.os.remove')
    def test_conditional_formatting_applied(self, mock_remove, mock_copyfile, mock_load_workbook, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_ws_summary = MagicMock()
        mock_ws_vd = MagicMock()
        mock_wb = MagicMock()
        mock_wb.__getitem__.side_effect = lambda name: {'SUMMARY': mock_ws_summary, 'VALIDATION_DETAILS': mock_ws_vd}[name]
        mock_wb.sheetnames = ['SUMMARY', 'VALIDATION_DETAILS']
        mock_load_workbook.return_value = mock_wb

        from src.tp_utils.common import generate_formatting_report
        generate_formatting_report('12345', 'test_catalog')

        self.assertTrue(mock_ws_summary.conditional_formatting.add.called)

    @patch('src.tp_utils.common.get_logger')
    @patch('src.tp_utils.common.openpyxl.load_workbook')
    @patch('src.tp_utils.common.copyfile')
    @patch('src.tp_utils.common.os.remove')
    def test_path_traversal_protection(self, mock_remove, mock_copyfile, mock_load_workbook, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_ws_summary = MagicMock()
        mock_ws_vd = MagicMock()
        mock_wb = MagicMock()
        mock_wb.__getitem__.side_effect = lambda name: {'SUMMARY': mock_ws_summary, 'VALIDATION_DETAILS': mock_ws_vd}[name]
        mock_wb.sheetnames = ['SUMMARY', 'VALIDATION_DETAILS']
        mock_load_workbook.return_value = mock_wb

        from src.tp_utils.common import generate_formatting_report
        generate_formatting_report('12345', 'test_catalog')

        mock_remove.assert_called_once()
