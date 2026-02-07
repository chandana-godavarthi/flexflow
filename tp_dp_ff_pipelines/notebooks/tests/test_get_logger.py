import unittest
from unittest.mock import patch, MagicMock
import logging

# Assuming get_logger is imported from the module where it's defined
from src.tp_utils.common import get_logger

class TestGetLogger(unittest.TestCase):

    @patch('src.tp_utils.common.logging.getLogger')
    def test_logger_creation_and_configuration(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_logger.hasHandlers.return_value = False

        logger = get_logger()

        mock_get_logger.assert_called_once_with('TP_logger')
        mock_logger.setLevel.assert_called_once_with(logging.INFO)
        self.assertEqual(logger, mock_logger)

    @patch('src.tp_utils.common.logging.getLogger')
    def test_logger_clears_existing_handlers(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_logger.hasHandlers.return_value = True

        get_logger()

        mock_logger.handlers.clear.assert_called_once()

    @patch('src.tp_utils.common.logging.StreamHandler')
    @patch('src.tp_utils.common.logging.getLogger')
    @patch('src.tp_utils.common.logging.Formatter')
    def test_handler_and_formatter_setup(self, mock_formatter, mock_get_logger, mock_stream_handler):
        mock_logger = MagicMock()
        mock_handler = MagicMock()
        mock_formatter_instance = MagicMock()

        mock_get_logger.return_value = mock_logger
        mock_stream_handler.return_value = mock_handler
        mock_formatter.return_value = mock_formatter_instance
        mock_logger.hasHandlers.return_value = False

        get_logger()

        mock_stream_handler.assert_called_once()
        mock_handler.setLevel.assert_called_once_with(logging.INFO)
        mock_formatter.assert_called_once_with('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        mock_handler.setFormatter.assert_called_once_with(mock_formatter_instance)
        mock_logger.addHandler.assert_called_once_with(mock_handler)

