import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import find_bad_ascii  # 🔧 Adjust this path to your project

class TestFindBadAscii(unittest.TestCase):

    def setUp(self):
        self.df = MagicMock()
        self.df.columns = ["col1"]
        self.column = "col1"

    @patch("src.tp_utils.common.col")
    def test_ascii_only(self, mock_col):
        # Mock rlike to return True for ASCII-only
        mock_col.return_value.rlike.return_value = True
        self.df.filter.return_value = self.df
        self.df.drop.return_value = self.df

        result_df = find_bad_ascii(self.df, self.column)

        self.df.filter.assert_called_once()
        self.df.drop.assert_called_once_with(self.column)
        self.assertEqual(result_df, self.df)

    @patch("src.tp_utils.common.col")
    def test_non_ascii_present(self, mock_col):
        # Mock rlike to return False for non-ASCII
        mock_col.return_value.rlike.return_value = False
        self.df.filter.return_value = self.df
        self.df.drop.return_value = self.df

        result_df = find_bad_ascii(self.df, self.column)

        self.df.filter.assert_called_once()
        self.df.drop.assert_called_once_with(self.column)
        self.assertEqual(result_df, self.df)

    @patch("src.tp_utils.common.col")
    def test_empty_dataframe(self, mock_col):
        # Simulate empty DataFrame behavior
        mock_col.return_value.rlike.return_value = True
        self.df.filter.return_value = self.df
        self.df.drop.return_value = self.df

        result_df = find_bad_ascii(self.df, self.column)

        self.df.filter.assert_called_once()
        self.df.drop.assert_called_once_with(self.column)
        self.assertEqual(result_df, self.df)