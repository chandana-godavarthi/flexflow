import unittest
from unittest.mock import MagicMock
from src.tp_utils.common import mm_time_perd_assoc_tier1_vw

class TestMmTimePerdAssocTier1VW(unittest.TestCase):

    def setUp(self):
        # Fresh mock SparkSession and DataFrame for each test
        self.mock_spark = MagicMock()
        self.mock_df = MagicMock()
        self.mock_spark.sql.return_value = self.mock_df

        # Dummy inputs (not used in SQL logic but required by function signature)
        self.mock_mm_time_perd_assoc = MagicMock()
        self.mock_mm_time_perd_fdim = MagicMock()
        self.mock_mm_time_perd_assoc_type = MagicMock()

    def test_query_execution_success(self):
        """Test that the SQL query executes successfully and returns a DataFrame."""
        result_df = mm_time_perd_assoc_tier1_vw(
            self.mock_spark,
            self.mock_mm_time_perd_assoc,
            self.mock_mm_time_perd_fdim,
            self.mock_mm_time_perd_assoc_type
        )

        self.mock_spark.sql.assert_called_once()
        self.assertEqual(result_df, self.mock_df)

    def test_query_execution_failure(self):
        """Test that an exception is raised when spark.sql fails."""
        self.mock_spark.sql.side_effect = Exception("SQL execution failed")

        with self.assertRaises(Exception) as context:
            mm_time_perd_assoc_tier1_vw(
                self.mock_spark,
                self.mock_mm_time_perd_assoc,
                self.mock_mm_time_perd_fdim,
                self.mock_mm_time_perd_assoc_type
            )

        self.assertIn("SQL execution failed", str(context.exception))