import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import read_fact

class TestReadFact(unittest.TestCase):

    def setUp(self):
        self.catalog_name = "test_catalog"
        self.time_perd = "202501"
        self.srce_sys_id = 1001
        self.cntrt_id = 1231
        self.spark = MagicMock()

    @patch("src.tp_utils.common.match_time_perd_class")
    @patch("src.tp_utils.common.sanitize_variable")
    def test_read_fact_success(self, mock_sanitize, mock_match_class):
        # Scenario: successful read
        mock_match_class.return_value = "mth"
        mock_sanitize.side_effect = lambda x: f"sanitized_{x}"

        mock_df = MagicMock()
        self.spark.sql.return_value = mock_df

        result = read_fact(
            self.catalog_name,
            self.spark,
            self.time_perd,
            self.srce_sys_id,
            self.cntrt_id
        )

        expected_table = f"{self.catalog_name}.gold_tp.tp_mth_fct"
        expected_query = f"select * from {expected_table} where part_srce_sys_id = :srce_sys_id and part_cntrt_id = :cntrt_id"
        expected_params = {
            "srce_sys_id": "sanitized_1001",
            "cntrt_id": "sanitized_1231"
        }

        mock_match_class.assert_called_once_with(self.time_perd)
        mock_sanitize.assert_any_call(self.srce_sys_id)
        mock_sanitize.assert_any_call(self.cntrt_id)
        self.spark.sql.assert_called_once_with(expected_query, expected_params)
        self.assertEqual(result, mock_df)

    @patch("src.tp_utils.common.match_time_perd_class")
    @patch("src.tp_utils.common.sanitize_variable")
    def test_read_fact_invalid_class_code(self, mock_sanitize, mock_match_class):
        # Scenario: match_time_perd_class throws exception
        mock_match_class.side_effect = Exception("Invalid time period")
        mock_sanitize.side_effect = lambda x: f"sanitized_{x}"

        with self.assertRaises(Exception) as context:
            read_fact(
                self.catalog_name,
                self.spark,
                self.time_perd,
                self.srce_sys_id,
                self.cntrt_id
            )
        self.assertIn("Invalid time period", str(context.exception))

    @patch("src.tp_utils.common.match_time_perd_class")
    @patch("src.tp_utils.common.sanitize_variable")
    def test_read_fact_spark_sql_failure(self, mock_sanitize, mock_match_class):
        # Scenario: spark.sql throws exception
        mock_match_class.return_value = "mth"
        mock_sanitize.side_effect = lambda x: f"sanitized_{x}"
        self.spark.sql.side_effect = Exception("Spark SQL failed")

        with self.assertRaises(Exception) as context:
            read_fact(
                self.catalog_name,
                self.spark,
                self.time_perd,
                self.srce_sys_id,
                self.cntrt_id
            )
        self.assertIn("Spark SQL failed", str(context.exception))

    @patch("src.tp_utils.common.match_time_perd_class")
    @patch("src.tp_utils.common.sanitize_variable")
    def test_read_fact_empty_inputs(self, mock_sanitize, mock_match_class):
        # Scenario: empty srce_sys_id and cntrt_id
        mock_match_class.return_value = "mth"
        mock_sanitize.side_effect = lambda x: f"sanitized_{x}"

        mock_df = MagicMock()
        self.spark.sql.return_value = mock_df

        result = read_fact(
            self.catalog_name,
            self.spark,
            self.time_perd,
            "",  # empty srce_sys_id
            None  # None cntrt_id
        )

        expected_table = f"{self.catalog_name}.gold_tp.tp_mth_fct"
        expected_query = f"select * from {expected_table} where part_srce_sys_id = :srce_sys_id and part_cntrt_id = :cntrt_id"
        expected_params = {
            "srce_sys_id": "sanitized_",
            "cntrt_id": "sanitized_None"
        }

        self.spark.sql.assert_called_once_with(expected_query, expected_params)
        self.assertEqual(result, mock_df)