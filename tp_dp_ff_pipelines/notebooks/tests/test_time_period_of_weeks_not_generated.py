import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.tp_utils.common import time_period_of_weeks_not_generated

class TestTimePeriodOfWeeksNotGenerated(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock(spec=SparkSession)
        self.run_id = "RUN123"
        self.cntrt_id = "CNTRT456"
        self.file_name = "file.parquet"
        self.ref_db_jdbc_url = "jdbc:postgresql://localhost:5432/ref_db"
        self.ref_db_name = "ref_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.catalog_name = "catalog"
        self.postgres_schema = "public"

    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.time_perd_class_codes")
    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.mm_time_perd_assoc_tier1_vw")
    def test_week_class_code_query_execution(
        self, mock_assoc_vw, mock_dq_query, mock_read_pg, mock_materialise_path,
        mock_time_class, mock_cntrt_lkp
    ):
        # Scenario: time_perd_class_code = 'WK'
        mock_row = MagicMock()
        mock_row.cntrt_code = "C123"
        mock_row.cntry_name = "India"
        mock_row.categ_name = "Retail"
        mock_row.srce_sys_id = "SYS1"
        mock_row.time_perd_type_code = "WEEK"
        mock_row.vendr_id = "V123"
        mock_cntrt_lkp.return_value.collect.return_value = [mock_row]

        mock_time_class.return_value = "WK"
        mock_materialise_path.return_value = "/mnt/data"
        mock_read_pg.return_value = MagicMock()
        mock_assoc_vw.return_value = MagicMock()
        mock_dq_query.return_value.collect.return_value = [{"qry_txt": "SELECT 1 AS test_col"}]

        mock_df = MagicMock()
        self.spark.sql.return_value = mock_df

        result_df = time_period_of_weeks_not_generated(
            self.run_id, self.cntrt_id, self.file_name, self.spark,
            self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
            self.catalog_name, self.postgres_schema
        )

        self.spark.sql.assert_called_with("SELECT 1 AS test_col")
        self.assertEqual(result_df, mock_df)

    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.time_perd_class_codes")
    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.mm_time_perd_assoc_tier1_vw")
    def test_non_week_class_code_query_execution(
        self, mock_assoc_vw, mock_dq_query, mock_read_pg, mock_materialise_path,
        mock_time_class, mock_cntrt_lkp
    ):
        # Scenario: time_perd_class_code != 'WK'
        mock_row = MagicMock()
        mock_row.cntrt_code = "C123"
        mock_row.cntry_name = "India"
        mock_row.categ_name = "Retail"
        mock_row.srce_sys_id = "SYS1"
        mock_row.time_perd_type_code = "MONTH"
        mock_row.vendr_id = "V123"
        mock_cntrt_lkp.return_value.collect.return_value = [mock_row]

        mock_time_class.return_value = "MN"
        mock_materialise_path.return_value = "/mnt/data"
        mock_read_pg.return_value = MagicMock()
        mock_assoc_vw.return_value = MagicMock()
        mock_dq_query.return_value.collect.return_value = [{"qry_txt": "SELECT 2 AS test_col"}]

        mock_df = MagicMock()
        self.spark.sql.return_value = mock_df

        result_df = time_period_of_weeks_not_generated(
            self.run_id, self.cntrt_id, self.file_name, self.spark,
            self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
            self.catalog_name, self.postgres_schema
        )

        self.spark.sql.assert_called_with("SELECT 2 AS test_col")
        self.assertEqual(result_df, mock_df)

    @patch("src.tp_utils.common.load_cntrt_lkp")
    def test_missing_contract_lookup(self, mock_cntrt_lkp):
        # Scenario: contract lookup returns empty
        mock_cntrt_lkp.return_value.collect.return_value = []

        with self.assertRaises(IndexError):
            time_period_of_weeks_not_generated(
                self.run_id, self.cntrt_id, self.file_name, self.spark,
                self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
                self.catalog_name, self.postgres_schema
            )

    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.time_perd_class_codes")
    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.mm_time_perd_assoc_tier1_vw")
    def test_invalid_query_text(
        self, mock_assoc_vw, mock_dq_query, mock_read_pg, mock_materialise_path,
        mock_time_class, mock_cntrt_lkp
    ):
        # Scenario: invalid SQL query string
        mock_row = MagicMock()
        mock_row.cntrt_code = "C123"
        mock_row.cntry_name = "India"
        mock_row.categ_name = "Retail"
        mock_row.srce_sys_id = "SYS1"
        mock_row.time_perd_type_code = "WEEK"
        mock_row.vendr_id = "V123"
        mock_cntrt_lkp.return_value.collect.return_value = [mock_row]

        mock_time_class.return_value = "WK"
        mock_materialise_path.return_value = "/mnt/data"
        mock_read_pg.return_value = MagicMock()
        mock_assoc_vw.return_value = MagicMock()
        mock_dq_query.return_value.collect.return_value = [{"qry_txt": "SELECT * FROM"}]  # incomplete query

        self.spark.sql.side_effect = Exception("Invalid SQL")

        with self.assertRaises(Exception) as context:
            time_period_of_weeks_not_generated(
                self.run_id, self.cntrt_id, self.file_name, self.spark,
                self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
                self.catalog_name, self.postgres_schema
            )