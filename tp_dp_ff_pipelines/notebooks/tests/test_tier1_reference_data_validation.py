import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import tier1_reference_data_validation

class TestTier1ReferenceDataValidation(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.dbutils = MagicMock()
        self.run_id = "R001"
        self.cntrt_id = "C001"
        self.catalog_name = "test_catalog"
        self.postgres_schema = "ref_schema"
        self.file_name = "source_file.csv"
        self.notebook_name = "tier1_validation"
        self.validation_name = "Tier1 Reference Validation"
        self.jdbc_url = "jdbc:postgresql://localhost:5432/refdb"
        self.db_name = "refdb"
        self.db_user = "user"
        self.db_pwd = "pwd"

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.send_emails")
    @patch("src.tp_utils.common.generate_formatting_report")
    @patch("src.tp_utils.common.generate_report")
    @patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl")
    @patch("src.tp_utils.common.validation", return_value="validation_result")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.mm_time_perd_assoc_tier1_vw", return_value=MagicMock())
    @patch("src.tp_utils.common.read_fact", return_value=MagicMock())
    @patch("src.tp_utils.common.safe_read_and_register", return_value=MagicMock())
    @patch("src.tp_utils.common.time_perd_class_codes_new", return_value="mth")
    @patch("src.tp_utils.common.read_query_from_postgres", return_value=MagicMock())
    @patch("src.tp_utils.common.load_and_register_postgres_view", return_value=MagicMock())
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.get_logger")
    def test_successful_execution(
        self,
        mock_get_logger,
        mock_sanitize,
        mock_load_cntrt_lkp,
        mock_load_pg_view,
        mock_read_pg,
        mock_time_perd_class,
        mock_safe_read,
        mock_read_fact,
        mock_mm_time_perd_assoc,
        mock_dq_query,
        mock_validation,
        mock_merge,
        mock_generate_report,
        mock_generate_formatting,
        mock_send_email
    ):
        mock_cntrt_row = MagicMock()
        mock_cntrt_row.srce_sys_id = "SYS001"
        mock_cntrt_row.vendr_id = "V001"
        mock_cntrt_row.cntry_name = "India"
        mock_cntrt_row.time_perd_type_code = "MTH"
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [mock_cntrt_row]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        mock_query_df = MagicMock()
        mock_query_df.collect.return_value = [{'qry_txt': "SELECT 1"}]
        mock_dq_query.return_value = mock_query_df

        self.spark.sql.return_value = MagicMock()

        tier1_reference_data_validation(
            self.validation_name,
            self.file_name,
            self.cntrt_id,
            self.run_id,
            self.spark,
            self.jdbc_url,
            self.db_name,
            self.db_user,
            self.db_pwd,
            self.postgres_schema,
            self.catalog_name
        )

        mock_merge.assert_called_once()
        mock_generate_report.assert_called_once()
        mock_send_email.assert_called_once()

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp", side_effect=Exception("Contract lookup failed"))
    def test_contract_lookup_failure(self, mock_sanitize, mock_load_cntrt_lkp, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with self.assertRaises(Exception) as context:
            tier1_reference_data_validation(
                self.validation_name,
                self.file_name,
                self.cntrt_id,
                self.run_id,
                self.spark,
                self.jdbc_url,
                self.db_name,
                self.db_user,
                self.db_pwd,
                self.postgres_schema,
                self.catalog_name
            )

        self.assertIn("Contract lookup failed", str(context.exception))
        mock_logger.error.assert_called()

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.time_perd_class_codes_new", side_effect=Exception("Time period class code error"))
    def test_time_period_class_code_failure(
        self, mock_load_cntrt_lkp, mock_time_perd_class, mock_sanitize, mock_get_logger
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_cntrt_row = MagicMock()
        mock_cntrt_row.srce_sys_id = "SYS001"
        mock_cntrt_row.vendr_id = "V001"
        mock_cntrt_row.cntry_name = "India"
        mock_cntrt_row.time_perd_type_code = "MTH"
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [mock_cntrt_row]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        with self.assertRaises(Exception) as context:
            tier1_reference_data_validation(
                self.validation_name,
                self.file_name,
                self.cntrt_id,
                self.run_id,
                self.spark,
                self.jdbc_url,
                self.db_name,
                self.db_user,
                self.db_pwd,
                self.postgres_schema,
                self.catalog_name
            )

        self.assertIn("Time period class code error", str(context.exception))
        mock_logger.error.assert_called()
