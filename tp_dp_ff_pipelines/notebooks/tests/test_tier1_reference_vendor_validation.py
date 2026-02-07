import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import tier1_reference_vendor_validation

class TestTier1ReferenceVendorValidation(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.dbutils = MagicMock()
        self.run_id = "R001"
        self.cntrt_id = "C001"
        self.catalog_name = "test_catalog"
        self.postgres_schema = "ref_schema"
        self.file_name = "source_file.csv"
        self.notebook_name = "tier1_reference_vendor_validation"
        self.validation_name = "Tier1 Reference Vendor Validation"
        self.jdbc_url = "jdbc:postgresql://localhost:5432/refdb"
        self.db_name = "refdb"
        self.db_user = "user"
        self.db_pwd = "pwd"

    def mock_contract_row(self):
        row = MagicMock()
        row.srce_sys_id = "SYS001"
        return row

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.send_emails")
    @patch("src.tp_utils.common.F.regexp_replace", return_value=MagicMock())
    @patch("src.tp_utils.common.generate_formatting_report")
    @patch("src.tp_utils.common.generate_report")
    @patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl")
    @patch("src.tp_utils.common.validation", return_value="validation_result")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.read_query_from_postgres", return_value=MagicMock())
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.get_logger")
    def test_successful_execution(
        self, mock_get_logger, mock_load_cntrt_lkp, mock_read_query,
        mock_dq_query, mock_validation, mock_merge, mock_generate_report,
        mock_generate_formatting, mock_regexp_replace,mock_send_email
    ):
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [self.mock_contract_row()]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df
        mock_query_df = MagicMock()
        mock_query_df.collect.return_value = [{'qry_txt': "SELECT 1"}]
        mock_dq_query.return_value = mock_query_df

        self.spark.sql.return_value = MagicMock()
        self.spark.read.parquet.return_value = MagicMock()

        tier1_reference_vendor_validation( self.validation_name, self.file_name, self.cntrt_id,
            self.run_id, self.spark, self.jdbc_url, self.db_name,
            self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
        )

        self.assertTrue(mock_merge.called)
        self.assertTrue(mock_generate_report.called)
        self.assertTrue(mock_generate_formatting.called)
        self.assertGreaterEqual(mock_validation.call_count, 5)
        # mock_send_email.assert_called_once()

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.load_cntrt_lkp", side_effect=Exception("Contract lookup failed"))
    def test_contract_lookup_failure(self, mock_load_cntrt_lkp, mock_get_logger):
        with self.assertRaises(Exception) as context:
            tier1_reference_vendor_validation(self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark,  self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
            )
        self.assertIn("Contract lookup failed", str(context.exception))

    @patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))  # ✅ Patch DBUtils
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("Reference table read failed"))
    def test_reference_table_read_failure(self, mock_read_query, mock_load_cntrt_lkp, mock_get_logger):
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [self.mock_contract_row()]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        with self.assertRaises(Exception) as context:
            tier1_reference_vendor_validation( self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark, self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
            )
        self.assertIn("Reference table read failed", str(context.exception))
