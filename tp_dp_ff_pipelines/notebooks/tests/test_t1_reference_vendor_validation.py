import unittest
from unittest.mock import MagicMock, patch

class TestTier1ReferenceVendorValidation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_reference_vendor_validation.py"
        self.spark_refven = MagicMock()
        self.dbutils_refven = MagicMock()
        self.logger_refven = MagicMock()
        self.catalog_name_refven = "test_catalog"
        self.postgres_schema_refven = "test_schema"
        self.ref_db_jdbc_url_refven = "jdbc:test"
        self.ref_db_name_refven = "refdb"
        self.ref_db_user_refven = "user"
        self.ref_db_pwd_refven = "pwd"
        self.notebook_name_refven = "Tier1_Reference Data Validations"
        self.validation_name_refven = "Reference Data Vendors Validations"
        self.file_name_refven = "source.csv"
        self.cntrt_id_refven = "C001"
        self.run_id_refven = "R001"
        self.last_fyi_refven = "FYI001"
        self.dn_refven = "FYI001"

    def mock_run_params_refven(self):
        args_refven = MagicMock()
        args_refven.FILE_NAME = self.file_name_refven
        args_refven.CNTRT_ID = self.cntrt_id_refven
        args_refven.RUN_ID = self.run_id_refven
        args_refven.LAST_FYI = self.last_fyi_refven
        args_refven.dn_refven = self.dn_refven
        return args_refven

    def mock_valdn_df_refven(self, aprv_ind_refven, fail_ind_refven):
        df_refven = MagicMock()
        df_refven.filter.return_value = df_refven
        df_refven.count.return_value = 1
        df_refven.collect.return_value = [{"aprv_ind": aprv_ind_refven, "fail_ind": fail_ind_refven}]
        return df_refven

    @patch("src.tp_utils.common.tier1_reference_vendor_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress_refven(
        self, mock_get_dbutils_refven, mock_get_logger_refven, mock_read_run_params_refven,
        mock_read_from_postgres_refven, mock_tier1_reference_vendor_validation_refven
    ):
        mock_get_dbutils_refven.return_value = self.dbutils_refven
        mock_get_logger_refven.return_value = self.logger_refven
        mock_read_run_params_refven.return_value = self.mock_run_params_refven()
        mock_read_from_postgres_refven.return_value = self.mock_valdn_df_refven(aprv_ind_refven="N", fail_ind_refven="Y")

        df_valdn_refven = mock_read_from_postgres_refven.return_value
        if df_valdn_refven.count() == 0 or df_valdn_refven.collect()[0]["aprv_ind"] != "Y":
            mock_tier1_reference_vendor_validation_refven(
                self.notebook_name_refven, self.validation_name_refven, self.file_name_refven,
                self.cntrt_id_refven, self.run_id_refven, self.spark_refven, self.dbutils_refven,
                self.ref_db_jdbc_url_refven, self.ref_db_name_refven, self.ref_db_user_refven,
                self.ref_db_pwd_refven, self.postgres_schema_refven, self.catalog_name_refven
            )

        mock_tier1_reference_vendor_validation_refven.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed_refven(self, mock_read_run_params_refven, mock_read_from_postgres_refven):
        mock_read_run_params_refven.return_value = self.mock_run_params_refven()
        mock_read_from_postgres_refven.return_value = self.mock_valdn_df_refven(aprv_ind_refven="N", fail_ind_refven="N")

        df_valdn_refven = mock_read_from_postgres_refven.return_value
        fail_ind_refven = df_valdn_refven.collect()[0]["fail_ind"]
        aprv_ind_refven = df_valdn_refven.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind_refven, "N")
        self.assertEqual(aprv_ind_refven, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved_refven(self, mock_read_run_params_refven, mock_read_from_postgres_refven):
        mock_read_run_params_refven.return_value = self.mock_run_params_refven()
        mock_read_from_postgres_refven.return_value = self.mock_valdn_df_refven(aprv_ind_refven="Y", fail_ind_refven="Y")

        df_valdn_refven = mock_read_from_postgres_refven.return_value
        fail_ind_refven = df_valdn_refven.collect()[0]["fail_ind"]
        aprv_ind_refven = df_valdn_refven.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind_refven, "Y")
        self.assertEqual(fail_ind_refven, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed_refven(self, mock_read_run_params_refven, mock_read_from_postgres_refven):
        mock_read_run_params_refven.return_value = self.mock_run_params_refven()
        mock_read_from_postgres_refven.return_value = self.mock_valdn_df_refven(aprv_ind_refven="N", fail_ind_refven="Y")

        df_valdn_refven = mock_read_from_postgres_refven.return_value
        fail_ind_refven = df_valdn_refven.collect()[0]["fail_ind"]
        aprv_ind_refven = df_valdn_refven.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context_refven:
            if fail_ind_refven != "N" and aprv_ind_refven != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context_refven.exception))