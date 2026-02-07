import unittest
from unittest.mock import MagicMock, patch

class TestTier1ReferenceDataValidation(unittest.TestCase):

    def setUp(self):
        self.spark_ref = MagicMock()
        self.dbutils_ref = MagicMock()
        self.logger_ref = MagicMock()
        self.catalog_name_ref = "test_catalog"
        self.postgres_schema_ref = "test_schema"
        self.ref_db_jdbc_url_ref = "jdbc:test"
        self.ref_db_name_ref = "refdb"
        self.ref_db_user_ref = "user"
        self.ref_db_pwd_ref = "pwd"
        self.notebook_name_ref = "Tier1_Reference Data Validations"
        self.validation_name_ref = "Reference Data Validations"
        self.file_name_ref = "source.csv"
        self.cntrt_id_ref = "C001"
        self.run_id_ref = "R001"
        self.last_fyi_ref = "FYI001"
        self.dn_ref = "FYI001"

    def mock_run_params_ref(self):
        args_ref = MagicMock()
        args_ref.FILE_NAME = self.file_name_ref
        args_ref.CNTRT_ID = self.cntrt_id_ref
        args_ref.RUN_ID = self.run_id_ref
        args_ref.LAST_FYI = self.last_fyi_ref
        args_ref.dn_ref = self.dn_ref
        return args_ref

    def mock_valdn_df_ref(self, aprv_ind_ref, fail_ind_ref):
        df_ref = MagicMock()
        df_ref.filter.return_value = df_ref
        df_ref.count.return_value = 1
        df_ref.collect.return_value = [{"aprv_ind": aprv_ind_ref, "fail_ind": fail_ind_ref}]
        return df_ref

    @patch("src.tp_utils.common.tier1_reference_data_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress_ref(
        self, mock_get_dbutils_ref, mock_get_logger_ref, mock_read_run_params_ref,
        mock_read_from_postgres_ref, mock_tier1_reference_data_validation_ref
    ):
        mock_get_dbutils_ref.return_value = self.dbutils_ref
        mock_get_logger_ref.return_value = self.logger_ref
        mock_read_run_params_ref.return_value = self.mock_run_params_ref()
        mock_read_from_postgres_ref.return_value = self.mock_valdn_df_ref(aprv_ind_ref="N", fail_ind_ref="Y")

        df_valdn_ref = mock_read_from_postgres_ref.return_value
        if df_valdn_ref.count() == 0 or df_valdn_ref.collect()[0]["aprv_ind"] != "Y":
            mock_tier1_reference_data_validation_ref(
                self.notebook_name_ref, self.validation_name_ref, self.file_name_ref,
                self.cntrt_id_ref, self.run_id_ref, self.spark_ref, self.dbutils_ref,
                self.ref_db_jdbc_url_ref, self.ref_db_name_ref, self.ref_db_user_ref,
                self.ref_db_pwd_ref, self.postgres_schema_ref, self.catalog_name_ref
            )

        mock_tier1_reference_data_validation_ref.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed_ref(self, mock_read_run_params_ref, mock_read_from_postgres_ref):
        mock_read_run_params_ref.return_value = self.mock_run_params_ref()
        mock_read_from_postgres_ref.return_value = self.mock_valdn_df_ref(aprv_ind_ref="N", fail_ind_ref="N")

        df_valdn_ref = mock_read_from_postgres_ref.return_value
        fail_ind_ref = df_valdn_ref.collect()[0]["fail_ind"]
        aprv_ind_ref = df_valdn_ref.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind_ref, "N")
        self.assertEqual(aprv_ind_ref, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved_ref(self, mock_read_run_params_ref, mock_read_from_postgres_ref):
        mock_read_run_params_ref.return_value = self.mock_run_params_ref()
        mock_read_from_postgres_ref.return_value = self.mock_valdn_df_ref(aprv_ind_ref="Y", fail_ind_ref="Y")

        df_valdn_ref = mock_read_from_postgres_ref.return_value
        fail_ind_ref = df_valdn_ref.collect()[0]["fail_ind"]
        aprv_ind_ref = df_valdn_ref.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind_ref, "Y")
        self.assertEqual(fail_ind_ref, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed_ref(self, mock_read_run_params_ref, mock_read_from_postgres_ref):
        mock_read_run_params_ref.return_value = self.mock_run_params_ref()
        mock_read_from_postgres_ref.return_value = self.mock_valdn_df_ref(aprv_ind_ref="N", fail_ind_ref="Y")

        df_valdn_ref = mock_read_from_postgres_ref.return_value
        fail_ind_ref = df_valdn_ref.collect()[0]["fail_ind"]
        aprv_ind_ref = df_valdn_ref.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context_ref:
            if fail_ind_ref != "N" and aprv_ind_ref != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context_ref.exception))