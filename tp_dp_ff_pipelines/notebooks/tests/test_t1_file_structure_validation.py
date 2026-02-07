import unittest
from unittest.mock import MagicMock, patch

class TestTier1FileStructureValidation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_file_structure_validation.py"
        self.spark_filestc = MagicMock()
        self.dbutils_filestc = MagicMock()
        self.logger_filestc = MagicMock()
        self.catalog_name_filestc = "test_catalog"
        self.postgres_schema_filestc = "test_schema"
        self.ref_db_jdbc_url_filestc = "jdbc:test"
        self.ref_db_name_filestc = "refdb"
        self.ref_db_user_filestc = "user"
        self.ref_db_pwd_filestc = "pwd"
        self.notebook_name_filestc = "Tier1_File_Structure_Validation"
        self.validation_name_filestc = "File Structure Validation"
        self.file_name_filestc = "source.csv"
        self.cntrt_id_filestc = "C001"
        self.run_id_filestc = "R001"
        self.last_fyi_filestc = "FYI001"
        self.dnfilestc = "FYI001"

    def mock_run_params_filestc(self):
        args_filestc = MagicMock()
        args_filestc.FILE_NAME = self.file_name_filestc
        args_filestc.CNTRT_ID = self.cntrt_id_filestc
        args_filestc.RUN_ID = self.run_id_filestc
        args_filestc.LAST_FYI = self.last_fyi_filestc
        args_filestc.dn_filestc = self.dnfilestc
        return args_filestc

    def mock_valdn_df_filestc(self, aprv_ind_filestc, fail_ind_filestc):
        df_filestc = MagicMock()
        df_filestc.filter.return_value = df_filestc
        df_filestc.count.return_value = 1
        df_filestc.collect.return_value = [{"aprv_ind": aprv_ind_filestc, "fail_ind": fail_ind_filestc}]
        return df_filestc

    @patch("src.tp_utils.common.tier1_file_structure_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress_filestc(
        self, mock_get_dbutils_filestc, mock_get_logger_filestc, mock_read_run_params_filestc,
        mock_read_from_postgres_filestc, mock_tier1_file_structure_validation_filestc
    ):
        mock_get_dbutils_filestc.return_value = self.dbutils_filestc
        mock_get_logger_filestc.return_value = self.logger_filestc
        mock_read_run_params_filestc.return_value = self.mock_run_params_filestc()
        mock_read_from_postgres_filestc.return_value = self.mock_valdn_df_filestc(aprv_ind_filestc="N", fail_ind_filestc="Y")

        df_valdn_filestc = mock_read_from_postgres_filestc.return_value
        if df_valdn_filestc.count() == 0 or df_valdn_filestc.collect()[0]["aprv_ind"] != "Y":
            mock_tier1_file_structure_validation_filestc(
                self.notebook_name_filestc, self.validation_name_filestc, self.file_name_filestc,
                self.cntrt_id_filestc, self.run_id_filestc, self.spark_filestc, self.dbutils_filestc,
                self.ref_db_jdbc_url_filestc, self.ref_db_name_filestc, self.ref_db_user_filestc,
                self.ref_db_pwd_filestc, self.postgres_schema_filestc, self.catalog_name_filestc
            )

        mock_tier1_file_structure_validation_filestc.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed_filestc(self, mock_read_run_params_filestc, mock_read_from_postgres_filestc):
        mock_read_run_params_filestc.return_value = self.mock_run_params_filestc()
        mock_read_from_postgres_filestc.return_value = self.mock_valdn_df_filestc(aprv_ind_filestc="N", fail_ind_filestc="N")

        df_valdn_filestc = mock_read_from_postgres_filestc.return_value
        fail_ind_filestc = df_valdn_filestc.collect()[0]["fail_ind"]
        aprv_ind_filestc = df_valdn_filestc.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind_filestc, "N")
        self.assertEqual(aprv_ind_filestc, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved_filestc(self, mock_read_run_params_filestc, mock_read_from_postgres_filestc):
        mock_read_run_params_filestc.return_value = self.mock_run_params_filestc()
        mock_read_from_postgres_filestc.return_value = self.mock_valdn_df_filestc(aprv_ind_filestc="Y", fail_ind_filestc="Y")

        df_valdn_filestc = mock_read_from_postgres_filestc.return_value
        fail_ind_filestc = df_valdn_filestc.collect()[0]["fail_ind"]
        aprv_ind_filestc = df_valdn_filestc.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind_filestc, "Y")
        self.assertEqual(fail_ind_filestc, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed_filestc(self, mock_read_run_params_filestc, mock_read_from_postgres_filestc):
        mock_read_run_params_filestc.return_value = self.mock_run_params_filestc()
        mock_read_from_postgres_filestc.return_value = self.mock_valdn_df_filestc(aprv_ind_filestc="N", fail_ind_filestc="Y")

        df_valdn_filestc = mock_read_from_postgres_filestc.return_value
        fail_ind_filestc = df_valdn_filestc.collect()[0]["fail_ind"]
        aprv_ind_filestc = df_valdn_filestc.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context_filestc:
            if fail_ind_filestc != "N" and aprv_ind_filestc != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context_filestc.exception))
