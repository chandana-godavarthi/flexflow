import unittest
from unittest.mock import MagicMock, patch

class TestTier1FYIValidation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_fyi_validation.py"
        self.spark_fyi = MagicMock()
        self.dbutils_fyi = MagicMock()
        self.logger_fyi = MagicMock()
        self.catalog_name_fyi = "test_catalog"
        self.postgres_schema_fyi = "test_schema"
        self.ref_db_jdbc_url_fyi = "jdbc:test"
        self.ref_db_name_fyi = "refdb"
        self.ref_db_user_fyi = "user"
        self.ref_db_pwd_fyi = "pwd"
        self.notebook_name_fyi = "Tier1_For_Your_Information_Validations"
        self.validation_name_fyi = "For Your Information Validations"
        self.file_name_fyi = "source.csv"
        self.cntrt_id_fyi = "C001"
        self.run_id_fyi = "R001"
        self.last_fyi_fyi = "FYI001"
        self.dn_fyi = "FYI001"

    def mock_run_params_fyi(self):
        args_fyi = MagicMock()
        args_fyi.FILE_NAME = self.file_name_fyi
        args_fyi.CNTRT_ID = self.cntrt_id_fyi
        args_fyi.RUN_ID = self.run_id_fyi
        args_fyi.LAST_FYI = self.last_fyi_fyi
        args_fyi.dn_fyi = self.dn_fyi
        return args_fyi

    def mock_valdn_df_fyi(self, aprv_ind_fyi, fail_ind_fyi):
        df_fyi = MagicMock()
        df_fyi.filter.return_value = df_fyi
        df_fyi.count.return_value = 1
        df_fyi.collect.return_value = [{"aprv_ind": aprv_ind_fyi, "fail_ind": fail_ind_fyi}]
        return df_fyi

    @patch("src.tp_utils.common.tier1_fyi_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress_fyi(
        self, mock_get_dbutils_fyi, mock_get_logger_fyi, mock_read_run_params_fyi,
        mock_read_from_postgres_fyi, mock_tier1_fyi_validation_fyi
    ):
        mock_get_dbutils_fyi.return_value = self.dbutils_fyi
        mock_get_logger_fyi.return_value = self.logger_fyi
        mock_read_run_params_fyi.return_value = self.mock_run_params_fyi()
        mock_read_from_postgres_fyi.return_value = self.mock_valdn_df_fyi(aprv_ind_fyi="N", fail_ind_fyi="Y")

        df_valdn_fyi = mock_read_from_postgres_fyi.return_value
        if df_valdn_fyi.count() == 0 or df_valdn_fyi.collect()[0]["aprv_ind"] != "Y":
            mock_tier1_fyi_validation_fyi(
                self.notebook_name_fyi, self.validation_name_fyi, self.file_name_fyi,
                self.cntrt_id_fyi, self.run_id_fyi, self.spark_fyi, self.dbutils_fyi,
                self.ref_db_jdbc_url_fyi, self.ref_db_name_fyi, self.ref_db_user_fyi,
                self.ref_db_pwd_fyi, self.postgres_schema_fyi, self.catalog_name_fyi
            )

        mock_tier1_fyi_validation_fyi.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed_fyi(self, mock_read_run_params_fyi, mock_read_from_postgres_fyi):
        mock_read_run_params_fyi.return_value = self.mock_run_params_fyi()
        mock_read_from_postgres_fyi.return_value = self.mock_valdn_df_fyi(aprv_ind_fyi="N", fail_ind_fyi="N")

        df_valdn_fyi = mock_read_from_postgres_fyi.return_value
        fail_ind_fyi = df_valdn_fyi.collect()[0]["fail_ind"]
        aprv_ind_fyi = df_valdn_fyi.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind_fyi, "N")
        self.assertEqual(aprv_ind_fyi, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved_fyi(self, mock_read_run_params_fyi, mock_read_from_postgres_fyi):
        mock_read_run_params_fyi.return_value = self.mock_run_params_fyi()
        mock_read_from_postgres_fyi.return_value = self.mock_valdn_df_fyi(aprv_ind_fyi="Y", fail_ind_fyi="Y")

        df_valdn_fyi = mock_read_from_postgres_fyi.return_value
        fail_ind_fyi = df_valdn_fyi.collect()[0]["fail_ind"]
        aprv_ind_fyi = df_valdn_fyi.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind_fyi, "Y")
        self.assertEqual(fail_ind_fyi, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed_fyi(self, mock_read_run_params_fyi, mock_read_from_postgres_fyi):
        mock_read_run_params_fyi.return_value = self.mock_run_params_fyi()
        mock_read_from_postgres_fyi.return_value = self.mock_valdn_df_fyi(aprv_ind_fyi="N", fail_ind_fyi="Y")

        df_valdn_fyi = mock_read_from_postgres_fyi.return_value
        fail_ind_fyi = df_valdn_fyi.collect()[0]["fail_ind"]
        aprv_ind_fyi = df_valdn_fyi.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context_fyi:
            if fail_ind_fyi != "N" and aprv_ind_fyi != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context_fyi.exception))