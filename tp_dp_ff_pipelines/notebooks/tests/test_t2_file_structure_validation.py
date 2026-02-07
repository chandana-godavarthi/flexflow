import unittest
from unittest.mock import MagicMock, patch

class TestFileStructureValidation(unittest.TestCase):

    def setUp(self):
        self.script_path_t2filstc = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_file_structure_validation.py"
        self.spark_t2filstc = MagicMock()
        self.dbutils_t2filstc = MagicMock()
        self.logger_t2filstc = MagicMock()
        self.catalog_name_t2filstc = "test_catalog"
        self.postgres_schema_t2filstc = "test_schema"
        self.ref_db_jdbc_url_t2filstc = "jdbc:test"
        self.ref_db_name_t2filstc = "refdb"
        self.ref_db_user_t2filstc = "user"
        self.ref_db_pwd_t2filstc = "pwd"
        self.notebook_name_t2filstc = "File_Structure_Validation"
        self.validation_name_t2filstc = "File Structure Validation"
        self.file_name_t2filstc = "source.csv"
        self.cntrt_id_t2filstc = "C001"
        self.run_id_t2filstc = "R001"

    def mock_run_params_t2filstc(self):
        args_t2filstc = MagicMock()
        args_t2filstc.FILE_NAME = self.file_name_t2filstc
        args_t2filstc.CNTRT_ID = self.cntrt_id_t2filstc
        args_t2filstc.RUN_ID = self.run_id_t2filstc
        return args_t2filstc

    def mock_valdn_df_t2filstc(self, aprv_ind_t2filstc, fail_ind_t2filstc):
        df_t2filstc = MagicMock()
        df_t2filstc.filter.return_value = df_t2filstc
        df_t2filstc.count.return_value = 1
        df_t2filstc.collect.return_value = [{"aprv_ind": aprv_ind_t2filstc, "fail_ind": fail_ind_t2filstc}]
        return df_t2filstc

    @patch("src.tp_utils.common.file_structure_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress_t2filstc(
        self, mock_get_dbutils_t2filstc, mock_get_logger_t2filstc, mock_read_run_params_t2filstc,
        mock_read_from_postgres_t2filstc, mock_file_structure_validation_t2filstc
    ):
        mock_get_dbutils_t2filstc.return_value = self.dbutils_t2filstc
        mock_get_logger_t2filstc.return_value = self.logger_t2filstc
        mock_read_run_params_t2filstc.return_value = self.mock_run_params_t2filstc()
        mock_read_from_postgres_t2filstc.return_value = self.mock_valdn_df_t2filstc(aprv_ind_t2filstc="N", fail_ind_t2filstc="Y")

        df_valdn_t2filstc = mock_read_from_postgres_t2filstc.return_value
        if df_valdn_t2filstc.count() == 0 or df_valdn_t2filstc.collect()[0]["aprv_ind"] != "Y":
            mock_file_structure_validation_t2filstc(
                self.notebook_name_t2filstc, self.validation_name_t2filstc, self.file_name_t2filstc,
                self.cntrt_id_t2filstc, self.run_id_t2filstc, self.spark_t2filstc, self.ref_db_jdbc_url_t2filstc,
                self.ref_db_name_t2filstc, self.ref_db_user_t2filstc, self.ref_db_pwd_t2filstc,
                self.postgres_schema_t2filstc, self.catalog_name_t2filstc
            )

        mock_file_structure_validation_t2filstc.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed_t2filstc(self, mock_read_run_params_t2filstc, mock_read_from_postgres_t2filstc):
        mock_read_run_params_t2filstc.return_value = self.mock_run_params_t2filstc()
        mock_read_from_postgres_t2filstc.return_value = self.mock_valdn_df_t2filstc(aprv_ind_t2filstc="N", fail_ind_t2filstc="N")

        df_valdn_t2filstc = mock_read_from_postgres_t2filstc.return_value
        fail_ind_t2filstc = df_valdn_t2filstc.collect()[0]["fail_ind"]
        aprv_ind_t2filstc = df_valdn_t2filstc.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind_t2filstc, "N")
        self.assertEqual(aprv_ind_t2filstc, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved_t2filstc(self, mock_read_run_params_t2filstc, mock_read_from_postgres_t2filstc):
        mock_read_run_params_t2filstc.return_value = self.mock_run_params_t2filstc()
        mock_read_from_postgres_t2filstc.return_value = self.mock_valdn_df_t2filstc(aprv_ind_t2filstc="Y", fail_ind_t2filstc="Y")

        df_valdn_t2filstc = mock_read_from_postgres_t2filstc.return_value
        fail_ind_t2filstc = df_valdn_t2filstc.collect()[0]["fail_ind"]
        aprv_ind_t2filstc = df_valdn_t2filstc.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind_t2filstc, "Y")
        self.assertEqual(fail_ind_t2filstc, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed_t2filstc(self, mock_read_run_params_t2filstc, mock_read_from_postgres_t2filstc):
        mock_read_run_params_t2filstc.return_value = self.mock_run_params_t2filstc()
        mock_read_from_postgres_t2filstc.return_value = self.mock_valdn_df_t2filstc(aprv_ind_t2filstc="N", fail_ind_t2filstc="Y")

        df_valdn_t2filstc = mock_read_from_postgres_t2filstc.return_value
        fail_ind_t2filstc = df_valdn_t2filstc.collect()[0]["fail_ind"]
        aprv_ind_t2filstc = df_valdn_t2filstc.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context_t2filstc:
            if fail_ind_t2filstc != "N" and aprv_ind_t2filstc != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context_t2filstc.exception))

    def test_t2_filst_summary_t2filstc(self):
        fail_ind_t2filstc = "N"
        aprv_ind_t2filstc = "Y"
        t2_filst_t2filstc = {
            "notebook_name_t2filstc": self.notebook_name_t2filstc,
            "validation_name_t2filstc": self.validation_name_t2filstc,
            "file_name_t2filstc": self.file_name_t2filstc,
            "cntrt_id_t2filstc": self.cntrt_id_t2filstc,
            "run_id_t2filstc": self.run_id_t2filstc,
            "ref_db_jdbc_url_t2filstc": self.ref_db_jdbc_url_t2filstc,
            "ref_db_name_t2filstc": self.ref_db_name_t2filstc,
            "ref_db_user_t2filstc": self.ref_db_user_t2filstc,
            "ref_db_pwd_t2filstc": "****",
            "catalog_name_t2filstc": self.catalog_name_t2filstc,
            "postgres_schema_t2filstc": self.postgres_schema_t2filstc,
            "fail_ind_t2filstc": fail_ind_t2filstc,
            "aprv_ind_t2filstc": aprv_ind_t2filstc
        }

        self.assertEqual(t2_filst_t2filstc["notebook_name_t2filstc"], "File_Structure_Validation")
        self.assertEqual(t2_filst_t2filstc["fail_ind_t2filstc"], "N")
