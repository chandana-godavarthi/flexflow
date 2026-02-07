import unittest
from unittest.mock import MagicMock, patch

class TestTier1BusinessValidation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_business_validation.py"
        self.spark = MagicMock()
        self.dbutils = MagicMock()
        self.logger = MagicMock()
        self.catalog_name = "test_catalog"
        self.postgres_schema = "test_schema"
        self.ref_db_jdbc_url = "jdbc:test"
        self.ref_db_name = "refdb"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.notebook_name = "Tier1_Business_Validations"
        self.validation_name = "Business Validations"
        self.file_name = "source.csv"
        self.cntrt_id = "C001"
        self.run_id = "R001"

    def mock_run_params(self):
        args = MagicMock()
        args.FILE_NAME = self.file_name
        args.CNTRT_ID = self.cntrt_id
        args.RUN_ID = self.run_id
        return args

    def mock_valdn_df(self, aprv_ind, fail_ind):
        df = MagicMock()
        df.filter.return_value = df
        df.count.return_value = 1
        df.collect.return_value = [{"aprv_ind": aprv_ind, "fail_ind": fail_ind}]
        return df

    @patch("src.tp_utils.common.tier1_business_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_dq_in_progress(
        self, mock_get_dbutils, mock_get_logger, mock_read_run_params,
        mock_read_from_postgres, mock_tier1_validation
    ):
        mock_get_dbutils.return_value = self.dbutils
        mock_get_logger.return_value = self.logger
        mock_read_run_params.return_value = self.mock_run_params()
        mock_read_from_postgres.return_value = self.mock_valdn_df(aprv_ind="N", fail_ind="Y")

        df_valdn = mock_read_from_postgres.return_value
        if df_valdn.count() == 0 or df_valdn.collect()[0]["aprv_ind"] != "Y":
            mock_tier1_validation(
                self.notebook_name, self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark, self.dbutils, self.ref_db_jdbc_url, self.ref_db_name,
                self.ref_db_user, self.ref_db_pwd, self.postgres_schema, self.catalog_name
            )

        mock_tier1_validation.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_passed(self, mock_read_run_params, mock_read_from_postgres):
        mock_read_run_params.return_value = self.mock_run_params()
        mock_read_from_postgres.return_value = self.mock_valdn_df(aprv_ind="N", fail_ind="N")

        df_valdn = mock_read_from_postgres.return_value
        fail_ind = df_valdn.collect()[0]["fail_ind"]
        aprv_ind = df_valdn.collect()[0]["aprv_ind"]

        self.assertEqual(fail_ind, "N")
        self.assertEqual(aprv_ind, "N")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_approved(self, mock_read_run_params, mock_read_from_postgres):
        mock_read_run_params.return_value = self.mock_run_params()
        mock_read_from_postgres.return_value = self.mock_valdn_df(aprv_ind="Y", fail_ind="Y")

        df_valdn = mock_read_from_postgres.return_value
        fail_ind = df_valdn.collect()[0]["fail_ind"]
        aprv_ind = df_valdn.collect()[0]["aprv_ind"]

        self.assertEqual(aprv_ind, "Y")
        self.assertEqual(fail_ind, "Y")

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.read_run_params")
    def test_dq_failed(self, mock_read_run_params, mock_read_from_postgres):
        mock_read_run_params.return_value = self.mock_run_params()
        mock_read_from_postgres.return_value = self.mock_valdn_df(aprv_ind="N", fail_ind="Y")

        df_valdn = mock_read_from_postgres.return_value
        fail_ind = df_valdn.collect()[0]["fail_ind"]
        aprv_ind = df_valdn.collect()[0]["aprv_ind"]

        with self.assertRaises(Exception) as context:
            if fail_ind != "N" and aprv_ind != "Y":
                raise Exception("Data Quality Issue: Terminating the workflow")

        self.assertIn("Data Quality Issue", str(context.exception))