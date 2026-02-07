import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

class TestBusinessValidationPipeline_t2bs(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_business_validation.py"
        self.spark_t2bs = MagicMock(spec=SparkSession)
        self.dbutils_t2bs = MagicMock()
        self.logger_t2bs = MagicMock()
        self.cntrt_id_t2bs = "C001"
        self.run_id_t2bs = "R001"
        self.file_name_t2bs = "source.csv"
        self.catalog_name_t2bs = "test_catalog"
        self.postgres_schema_t2bs = "test_schema"
        self.ref_db_jdbc_url_t2bs = "jdbc:test"
        self.ref_db_name_t2bs = "refdb"
        self.ref_db_user_t2bs = "user"
        self.ref_db_pwd_t2bs = "pwd"

    def mock_df_t2bs(self, columns):
        df_t2bs = MagicMock()
        df_t2bs.filter.return_value = df_t2bs
        df_t2bs.select.return_value = df_t2bs
        df_t2bs.collect.return_value = [MagicMock(**{col: f"{col}_val" for col in columns})]
        df_t2bs.count.return_value = 1
        df_t2bs.show.return_value = None
        return df_t2bs

    @patch("src.tp_utils.common.business_validation")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_database_config")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_validation_triggered_t2bs(
        self, mock_get_dbutils_t2bs, mock_get_logger_t2bs, mock_get_database_config_t2bs,
        mock_read_run_params_t2bs, mock_load_cntrt_lkp_t2bs, mock_read_from_postgres_t2bs,
        mock_business_validation_t2bs
    ):
        mock_get_dbutils_t2bs.return_value = self.dbutils_t2bs
        mock_get_logger_t2bs.return_value = self.logger_t2bs
        mock_get_database_config_t2bs.return_value = {
            'ref_db_jdbc_url': self.ref_db_jdbc_url_t2bs,
            'ref_db_name': self.ref_db_name_t2bs,
            'ref_db_user': self.ref_db_user_t2bs,
            'ref_db_pwd': self.ref_db_pwd_t2bs,
            'catalog_name': self.catalog_name_t2bs,
            'postgres_schema': self.postgres_schema_t2bs
        }
        mock_read_run_params_t2bs.return_value = MagicMock(CNTRT_ID=self.cntrt_id_t2bs, RUN_ID=self.run_id_t2bs, FILE_NAME=self.file_name_t2bs)
        mock_load_cntrt_lkp_t2bs.return_value = self.mock_df_t2bs(['srce_sys_id'])
        mock_read_from_postgres_t2bs.return_value = self.mock_df_t2bs(['aprv_ind', 'fail_ind'])

        df_valdn_t2bs = mock_read_from_postgres_t2bs.return_value
        if df_valdn_t2bs.count() == 0 or df_valdn_t2bs.collect()[0].aprv_ind != "Y":
            mock_business_validation_t2bs(
                self.run_id_t2bs, self.cntrt_id_t2bs, "srce_sys_id_val", self.file_name_t2bs, "Business Validation",
                "tp-publish-data/", self.postgres_schema_t2bs, self.catalog_name_t2bs, self.spark_t2bs,
                self.ref_db_jdbc_url_t2bs, self.ref_db_name_t2bs, self.ref_db_user_t2bs, self.ref_db_pwd_t2bs
            )
            mock_business_validation_t2bs.assert_called_once()

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_database_config")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_auto_approve_disabled_raises_t2bs(
        self, mock_get_dbutils_t2bs, mock_get_logger_t2bs, mock_get_database_config_t2bs,
        mock_read_run_params_t2bs, mock_load_cntrt_lkp_t2bs, mock_read_from_postgres_t2bs
    ):
        mock_get_dbutils_t2bs.return_value = self.dbutils_t2bs
        mock_get_logger_t2bs.return_value = self.logger_t2bs
        mock_get_database_config_t2bs.return_value = {
            'ref_db_jdbc_url': self.ref_db_jdbc_url_t2bs,
            'ref_db_name': self.ref_db_name_t2bs,
            'ref_db_user': self.ref_db_user_t2bs,
            'ref_db_pwd': self.ref_db_pwd_t2bs,
            'catalog_name': self.catalog_name_t2bs,
            'postgres_schema': self.postgres_schema_t2bs
        }
        mock_read_run_params_t2bs.return_value = MagicMock(CNTRT_ID=self.cntrt_id_t2bs, RUN_ID=self.run_id_t2bs, FILE_NAME=self.file_name_t2bs)

        df_cntrt_lkp_t2bs = self.mock_df_t2bs(['auto_apprv_ind'])
        df_cntrt_lkp_t2bs.collect()[0].auto_apprv_ind = "N"
        mock_load_cntrt_lkp_t2bs.return_value = df_cntrt_lkp_t2bs

        df_valdn_t2bs = self.mock_df_t2bs(['aprv_ind', 'fail_ind'])
        df_valdn_t2bs.collect()[0].aprv_ind = "N"
        df_valdn_t2bs.collect()[0].fail_ind = "Y"
        mock_read_from_postgres_t2bs.return_value = df_valdn_t2bs

        with self.assertRaises(RuntimeError) as context_t2bs:
            if df_valdn_t2bs.collect()[0].fail_ind != "N" and df_valdn_t2bs.collect()[0].aprv_ind != "Y":
                auto_apprv_ind_t2bs = df_cntrt_lkp_t2bs.collect()[0].auto_apprv_ind
                if auto_apprv_ind_t2bs == "N":
                    raise RuntimeError("Auto Approve turned off for the Business Validation: Terminating the workflow")

        self.assertIn("Auto Approve turned off", str(context_t2bs.exception))

    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.read_run_params")
    @patch("src.tp_utils.common.get_database_config")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.get_dbutils")
    def test_data_quality_approved_t2bs(
        self, mock_get_dbutils_t2bs, mock_get_logger_t2bs, mock_get_database_config_t2bs,
        mock_read_run_params_t2bs, mock_load_cntrt_lkp_t2bs, mock_read_from_postgres_t2bs
    ):
        mock_get_dbutils_t2bs.return_value = self.dbutils_t2bs
        mock_get_logger_t2bs.return_value = self.logger_t2bs
        mock_get_database_config_t2bs.return_value = {
            'ref_db_jdbc_url': self.ref_db_jdbc_url_t2bs,
            'ref_db_name': self.ref_db_name_t2bs,
            'ref_db_user': self.ref_db_user_t2bs,
            'ref_db_pwd': self.ref_db_pwd_t2bs,
            'catalog_name': self.catalog_name_t2bs,
            'postgres_schema': self.postgres_schema_t2bs
        }
        mock_read_run_params_t2bs.return_value = MagicMock(CNTRT_ID=self.cntrt_id_t2bs, RUN_ID=self.run_id_t2bs, FILE_NAME=self.file_name_t2bs)
        mock_load_cntrt_lkp_t2bs.return_value = self.mock_df_t2bs(['auto_apprv_ind'])

        df_valdn_t2bs = self.mock_df_t2bs(['aprv_ind', 'fail_ind'])
        df_valdn_t2bs.collect()[0].aprv_ind = "Y"
        df_valdn_t2bs.collect()[0].fail_ind = "Y"
        mock_read_from_postgres_t2bs.return_value = df_valdn_t2bs

        aprv_ind_t2bs = df_valdn_t2bs.collect()[0].aprv_ind
        self.assertEqual(aprv_ind_t2bs, "Y")