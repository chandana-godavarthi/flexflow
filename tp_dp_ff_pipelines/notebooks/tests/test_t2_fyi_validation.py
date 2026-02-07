import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

class TestFYIValidation_t2fyi(unittest.TestCase):

    def setUp(self):
        self.script_path_t2fyi = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_fyi_validation.py"

    def get_mock_modules_t2fyi(self):
        mock_spark_t2fyi = MagicMock()
        mock_get_or_create_t2fyi = MagicMock(return_value=mock_spark_t2fyi)
        mock_app_name_t2fyi = MagicMock(getOrCreate=mock_get_or_create_t2fyi)
        mock_builder_t2fyi = MagicMock(appName=MagicMock(return_value=mock_app_name_t2fyi))

        spark_session_module_t2fyi = types.ModuleType("pyspark.sql.session")
        spark_session_module_t2fyi.SparkSession = MagicMock(builder=mock_builder_t2fyi)

        spark_sql_module_t2fyi = types.ModuleType("pyspark.sql")
        spark_sql_module_t2fyi.SparkSession = spark_session_module_t2fyi.SparkSession

        mock_tp_utils_t2fyi = types.ModuleType("tp_utils")
        mock_common_t2fyi = types.ModuleType("tp_utils.common")

        mock_common_t2fyi.get_dbutils = MagicMock()
        mock_common_t2fyi.get_logger = MagicMock()
        mock_common_t2fyi.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        })

        mock_args_t2fyi = MagicMock()
        mock_args_t2fyi.CNTRT_ID = "C123"
        mock_args_t2fyi.RUN_ID = "R456"
        mock_args_t2fyi.FILE_NAME = "test_file.csv"
        mock_common_t2fyi.read_run_params = MagicMock(return_value=mock_args_t2fyi)

        mock_df_cntrt_lkp_t2fyi = MagicMock()
        mock_df_cntrt_lkp_t2fyi.select.return_value.collect.return_value = [Row(srce_sys_id='SYS1')]
        mock_common_t2fyi.load_cntrt_lkp = MagicMock(return_value=mock_df_cntrt_lkp_t2fyi)

        mock_common_t2fyi.read_from_postgres = MagicMock()
        mock_common_t2fyi.your_information_validations = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_t2fyi,
            "pyspark.sql.session": spark_session_module_t2fyi,
            "tp_utils": mock_tp_utils_t2fyi,
            "tp_utils.common": mock_common_t2fyi,
            "src.tp_utils.common": mock_common_t2fyi,
            "pyspark.sql.functions": types.ModuleType("pyspark.sql.functions")
        }, mock_common_t2fyi

    def run_script_with_mock_t2fyi(self, mock_common_t2fyi):
        with open(self.script_path_t2fyi) as f:
            code_t2fyi = compile(f.read(), "t2_fyi_validation.py", 'exec')
            exec(code_t2fyi, {"__name__": "__main__"})

    def test_fyi_validation_runs_when_no_records_t2fyi(self):
        mock_modules_t2fyi, mock_common_t2fyi = self.get_mock_modules_t2fyi()
        mock_df_table_t2fyi = MagicMock()
        mock_df_valdn_t2fyi = MagicMock()
        mock_df_valdn_t2fyi.count.return_value = 0
        mock_df_valdn_t2fyi.collect.return_value = [Row(aprv_ind='N', fail_ind='N')]
        mock_df_table_t2fyi.filter.return_value = mock_df_valdn_t2fyi
        mock_common_t2fyi.read_from_postgres.return_value = mock_df_table_t2fyi

        mock_modules_t2fyi["pyspark.sql.functions"].lit = MagicMock(return_value=MagicMock())

        with patch.dict(sys.modules, mock_modules_t2fyi):
            self.run_script_with_mock_t2fyi(mock_common_t2fyi)

        mock_common_t2fyi.your_information_validations.assert_called_once()

    def test_fyi_validation_runs_when_not_approved_t2fyi(self):
        mock_modules_t2fyi, mock_common_t2fyi = self.get_mock_modules_t2fyi()
        mock_df_table_t2fyi = MagicMock()
        mock_df_valdn_t2fyi = MagicMock()
        mock_df_valdn_t2fyi.count.return_value = 1
        mock_df_valdn_t2fyi.collect.return_value = [Row(aprv_ind='N', fail_ind='Y')]
        mock_df_table_t2fyi.filter.return_value = mock_df_valdn_t2fyi
        mock_common_t2fyi.read_from_postgres.return_value = mock_df_table_t2fyi

        mock_modules_t2fyi["pyspark.sql.functions"].lit = MagicMock(return_value=MagicMock())

        with patch.dict(sys.modules, mock_modules_t2fyi):
            self.run_script_with_mock_t2fyi(mock_common_t2fyi)

        mock_common_t2fyi.your_information_validations.assert_called_once()

    def test_fyi_validation_skipped_when_approved_t2fyi(self):
        mock_modules_t2fyi, mock_common_t2fyi = self.get_mock_modules_t2fyi()
        mock_df_table_t2fyi = MagicMock()
        mock_df_valdn_t2fyi = MagicMock()
        mock_df_valdn_t2fyi.count.return_value = 1
        mock_df_valdn_t2fyi.collect.return_value = [Row(aprv_ind='Y', fail_ind='Y')]
        mock_df_table_t2fyi.filter.return_value = mock_df_valdn_t2fyi
        mock_common_t2fyi.read_from_postgres.return_value = mock_df_table_t2fyi

        mock_modules_t2fyi["pyspark.sql.functions"].lit = MagicMock(return_value=MagicMock())

        with patch.dict(sys.modules, mock_modules_t2fyi):
            self.run_script_with_mock_t2fyi(mock_common_t2fyi)

        mock_common_t2fyi.your_information_validations.assert_not_called()