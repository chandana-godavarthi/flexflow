import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

class TestRetentionLogic(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/retention.py"  #Update this path

    def get_mock_modules(self):
        # Mock SparkSession
        mock_spark = MagicMock()
        mock_get_or_create = MagicMock(return_value=mock_spark)
        mock_app_name = MagicMock(getOrCreate=mock_get_or_create)
        mock_builder = MagicMock(appName=MagicMock(return_value=mock_app_name))

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        # Mock tp_utils.common
        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")

        mock_common.get_logger = MagicMock()
        mock_common.get_dbutils = MagicMock()
        mock_common.get_dbutils.return_value = MagicMock()
        mock_common.semaphore_acquisition = MagicMock()
        mock_common.release_semaphore = MagicMock()  # Add this line
        mock_common.time_perd_class_codes = MagicMock()
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        })

        mock_args = MagicMock()
        mock_args.FILE_NAME = "test.csv"
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R456"
        mock_common.read_run_params = MagicMock(return_value=mock_args)  #

        mock_common.calculate_retention_date = MagicMock()
        mock_common.recursive_delete = MagicMock()
        mock_common.load_cntrt_lkp = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_common

    def run_script_with_mock(self, mock_common):
        with open(self.script_path) as f:
            code = compile(f.read(), "Retention_Logic.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_missing_contract_data_should_raise_exception(self):
        mock_modules, mock_common = self.get_mock_modules()
        df_mock = MagicMock()
        df_mock.select.side_effect = Exception("Missing contract data")
        mock_common.load_cntrt_lkp.return_value = df_mock

        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception):
                self.run_script_with_mock(mock_common)
