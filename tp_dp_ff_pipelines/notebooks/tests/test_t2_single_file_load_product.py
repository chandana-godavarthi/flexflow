import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestSingleFileLoadProductScript(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_single_file_load_product.py"

    def get_mock_modules(self, run_id="R456", raise_exception=False):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")

        mock_logger = MagicMock()
        mock_dbutils = MagicMock()
        mock_t2_load_file = MagicMock()

        if raise_exception:
            mock_t2_load_file.side_effect = Exception("Load failed")

        mock_args = MagicMock(CNTRT_ID="C123", RUN_ID=run_id)

        mock_common.__dict__.update({
            "get_dbutils": MagicMock(return_value=mock_dbutils),
            "get_logger": MagicMock(return_value=mock_logger),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd',
                'catalog_name': 'catalog',
                'postgres_schema': 'schema'
            }),
            "read_run_params": MagicMock(return_value=mock_args),
            "t2_load_file": mock_t2_load_file,
            "load_file": MagicMock(),
            "load_cntrt_lkp": MagicMock(),
            "load_cntrt_file_lkp": MagicMock(),
            "load_cntrt_dlmtr_lkp": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_spark, mock_common, mock_t2_load_file

    def run_script(self, mock_spark):
        with open(self.script_path) as f:
            code = compile(f.read(), "t2_single_file_load_product.py", 'exec')
            exec(code, {"__name__": "__main__", "spark": mock_spark})

    def test_successful_execution(self):
        mock_modules, mock_spark, mock_common, mock_t2_load_file = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules, clear=True):
            self.run_script(mock_spark)

        mock_t2_load_file.assert_called_once_with(
            "C123", "COMMON", "prod", "R456", "load_product", "schema",
            mock_spark,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_missing_run_id(self):
        mock_modules, mock_spark, mock_common, mock_t2_load_file = self.get_mock_modules(run_id=None)
        with patch.dict(sys.modules, mock_modules, clear=True):
            self.run_script(mock_spark)

        mock_t2_load_file.assert_called_once_with(
            "C123", "COMMON", "prod", None, "load_product", "schema",
            mock_spark,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_t2_load_file_exception(self):
        mock_modules, mock_spark, _, _ = self.get_mock_modules(raise_exception=True)
        with patch.dict(sys.modules, mock_modules, clear=True):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_spark)

        self.assertIn("Load failed", str(context.exception))
