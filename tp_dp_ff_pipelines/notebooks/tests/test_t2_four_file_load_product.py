import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFourFileLoadProductScript(unittest.TestCase):

    def get_mock_modules_prd(self, run_id_prd="R654", raise_exception_prd=False):
        mock_spark_prd = MagicMock()
        mock_builder_prd = MagicMock()
        mock_builder_prd.appName.return_value.getOrCreate.return_value = mock_spark_prd

        spark_session_module_prd = types.ModuleType("pyspark.sql.session")
        spark_session_module_prd.SparkSession = MagicMock(builder=mock_builder_prd)

        spark_sql_module_prd = types.ModuleType("pyspark.sql")
        spark_sql_module_prd.SparkSession = spark_session_module_prd.SparkSession

        mock_tp_utils_prd = types.ModuleType("tp_utils")
        mock_common_prd = types.ModuleType("tp_utils.common")

        mock_common_prd.__dict__.update({
            "get_dbutils": MagicMock(),
            "get_logger": MagicMock(),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd',
                'catalog_name': 'catalog',
                'postgres_schema': 'schema'
            }),
            "read_run_params": MagicMock(return_value=MagicMock(CNTRT_ID="C321", RUN_ID=run_id_prd)),
            "t2_load_file": MagicMock(side_effect=Exception("Load failed") if raise_exception_prd else None),
            "load_file": MagicMock(),
            "load_cntrt_lkp": MagicMock(),
            "load_cntrt_file_lkp": MagicMock(),
            "load_cntrt_dlmtr_lkp": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_prd,
            "pyspark.sql.session": spark_session_module_prd,
            "tp_utils": mock_tp_utils_prd,
            "tp_utils.common": mock_common_prd,
            "src.tp_utils.common": mock_common_prd
        }, mock_common_prd, mock_spark_prd

    def run_script_prd(self, script_path_prd, spark_prd):
        with open(script_path_prd) as f_prd:
            code_prd = compile(f_prd.read(), script_path_prd.split("/")[-1], 'exec')
            exec(code_prd, {"__name__": "__main__", "spark": spark_prd})

    def test_successful_execution_prd(self):
        mock_modules_prd, mock_common_prd, mock_spark_prd = self.get_mock_modules_prd()
        script_path_prd = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_product.py"

        with patch.dict(sys.modules, mock_modules_prd):
            self.run_script_prd(script_path_prd, mock_spark_prd)

        mock_common_prd.t2_load_file.assert_called_once_with(
            "C321", "PROD", "prod", "R654", "load_product", "schema",
            mock_spark_prd,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_missing_run_id_prd(self):
        mock_modules_prd, mock_common_prd, mock_spark_prd = self.get_mock_modules_prd(run_id_prd=None)
        script_path_prd = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_product.py"

        with patch.dict(sys.modules, mock_modules_prd):
            self.run_script_prd(script_path_prd, mock_spark_prd)

        mock_common_prd.t2_load_file.assert_called_once_with(
            "C321", "PROD", "prod", None, "load_product", "schema",
            mock_spark_prd,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_t2_load_file_exception_prd(self):
        mock_modules_prd, mock_common_prd, mock_spark_prd = self.get_mock_modules_prd(raise_exception_prd=True)
        script_path_prd = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_product.py"

        with patch.dict(sys.modules, mock_modules_prd):
            with self.assertRaises(Exception) as context_prd:
                self.run_script_prd(script_path_prd, mock_spark_prd)

        self.assertIn("Load failed", str(context_prd.exception))
        mock_common_prd.t2_load_file.assert_called_once()
