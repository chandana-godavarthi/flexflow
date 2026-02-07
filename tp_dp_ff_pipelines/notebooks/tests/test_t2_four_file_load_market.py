import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFourFileLoadMarketScript(unittest.TestCase):

    def get_mock_modules_mrk(self, run_id_mrk="R456", raise_exception_mrk=False):
        mock_spark_mrk = MagicMock()
        mock_builder_mrk = MagicMock()
        mock_builder_mrk.appName.return_value.getOrCreate.return_value = mock_spark_mrk

        spark_session_module_mrk = types.ModuleType("pyspark.sql.session")
        spark_session_module_mrk.SparkSession = MagicMock(builder=mock_builder_mrk)

        spark_sql_module_mrk = types.ModuleType("pyspark.sql")
        spark_sql_module_mrk.SparkSession = spark_session_module_mrk.SparkSession

        mock_tp_utils_mrk = types.ModuleType("tp_utils")
        mock_common_mrk = types.ModuleType("tp_utils.common")

        mock_common_mrk.__dict__.update({
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
            "read_run_params": MagicMock(return_value=MagicMock(CNTRT_ID="C123", RUN_ID=run_id_mrk)),
            "t2_load_file": MagicMock(side_effect=Exception("Load failed") if raise_exception_mrk else None),
            "load_file": MagicMock(),
            "load_cntrt_lkp": MagicMock(),
            "load_cntrt_file_lkp": MagicMock(),
            "load_cntrt_dlmtr_lkp": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_mrk,
            "pyspark.sql.session": spark_session_module_mrk,
            "tp_utils": mock_tp_utils_mrk,
            "tp_utils.common": mock_common_mrk,
            "src.tp_utils.common": mock_common_mrk
        }, mock_common_mrk, mock_spark_mrk

    def run_script_mrk(self, script_path_mrk, spark_mrk):
        with open(script_path_mrk) as f_mrk:
            code_mrk = compile(f_mrk.read(), script_path_mrk.split("/")[-1], 'exec')
            exec(code_mrk, {"__name__": "__main__", "spark": spark_mrk})

    def test_successful_execution_mrk(self):
        mock_modules_mrk, mock_common_mrk, mock_spark_mrk = self.get_mock_modules_mrk()
        script_path_mrk = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_market.py"

        with patch.dict(sys.modules, mock_modules_mrk):
            self.run_script_mrk(script_path_mrk, mock_spark_mrk)

        mock_common_mrk.t2_load_file.assert_called_once_with(
            "C123", "MKT", "mkt", "R456", "load_market", "schema",
            mock_spark_mrk,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_missing_run_id_mrk(self):
        mock_modules_mrk, mock_common_mrk, mock_spark_mrk = self.get_mock_modules_mrk(run_id_mrk=None)
        script_path_mrk = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_market.py"

        with patch.dict(sys.modules, mock_modules_mrk):
            self.run_script_mrk(script_path_mrk, mock_spark_mrk)

        mock_common_mrk.t2_load_file.assert_called_once_with(
            "C123", "MKT", "mkt", None, "load_market", "schema",
            mock_spark_mrk,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_t2_load_file_exception_mrk(self):
        mock_modules_mrk, mock_common_mrk, mock_spark_mrk = self.get_mock_modules_mrk(raise_exception_mrk=True)
        script_path_mrk = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_market.py"

        with patch.dict(sys.modules, mock_modules_mrk):
            with self.assertRaises(Exception) as context_mrk:
                self.run_script_mrk(script_path_mrk, mock_spark_mrk)

        self.assertIn("Load failed", str(context_mrk.exception))
        mock_common_mrk.t2_load_file.assert_called_once()
        mock_common_mrk.get_dbutils.assert_called_once()