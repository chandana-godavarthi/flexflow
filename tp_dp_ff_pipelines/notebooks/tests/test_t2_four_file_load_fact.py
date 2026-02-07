import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFourFileLoadFactScript(unittest.TestCase):

    def get_mock_modules_fct(self, run_id="R456", raise_exception=False):
        mock_spark_fct = MagicMock()
        mock_builder_fct = MagicMock()
        mock_builder_fct.appName.return_value.getOrCreate.return_value = mock_spark_fct

        spark_session_module_fct = types.ModuleType("pyspark.sql.session")
        spark_session_module_fct.SparkSession = MagicMock(builder=mock_builder_fct)

        spark_sql_module_fct = types.ModuleType("pyspark.sql")
        spark_sql_module_fct.SparkSession = spark_session_module_fct.SparkSession

        mock_tp_utils_fct = types.ModuleType("tp_utils")
        mock_common_fct = types.ModuleType("tp_utils.common")

        mock_common_fct.__dict__.update({
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
            "read_run_params": MagicMock(return_value=MagicMock(CNTRT_ID="C123", RUN_ID=run_id)),
            "t2_load_file": MagicMock(side_effect=Exception("Load failed") if raise_exception else None),
            "fact_multipliers_trans": MagicMock(),
            "load_file": MagicMock(),
            "load_cntrt_lkp": MagicMock(),
            "load_cntrt_file_lkp": MagicMock(),
            "load_cntrt_dlmtr_lkp": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_fct,
            "pyspark.sql.session": spark_session_module_fct,
            "tp_utils": mock_tp_utils_fct,
            "tp_utils.common": mock_common_fct,
            "src.tp_utils.common": mock_common_fct
        }, mock_common_fct, mock_spark_fct

    def run_script_fct(self, script_path_fct):
        with open(script_path_fct) as fct:
            code_fct = compile(fct.read(), script_path_fct.split("/")[-1], 'exec')
            exec(code_fct, {"__name__": "__main__"})

    def test_successful_execution_fct(self):
        mock_modules_fct, mock_common_fct, mock_spark_fct = self.get_mock_modules_fct()
        script_path_fct = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_fact.py"

        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct(script_path_fct)

        mock_common_fct.t2_load_file.assert_called_once_with(
            "C123", "FCT", "fact", "R456", "load_fact", "schema",
            mock_spark_fct,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

        mock_common_fct.fact_multipliers_trans.assert_called_once_with("R456", "C123", mock_spark_fct)

    def test_missing_run_id_fct(self):
        mock_modules_fct, mock_common_fct, mock_spark_fct = self.get_mock_modules_fct(run_id=None)
        script_path_fct = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_fact.py"

        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct(script_path_fct)

        mock_common_fct.t2_load_file.assert_called_once_with(
            "C123", "FCT", "fact", None, "load_fact", "schema",
            mock_spark_fct,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

        mock_common_fct.fact_multipliers_trans.assert_called_once_with(None, "C123", mock_spark_fct)

    def test_t2_load_file_exception_fct(self):
        mock_modules_fct, mock_common_fct, _ = self.get_mock_modules_fct(raise_exception=True)
        script_path_fct = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_fact.py"

        with patch.dict(sys.modules, mock_modules_fct):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct(script_path_fct)

        self.assertIn("Load failed", str(context_fct.exception))
        mock_common_fct.fact_multipliers_trans.assert_not_called()