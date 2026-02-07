import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFourFileLoadTimeScript(unittest.TestCase):

    def get_mock_modules_tme(self, run_id_tme="R456", raise_exception_tme=False):
        mock_spark_tme = MagicMock()
        mock_builder_tme = MagicMock()
        mock_builder_tme.appName.return_value.getOrCreate.return_value = mock_spark_tme

        spark_session_module_tme = types.ModuleType("pyspark.sql.session")
        spark_session_module_tme.SparkSession = MagicMock(builder=mock_builder_tme)

        spark_sql_module_tme = types.ModuleType("pyspark.sql")
        spark_sql_module_tme.SparkSession = spark_session_module_tme.SparkSession

        mock_tp_utils_tme = types.ModuleType("tp_utils")
        mock_common_tme = types.ModuleType("tp_utils.common")

        mock_df_vendor_tme = MagicMock()
        mock_df_vendor_tme.select.return_value.collect.return_value = [MagicMock(file_patrn="vendor_pattern")]

        mock_df_step_tme = MagicMock()
        mock_df_step_tme.select.return_value.collect.return_value = [MagicMock(file_patrn="step_pattern")]

        mock_df_dlmtr_tme = MagicMock()
        mock_df_dlmtr_tme.collect.return_value = [MagicMock(dlmtr_val=",")]

        mock_logger_tme = MagicMock()

        mock_common_tme.__dict__.update({
            "get_dbutils": MagicMock(),
            "get_logger": MagicMock(return_value=mock_logger_tme),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd',
                'postgres_schema': 'schema'
            }),
            "read_run_params": MagicMock(return_value=MagicMock(CNTRT_ID="C123", RUN_ID=run_id_tme)),
            "load_cntrt_lkp": MagicMock(return_value=mock_df_vendor_tme),
            "load_cntrt_file_lkp": MagicMock(return_value=mock_df_step_tme),
            "load_cntrt_dlmtr_lkp": MagicMock(return_value=mock_df_dlmtr_tme),
            "load_file": MagicMock(side_effect=Exception("File load failed") if raise_exception_tme else None)
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_tme,
            "pyspark.sql.session": spark_session_module_tme,
            "tp_utils": mock_tp_utils_tme,
            "tp_utils.common": mock_common_tme,
            "src.tp_utils.common": mock_common_tme
        }, mock_common_tme, mock_spark_tme, mock_logger_tme

    def run_script_tme(self, script_path_tme):
        with open(script_path_tme) as f_tme:
            code_tme = compile(f_tme.read(), script_path_tme.split("/")[-1], 'exec')
            exec(code_tme, {"__name__": "__main__"})

    def test_successful_execution_tme(self):
        mock_modules_tme, mock_common_tme, mock_spark_tme, mock_logger_tme = self.get_mock_modules_tme()
        script_path_tme = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_time.py"

        with patch.dict(sys.modules, mock_modules_tme):
            self.run_script_tme(script_path_tme)

        mock_common_tme.load_file.assert_called_once_with(
            "time", "R456", "C123", "step_pattern", "load_time", ",",
            "schema", mock_spark_tme,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

        mock_logger_tme.info.assert_any_call("Vendor File Pattern: vendor_pattern")
        mock_logger_tme.info.assert_any_call("Step File Pattern: step_pattern")
        mock_logger_tme.info.assert_any_call("File Delimiter: ,")

    def test_missing_run_id_tme(self):
        mock_modules_tme, mock_common_tme, mock_spark_tme, _ = self.get_mock_modules_tme(run_id_tme=None)
        script_path_tme = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_time.py"

        with patch.dict(sys.modules, mock_modules_tme):
            self.run_script_tme(script_path_tme)

        mock_common_tme.load_file.assert_called_once_with(
            "time", None, "C123", "step_pattern", "load_time", ",",
            "schema", mock_spark_tme,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

    def test_load_file_exception_tme(self):
        mock_modules_tme, _, _, _ = self.get_mock_modules_tme(raise_exception_tme=True)
        script_path_tme = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_four_file_load_time.py"

        with patch.dict(sys.modules, mock_modules_tme):
            with self.assertRaises(Exception) as context_tme:
                self.run_script_tme(script_path_tme)

        self.assertIn("File load failed", str(context_tme.exception))