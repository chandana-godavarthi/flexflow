import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestSingleFileLoadFactScript(unittest.TestCase):
    def setUp(self):
        self.script_path_fct = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_single_file_load_fact.py"

    def get_mock_modules_fct(self, run_id_fct="R456", raise_load_exception_fct=False, raise_transform_exception_fct=False):
        mock_spark_fct = MagicMock()
        mock_builder_fct = MagicMock()
        mock_builder_fct.appName.return_value.getOrCreate.return_value = mock_spark_fct

        spark_session_module_fct = types.ModuleType("pyspark.sql.session")
        spark_session_module_fct.SparkSession = MagicMock(builder=mock_builder_fct)

        spark_sql_module_fct = types.ModuleType("pyspark.sql")
        spark_sql_module_fct.SparkSession = spark_session_module_fct.SparkSession

        mock_tp_utils_fct = types.ModuleType("tp_utils")
        mock_common_fct = types.ModuleType("tp_utils.common")

        mock_logger_fct = MagicMock()
        mock_dbutils_fct = MagicMock()
        mock_t2_load_file_fct = MagicMock()
        mock_fact_multipliers_trans_fct = MagicMock()

        if raise_load_exception_fct:
            mock_t2_load_file_fct.side_effect = Exception("Load failed")
        if raise_transform_exception_fct:
            mock_fact_multipliers_trans_fct.side_effect = Exception("Transformation failed")

        mock_args_fct = MagicMock()
        mock_args_fct.FILE_NAME = "fact_file.csv"
        mock_args_fct.CNTRT_ID = "C123"
        mock_args_fct.RUN_ID = run_id_fct

        mock_common_fct.__dict__.update({
            "get_dbutils": MagicMock(return_value=mock_dbutils_fct),
            "get_logger": MagicMock(return_value=mock_logger_fct),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd',
                'catalog_name': 'catalog',
                'postgres_schema': 'schema'
            }),
            "read_run_params": MagicMock(return_value=mock_args_fct),
            "load_file": MagicMock(),
            "load_cntrt_lkp": MagicMock(),
            "load_cntrt_file_lkp": MagicMock(),
            "load_cntrt_dlmtr_lkp": MagicMock(),
            "t2_load_file": mock_t2_load_file_fct,
            "fact_multipliers_trans": mock_fact_multipliers_trans_fct
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_fct,
            "pyspark.sql.session": spark_session_module_fct,
            "tp_utils": mock_tp_utils_fct,
            "tp_utils.common": mock_common_fct,
            "src.tp_utils.common": mock_common_fct
        }, mock_t2_load_file_fct, mock_fact_multipliers_trans_fct

    def run_script_fct(self):
        with open(self.script_path_fct) as f_fct:
            code_fct = compile(f_fct.read(), "t2_single_file_load_fact.py", 'exec')
            exec(code_fct, {"__name__": "__main__"})

    def test_successful_fact_file_execution_fct(self):
        mock_modules_fct, mock_t2_load_file_fct, mock_fact_multipliers_trans_fct = self.get_mock_modules_fct()
        with patch.dict(sys.modules, mock_modules_fct, clear=True):
            self.run_script_fct()
        mock_t2_load_file_fct.assert_called_once()
        mock_fact_multipliers_trans_fct.assert_called_once()

    def test_missing_run_id_fct(self):
        mock_modules_fct, mock_t2_load_file_fct, mock_fact_multipliers_trans_fct = self.get_mock_modules_fct(run_id_fct=None)
        with patch.dict(sys.modules, mock_modules_fct, clear=True):
            self.run_script_fct()
        mock_t2_load_file_fct.assert_called_once()
        mock_fact_multipliers_trans_fct.assert_called_once()

    def test_t2_load_file_exception_fct(self):
        mock_modules_fct, mock_t2_load_file_fct, mock_fact_multipliers_trans_fct = self.get_mock_modules_fct(raise_load_exception_fct=True)
        with patch.dict(sys.modules, mock_modules_fct, clear=True):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()
        self.assertIn("Load failed", str(context_fct.exception))
        mock_fact_multipliers_trans_fct.assert_not_called()

    def test_fact_multipliers_trans_exception_fct(self):
        mock_modules_fct, mock_t2_load_file_fct, mock_fact_multipliers_trans_fct = self.get_mock_modules_fct(raise_transform_exception_fct=True)
        with patch.dict(sys.modules, mock_modules_fct, clear=True):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()
        self.assertIn("Transformation failed", str(context_fct.exception))
        mock_t2_load_file_fct.assert_called_once()