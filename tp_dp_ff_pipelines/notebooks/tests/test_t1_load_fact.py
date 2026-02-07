import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestSingleFileLoadFactScript(unittest.TestCase):
    def setUp(self):
        self.script_path_fct = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_load_fact.py"

    def get_mock_modules_fct(self, run_id_fct="R456"):
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
        mock_dbutils_fct.secrets.get.return_value = "schema"

        mock_args_fct = MagicMock()
        mock_args_fct.FILE_NAME = "fact_file.csv"
        mock_args_fct.CNTRT_ID = "C123"
        mock_args_fct.RUN_ID = run_id_fct

        mock_df_lkp = MagicMock()
        mock_df_lkp.categ_id = "CAT123"
        mock_df_lkp.srce_sys_id = "SYS456"
        mock_df_lkp.file_formt = "FFS"
        mock_df_lkp.cntrt_code = "CODE789"

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
            "load_cntrt_categ_cntry_assoc": MagicMock(return_value=mock_df_lkp),
            "t1_load_file": MagicMock(),
            "load_fact_sfffile": MagicMock(),
            "materialize": MagicMock(),
            "materialise_path": MagicMock(return_value="/mock/raw/path"),  # ✅ Added
            "extract_fact_ffs": MagicMock(),
            "add_row_count": MagicMock(),
            "extract_fact_sff": MagicMock(),
            "extract_fact_tape": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module_fct,
            "pyspark.sql.session": spark_session_module_fct,
            "tp_utils": mock_tp_utils_fct,
            "tp_utils.common": mock_common_fct,
            "src.tp_utils.common": mock_common_fct
        }, mock_common_fct

    def run_script_fct(self):
        with open(self.script_path_fct) as f_fct:
            code_fct = compile(f_fct.read(), "t2_single_file_load_fact.py", 'exec')
            exec(code_fct, {"__name__": "__main__"})

    def test_successful_fact_file_execution_fct(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct()
        mock_common_fct.t1_load_file.assert_called_once()
        mock_common_fct.extract_fact_ffs.assert_called_once()
        mock_common_fct.materialize.assert_called()

    def test_missing_run_id_fct(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct(run_id_fct=None)
        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct()
        mock_common_fct.t1_load_file.assert_called_once()
        mock_common_fct.extract_fact_ffs.assert_called_once()

    def test_contract_lookup_exception(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.load_cntrt_categ_cntry_assoc.side_effect = Exception("Contract lookup failed")
        with patch.dict(sys.modules, mock_modules_fct):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()
        self.assertIn("Contract lookup failed", str(context_fct.exception))

    def test_successful_execution_with_sff_format(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.load_cntrt_categ_cntry_assoc.return_value.file_formt = "SFF"
        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct()
        mock_common_fct.load_fact_sfffile.assert_called_once()
        mock_common_fct.extract_fact_sff.assert_called_once()

    def test_successful_execution_with_tape_format(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.load_cntrt_categ_cntry_assoc.return_value.file_formt = "Tape2"
        with patch.dict(sys.modules, mock_modules_fct):
            self.run_script_fct()
        mock_common_fct.t1_load_file.assert_called_once()
        mock_common_fct.extract_fact_tape.assert_called_once()

    def test_extract_fact_ffs_exception(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.extract_fact_ffs.side_effect = Exception("FFS transformation failed")
        with patch.dict(sys.modules, mock_modules_fct):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()
        self.assertIn("FFS transformation failed", str(context_fct.exception))

    def test_extract_fact_sff_exception(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.load_cntrt_categ_cntry_assoc.return_value.file_formt = "SFF"
        mock_common_fct.extract_fact_sff.side_effect = Exception("SFF transformation failed")
        with patch.dict(sys.modules, mock_modules_fct):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()
        self.assertIn("SFF transformation failed", str(context_fct.exception))

    def test_extract_fact_tape_exception(self):
        mock_modules_fct, mock_common_fct = self.get_mock_modules_fct()
        mock_common_fct.load_cntrt_categ_cntry_assoc.return_value.file_formt = "Tape2"
        mock_common_fct.extract_fact_tape.side_effect = Exception("Tape transformation failed")
        with patch.dict(sys.modules, mock_modules_fct):
            with self.assertRaises(Exception) as context_fct:
                self.run_script_fct()