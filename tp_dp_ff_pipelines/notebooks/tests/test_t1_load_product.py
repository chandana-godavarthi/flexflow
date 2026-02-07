import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestProductFileLoadScript(unittest.TestCase):
    def setUp(self):
        self.script_path_product = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_load_product.py"

    def get_mock_modules_product(self, run_id="R321"):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark
        mock_spark.sql.return_value = MagicMock()
        mock_spark.sql.return_value.createOrReplaceTempView = MagicMock()

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")

        mock_logger = MagicMock()
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "schema"

        mock_args = MagicMock()
        mock_args.FILE_NAME = "product_file.csv"
        mock_args.CNTRT_ID = "C321"
        mock_args.RUN_ID = run_id

        mock_df_lkp = MagicMock()
        mock_df_lkp.categ_id = "CAT321"
        mock_df_lkp.srce_sys_id = "SYS321"
        mock_df_lkp.file_formt = "FFS"
        mock_df_lkp.cntrt_code = "CODE321"

        mock_df_max_lvl = MagicMock()

        mock_common.__dict__.update({
            "get_dbutils": MagicMock(return_value=mock_dbutils),
            "get_logger": MagicMock(return_value=mock_logger),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd'
            }),
            "read_run_params": MagicMock(return_value=mock_args),
            "load_cntrt_categ_cntry_assoc": MagicMock(return_value=mock_df_lkp),
            "load_max_lvl": MagicMock(return_value=mock_df_max_lvl),
            "t1_load_file": MagicMock(),
            "materialize": MagicMock(),
            "materialise_path": MagicMock(return_value="mock_path"),
            "extract_product_ffs": MagicMock(),
            "extract_product_ffs2": MagicMock(),
            "extract_product_sff3": MagicMock(return_value=(MagicMock(), MagicMock())),
            "extract_product_sff2": MagicMock(),
            "extract_product_sff": MagicMock(),
            "extract_product_tape": MagicMock()
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_common

    def run_script_product(self):
        with open(self.script_path_product) as f:
            code = compile(f.read(), "t2_single_file_load_product.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_execution_ffs(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_ffs.assert_called_once()
        mock_common.materialize.assert_called()

    def test_successful_execution_ffs2(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.return_value.file_formt = "FFS2"
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_ffs2.assert_called_once()

    def test_successful_execution_sff3(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.return_value.file_formt = "SFF3"
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_sff3.assert_called_once()
        mock_common.materialize.assert_called()

    def test_successful_execution_sff2(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.return_value.file_formt = "SFF2"
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_sff2.assert_called_once()
        mock_common.materialize.assert_called()

    def test_successful_execution_sff(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.return_value.file_formt = "SFF"
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_sff.assert_called_once()
        mock_common.materialize.assert_called()

    def test_successful_execution_tape(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.return_value.file_formt = "Tape2"
        with patch.dict(sys.modules, mock_modules):
            self.run_script_product()
        mock_common.extract_product_tape.assert_called_once()

    def test_contract_lookup_exception_product(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.load_cntrt_categ_cntry_assoc.side_effect = Exception("Contract lookup failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_product()
        self.assertIn("Contract lookup failed", str(context.exception))

    def test_extract_product_ffs_exception(self):
        mock_modules, mock_common = self.get_mock_modules_product()
        mock_common.extract_product_ffs.side_effect = Exception("FFS product transformation failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_product()
        self.assertIn("FFS product transformation failed", str(context.exception))