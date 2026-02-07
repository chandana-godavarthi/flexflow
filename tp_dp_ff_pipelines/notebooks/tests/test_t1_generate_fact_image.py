import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFactImageGenerationScript(unittest.TestCase):
    def setUp(self):
        self.script_path_factimg = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_generate_fact_image.py"

    def get_mock_modules_factimg(self, run_id="R111"):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark
        mock_spark.sql.return_value = MagicMock()
        mock_spark.read.parquet.return_value = MagicMock()

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
        mock_args.CNTRT_ID = "C111"
        mock_args.RUN_ID = run_id

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
            "time_perd_class_codes": MagicMock(return_value="mth"),
            "materialize": MagicMock(),
            "column_complementer": MagicMock(return_value=MagicMock()),
            "match_time_perd_class": MagicMock(return_value="mth"),
            "materialise_path": MagicMock(return_value="/tmp/fake_path")  # ✅ Added mock
        })

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_common

    def run_script_factimg(self):
        with open(self.script_path_factimg) as f:
            code = compile(f.read(), "t1_generate_fact_image.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_fact_image_generation(self):
        mock_modules, mock_common = self.get_mock_modules_factimg()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_factimg()
        mock_common.time_perd_class_codes.assert_called_once()
        mock_common.materialize.assert_called()
        mock_common.column_complementer.assert_called_once()
        mock_common.match_time_perd_class.assert_called_once()

    def test_time_perd_class_code_exception(self):
        mock_modules, mock_common = self.get_mock_modules_factimg()
        mock_common.time_perd_class_codes.side_effect = Exception("Time period class code lookup failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_factimg()
        self.assertIn("Time period class code lookup failed", str(context.exception))

    def test_match_time_perd_class_exception(self):
        mock_modules, mock_common = self.get_mock_modules_factimg()
        mock_common.match_time_perd_class.side_effect = Exception("Match time period class failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_factimg()
        self.assertIn("Match time period class failed", str(context.exception))
