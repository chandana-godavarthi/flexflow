import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestContractColAssignMaterialization(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/column_mappings.py"

    def get_mock_modules(self):
        # Mock Spark and DataFrame
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.write.mode.return_value.format.return_value.save = MagicMock()

        mock_spark.read = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        # Mock tp_utils.common
        mock_logger = MagicMock()
        mock_dbutils = MagicMock()
        mock_args = MagicMock()
        mock_args.FILE_NAME = "file.csv"
        mock_args.CNTRT_ID = 123
        mock_args.RUN_ID = 1323

        mock_common = types.ModuleType("tp_utils.common")
        mock_common.get_logger = MagicMock(return_value=mock_logger)
        mock_common.get_dbutils = MagicMock(return_value=mock_dbutils)
        mock_common.read_run_params = MagicMock(return_value=mock_args)
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
            'ref_db_name': 'refdb',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'postgres_schema': 'public'
        })
        mock_common.load_cntrt_col_assign = MagicMock(return_value=mock_df)
        mock_common.read_from_postgres = MagicMock()
        mock_common.read_query_from_postgres = MagicMock()
        mock_common.materialise_path = MagicMock(return_value="/mnt/tp-source-data/temp/materialised/1323/column_mappings_df_dpf_col_asign_vw")

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils.common": mock_common
        }, mock_common, mock_df, mock_logger

    def run_script(self):
        with open(self.script_path) as f:
            code = compile(f.read(), "column_mappings.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_materialization(self):
        mock_modules, mock_common, mock_df, mock_logger = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script()

        mock_common.load_cntrt_col_assign.assert_called_once()
        mock_df.write.mode.return_value.format.return_value.save.assert_called_once()

        # Flexible logger assertion
        expected_fragment = "/mnt/tp-source-data/temp/materialised/1323/column_mappings_df_dpf_col_asign_vw"
        self.assertTrue(
            any(expected_fragment in str(call) for call in mock_logger.info.call_args_list),
            f"Expected log message containing: {expected_fragment} not found"
        )

    def test_load_contract_assign_exception(self):
        mock_modules, mock_common, _, _ = self.get_mock_modules()
        mock_common.load_cntrt_col_assign.side_effect = Exception("Load failed")

        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()

        self.assertIn("Load failed", str(context.exception))
