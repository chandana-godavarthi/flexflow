import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestMetadataRefreshScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/refresh_metadata_product.py"  # Update with actual path

    def get_mock_modules(self):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        mock_logger = MagicMock()
        mock_dbutils = MagicMock()
        mock_args = MagicMock()
        mock_args.FILE_NAME = "prod_file.csv"
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R123"

        mock_common = types.ModuleType("tp_utils.common")
        mock_common.get_logger = MagicMock(return_value=mock_logger)
        mock_common.get_dbutils = MagicMock(return_value=mock_dbutils)
        mock_common.read_run_params = MagicMock(return_value=mock_args)
        mock_common.cdl_publishing = MagicMock()

        config_module = types.ModuleType("configuration")
        config_module.Configuration = MagicMock()

        meta_client_module = types.ModuleType("main_meta_client")
        meta_client_module.MetaPSClient = MagicMock()

        azure_token_provider_module = types.ModuleType("azure_token_provider")
        azure_token_provider_module.SPAuthClient = MagicMock()

        rest_module = types.ModuleType("rest")
        rest_module.azure_token_provider = azure_token_provider_module

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils.common": mock_common,
            "pg_composite_pipelines_configuration": types.ModuleType("pg_composite_pipelines_configuration"),
            "pg_composite_pipelines_configuration.configuration": config_module,
            "pg_composite_pipelines_cdl": types.ModuleType("pg_composite_pipelines_cdl"),
            "pg_composite_pipelines_cdl.main_meta_client": meta_client_module,
            "pg_composite_pipelines_cdl.rest": rest_module,
            "pg_composite_pipelines_cdl.rest.azure_token_provider": azure_token_provider_module
        }, mock_common

    def run_script(self):
        with open(self.script_path) as f:
            code = compile(f.read(), "t1_refresh_metadata.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_metadata_refresh(self):
        mock_modules, mock_common = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.cdl_publishing.assert_called_once()
        mock_common.get_logger().info.assert_any_call("Started refreshing Metadata")
        mock_common.get_logger().info.assert_any_call("Metadata Refreshed")

    def test_metadata_refresh_exception(self):
        mock_modules, mock_common = self.get_mock_modules()
        mock_common.cdl_publishing.side_effect = Exception("Publishing failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Publishing failed", str(context.exception))