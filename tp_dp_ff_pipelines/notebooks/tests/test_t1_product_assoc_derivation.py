import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestProductAssocDerivationScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_product_assoc_derivation.py"  # Update with actual path if needed

    def get_mock_modules(self, raise_postgres_exception=False, raise_derivation_exception=False, raise_publish_exception=False):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.createOrReplaceTempView = MagicMock()
        mock_spark.sql.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.__path__ = []
        pyspark_sql_module.SparkSession = spark_session_module.SparkSession

        spark_functions_module = types.ModuleType("pyspark.sql.functions")
        spark_functions_module.col = MagicMock()

        spark_connect_module = types.ModuleType("pyspark.sql.connect")

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.side_effect = lambda scope, key: "mock_value"

        mock_logger = MagicMock()

        mock_args = MagicMock()
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R789"

        mock_common = types.ModuleType("tp_utils.common")
        mock_common.get_dbutils = MagicMock(return_value=mock_dbutils)
        mock_common.get_logger = MagicMock(return_value=mock_logger)
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
            'ref_db_name': 'refdb',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'catalog',
            'postgres_schema': 'schema'
        })
        mock_common.read_run_params = MagicMock(return_value=mock_args)
        mock_common.materialize = MagicMock()

        mock_common.read_from_postgres = MagicMock(return_value=mock_df)
        if raise_postgres_exception:
            mock_common.read_from_postgres.side_effect = Exception("Postgres read failed")

        mock_common.derive_product_assoc = MagicMock(return_value=mock_df)
        if raise_derivation_exception:
            mock_common.derive_product_assoc.side_effect = Exception("Derivation failed")

        mock_common.cdl_publishing = MagicMock()
        if raise_publish_exception:
            mock_common.cdl_publishing.side_effect = Exception("Publishing failed")

        mock_common.column_complementer = MagicMock(return_value=mock_df)

        mock_config_module = types.ModuleType("pg_composite_pipelines_configuration.configuration")
        mock_config_module.Configuration = MagicMock()

        mock_meta_client_module = types.ModuleType("pg_composite_pipelines_cdl.main_meta_client")
        mock_meta_client_module.MetaPSClient = MagicMock()

        mock_auth_module = types.ModuleType("pg_composite_pipelines_cdl.rest.azure_token_provider")
        mock_auth_module.SPAuthClient = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": pyspark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "pyspark.sql.functions": spark_functions_module,
            "pyspark.sql.connect": spark_connect_module,
            "tp_utils": types.ModuleType("tp_utils"),
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common,
            "pg_composite_pipelines_configuration.configuration": mock_config_module,
            "pg_composite_pipelines_cdl.main_meta_client": mock_meta_client_module,
            "pg_composite_pipelines_cdl.rest.azure_token_provider": mock_auth_module
        }, mock_common, mock_config_module, mock_meta_client_module, mock_auth_module

    def run_script(self, mock_common, mock_config_module, mock_meta_client_module, mock_auth_module):
        globals()["dbutils"] = mock_common.get_dbutils(MagicMock())
        globals()["Configuration"] = mock_config_module.Configuration
        globals()["MetaPSClient"] = mock_meta_client_module.MetaPSClient
        globals()["SPAuthClient"] = mock_auth_module.SPAuthClient
        with open(self.script_path) as f:
            code = compile(f.read(), self.script_path, 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_execution(self):
        mock_modules, mock_common, mock_config, mock_meta, mock_auth = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script(mock_common, mock_config, mock_meta, mock_auth)
        mock_common.read_from_postgres.assert_called_once()
        mock_common.derive_product_assoc.assert_called_once()
        mock_common.materialize.assert_called_once()
        mock_common.cdl_publishing.assert_called_once()

    def test_postgres_read_exception(self):
        mock_modules, mock_common, mock_config, mock_meta, mock_auth = self.get_mock_modules(raise_postgres_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_common, mock_config, mock_meta, mock_auth)
        self.assertIn("Postgres read failed", str(context.exception))

    def test_derivation_exception(self):
        mock_modules, mock_common, mock_config, mock_meta, mock_auth = self.get_mock_modules(raise_derivation_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_common, mock_config, mock_meta, mock_auth)
        self.assertIn("Derivation failed", str(context.exception))

    def test_publishing_exception(self):
        mock_modules, mock_common, mock_config, mock_meta, mock_auth = self.get_mock_modules(raise_publish_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_common, mock_config, mock_meta, mock_auth)
        self.assertIn("Publishing failed", str(context.exception))