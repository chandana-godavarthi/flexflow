import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestContractPurgeScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/purge.py"

    def get_mock_modules(self, run_id="R789", cntrt_id="C456", df_count=1):
        mock_spark = MagicMock()
        mock_spark.sql.return_value = MagicMock()

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "catalog"

        mock_args = MagicMock()
        mock_args.CNTRT_ID = cntrt_id
        mock_args.RUN_ID = run_id

        mock_df = MagicMock()
        mock_df.count.return_value = df_count
        mock_df.select.return_value.first.return_value = {"srce_sys_id": "SYS789"}
        mock_df.show.return_value = None

        mock_common = types.ModuleType("tp_utils.common")
        mock_common.__dict__.update({
            "get_dbutils": MagicMock(return_value=mock_dbutils),
            "get_logger": MagicMock(return_value=MagicMock()),
            "get_database_config": MagicMock(return_value={
                'ref_db_jdbc_url': 'jdbc:postgresql://localhost:5432/refdb',
                'ref_db_name': 'refdb',
                'ref_db_user': 'user',
                'ref_db_pwd': 'pwd',
                'catalog_name': 'catalog',
                'postgres_schema': 'schema',
                'ref_db_hostname': 'localhost'
            }),
            "read_run_params": MagicMock(return_value=mock_args),
            "load_cntrt_lkp": MagicMock(return_value=mock_df),
            "time_perd_class_codes": MagicMock(return_value="WEEKLY"),
            "recursive_delete": MagicMock(),
            "cdl_publishing": MagicMock(),
            "derive_publish_path": MagicMock(return_value="/tmp/fake_publish_path"),
            "update_to_postgres": MagicMock()  #  Added to fix import error
        })

        mock_config_module = types.ModuleType("pg_composite_pipelines_configuration.configuration")
        mock_config_module.Configuration = MagicMock()

        mock_meta_client_module = types.ModuleType("pg_composite_pipelines_cdl.main_meta_client")
        mock_meta_client_module.MetaPSClient = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils.common": mock_common,
            "pg_composite_pipelines_configuration.configuration": mock_config_module,
            "pg_composite_pipelines_cdl.main_meta_client": mock_meta_client_module
        }, mock_common

    def run_script(self):
        with open(self.script_path) as f:
            code = compile(f.read(), "contract_purge_script.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_purge(self):
        mock_modules, mock_common = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.load_cntrt_lkp.assert_called_once()
        mock_common.cdl_publishing.assert_called_once()
        mock_common.update_to_postgres.assert_called_once()  #  Optional assertion

    def test_no_contract_found(self):
        mock_modules, mock_common = self.get_mock_modules(df_count=0)
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.recursive_delete.assert_not_called()
        mock_common.cdl_publishing.assert_not_called()
        mock_common.update_to_postgres.assert_not_called()

    def test_contract_lookup_exception(self):
        mock_modules, mock_common = self.get_mock_modules()
        mock_common.load_cntrt_lkp.side_effect = Exception("Lookup failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Lookup failed", str(context.exception))


    def test_cdl_publishing_exception(self):
        mock_modules, mock_common = self.get_mock_modules()
        mock_common.cdl_publishing.side_effect = Exception("Publishing failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Publishing failed", str(context.exception))