import sys
import types
from unittest import TestCase
from unittest.mock import MagicMock, patch

class TestPublishRunLogs(TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_atomic_measure_calculations.py"

    def get_base_mocks(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.count.return_value = 1
        mock_df.columns = ["col1", "col2"]
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.write.mode.return_value.format.return_value.saveAsTable = MagicMock()
        mock_df.collect.return_value = [MagicMock(RUN_ID=789)]

        mock_spark.read.parquet.return_value = mock_df
        mock_spark.sql.return_value = mock_df
        mock_spark.table.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.__path__ = []
        pyspark_sql_module.SparkSession = spark_session_module.SparkSession

        spark_functions_module = types.ModuleType("pyspark.sql.functions")
        for func in ["col", "lower", "lit", "length", "when", "max", "min"]:
            setattr(spark_functions_module, func, MagicMock())

        spark_connect_module = types.ModuleType("pyspark.sql.connect")

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.side_effect = lambda scope, key: "mock_value"

        mock_logger = MagicMock()

        mock_args = MagicMock()
        mock_args.CNTRT_ID = 123
        mock_args.RUN_ID = 789

        mock_common = types.ModuleType("tp_utils.common")
        for attr in [
            'get_dbutils', 'get_logger', 'get_database_config', 'read_run_params',
            'materialize', 'materialise_path',
            'read_from_postgres', 'column_complementer',
            'add_secure_group_key', 'time_perd_class_codes',
            'load_cntrt_categ_cntry_assoc', 'cdl_publishing', 'write_to_postgres',
            'dynamic_expression', 'load_measr_vendr_factr_lkp', 'load_measr_cntrt_factr_lkp','add_latest_time_perd'
        ]:
            setattr(mock_common, attr, MagicMock())

        mock_common.get_dbutils.return_value = mock_dbutils
        mock_common.get_logger.return_value = mock_logger
        mock_common.get_database_config.return_value = {
            'ref_db_jdbc_url': 'url',
            'ref_db_name': 'name',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'ref_db_hostname': 'hostname', 
            'catalog_name': 'catalog',
            'postgres_schema': 'schema'
        }
        mock_common.read_run_params.return_value = mock_args
        mock_common.read_from_postgres.return_value = mock_df
        mock_common.column_complementer.return_value = mock_df
        mock_common.add_latest_time_perd.return_value = mock_df
        mock_common.add_secure_group_key.return_value = mock_df
        mock_common.time_perd_class_codes.return_value = "TPC"
        mock_common.load_cntrt_categ_cntry_assoc.return_value = MagicMock(categ_id="CID", srce_sys_id=123)
        mock_common.materialise_path.return_value = "/mnt/tp-source-data/temp/materialised/789/atomic_measure_output"

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
            "pg_composite_pipelines_configuration.configuration": mock_config_module,
            "pg_composite_pipelines_cdl.main_meta_client": mock_meta_client_module,
            "pg_composite_pipelines_cdl.rest.azure_token_provider": mock_auth_module
        }, mock_common, mock_config_module, mock_meta_client_module, mock_auth_module

    def run_script(self, mock_common, mock_config, mock_meta, mock_auth):
        globals()["dbutils"] = mock_common.get_dbutils(MagicMock())
        globals()["Configuration"] = mock_config.Configuration
        globals()["MetaPSClient"] = mock_meta.MetaPSClient
        globals()["SPAuthClient"] = mock_auth.SPAuthClient
        with open(self.script_path) as f:
            code = compile(f.read(), self.script_path, 'exec')
            exec(code, {"__name__": "__main__"})

    def test_contract_lookup_failure(self):
        mock_modules, mock_common, mock_config, mock_meta, mock_auth = self.get_base_mocks()
        mock_common.load_cntrt_categ_cntry_assoc.side_effect = Exception("Contract lookup failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_common, mock_config, mock_meta, mock_auth)
        self.assertIn("Contract lookup failed", str(context.exception))
