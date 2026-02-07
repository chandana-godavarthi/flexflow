import sys
import types
import unittest
from unittest.mock import MagicMock, patch, ANY

class TestMarketFileLoadScript(unittest.TestCase):
    def setUp(self):
        self.script_path_market = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_publish_run_logs.py"

    def get_mock_modules_market(self, run_id="R789"):
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.functions = MagicMock()
        pyspark_sql_module.types = MagicMock()
        pyspark_sql_module.connect = types.ModuleType("pyspark.sql.connect")
        pyspark_sql_session_module = types.ModuleType("pyspark.sql.session")
        pyspark_sql_session_module.SparkSession = MagicMock(builder=mock_builder)
        pyspark_sql_module.session = pyspark_sql_session_module
        pyspark_sql_module.SparkSession = pyspark_sql_session_module.SparkSession

        sys.modules["pyspark.sql"] = pyspark_sql_module
        sys.modules["pyspark.sql.functions"] = pyspark_sql_module.functions
        sys.modules["pyspark.sql.types"] = pyspark_sql_module.types
        sys.modules["pyspark.sql.connect"] = pyspark_sql_module.connect
        sys.modules["pyspark.sql.session"] = pyspark_sql_session_module

        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry

        mock_logger = MagicMock()
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "schema"

        mock_args = MagicMock()
        mock_args.FILE_NAME = "market_file.csv"
        mock_args.CNTRT_ID = "C789"
        mock_args.RUN_ID = run_id

        mock_df_lkp = MagicMock()
        mock_df_lkp.categ_id = "CAT789"
        mock_df_lkp.srce_sys_id = "SYS789"
        mock_df_lkp.file_formt = "FFS"
        mock_df_lkp.cntrt_code = "CODE789"

        mock_df = MagicMock()
        mock_df.columns = ['col1', 'col2']
        mock_df.count.return_value = 1
        mock_df.write.mode().format().saveAsTable = MagicMock()

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
            "t1_load_file": MagicMock(),
            "materialize": MagicMock(),
            "extract_market_ffs": MagicMock(),
            "extract_market_ffs2": MagicMock(),
            "extract_market_sff3": MagicMock(),
            "extract_market_sff": MagicMock(),
            "extract_market_tape": MagicMock(),
            "read_from_postgres": MagicMock(return_value=mock_df),
            "column_complementer": MagicMock(return_value=mock_df),
            "add_secure_group_key": MagicMock(return_value=mock_df),
            "time_perd_class_codes": MagicMock(return_value="TPC123"),
            "materialise_path": MagicMock(return_value="mock_path"),
            "cdl_publishing": MagicMock(),
            "semaphore_generate_path": MagicMock(return_value="mock_path"),
            "semaphore_acquisition": MagicMock(return_value="mock_path"),
            "release_semaphore": MagicMock(return_value="mock_path")
        })

        config_module = types.ModuleType("configuration")
        config_module.Configuration = MagicMock()
        sys.modules["pg_composite_pipelines_configuration"] = types.ModuleType("pg_composite_pipelines_configuration")
        sys.modules["pg_composite_pipelines_configuration.configuration"] = config_module

        meta_client_module = types.ModuleType("main_meta_client")
        meta_client_module.MetaPSClient = MagicMock()
        sys.modules["pg_composite_pipelines_cdl"] = types.ModuleType("pg_composite_pipelines_cdl")
        sys.modules["pg_composite_pipelines_cdl.main_meta_client"] = meta_client_module

        azure_token_provider_module = types.ModuleType("azure_token_provider")
        azure_token_provider_module.SPAuthClient = MagicMock()
        rest_module = types.ModuleType("rest")
        rest_module.azure_token_provider = azure_token_provider_module
        sys.modules["pg_composite_pipelines_cdl.rest"] = rest_module
        sys.modules["pg_composite_pipelines_cdl.rest.azure_token_provider"] = azure_token_provider_module

        return {
            "pyspark": types.ModuleType("pyspark"),
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common,
            "pg_composite_pipelines_configuration.configuration": config_module,
            "pg_composite_pipelines_cdl.main_meta_client": meta_client_module,
            "pg_composite_pipelines_cdl.rest.azure_token_provider": azure_token_provider_module
        }, mock_common

    def run_script_market(self):
        with open(self.script_path_market) as f:
            code = compile(f.read(), "t1_publish_run_logs.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def assert_materialize_called_with(self, mock_common, df_name, run_id="R789"):
        try:
            mock_common.materialize.assert_any_call(ANY, df_name, run_id)
        except AssertionError:
            print(f"❌ Expected materialize call not found for: {df_name}")
            print("Actual calls:")
            for call in mock_common.materialize.call_args_list:
                print(call)
            raise

    # --- Tests ---

    def test_contract_lookup_exception_market(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        mock_common.load_cntrt_categ_cntry_assoc.side_effect = Exception("Contract lookup failed")
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_market()
        self.assertIn("Contract lookup failed", str(context.exception))

    def test_successful_execution_measure_plc(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        self.assert_materialize_called_with(mock_common, 'Publish_Run_Logs_df_measr_plc')

    def test_successful_execution_time_perd_plc(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        self.assert_materialize_called_with(mock_common, 'Publish_Run_Logs_df_time_plc')

    def test_successful_execution_prttn_plc(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        self.assert_materialize_called_with(mock_common, 'Publish_Run_Logs_df_prttn_plc')

    def test_successful_execution_mkt_plc(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        mock_common.column_complementer.assert_called()


    def test_successful_execution_prod_plc(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        mock_common.column_complementer.assert_called()

    def test_successful_execution_cdl_publishing(self):
        mock_modules, mock_common = self.get_mock_modules_market()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_market()
        mock_common.cdl_publishing.assert_called_once_with(
            "TP_RUN_MEASR_PLC",
            "TP_RUN_MEASR_PLC",
            "TP_RUN_MEASR_PLC",
            "cntrt_id",
            mock_common.get_dbutils.return_value,
            sys.modules["pg_composite_pipelines_configuration.configuration"].Configuration,
            sys.modules["pg_composite_pipelines_cdl.main_meta_client"].MetaPSClient
        )
