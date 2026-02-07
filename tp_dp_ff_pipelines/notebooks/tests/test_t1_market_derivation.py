import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestMarketDerivationScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_market_derivation.py"

    def get_mock_modules(self, raise_lkp_exception=False, raise_mkt_exception=False):
        # Mock SparkSession and DataFrames
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.createOrReplaceTempView = MagicMock()

        mock_spark.read.parquet.return_value = mock_df
        mock_spark.sql.return_value = mock_df

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        pyspark_sql_module = types.ModuleType("pyspark.sql")
        pyspark_sql_module.__path__ = []
        pyspark_sql_module.SparkSession = spark_session_module.SparkSession

        spark_functions_module = types.ModuleType("pyspark.sql.functions")
        spark_functions_module.coalesce = MagicMock()
        spark_functions_module.col = MagicMock()
        spark_functions_module.lit = MagicMock()

        spark_connect_module = types.ModuleType("pyspark.sql.connect")

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.side_effect = lambda scope, key: "mock_value"

        mock_logger = MagicMock()

        mock_args = MagicMock()
        mock_args.FILE_NAME = "market_file.csv"
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R789"

        mock_lkp = MagicMock()
        mock_lkp.vendr_id = "V001"
        mock_lkp.srce_sys_id = "SYS001"
        mock_lkp.cntry_id = "CNTRY001"

        if raise_lkp_exception:
            load_cntrt_categ_cntry_assoc = MagicMock(side_effect=Exception("Contract lookup failed"))
        else:
            load_cntrt_categ_cntry_assoc = MagicMock(return_value=mock_lkp)

        if raise_mkt_exception:
            load_mkt_skid_lkp = MagicMock(side_effect=Exception("Market lookup failed"))
        else:
            load_mkt_skid_lkp = MagicMock(return_value=mock_df)

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
        mock_common.load_cntrt_categ_cntry_assoc = load_cntrt_categ_cntry_assoc
        mock_common.load_mkt_skid_lkp = load_mkt_skid_lkp
        mock_common.materialize = MagicMock()
        mock_common.materialise_path = MagicMock(return_value="mock_path")  # ✅ Fix for ImportError

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": pyspark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "pyspark.sql.functions": spark_functions_module,
            "pyspark.sql.connect": spark_connect_module,
            "tp_utils": types.ModuleType("tp_utils"),
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_common

    def run_script(self):
        with open(self.script_path) as f:
            code = compile(f.read(), self.script_path, 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_market_derivation(self):
        mock_modules, mock_common = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.load_cntrt_categ_cntry_assoc.assert_called_once()
        mock_common.load_mkt_skid_lkp.assert_called_once()
        mock_common.materialize.assert_called_once()

    def test_contract_lookup_exception(self):
        mock_modules, mock_common = self.get_mock_modules(raise_lkp_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Contract lookup failed", str(context.exception))

    def test_market_lookup_exception(self):
        mock_modules, mock_common = self.get_mock_modules(raise_mkt_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Market lookup failed", str(context.exception))
