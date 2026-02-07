import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestFactDerivationScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_fact_derivation.py"

    def get_mock_modules(self, file_formt="SFF", raise_lkp_exception=False, raise_derivation_exception=False):
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
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R789"

        mock_lkp = MagicMock()
        mock_lkp.vendr_id = "V001"
        mock_lkp.srce_sys_id = "SYS001"
        mock_lkp.cntry_id = "CNTRY001"
        mock_lkp.file_formt = file_formt

        # Fix: Accept any arguments in lambda
        load_cntrt_categ_cntry_assoc = MagicMock(
            side_effect=Exception("Lookup failed") if raise_lkp_exception else lambda *args, **kwargs: mock_lkp
        )

        derive_fact_sff = MagicMock(
            side_effect=Exception("Derivation failed") if raise_derivation_exception else lambda *args, **kwargs: mock_df
        )
        derive_fact_non_sff = MagicMock(
            side_effect=Exception("Derivation failed") if raise_derivation_exception else lambda *args, **kwargs: mock_df
        )

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
        mock_common.materialize = MagicMock()
        mock_common.derive_fact_sff = derive_fact_sff
        mock_common.derive_fact_non_sff = derive_fact_non_sff
        mock_common.load_measr_id_lkp = MagicMock(return_value=mock_df)
        mock_common.time_perd_class_codes = MagicMock()
        mock_common.materialise_path = MagicMock(return_value="/tmp/fake_path")  # ✅ Required mock

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

    def test_sff_format_derivation(self):
        mock_modules, mock_common = self.get_mock_modules(file_formt="SFF")
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.derive_fact_sff.assert_called_once()
        mock_common.materialize.assert_called()

    def test_non_sff_format_derivation(self):
        mock_modules, mock_common = self.get_mock_modules(file_formt="Tape")
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.derive_fact_non_sff.assert_called_once()
        mock_common.materialize.assert_called()

    def test_measure_mapping_execution(self):
        mock_modules, mock_common = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script()
        mock_common.load_measr_id_lkp.assert_called_once()
        mock_common.materialize.assert_called()

    def test_contract_lookup_exception(self):
        mock_modules, mock_common = self.get_mock_modules(raise_lkp_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Lookup failed", str(context.exception))

    def test_derivation_exception(self):
        mock_modules, mock_common = self.get_mock_modules(raise_derivation_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script()
        self.assertIn("Derivation failed", str(context.exception))
