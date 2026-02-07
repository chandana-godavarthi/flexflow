import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestProductDerivationScript(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_product_derivation.py"

    def create_mock_df(self, count_value=1):
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.count = MagicMock(return_value=count_value)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.withColumn = MagicMock(return_value=mock_df)
        mock_df.drop = MagicMock(return_value=mock_df)
        mock_df.createOrReplaceTempView = MagicMock()
        return mock_df

    def get_mock_modules(self, raise_lkp_exception=False, raise_skid_exception=False, new_product_count=1):
        mock_spark = MagicMock()
        mock_df_main = self.create_mock_df(count_value=new_product_count)
        mock_df_other = self.create_mock_df(count_value=1)

        mock_spark.read.parquet.return_value = mock_df_main
        mock_spark.sql.return_value = mock_df_other

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

        mock_lkp = MagicMock()
        mock_lkp.categ_id = "CAT001"
        mock_lkp.srce_sys_id = "SYS001"
        mock_lkp.file_formt = "SFF"
        mock_lkp.cntrt_code = "CODE001"

        if raise_lkp_exception:
            load_cntrt_categ_cntry_assoc = MagicMock(side_effect=Exception("Contract lookup failed"))
        else:
            load_cntrt_categ_cntry_assoc = MagicMock(return_value=mock_lkp)

        if raise_skid_exception:
            skid_service = MagicMock(side_effect=Exception("SKID service failed"))
        else:
            skid_service = MagicMock(return_value=mock_df_main)

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
        mock_common.convert_cols_to_lower = MagicMock(return_value=mock_df_main)
        mock_common.column_complementer = MagicMock(return_value=mock_df_main)
        mock_common.t1_get_attribute_codes = MagicMock(return_value=(mock_df_main, mock_df_main))
        mock_common.t1_get_attribute_values = MagicMock(return_value=mock_df_main)
        mock_common.t1_add_row_change_description = MagicMock(return_value=mock_df_main)
        mock_common.skid_service = skid_service
        mock_common.t1_normalise_product_attributes = MagicMock()
        mock_common.t1_generate_product_description = MagicMock()
        mock_common.t1_product_publish = MagicMock()
        mock_common.materialise_path = MagicMock(return_value="mock_path")

        mock_skidv2 = types.ModuleType("dnalib.skidv2")
        mock_skidv2.SkidClientv2 = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": pyspark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "pyspark.sql.functions": spark_functions_module,
            "pyspark.sql.connect": spark_connect_module,
            "tp_utils": types.ModuleType("tp_utils"),
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common,
            "dnalib.skidv2": mock_skidv2
        }, mock_common, mock_dbutils

    def run_script(self, mock_dbutils=None):
        with open(self.script_path) as f:
            code = compile(f.read(), self.script_path, 'exec')
            globals_dict = {"__name__": "__main__"}
            if mock_dbutils:
                globals_dict["dbutils"] = mock_dbutils
            exec(code, globals_dict)

    def test_successful_run(self):
        mock_modules, mock_common, mock_dbutils = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            try:
                self.run_script(mock_dbutils=mock_dbutils)
            except Exception as e:
                self.fail(f"Script raised an exception unexpectedly: {e}")

    def test_contract_lookup_exception(self):
        mock_modules, _, mock_dbutils = self.get_mock_modules(raise_lkp_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_dbutils=mock_dbutils)
        self.assertIn("Contract lookup failed", str(context.exception))

    def test_skid_service_exception(self):
        mock_modules, _, mock_dbutils = self.get_mock_modules(raise_skid_exception=True)
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script(mock_dbutils=mock_dbutils)
        self.assertIn("SKID service failed", str(context.exception))

    def test_no_new_products(self):
        mock_modules, _, mock_dbutils = self.get_mock_modules(new_product_count=0)
        with patch.dict(sys.modules, mock_modules):
            try:
                self.run_script(mock_dbutils=mock_dbutils)
            except Exception as e:
                self.fail(f"Script raised an exception unexpectedly: {e}")

    def test_full_match_filtering(self):
        mock_modules, mock_common, mock_dbutils = self.get_mock_modules()
        mock_df = self.create_mock_df(count_value=0)
        mock_common.t1_add_row_change_description.return_value = mock_df
        with patch.dict(sys.modules, mock_modules):
            try:
                self.run_script(mock_dbutils=mock_dbutils)
            except Exception as e:
                self.fail(f"Script raised an exception unexpectedly: {e}")

    def test_products_with_null_skid_and_full_match(self):
        mock_modules, mock_common, mock_dbutils = self.get_mock_modules()

        # Create a mock DataFrame with specific behavior
        mock_df = MagicMock()
        mock_df.show = MagicMock()
        mock_df.count = MagicMock(return_value=1)
        mock_df.filter = MagicMock(return_value=mock_df)
        mock_df.withColumn = MagicMock(return_value=mock_df)
        mock_df.drop = MagicMock(return_value=mock_df)
        mock_df.createOrReplaceTempView = MagicMock()
        mock_df.isNull = MagicMock(return_value=False)

        # Patch all relevant functions to return this mock_df
        mock_common.t1_add_row_change_description.return_value = mock_df
        mock_common.t1_get_attribute_codes.return_value = (mock_df, mock_df)
        mock_common.t1_get_attribute_values.return_value = mock_df
        mock_common.convert_cols_to_lower.return_value = mock_df
        mock_common.column_complementer.return_value = mock_df
        mock_common.skid_service.return_value = mock_df

        with patch.dict(sys.modules, mock_modules):
            try:
                self.run_script(mock_dbutils=mock_dbutils)
            except Exception as e:
                self.fail(f"Script raised an exception unexpectedly: {e}")


if __name__ == "__main__":
    unittest.main()