import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestT2AcnProductTransformation(unittest.TestCase):

    def get_mock_modules(self, new_product_count=3):
        # Mock SparkSession
        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        spark_sql_connect_module = types.ModuleType("pyspark.sql.connect")  # Fix for ModuleNotFoundError

        # Mock tp_utils.common
        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")

        mock_common.get_dbutils = MagicMock()
        mock_common.get_logger = MagicMock()
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:test_url',
            'ref_db_name': 'test_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'test_catalog',
            'postgres_schema': 'test_schema'
        })
        mock_common.read_run_params = MagicMock(return_value=MagicMock(
            FILE_NAME='test_file', CNTRT_ID='123', RUN_ID='run_001'
        ))

        mock_df_cntrt_lkp = MagicMock()
        mock_df_cntrt_lkp.collect.return_value = [
            MagicMock(srce_sys_id='SYS1', time_perd_type_code='MONTHLY')
        ]
        mock_common.load_cntrt_lkp = MagicMock(return_value=mock_df_cntrt_lkp)

        mock_df_prod_trans = MagicMock()
        mock_df_prod_trans.count.return_value = 10
        mock_df_prod_trans.filter.return_value.count.return_value = new_product_count

        mock_common.acn_prod_trans = MagicMock(return_value=mock_df_prod_trans)
        mock_common.assign_skid = MagicMock(return_value=MagicMock())
        mock_common.acn_prod_trans_materialize = MagicMock()

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "pyspark.sql.connect": spark_sql_connect_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_common

    def run_script(self, script_path):
        with open(script_path) as f:
            code = compile(f.read(), script_path.split("/")[-1], 'exec')
            exec(code, {"__name__": "__main__"})

    def test_script_with_new_products(self):
        mock_modules, mock_common = self.get_mock_modules(new_product_count=3)
        script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_acn_product_transformation.py"

        with patch.dict(sys.modules, mock_modules):
            self.run_script(script_path)

        mock_common.acn_prod_trans.assert_called_once()
        mock_common.assign_skid.assert_called_once()
        mock_common.acn_prod_trans_materialize.assert_called_once()

    def test_script_with_no_new_products(self):
        mock_modules, mock_common = self.get_mock_modules(new_product_count=0)
        script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_acn_product_transformation.py"

        with patch.dict(sys.modules, mock_modules):
            self.run_script(script_path)

        mock_common.acn_prod_trans.assert_called_once()
        mock_common.assign_skid.assert_called_once()
        mock_common.acn_prod_trans_materialize.assert_called_once()  # ✅ Updated expectation
