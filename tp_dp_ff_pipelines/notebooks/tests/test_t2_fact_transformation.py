import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
import builtins  # Needed to patch eval

class TestFactTransformation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_fact_transformation.py"

    def get_mock_modules(self):
        # Mock DataFrame (define first!)
        mock_df = MagicMock()
        mock_df.collect.return_value = [Row(srce_sys_id="SYS1", crncy_id="USD", time_perd_type_code="MONTHLY")]
        mock_df.count.return_value = 1
        mock_df.filter.return_value = mock_df
        mock_df.unionByName.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.withColumn.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.mode.return_value.format.return_value = mock_df.write.mode.return_value.format
        mock_df.write.mode.return_value.format.return_value.saveAsTable = MagicMock()
        mock_df.write.mode.return_value.format.return_value.save = MagicMock()
        mock_df.show = MagicMock()
        mock_df.createOrReplaceTempView = MagicMock()
        mock_df.columns = ["extrn_prod_id", "extrn_mkt_id", "extrn_time_perd_id", "mm_time_perd_end_date", "time_perd_id", "other_col"]

        # Mock SparkSession
        mock_spark = MagicMock()
        mock_spark.sql = MagicMock(return_value=mock_df)
        mock_get_or_create = MagicMock(return_value=mock_spark)
        mock_app_name = MagicMock(getOrCreate=mock_get_or_create)
        mock_builder = MagicMock(appName=MagicMock(return_value=mock_app_name))

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        pyspark_sql_module = types.ModuleType("pyspark.sql")
        setattr(pyspark_sql_module, "session", spark_session_module)
        setattr(pyspark_sql_module, "SparkSession", spark_session_module.SparkSession)

        pyspark_module = types.ModuleType("pyspark")
        setattr(pyspark_module, "sql", pyspark_sql_module)

        # Create tp_utils and tp_utils.common as mock modules
        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")
        mock_tp_utils.common = mock_common
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry

        # Mock job args
        mock_args = MagicMock()
        mock_args.FILE_NAME = "fact_file"
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R456"

        # Mock common functions
        mock_common.get_dbutils = MagicMock()
        mock_common.get_logger = MagicMock()
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'ref_db_hostname': 'mock_host',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        })
        mock_common.read_run_params = MagicMock(return_value=mock_args)
        mock_common.load_cntrt_lkp = MagicMock(return_value=mock_df)
        mock_common.read_from_postgres = MagicMock(return_value=mock_df)
        mock_common.dynamic_expression = MagicMock(return_value="'SELECT * FROM df_fact_extrn'")
        mock_common.column_complementer = MagicMock(return_value=mock_df)
        mock_common.add_partition_columns = MagicMock(return_value=mock_df)
        mock_common.add_secure_group_key = MagicMock(return_value=mock_df)
        mock_common.add_latest_time_perd = MagicMock(return_value=mock_df)
        mock_common.materialise_path = MagicMock(return_value="mock_path")
        mock_common.semaphore_generate_path = MagicMock(return_value="mock_path")
        mock_common.semaphore_acquisition = MagicMock(return_value="mock_path")
        mock_common.release_semaphore = MagicMock(return_value="mock_path")

        return {
            "pyspark": pyspark_module,
            "pyspark.sql": pyspark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common,
        }, mock_common, mock_df, mock_spark

    def run_script_with_mock(self, mock_common, mock_df):
        mock_common.semaphore_generate_path = MagicMock(return_value="mock_path")
        with open(self.script_path) as f:
            code = compile(f.read(), "t2_fact_transformation.py", 'exec')
            with patch.object(builtins, 'eval', return_value="SELECT * FROM df_fact_extrn"):
                exec(code, {"__name__": "__main__"})

    def test_successful_execution(self):
        mock_modules, mock_common, mock_df, mock_spark = self.get_mock_modules()
        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common, mock_df)

        mock_common.load_cntrt_lkp.assert_called_once()
        mock_common.read_from_postgres.assert_called()
        mock_common.dynamic_expression.assert_called()
        mock_common.column_complementer.assert_called()
        mock_common.add_partition_columns.assert_called()
        mock_common.add_secure_group_key.assert_called()
        mock_common.add_latest_time_perd.assert_called()
        mock_df.write.mode.return_value.format.return_value.save.assert_called()
        mock_common.semaphore_generate_path = MagicMock(return_value="mock_path")

    def test_dynamic_expression_failure(self):
        mock_modules, mock_common, mock_df, mock_spark = self.get_mock_modules()
        mock_common.dynamic_expression.side_effect = Exception("Dynamic expression failed")
        mock_common.semaphore_generate_path = MagicMock(return_value="mock_path")

        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                with patch.object(builtins, 'eval', return_value="SELECT * FROM df_fact_extrn"):
                    self.run_script_with_mock(mock_common, mock_df)

        self.assertIn("Dynamic expression failed", str(context.exception))