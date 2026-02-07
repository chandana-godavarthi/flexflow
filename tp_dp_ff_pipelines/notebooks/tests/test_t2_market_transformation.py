import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call
from pyspark.sql import Row

class TestMarketTransformation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_market_transformation.py"

    def get_mock_modules(self):
        mock_spark = MagicMock()
        mock_get_or_create = MagicMock(return_value=mock_spark)
        mock_app_name = MagicMock(getOrCreate=mock_get_or_create)
        mock_builder = MagicMock(appName=MagicMock(return_value=mock_app_name))

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")
        mock_tp_utils.common = mock_common
        mock_safe_merge_with_retry = MagicMock()
        mock_common.safe_merge_with_retry = mock_safe_merge_with_retry
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry
        mock_args = MagicMock()
        mock_args.FILE_NAME = "market_file"
        mock_args.CNTRT_ID = "C123"
        mock_args.RUN_ID = "R456"

        mock_df = MagicMock()
        mock_df.collect.return_value = [MagicMock(srce_sys_id="SYS1", time_perd_type_code="MONTHLY", cntry_name="India")]
        mock_df.count.return_value = 1
        mock_df.filter.return_value = mock_df
        mock_df.unionByName.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.write = MagicMock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.mode.return_value.format.return_value = mock_df.write.mode.return_value.format
        mock_df.write.mode.return_value.format.return_value.saveAsTable = MagicMock()
        mock_df.write.mode.return_value.format.return_value.save = MagicMock()
        mock_df.show = MagicMock()
        mock_df.createOrReplaceTempView = MagicMock()

        mock_common.get_dbutils = MagicMock()
        mock_common.get_logger = MagicMock()
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        })
        mock_common.read_run_params = MagicMock(return_value=mock_args)
        mock_common.load_cntrt_lkp = MagicMock(return_value=mock_df)
        mock_common.read_from_postgres = MagicMock(return_value=mock_df)
        mock_common.dynamic_expression = MagicMock(return_value="'SELECT * FROM df_mkt_stgng_vw'")
        mock_common.column_complementer = MagicMock(return_value=mock_df)
        mock_common.assign_skid = MagicMock(return_value=mock_df)
        mock_common.semaphore_acquisition = MagicMock(side_effect=[
            "/mnt/tp-publish-data/TP_MKT_SDIM/...",
            "/mnt/tp-publish-data/TP_RUN_MKT_PLC/..."
        ])
        mock_common.materialise_path = MagicMock(return_value="mock_path")
        mock_common.release_semaphore = MagicMock()
        mock_common.semaphore_generate_path = MagicMock(side_effect=[
            "/mnt/tp-publish-data/TP_MKT_SDIM/part_srce_sys_id=SYS1/part_cntrt_id=C123",
            "/mnt/tp-publish-data/TP_RUN_MKT_PLC/part_srce_sys_id=SYS1/part_cntrt_id=C123"
        ])

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common,
        }, mock_common, mock_df

    def run_script_with_mock(self, mock_common, mock_df):
        with open(self.script_path) as f:
            code = compile(f.read(), "t2_market_transformation.py", 'exec')
            exec(code, {
                "__name__": "__main__",
                "safe_write_with_retry": mock_common.safe_write_with_retry,
                "safe_merge_with_retry": mock_common.safe_merge_with_retry,
                "semaphore_acquisition": mock_common.semaphore_acquisition,
                "release_semaphore": mock_common.release_semaphore,
                "semaphore_generate_path": mock_common.semaphore_generate_path,
                "assign_skid": mock_common.assign_skid,
                "get_logger": mock_common.get_logger,
                "get_dbutils": mock_common.get_dbutils,
                "get_database_config": mock_common.get_database_config,
                "read_run_params": mock_common.read_run_params,
                "load_cntrt_lkp": mock_common.load_cntrt_lkp,
                "read_from_postgres": mock_common.read_from_postgres,
                "dynamic_expression": mock_common.dynamic_expression,
                "column_complementer": mock_common.column_complementer,
            })

    def test_successful_execution(self):
        mock_modules, mock_common, mock_df = self.get_mock_modules()
        mock_logger = MagicMock()
        mock_common.get_logger.return_value = mock_logger
        mock_safe_merge_with_retry = MagicMock()
        mock_common.safe_merge_with_retry = mock_safe_merge_with_retry
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry

        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common, mock_df)

        logged_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        self.assertTrue(
            any("Started Market Transformation." in msg for msg in logged_messages),
            "Expected market transformation log message not found"
        )
        self.assertTrue(
            any("Semaphore released for run_id=R456" in msg for msg in logged_messages),
            "Expected semaphore release log message not found"
        )

        # ✅ Check call count
        self.assertEqual(mock_common.release_semaphore.call_count, 2)

        # ✅ Extract actual calls
        actual_calls = mock_common.release_semaphore.call_args_list

        # ✅ Validate each call individually
        self.assertEqual(actual_calls[0][0][0], 'mock_catalog')
        self.assertEqual(actual_calls[0][0][1], 'R456')
        self.assertTrue(actual_calls[0][0][2].startswith('/mnt/tp-publish-data/TP_MKT_SDIM/'))
        self.assertIsInstance(actual_calls[0][0][3], MagicMock)

        self.assertEqual(actual_calls[1][0][0], 'mock_catalog')
        self.assertEqual(actual_calls[1][0][1], 'R456')
        self.assertTrue(actual_calls[1][0][2].startswith('/mnt/tp-publish-data/TP_RUN_MKT_PLC/'))
        self.assertIsInstance(actual_calls[1][0][3], MagicMock)

    def test_dynamic_expression_failure(self):
        mock_modules, mock_common, mock_df = self.get_mock_modules()
        mock_common.dynamic_expression.side_effect = Exception("Dynamic expression failed")
        mock_safe_merge_with_retry = MagicMock()
        mock_common.safe_merge_with_retry = mock_safe_merge_with_retry
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry
        with patch.dict(sys.modules, mock_modules):
            with self.assertRaises(Exception) as context:
                self.run_script_with_mock(mock_common, mock_df)

        self.assertIn("Dynamic expression failed", str(context.exception))