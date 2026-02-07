import sys
import types
import unittest
from unittest.mock import MagicMock, patch

class TestSingleFileTimeTransformation(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_single_file_time_transformation.py"

    def get_mock_modules(self):
        mock_spark = MagicMock()

        # SparkSession
        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark
        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)
        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        # Functions
        functions_module = types.ModuleType("pyspark.sql.functions")
        functions_module.lit = MagicMock()

        # tp_utils.common
        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")
        mock_logger = MagicMock()
        mock_column_complementer = MagicMock()
        mock_common.column_complementer = mock_column_complementer
        mock_common.semaphore_generate_path = MagicMock(return_value="mock_semaphore_generate_path")
        mock_common.semaphore_acquisition = MagicMock(return_value="mock_semaphore_acquisition")
        mock_common.release_semaphore = MagicMock(return_value="mock_release_semaphore")
        mock_safe_write_with_retry = MagicMock()
        mock_common.safe_write_with_retry = mock_safe_write_with_retry

        mock_df_cntrt_lkp = MagicMock()
        mock_df_cntrt_lkp.collect.return_value = [MagicMock(time_exprn_id="TEXP1")]

        mock_df_time_exprn = MagicMock()
        mock_df_time_exprn.columns = ['start_date_val', 'end_date_val', 'time_perd_type_val']
        mock_df_time_exprn.first.return_value = ['2023-01-01', '2023-12-31', 'MONTH']

        mock_df_time_extrn = MagicMock()
        mock_df_time_extrn.createOrReplaceTempView.return_value = None

        mock_df_fdim = MagicMock()
        mock_df_fdim.createOrReplaceTempView.return_value = None

        mock_df_stgng = MagicMock()
        mock_df_stgng.distinct.return_value = mock_df_stgng
        mock_df_stgng.write.mode.return_value.format.return_value.save.return_value = None

        def mock_sql(query):
            if "tp_time_perd_fdim" in query:
                return mock_df_fdim
            elif "SELECT t.extrn_time_perd_id" in query:
                return mock_df_stgng
            else:
                return MagicMock()

        mock_spark.sql.side_effect = mock_sql
        mock_spark.read.parquet.return_value = mock_df_time_extrn

        mock_common.get_logger = MagicMock(return_value=mock_logger)
        mock_common.get_dbutils = MagicMock(return_value=MagicMock())
        mock_common.get_database_config = MagicMock(return_value={
            'ref_db_jdbc_url': 'jdbc:mock',
            'ref_db_name': 'mock_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'mock_catalog',
            'postgres_schema': 'mock_schema'
        })
        mock_common.read_run_params = MagicMock(return_value=MagicMock(CNTRT_ID="C123", RUN_ID="R456"))
        mock_common.load_cntrt_lkp = MagicMock(return_value=mock_df_cntrt_lkp)
        mock_common.load_time_exprn_id = MagicMock(return_value=mock_df_time_exprn)
        mock_common.materialise_path = MagicMock(return_value="mock_path")

        self.mock_common = mock_common  # ✅ Store for use in other methods

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "pyspark.sql.functions": functions_module,
            "tp_utils": mock_tp_utils,
            "tp_utils.common": mock_common,
            "src.tp_utils.common": mock_common
        }, mock_spark

    def run_script(self, mock_spark):
        self.mock_common.semaphore_generate_path = MagicMock(return_value="mock_semaphore_generate_path")
        self.mock_common.semaphore_acquisition = MagicMock(return_value="mock_semaphore_acquisition")
        self.mock_common.release_semaphore = MagicMock(return_value="mock_release_semaphore")
        with open(self.script_path) as f:
            code = compile(f.read(), "t2_single_file_time_transformation.py", 'exec')
            exec(code, {"__name__": "__main__", "spark": mock_spark})

    def test_script_executes_successfully(self):
        mock_modules, mock_spark = self.get_mock_modules()
        self.mock_common.semaphore_generate_path = MagicMock(return_value="mock_semaphore_generate_path")
        self.mock_common.semaphore_acquisition = MagicMock(return_value="mock_semaphore_acquisition")
        self.mock_common.release_semaphore = MagicMock(return_value="mock_release_semaphore")
        with patch.dict(sys.modules, mock_modules, clear=True):
            self.run_script(mock_spark)