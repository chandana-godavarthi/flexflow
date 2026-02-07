import sys
import types
import unittest
import builtins
from unittest.mock import MagicMock, patch, mock_open

class TestDDLExtractor(unittest.TestCase):
    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/create_tables.py"

    def get_mock_modules(self):
        # Mock SparkSession and dbutils
        mock_spark = MagicMock()
        mock_spark.sql = MagicMock()

        mock_builder = MagicMock()
        mock_builder.appName.return_value.getOrCreate.return_value = mock_spark

        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(builder=mock_builder)

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        # ✅ Add top-level pyspark module
        pyspark_module = types.ModuleType("pyspark")
        pyspark_module.sql = spark_sql_module

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "mock_storage_name"

        mock_common = types.ModuleType("tp_utils.common")
        mock_common.get_dbutils = MagicMock(return_value=mock_dbutils)
        mock_common.get_database_config = MagicMock(return_value={
            'catalog_name': 'mock_catalog'
        })

        return {
            "pyspark": pyspark_module,
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils.common": mock_common
        }, mock_common, mock_spark, mock_dbutils

    def test_successful_execution(self):
        mock_modules, mock_common, mock_spark, mock_dbutils = self.get_mock_modules()
        sql_content = """CREATE TABLE mock_table (id INT) TBLPROPERTIES ('key'='value')"""
        real_open = builtins.open

        with patch.dict(sys.modules, mock_modules):
            with patch("argparse.ArgumentParser.parse_args", return_value=MagicMock(TABLES="gold_tp.tp_time_perd_type_test1")):
                with patch("os.chdir"), patch("os.getcwd", return_value="/mock/path"):
                    with patch("builtins.open", mock_open(read_data=sql_content)):
                        with real_open(self.script_path) as f:
                            code = compile(f.read(), self.script_path, 'exec')
                            exec(code, {"__name__": "__main__"})

        # ✅ Relaxed assertions using partial match
        sql_calls = [call[0][0] for call in mock_spark.sql.call_args_list]

        self.assertTrue(any("drop table if exists mock_catalog.gold_tp.tp_time_perd_type_test1;" in sql for sql in sql_calls),
                        "Expected DROP TABLE SQL not found")
        self.assertTrue(any("CREATE TABLE" in sql for sql in sql_calls),
                        "Expected CREATE TABLE SQL not found")
        self.assertTrue(any("alter table" in sql for sql in sql_calls),
                        "Expected ALTER TABLE SQL not found")

    def test_sql_file_missing(self):
        mock_modules, _, _, _ = self.get_mock_modules()
        real_open = builtins.open

        with patch.dict(sys.modules, mock_modules):
            with patch("argparse.ArgumentParser.parse_args", return_value=MagicMock(TABLES="missing_table")):
                with patch("os.chdir"), patch("os.getcwd", return_value="/mock/path"):
                    def open_side_effect(path, *args, **kwargs):
                        if path.endswith("missing_table.sql"):
                            raise FileNotFoundError("File not found")
                        return real_open(path, *args, **kwargs)

                    with patch("builtins.open", side_effect=open_side_effect):
                        with self.assertRaises(FileNotFoundError):
                            with real_open(self.script_path) as f:
                                code = compile(f.read(), self.script_path, 'exec')
                                exec(code, {"__name__": "__main__"})