import unittest
from unittest.mock import MagicMock, patch, mock_open
from src.extract_and_write_ddl import extract_table_ddl  # Adjust import path as needed

class TestExtractTableDDL(unittest.TestCase):

    def setUp(self):
        self.mock_catalog = "mock_catalog"
        self.mock_table = "mock_schema.mock_table"
        self.mock_file_name = "mock_output"

        # ✅ Raw DDL with mocked catalog, table, and location
        self.mock_ddl_raw = (
        "CREATE TABLE cdl_tp_dev.mock_schema.mock_table USING DELTA LOCATION 'dbfs:/mnt/sa01flexflowtp01dev/...'"
        )


        # ✅ Expected DDL with placeholders
        self.expected_ddl = (
            "CREATE OR REPLACE TABLE {catalog_name}.mock_schema.mock_table USING DELTA LOCATION 'dbfs:/mnt/{storage_name}/...'"
        )

    @patch("builtins.open", new_callable=mock_open)
    @patch("os.getcwd", return_value="/mock/path")
    @patch("os.chdir")
    def test_extract_table_ddl_success(self, mock_chdir, mock_getcwd, mock_file):
        # ✅ Mock SparkSession and SQL result
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.collect.return_value = [[self.mock_ddl_raw]]
        mock_spark.sql.return_value = mock_df

        # ✅ Call the function
        result = extract_table_ddl(mock_spark, self.mock_catalog, self.mock_table, self.mock_file_name)

        # ✅ Assert SQL was called correctly
        mock_spark.sql.assert_called_once_with(f"SHOW CREATE TABLE  {self.mock_catalog}.{self.mock_table}")

        # ✅ Assert file was written with transformed DDL
        mock_file().write.assert_called_once_with(self.expected_ddl)

        # ✅ Assert returned DDL is transformed
        self.assertEqual(result, self.expected_ddl)

    @patch("builtins.open", new_callable=mock_open)
    def test_extract_table_ddl_handles_sql_exception(self, mock_file):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("SQL failed")

        with self.assertRaises(Exception) as context:
            extract_table_ddl(mock_spark, self.mock_catalog, self.mock_table, self.mock_file_name)

        self.assertIn("SQL failed", str(context.exception))
