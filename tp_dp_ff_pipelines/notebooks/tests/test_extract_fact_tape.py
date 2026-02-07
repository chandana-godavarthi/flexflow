import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.tp_utils.common import extract_fact_tape

class TestExtractFactTape(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock(spec=SparkSession)
        self.run_id = "R001"
        self.cntrt_id = "C001"
        self.jdbc_url = "jdbc:postgresql://localhost:5432/refdb"
        self.db_name = "refdb"
        self.db_user = "user"
        self.db_pwd = "pwd"
        self.postgres_schema = "mock_schema"

        # Patch DBUtils constructor globally
        dbutils_patcher = patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))
        self.mock_dbutils_class = dbutils_patcher.start()
        self.addCleanup(dbutils_patcher.stop)

        # Create a mock dbutils instance
        self.dbutils = self.mock_dbutils_class(self.spark)

        # Minimal mock for df_raw1
        self.df_raw1 = MagicMock()
        self.df_raw1.withColumn.return_value = self.df_raw1
        self.df_raw1.filter.return_value = self.df_raw1
        self.df_raw1.select.return_value = self.df_raw1
        self.df_raw1.drop.return_value = self.df_raw1
        self.df_raw1.count.return_value = 10
        self.df_raw1.show.return_value = None

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.materialise_path", side_effect=Exception("Materialisation failed"))
    def test_materialise_path_failure(self, mock_materialise_path, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with self.assertRaises(Exception) as context:
            extract_fact_tape(
                self.df_raw1, self.run_id, self.cntrt_id,
                self.dbutils, self.spark, self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema
            )

        self.assertIn("Materialisation failed", str(context.exception))