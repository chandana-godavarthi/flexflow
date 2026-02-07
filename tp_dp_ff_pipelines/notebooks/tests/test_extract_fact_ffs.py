import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_fact_ffs

class TestExtractFactFFS(unittest.TestCase):

    def setUp(self):
        self.df_raw = MagicMock()
        self.run_id = "run_001"
        self.cntrt_id = "C001"
        self.spark = MagicMock()
        self.ref_db_jdbc_url = "jdbc:mock"
        self.ref_db_name = "mock_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.postgres_schema = "mock_schema"

        # Mock dbutils
        dbutils_patcher = patch("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))
        self.mock_dbutils_class = dbutils_patcher.start()
        self.addCleanup(dbutils_patcher.stop)
        self.dbutils = self.mock_dbutils_class(self.spark)

    @patch("src.tp_utils.common.concat")
    @patch("src.tp_utils.common.when")
    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.get_logger")
    def test_extract_fact_ffs_success(
        self, mock_get_logger, mock_col, mock_ltrim, mock_rtrim,
        mock_substring, mock_row_number, mock_window,
        mock_monotonically_id, mock_materialise_path, mock_when, mock_concat
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df_filtered = MagicMock()
        df_transformed = MagicMock()
        df_trimmed = MagicMock()
        df_final = MagicMock()
        df_final.count.return_value = 50
        df_final.columns = ["col1", "col2"]
        df_final.select.return_value = df_final
        df_final.withColumn.return_value = df_final
        df_final.drop.return_value = df_final

        self.df_raw.filter.return_value = df_filtered
        df_filtered.withColumn.return_value = df_transformed
        df_transformed.select.return_value = df_trimmed
        df_trimmed.drop.return_value = df_final
        self.spark.createDataFrame.return_value = df_final

        result = extract_fact_ffs(
            self.df_raw, self.run_id, self.cntrt_id,
            self.dbutils, self.spark, self.ref_db_jdbc_url,
            self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
            self.postgres_schema
        )

        self.assertIsInstance(result, MagicMock)
        mock_materialise_path.assert_called_once()

    @patch("src.tp_utils.common.materialise_path", side_effect=Exception("File listing failed"))
    @patch("src.tp_utils.common.get_logger")
    def test_file_listing_failure(self, mock_get_logger, mock_materialise_path):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with self.assertRaises(Exception) as context:
            extract_fact_ffs(
                self.df_raw, self.run_id, self.cntrt_id,
                self.dbutils, self.spark, self.ref_db_jdbc_url,
                self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
                self.postgres_schema
            )

        self.assertIn("File listing failed", str(context.exception))

    @patch("src.tp_utils.common.concat")
    @patch("src.tp_utils.common.when")
    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.monotonically_increasing_id")
    @patch("src.tp_utils.common.Window")
    @patch("src.tp_utils.common.row_number")
    @patch("src.tp_utils.common.substring")
    @patch("src.tp_utils.common.rtrim")
    @patch("src.tp_utils.common.ltrim")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.get_logger")
    def test_empty_input(
        self, mock_get_logger, mock_col, mock_ltrim, mock_rtrim,
        mock_substring, mock_row_number, mock_window,
        mock_monotonically_id, mock_materialise_path, mock_when, mock_concat
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df_filtered = MagicMock()
        df_filtered.count.return_value = 0
        df_filtered.withColumn.return_value = df_filtered
        df_filtered.select.return_value = df_filtered
        df_filtered.drop.return_value = df_filtered
        df_filtered.columns = ["col1", "col2"]

        self.df_raw.filter.return_value = df_filtered
        self.spark.createDataFrame.return_value = df_filtered

        result = extract_fact_ffs(
            self.df_raw, self.run_id, self.cntrt_id,
            self.dbutils, self.spark, self.ref_db_jdbc_url,
            self.ref_db_name, self.ref_db_user, self.ref_db_pwd,
            self.postgres_schema
        )

        self.assertIsInstance(result, MagicMock)