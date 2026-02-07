import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import t1_get_attribute_values  # Adjust path as needed

class TestT1GetAttributeValues(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.df_mm_prod_csdim = MagicMock()
        self.df_prod_gan = MagicMock()
        self.postgres_schema = "test_schema"
        self.ref_db_jdbc_url = "jdbc:test"
        self.ref_db_name = "test_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.fileformat = "SFF"
        self.run_id = "RUN001"
        self.raw_path = "/tmp/test"

    def mock_df(self, name):
        df = MagicMock()
        df.withColumn.return_value = df
        df.withColumnRenamed.return_value = df
        df.createOrReplaceTempView.return_value = None
        df.show.return_value = None
        df.count.return_value = 42
        df.columns = [f"{name}_col1", f"{name}_col2"]
        return df

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.materialize")
    @patch("src.tp_utils.common.read_query_from_postgres")
    def test_successful_execution_with_mocked_sql(self, mock_read_query, mock_materialize, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock lookup table
        mock_df_attr_val_lkp = self.mock_df("attr_val_lkp")
        mock_read_query.return_value = mock_df_attr_val_lkp

        # Mock SQL outputs
        mock_df_gav_initial = self.mock_df("gav_initial")
        mock_df_gav_final = self.mock_df("gav_final")

        def sql_side_effect(query):
            if "SELECT input.*" in query:
                return mock_df_gav_initial
            elif "SELECT * ," in query:
                return mock_df_gav_final
            return MagicMock()

        self.spark.sql.side_effect = sql_side_effect

        result = t1_get_attribute_values(
            self.df_mm_prod_csdim,
            self.df_prod_gan,
            self.postgres_schema,
            self.spark,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd,
            self.fileformat,
            self.run_id
        )

        self.assertEqual(result, mock_df_gav_initial)
        self.assertTrue(mock_read_query.called)
        self.assertTrue(mock_materialize.called)
        self.assertEqual(self.spark.sql.call_count, 2)
        self.assertTrue(mock_logger.info.called)

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB read failed"))
    def test_postgres_read_failure(self, mock_read_query, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        with self.assertRaises(Exception) as context:
            t1_get_attribute_values(
                self.df_mm_prod_csdim,
                self.df_prod_gan,
                self.postgres_schema,
                self.spark,
                self.ref_db_jdbc_url,
                self.ref_db_name,
                self.ref_db_user,
                self.ref_db_pwd,
                self.fileformat,
                self.run_id
            )
        self.assertIn("DB read failed", str(context.exception))

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.materialize", side_effect=Exception("Materialization failed"))
    @patch("src.tp_utils.common.read_query_from_postgres")
    def test_materialization_failure(self, mock_read_query, mock_materialize, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_df_attr_val_lkp = self.mock_df("attr_val_lkp")
        mock_read_query.return_value = mock_df_attr_val_lkp

        mock_df_gav_initial = self.mock_df("gav_initial")
        self.spark.sql.return_value = mock_df_gav_initial

        with self.assertRaises(Exception) as context:
            t1_get_attribute_values(
                self.df_mm_prod_csdim,
                self.df_prod_gan,
                self.postgres_schema,
                self.spark,
                self.ref_db_jdbc_url,
                self.ref_db_name,
                self.ref_db_user,
                self.ref_db_pwd,
                self.fileformat,
                self.run_id
            )
        self.assertIn("Materialization failed", str(context.exception))

if __name__ == "__main__":
    unittest.main()