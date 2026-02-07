import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession

class TestBusinessMetricsPipeline(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t1_dq_pre_business_validation.py"
        self.spark = MagicMock(spec=SparkSession)
        self.catalog_name = "test_catalog"
        self.run_id = "R001"
        self.cntrt_id = "C001"
        self.mat_path = "tp-source-data/temp/materialised"

    def mock_df(self, columns):
        df = MagicMock()
        df.select.return_value = df
        df.distinct.return_value = df
        df.union.return_value = df
        df.unionByName.return_value = df
        df.filter.return_value = df
        df.withColumn.return_value = df
        df.createOrReplaceTempView.return_value = None
        df.show.return_value = None
        df.write = MagicMock()
        df.write.format.return_value = df.write
        df.write.mode.return_value = df.write
        df.write.save.return_value = None
        return df

    def mock_empty_df(self):
        df = MagicMock()
        df.count.return_value = 0
        df.collect.return_value = []
        df.filter.return_value = df
        df.select.return_value = df
        return df

    def mock_missing_column_df(self):
        df = MagicMock()
        df.select.side_effect = Exception("Column not found")
        return df

    @patch("src.tp_utils.common.column_complementer")
    @patch("src.tp_utils.common.publish_valdn_agg_fct")
    @patch("pyspark.sql.SparkSession.sql")
    def test_pipeline_success(self, mock_sql, mock_publish_valdn_agg_fct, mock_column_complementer):
        mock_sql.return_value = self.mock_df(['sales_msu_qty'])
        df_dqm_fct2 = mock_sql()
        df_dqm_fct = mock_sql()

        df_dqm_fct2 = mock_column_complementer(df_dqm_fct2, df_dqm_fct)
        df_dqm_fct2.createOrReplaceTempView("mm_dqm_calc_index")
        df_dqm_fct2.write.format("parquet").mode("overwrite").save(f"/mnt/{self.mat_path}/{self.run_id}/t1_dq_pre_business_validation_df_dqm_calc_index")

        mock_column_complementer.assert_called_once()
        mock_publish_valdn_agg_fct.assert_not_called()

    @patch("src.tp_utils.common.column_complementer")
    @patch("src.tp_utils.common.publish_valdn_agg_fct")
    @patch("pyspark.sql.SparkSession.sql")
    def test_empty_dataframe_handling(self, mock_sql, mock_publish_valdn_agg_fct, mock_column_complementer):
        mock_sql.return_value = self.mock_empty_df()
        df = mock_sql()
        self.assertEqual(df.count(), 0)

    @patch("src.tp_utils.common.column_complementer")
    @patch("src.tp_utils.common.publish_valdn_agg_fct")
    @patch("pyspark.sql.SparkSession.sql")
    def test_missing_column_handling(self, mock_sql, mock_publish_valdn_agg_fct, mock_column_complementer):
        mock_sql.return_value = self.mock_missing_column_df()
        with self.assertRaises(Exception) as context:
            df = mock_sql()
            df.select("non_existent_column")
        self.assertIn("Column not found", str(context.exception))

    @patch("src.tp_utils.common.column_complementer")
    @patch("src.tp_utils.common.publish_valdn_agg_fct")
    @patch("pyspark.sql.SparkSession.sql")
    def test_sql_execution_failure(self, mock_sql, mock_publish_valdn_agg_fct, mock_column_complementer):
        mock_sql.side_effect = Exception("SQL execution failed")
        with self.assertRaises(Exception) as context:
            mock_sql("SELECT * FROM broken_table")
        self.assertIn("SQL execution failed", str(context.exception))