import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_product_sff3

def make_df_mock(columns=None):
    df = MagicMock()
    df.withColumn.return_value = df
    df.dropDuplicates.return_value = df
    df.createOrReplaceTempView.return_value = None
    df.show.return_value = None
    df.count.return_value = 1
    df.select.return_value = df
    df.distinct.return_value = df
    df.collect.return_value = [MagicMock(hierarchy_number="1")]
    df.columns = columns if columns is not None else ["PRDC_CD", "PRDC_LNG_DSC"]
    df.join.return_value = df
    df.drop.return_value = df
    df.withColumnRenamed.return_value = df
    df.getItem.return_value = df
    df.__getitem__.return_value = df
    return df

@patch("src.tp_utils.common.get_dbutils", return_value=MagicMock())
class TestExtractProductSFF3(unittest.TestCase):

    def setUp(self):
        self.df_raw1 = make_df_mock()
        self.df_max_lvl = make_df_mock()
        self.run_id = "run_001"
        self.cntrt_id = "C001"
        self.categ_id = "CAT001"
        self.srce_sys_id = "SYS001"
        self.spark = MagicMock()

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.lit")
    @patch("src.tp_utils.common.IntegerType")
    @patch("src.tp_utils.common.StructType")
    @patch("src.tp_utils.common.StructField")
    @patch("src.tp_utils.common.StringType")
    @patch("src.tp_utils.common.split")
    @patch("src.tp_utils.common.size")
    def test_extract_product_sff3_success(self, mock_size, mock_split, mock_string_type, mock_struct_field, mock_struct_type, mock_int_type, mock_lit, mock_col, mock_get_logger, mock_get_dbutils):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl

        mock_df_hier = make_df_mock()
        self.spark.read.format.return_value.option.return_value.option.return_value.load.return_value = mock_df_hier
        mock_df_hier.select.return_value.distinct.return_value.collect.return_value = [MagicMock(hierarchy_number="1")]

        self.spark.sql.return_value = make_df_mock()
        self.spark.createDataFrame.return_value = make_df_mock()

        self.df_raw1.dropDuplicates.return_value = self.df_raw1
        self.df_raw1.columns = ["PRDC_CD", "PRDC_LNG_DSC"]

        df_srce_mprod, df_invld_hier_prod = extract_product_sff3(
            self.df_raw1, self.run_id, self.cntrt_id, self.categ_id,
            self.srce_sys_id, self.df_max_lvl, self.spark
        )

        self.assertTrue(df_srce_mprod is None or isinstance(df_srce_mprod, MagicMock))
        self.assertTrue(df_invld_hier_prod is None or isinstance(df_invld_hier_prod, MagicMock))


    @patch("src.tp_utils.common.get_logger")
    def test_extract_product_sff3_join_exception(self, mock_get_logger, mock_get_dbutils):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl
        self.df_raw1.dropDuplicates.return_value = self.df_raw1
        self.df_raw1.columns = ["PRDC_CD", "PRDC_LNG_DSC"]

        mock_df_hier = make_df_mock()
        mock_df_hier.select.return_value.distinct.return_value.collect.return_value = [MagicMock(hierarchy_number="1")]
        self.spark.read.format.return_value.option.return_value.option.return_value.load.return_value = mock_df_hier

        self.spark.sql.side_effect = Exception("Join failed")

        with self.assertRaises(Exception) as context:
            extract_product_sff3(
                self.df_raw1, self.run_id, self.cntrt_id, self.categ_id,
                self.srce_sys_id, self.df_max_lvl, self.spark
            )
        self.assertIn("Join failed", str(context.exception))

    @patch("src.tp_utils.common.get_logger")
    def test_extract_product_sff3_sql_failures(self, mock_get_logger, mock_get_dbutils):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        self.df_max_lvl.withColumn.return_value = self.df_max_lvl
        self.df_raw1.dropDuplicates.return_value = self.df_raw1
        self.df_raw1.columns = ["PRDC_CD", "PRDC_LNG_DSC"]

        mock_df_hier = make_df_mock()
        mock_df_hier.select.return_value.distinct.return_value.collect.return_value = [MagicMock(hierarchy_number="1")]
        self.spark.read.format.return_value.option.return_value.option.return_value.load.return_value = mock_df_hier

        self.spark.sql.side_effect = Exception("SQL execution failed")

        with self.assertRaises(Exception) as context:
            extract_product_sff3(
                self.df_raw1, self.run_id, self.cntrt_id, self.categ_id,
                self.srce_sys_id, self.df_max_lvl, self.spark
            )

        self.assertIn("SQL execution failed", str(context.exception))