import unittest
from unittest.mock import patch, MagicMock

# Adjust the import path to match your project structure
from src.tp_utils.common import t1_normalise_product_attributes

class TestT1NormaliseProductAttributes(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.df_prod_dsdim = MagicMock()
        self.df_prod_dsdim_cols = MagicMock()
        self.df_prod_dsdim_cols.columns = ['ATTR1', 'ATTR2', 'PG_CATEG_TXT', 'PROD_LVL_ID']
        self.df_prod_dsdim.columns = ['ATTR1', 'ATTR2', 'PG_CATEG_TXT', 'PROD_LVL_ID']
        self.df_prod_dsdim.select.return_value = self.df_prod_dsdim
        self.df_prod_dsdim.createOrReplaceTempView = MagicMock()

        self.postgres_schema = "test_schema"
        self.run_id = "RUN001"
        self.raw_path = "/tmp/raw"
        self.ref_db_jdbc_url = "jdbc:postgresql://localhost:5432/test"
        self.ref_db_name = "testdb"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"

    def mock_lookup_tables(self):
        mock_attr_df = MagicMock()
        mock_attr_df.select.return_value.collect.return_value = [MagicMock(ATTR_PHYS_NAME='ATTR1')]
        mock_attr_df.createOrReplaceTempView = MagicMock()

        mock_val_df = MagicMock()
        mock_val_df.createOrReplaceTempView = MagicMock()

        mock_lvl_df = MagicMock()
        mock_lvl_df.createOrReplaceTempView = MagicMock()

        return [mock_val_df, mock_attr_df, mock_lvl_df]

    def mock_spark_sql_views(self):
        # SQL calls: prod_lvls, lvl_mapping, df_nav_output, final query
        prod_lvls = MagicMock()
        lvl_mapping = MagicMock()

        df_nav_output = MagicMock()
        df_nav_output.select.return_value.collect.return_value = [MagicMock(attr_phys_name='ATTR1')]
        df_nav_output.columns = ['ATTR1']
        df_nav_output.select.return_value = df_nav_output

        final_df = MagicMock()
        final_df.count.return_value = 10
        final_df.show = MagicMock()
        final_df.columns = ['ATTR1', 'ATTR2', 'PG_CATEG_TXT', 'PROD_LVL_ID']
        final_df.select.return_value = final_df

        return [prod_lvls, lvl_mapping, df_nav_output, final_df]

    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB read failed"))
    def test_postgres_read_failure(self, mock_read_query, mock_get_logger):
        with self.assertRaises(Exception) as context:
            t1_normalise_product_attributes(
                self.df_prod_dsdim,
                self.df_prod_dsdim_cols,
                self.postgres_schema,
                self.run_id,
                self.spark,
                self.ref_db_jdbc_url,
                self.ref_db_name,
                self.ref_db_user,
                self.ref_db_pwd
            )
        self.assertIn("DB read failed", str(context.exception))

    
    @patch("src.tp_utils.common.materialize")
    @patch("src.tp_utils.common.convert_cols_to_lower")
    @patch("src.tp_utils.common.convert_cols_to_upper", return_value=MagicMock())
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.read_query_from_postgres")
    def test_missing_attribute_column(
        self, mock_read_query, mock_get_logger, mock_convert_upper, mock_convert_lower, mock_materialize
    ):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        mock_attr_df = MagicMock()
        mock_attr_df.select.return_value.collect.return_value = [MagicMock(ATTR_PHYS_NAME='MISSING_ATTR')]
        mock_attr_df.createOrReplaceTempView = MagicMock()

        mock_val_df = MagicMock()
        mock_val_df.createOrReplaceTempView = MagicMock()

        mock_lvl_df = MagicMock()
        mock_lvl_df.createOrReplaceTempView = MagicMock()

        mock_read_query.side_effect = [mock_val_df, mock_attr_df, mock_lvl_df]

        df_nav_output = MagicMock()
        df_nav_output.select.return_value.collect.return_value = [MagicMock(attr_phys_name='MISSING_ATTR')]
        df_nav_output.columns = ['MISSING_ATTR']
        df_nav_output.select.return_value = df_nav_output

        final_df = MagicMock()
        final_df.count.return_value = 5
        final_df.show = MagicMock()
        final_df.columns = ['ATTR1', 'ATTR2', 'PG_CATEG_TXT', 'PROD_LVL_ID']
        final_df.select.return_value = final_df

        self.spark.sql.side_effect = [MagicMock(), MagicMock(), df_nav_output, final_df]
        self.spark.createDataFrame.return_value = MagicMock()
        self.spark.createDataFrame.return_value.createOrReplaceTempView = MagicMock()

        t1_normalise_product_attributes(
            self.df_prod_dsdim,
            self.df_prod_dsdim_cols,
            self.postgres_schema,
            self.run_id,
            self.spark,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        self.assertTrue(mock_materialize.called)
        self.assertTrue(mock_logger.info.called)
        self.assertEqual(mock_read_query.call_count, 3)
        self.assertEqual(self.spark.sql.call_count, 4)