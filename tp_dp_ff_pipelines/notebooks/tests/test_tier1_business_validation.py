import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import tier1_business_validation

class TestTier1BusinessValidation(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.run_id = "R001"
        self.cntrt_id = "C001"
        self.catalog_name = "test_catalog"
        self.postgres_schema = "ref_schema"
        self.file_name = "source_file.csv"
        self.notebook_name = "tier1_business_validation"
        self.validation_name = "Tier1 Business Validation"
        self.jdbc_url = "jdbc:postgresql://localhost:5432/refdb"
        self.db_name = "refdb"
        self.db_user = "user"
        self.db_pwd = "pwd"

        self.dbutils = MagicMock()
        self.dbutils.secrets.get.return_value = "localhost"

        self.mock_df = MagicMock()
        self.mock_df.collect.return_value = [{"qry_txt": "SELECT 1"}]
        self.mock_df.count.return_value = 1
        self.mock_df.createOrReplaceTempView = MagicMock()

        self.spark.read.parquet.return_value = self.mock_df
        self.spark.sql.return_value = self.mock_df
        self.spark.table.return_value = self.mock_df

    def mock_contract_row(self):
        row = MagicMock()
        row.srce_sys_id = "SYS001"
        row.cntrt_code = "CODE001"
        row.time_perd_type_code = "MTH"
        row.cntry_name = "India"
        row.vendr_id = "V001"
        return row

    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.load_mm_cntrt_categ_assoc")
    @patch("src.tp_utils.common.time_perd_class_codes_new", return_value="mth")
    @patch("src.tp_utils.common.load_cntry_lkp")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.mm_time_perd_assoc_tier1_vw")
    @patch("src.tp_utils.common.dq_query_retrieval")
    @patch("src.tp_utils.common.validation", return_value="validation_result")
    @patch("src.tp_utils.common.merge_into_tp_data_vldtn_rprt_tbl")
    @patch("src.tp_utils.common.generate_report")
    @patch("src.tp_utils.common.generate_formatting_report")
    @patch("builtins.eval", return_value="SELECT 1")
    def test_successful_execution(
        self, mock_eval, mock_generate_formatting, mock_generate_report, mock_merge, mock_validation,
        mock_dq_query, mock_mm_time_perd_assoc, mock_read_pg, mock_load_cntry,
        mock_time_perd_class, mock_load_mm_categ, mock_load_cntrt_lkp,
        mock_sanitize, mock_get_logger, mock_get_dbutils
    ):
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [self.mock_contract_row()]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        mock_categ_df = MagicMock()
        mock_categ_df.collect.return_value = [MagicMock(categ_id="CAT001")]
        mock_load_mm_categ.return_value = mock_categ_df

        mock_cntry_df = MagicMock()
        mock_cntry_df.collect.return_value = [MagicMock(cntry_id="IN")]
        mock_load_cntry.return_value = mock_cntry_df

        mock_query_df = MagicMock()
        mock_query_df.collect.return_value = [{'qry_txt': "SELECT 1"}]
        mock_dq_query.return_value = mock_query_df

        mock_read_pg.return_value = self.mock_df
        mock_mm_time_perd_assoc.return_value = self.mock_df
        mock_get_dbutils.return_value = self.dbutils

        tier1_business_validation(
            self.validation_name, self.file_name, self.cntrt_id,
            self.run_id, self.spark, self.jdbc_url, self.db_name,
            self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
        )

        mock_merge.assert_called_once()
        mock_generate_report.assert_called_once()
        mock_generate_formatting.assert_called_once()

    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp", side_effect=Exception("Contract lookup failed"))
    def test_contract_lookup_failure(self, mock_load_cntrt_lkp, mock_sanitize, mock_get_logger, mock_get_dbutils):
        mock_get_dbutils.return_value = self.dbutils
        with self.assertRaises(Exception) as context:
            tier1_business_validation(
                self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark, self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
            )
        self.assertIn("Contract lookup failed", str(context.exception))

    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.load_mm_cntrt_categ_assoc")
    @patch("src.tp_utils.common.time_perd_class_codes_new", side_effect=Exception("Time period class code error"))
    def test_time_period_class_code_failure(
        self, mock_time_perd_class, mock_load_mm_categ, mock_load_cntrt_lkp, mock_sanitize, mock_get_logger, mock_get_dbutils
    ):
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [self.mock_contract_row()]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        mock_categ_df = MagicMock()
        mock_categ_df.collect.return_value = [MagicMock(categ_id="CAT001")]
        mock_load_mm_categ.return_value = mock_categ_df
        mock_get_dbutils.return_value = self.dbutils

        with self.assertRaises(Exception) as context:
            tier1_business_validation(
                 self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark,  self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
            )
        self.assertIn("Time period class code error", str(context.exception))

    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.sanitize_variable", side_effect=lambda x: x)
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.load_mm_cntrt_categ_assoc")
    @patch("src.tp_utils.common.time_perd_class_codes_new", return_value="mth")
    @patch("src.tp_utils.common.load_cntry_lkp")
    @patch("src.tp_utils.common.dq_query_retrieval", side_effect=Exception("DQ query retrieval failed"))
    def test_dq_query_failure(
        self, mock_dq_query, mock_load_cntry, mock_time_perd_class,
        mock_load_mm_categ, mock_load_cntrt_lkp, mock_sanitize, mock_get_logger, mock_get_dbutils
    ):
        mock_cntrt_df = MagicMock()
        mock_cntrt_df.collect.return_value = [self.mock_contract_row()]
        mock_load_cntrt_lkp.return_value = mock_cntrt_df

        mock_categ_df = MagicMock()
        mock_categ_df.collect.return_value = [MagicMock(categ_id="CAT001")]
        mock_load_mm_categ.return_value = mock_categ_df

        mock_cntry_df = MagicMock()
        mock_cntry_df.collect.return_value = [MagicMock(cntry_id="IN")]
        mock_load_cntry.return_value = mock_cntry_df
        mock_get_dbutils.return_value = self.dbutils

        with self.assertRaises(Exception) as context:
            tier1_business_validation(
                 self.validation_name, self.file_name, self.cntrt_id,
                self.run_id, self.spark, self.jdbc_url, self.db_name,
                self.db_user, self.db_pwd, self.postgres_schema, self.catalog_name
            )
        self.assertIn("DQ query retrieval failed", str(context.exception))