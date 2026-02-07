import unittest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import generate_report

class TestGenerateReport(unittest.TestCase):

    def setUp(self):
        self.run_id = "test_run"
        self.cntrt_id = "C001"
        self.file_name = "test_file.csv"
        self.catalog_name = "tp_catalog"
        self.postgres_schema = "public"
        self.ref_db_jdbc_url = "jdbc:postgresql://localhost:5432/ref_db"
        self.ref_db_name = "ref_db"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"
        self.mock_spark = MagicMock()

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.pd.ExcelWriter")
    @patch("src.tp_utils.common.copyfile")
    def test_generate_report_success(self, mock_copyfile, mock_excel_writer, mock_load_cntrt_lkp, mock_get_logger, mock_col):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_col.return_value = MagicMock()

        mock_load_cntrt_lkp.return_value.collect.return_value = [
            MagicMock(cntrt_code="TP001", cntry_name="India", categ_name="Retail")
        ]

        mock_df_summary = MagicMock()
        mock_df_summary.collect.return_value = [
            ("Validation1", "FAILED", "Details1", "Type1"),
            ("Validation2", "PASSED", "Details2", "Type1")
        ]
        mock_df_summary.filter.return_value.count.return_value = 1
        def sql_side_effect(*args, **kwargs):
            query = args[0] if args else ""
            if "summary" in query.lower():
                return mock_df_summary
            else:
                return MagicMock()

        self.mock_spark.sql.side_effect = sql_side_effect

        formatted_df_mock = MagicMock()
        formatted_df_mock.filter.return_value.count.return_value = 1
        formatted_df_mock.select.return_value.toPandas.return_value = MagicMock()
        self.mock_spark.createDataFrame.return_value = formatted_df_mock

        generate_report(
            self.run_id, self.cntrt_id, self.file_name, self.mock_spark,
            self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user,
            self.ref_db_pwd, self.catalog_name, self.postgres_schema
        )

        mock_logger.info.assert_any_call("Report generation completed successfully")
        mock_copyfile.assert_called_once()

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.pd.ExcelWriter")
    @patch("src.tp_utils.common.copyfile")
    def test_generate_report_no_failed_validations(self, mock_copyfile, mock_excel_writer, mock_load_cntrt_lkp, mock_get_logger, mock_col):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_col.return_value = MagicMock()

        mock_load_cntrt_lkp.return_value.collect.return_value = [
            MagicMock(cntrt_code="TP001", cntry_name="India", categ_name="Retail")
        ]

        mock_df_summary = MagicMock()
        mock_df_summary.collect.return_value = [
            ("Validation1", "PASSED", "Details1", "Type1")
        ]
        mock_df_summary.filter.return_value.count.return_value = 0

        def sql_side_effect(*args, **kwargs):
            query = args[0] if args else ""
            if "summary" in query.lower():
                return mock_df_summary
            else:
                return MagicMock()

        self.mock_spark.sql.side_effect = sql_side_effect

        formatted_df_mock = MagicMock()
        formatted_df_mock.filter.return_value.count.return_value = 0
        formatted_df_mock.select.return_value.toPandas.return_value = MagicMock()
        self.mock_spark.createDataFrame.return_value = formatted_df_mock

        generate_report(
            self.run_id, self.cntrt_id, self.file_name, self.mock_spark,
            self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user,
            self.ref_db_pwd, self.catalog_name, self.postgres_schema
        )

        mock_logger.info.assert_any_call("Report generation completed successfully")
        mock_copyfile.assert_called_once()

    @patch("src.tp_utils.common.col")
    @patch("src.tp_utils.common.get_logger")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.pd.ExcelWriter")
    @patch("src.tp_utils.common.copyfile")
    def test_generate_report_sql_exception(self, mock_copyfile, mock_excel_writer, mock_load_cntrt_lkp, mock_get_logger, mock_col):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_col.return_value = MagicMock()

        mock_load_cntrt_lkp.return_value.collect.return_value = [
            MagicMock(cntrt_code="TP001", cntry_name="India", categ_name="Retail")
        ]

        self.mock_spark.sql.side_effect = Exception("SQL failure")

        with self.assertRaises(Exception) as context:
            generate_report(
                self.run_id, self.cntrt_id, self.file_name, self.mock_spark,
                self.ref_db_jdbc_url, self.ref_db_name, self.ref_db_user,
                self.ref_db_pwd, self.catalog_name, self.postgres_schema
            )

        self.assertIn("SQL failure", str(context.exception))
