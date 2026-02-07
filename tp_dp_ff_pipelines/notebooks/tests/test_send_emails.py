import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

from src.tp_utils.common import send_emails

class TestSendEmails(unittest.TestCase):

    def setUp(self):
        self.spark = MagicMock()
        self.run_id = "R123"
        self.cntrt_id = "C123"
        self.catalog_name = "test_catalog"
        self.postgres_schema = "ref_schema"
        self.file_name = "source_file.csv"
        self.tier1_vendor_id = "V123"
        self.validation_name = "Reference Data Validations"
        self.ref_db_jdbc_url = "jdbc:postgresql://localhost:5432/refdb"
        self.ref_db_name = "refdb"
        self.ref_db_user = "user"
        self.ref_db_pwd = "pwd"

    @patch("src.tp_utils.common.smtplib.SMTP")  # ✅ Correct patch path
    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.read_query_from_postgres")
    @patch("src.tp_utils.common.read_from_postgres")
    @patch("src.tp_utils.common.load_cntrt_lkp")
    @patch("src.tp_utils.common.pd.DataFrame")
    @patch("src.tp_utils.common.os.path.getsize", return_value=1048576)  # 1MB
    @patch("src.tp_utils.common.os.path.abspath", side_effect=lambda x: f"/safe/path/{x}")
    @patch("src.tp_utils.common.os.path.basename", side_effect=lambda x: x.split("/")[-1])
    @patch("src.tp_utils.common.open", create=True)
    @patch("src.tp_utils.common.MIMEMultipart")
    @patch("src.tp_utils.common.MIMEText")
    @patch("src.tp_utils.common.MIMEBase")
    @patch("src.tp_utils.common.encoders.encode_base64")
    @patch("src.tp_utils.common.print")
    def test_email_sent_when_validation_failed(
        self, mock_print, mock_encode, mock_mimebase, mock_mimetext,
        mock_multipart, mock_open, mock_basename, mock_abspath,
        mock_getsize, mock_df_constructor, mock_load_cntrt_lkp,
        mock_read_from_postgres, mock_read_query, mock_get_dbutils, mock_smtp
    ):
        # Setup mocks
        mock_df_report = MagicMock()
        mock_df_report.count.return_value = 1
        self.spark.sql.return_value = mock_df_report

        mock_df_constructor.return_value = pd.DataFrame({'Email': ['test@example.com']})
        mock_read_query.return_value = MagicMock()
        mock_read_from_postgres.return_value = MagicMock()
        mock_read_from_postgres.return_value.collect.return_value = [{'end_time': 'September 09,2025'}]
        mock_load_cntrt_lkp.return_value.collect.return_value = [MagicMock(cntrt_code="C_CODE")]

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.side_effect = lambda scope, key: f"mock_{key}"
        mock_get_dbutils.return_value = mock_dbutils

        # Call function
        send_emails(
            self.spark,
            self.run_id,
            self.cntrt_id,
            self.catalog_name,
            self.postgres_schema,
            self.file_name,
            self.validation_name,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        # ✅ Assert SMTP was called
        assert mock_smtp.called, "SMTP was not called"
        mock_smtp.assert_called_once()

    @patch("src.tp_utils.common.get_dbutils")
    @patch("src.tp_utils.common.print")
    def test_email_not_sent_when_no_failed_validation(self, mock_print, mock_get_dbutils):
        mock_df_report = MagicMock()
        mock_df_report.count.return_value = 0
        self.spark.sql.return_value = mock_df_report

        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "mock_value"
        mock_get_dbutils.return_value = mock_dbutils

        send_emails(
            self.spark,
            self.run_id,
            self.cntrt_id,
            self.catalog_name,
            self.postgres_schema,
            self.file_name,
            self.validation_name,
            self.ref_db_jdbc_url,
            self.ref_db_name,
            self.ref_db_user,
            self.ref_db_pwd
        )

        mock_print.assert_not_called()
