import unittest
from unittest.mock import MagicMock, patch

from src.tp_utils.common import validation

class TestValidationFunction(unittest.TestCase):

    def setUp(self):
        self.catalog_name = "test_catalog"
        self.value = "SELECT * FROM dummy_table"
        self.validation_name = "dummy_validation"
        self.run_id = "12345"
        self.validation_type = "Type A"
        self.spark = MagicMock()

    @patch("src.tp_utils.common.derive_publish_path")
    def test_validation_passed(self, mock_derive_publish_path):
        # Scenario: df_val.count() == 0, should return PASSED
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        self.spark.sql.return_value = mock_df

        result = validation(
            self.catalog_name,
            self.value,
            self.validation_name,
            self.run_id,
            self.spark,
            self.validation_type
        )

        expected = (int(self.run_id), self.validation_name, 'PASSED', '', self.validation_type, '')
        self.assertEqual(result, expected)

    @patch("src.tp_utils.common.derive_publish_path")
    def test_validation_failed(self, mock_derive_publish_path):
        # Scenario: df_val.count() > 0, should return FAILED and write parquet
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        self.spark.sql.side_effect = [mock_df, MagicMock()]  # First for value, second for tp_tab_name_df

        # Mock tp_tab_name_df.collect()[0]['TAB_NAME']
        mock_tp_tab_name_df = MagicMock()
        mock_tp_tab_name_df.collect.return_value = [{'TAB_NAME': 'HyperlinkTab'}]
        self.spark.sql.side_effect = [mock_df, mock_tp_tab_name_df]

        mock_derive_publish_path.return_value = "/tmp/publish_path"
        mock_df.coalesce.return_value = mock_df
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        result = validation(
            self.catalog_name,
            self.value,
            self.validation_name,
            self.run_id,
            self.spark,
            self.validation_type
        )

        cleaned_validation_type = self.validation_type.replace(" ", "_")
        expected_path = f"/tmp/publish_path/tp_dvm_rpt/{self.run_id}/{cleaned_validation_type}/HyperlinkTab.parquet"
        expected_hyperlink = '=HYPERLINK("#\'HyperlinkTab\'!A1","click_here")'
        expected = (int(self.run_id), self.validation_name, 'FAILED', expected_hyperlink, self.validation_type, expected_path)
        self.assertEqual(result, expected)
        mock_df.write.save.assert_called_once_with(expected_path)

    @patch("src.tp_utils.common.derive_publish_path")
    def test_validation_value_is_dataframe(self, mock_derive_publish_path):
        # Scenario: value is already a DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        result = validation(
            self.catalog_name,
            mock_df,
            self.validation_name,
            self.run_id,
            self.spark,
            self.validation_type
        )
        expected = (int(self.run_id), self.validation_name, 'PASSED', '', self.validation_type, '')
        self.assertEqual(result, expected)

    @patch("src.tp_utils.common.derive_publish_path")
    def test_validation_failed_with_spaces_in_type(self, mock_derive_publish_path):
        # Scenario: validation_type has spaces, check cleaned_validation_type
        mock_df = MagicMock()
        mock_df.count.return_value = 2
        self.spark.sql.side_effect = [mock_df, MagicMock()]
        mock_tp_tab_name_df = MagicMock()
        mock_tp_tab_name_df.collect.return_value = [{'TAB_NAME': 'TabWithSpace'}]
        self.spark.sql.side_effect = [mock_df, mock_tp_tab_name_df]

        mock_derive_publish_path.return_value = "/tmp/path"
        mock_df.coalesce.return_value = mock_df
        mock_df.write.format.return_value = mock_df.write
        mock_df.write.options.return_value = mock_df.write
        mock_df.write.mode.return_value = mock_df.write

        validation_type = "Type With Spaces"
        result = validation(
            self.catalog_name,
            self.value,
            self.validation_name,
            self.run_id,
            self.spark,
            validation_type
        )

        cleaned_validation_type = validation_type.replace(" ", "_")
        expected_path = f"/tmp/path/tp_dvm_rpt/{self.run_id}/{cleaned_validation_type}/TabWithSpace.parquet"
        expected_hyperlink = '=HYPERLINK("#\'TabWithSpace\'!A1","click_here")'
        expected = (int(self.run_id), self.validation_name, 'FAILED', expected_hyperlink, validation_type, expected_path)
        self.assertEqual(result, expected)
        mock_df.write.save.assert_called_once_with(expected_path)