import unittest
from unittest.mock import MagicMock
from src.tp_utils.common import cdl_publishing

class TestCdlPublishing(unittest.TestCase):

    def test_successful_publishing_flow(self):
        # Mock configuration and metapsclient passed as arguments
        mock_config = MagicMock()
        mock_configuration = MagicMock()
        mock_configuration.load_for_default_environment_notebook.return_value = mock_config

        mock_meta_client = MagicMock()
        mock_metapsclient = MagicMock()
        mock_metapsclient.configure.return_value.get_client.return_value = mock_meta_client
        mock_meta_client.mode.return_value = mock_meta_client

        cdl_publishing(
            logical_table_name="TP_WK_FCT",
            physical_table_name="TP_WK_FCT",
            unity_catalog_table_name="TP_WK_FCT",
            partition_definition_value="year",
            dbutils="mock_dbutils",
            configuration=mock_configuration,
            metapsclient=mock_metapsclient
        )

        mock_configuration.load_for_default_environment_notebook.assert_called_once_with(dbutils="mock_dbutils")
        mock_metapsclient.configure.assert_called_once_with(mock_config)
        mock_meta_client.mode.assert_called_once_with(publish_mode="update")
        mock_meta_client.publish_table.assert_called_once()
        mock_meta_client.start_publishing.assert_called_once()

    def test_publish_table_raises_exception(self):
        mock_config = MagicMock()
        mock_configuration = MagicMock()
        mock_configuration.load_for_default_environment_notebook.return_value = mock_config

        mock_meta_client = MagicMock()
        mock_metapsclient = MagicMock()
        mock_metapsclient.configure.return_value.get_client.return_value = mock_meta_client
        mock_meta_client.mode.return_value = mock_meta_client
        mock_meta_client.publish_table.side_effect = Exception("Publishing failed")

        with self.assertRaises(Exception) as context:
            cdl_publishing(
                logical_table_name="TP_WK_FCT",
                physical_table_name="TP_WK_FCT",
                unity_catalog_table_name="TP_WK_FCT",
                partition_definition_value="year",
                dbutils="mock_dbutils",
                configuration=mock_configuration,
                metapsclient=mock_metapsclient
            )

        self.assertIn("Publishing failed", str(context.exception))

    def test_missing_config_raises_exception(self):
        mock_configuration = MagicMock()
        mock_configuration.load_for_default_environment_notebook.side_effect = Exception("Config error")

        mock_metapsclient = MagicMock()

        with self.assertRaises(Exception) as context:
            cdl_publishing(
                logical_table_name="TP_WK_FCT",
                physical_table_name="TP_WK_FCT",
                unity_catalog_table_name="TP_WK_FCT",
                partition_definition_value="year",
                dbutils="mock_dbutils",
                configuration=mock_configuration,
                metapsclient=mock_metapsclient
            )

        self.assertIn("Config error", str(context.exception))
