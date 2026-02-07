import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import materialize
#logic
class TestMaterialize(unittest.TestCase):

    def setUp(self):
        self.df = MagicMock()
        self.df_name = "mock_df"
        self.run_id = "run_001"
        self.materialised_base_path = "/mnt/raw_data"
        self.result_path = f"{self.materialised_base_path}/{self.run_id}/{self.df_name}"

    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.get_logger")
    def test_materialize_success(self, mock_get_logger, mock_materialise_path):
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock materialise_path to return a predictable path
        mock_materialise_path.return_value = self.materialised_base_path

        # Mock write chain
        self.df.write.mode.return_value.format.return_value.option.return_value.save.return_value = None

        # Act
        materialize(self.df, self.df_name, self.run_id)

        # Assert
        mock_get_logger.assert_called_once()
        self.df.write.mode.assert_called_once_with("overwrite")
        self.df.write.mode().format.assert_called_once_with("parquet")
        self.df.write.mode().format().option.assert_called_once_with("compression", "snappy")
        self.df.write.mode().format().option().save.assert_called_once_with(self.result_path)
        mock_logger.info.assert_called_once_with(f"[Materialize] {self.df_name} saved to path: {self.result_path}")

    @patch("src.tp_utils.common.materialise_path")
    @patch("src.tp_utils.common.get_logger")
    def test_materialize_failure(self, mock_get_logger, mock_materialise_path):
        # Setup mock logger
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Mock materialise_path to return a predictable path
        mock_materialise_path.return_value = self.materialised_base_path

        # Simulate exception during save
        self.df.write.mode.return_value.format.return_value.option.return_value.save.side_effect = Exception("Write failed")

        with self.assertRaises(Exception) as context:
            materialize(self.df, self.df_name, self.run_id)

        self.assertEqual(str(context.exception), "Write failed")
        mock_get_logger.assert_called_once()
        mock_logger.error.assert_called_once_with(
            f"[Materialize] Failed to write DataFrame '{self.df_name}' to {self.result_path}. Error: Write failed"
        )
