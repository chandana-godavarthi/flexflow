import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import acn_prod_trans_materialize  # 🔧 Adjust path as needed

class TestACNProdTransMaterialize(unittest.TestCase):

    def setUp(self):
        self.mock_df = MagicMock()
        self.run_id = "RUN123"
        self.mock_spark = MagicMock()
        self.mock_path = "/mock/path/RUN123/product_transformation_df_prod_stgng_vw"

    @patch("src.tp_utils.common.get_spark_session")
    @patch("src.tp_utils.common.materialise_path")
    def test_materialize_success(self, mock_materialise_path, mock_get_spark_session):
        # Setup mocks
        mock_get_spark_session.return_value = self.mock_spark
        mock_materialise_path.return_value = "/mock/path"

        mock_writer = MagicMock()
        self.mock_df.coalesce.return_value = self.mock_df
        self.mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.options.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer

        # Call the function
        acn_prod_trans_materialize(self.mock_df, self.run_id)

        # Assertions
        mock_get_spark_session.assert_called_once()
        mock_materialise_path.assert_called_once_with(self.mock_spark)
        mock_writer.format.assert_called_once_with("parquet")
        mock_writer.options.assert_called_once_with(header=True)
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.save.assert_called_once_with(self.mock_path)

    @patch("src.tp_utils.common.get_spark_session", side_effect=Exception("Spark session error"))
    @patch("src.tp_utils.common.materialise_path")
    def test_spark_session_failure(self, mock_materialise_path, mock_get_spark_session):
        with self.assertRaises(Exception) as context:
            acn_prod_trans_materialize(self.mock_df, self.run_id)
        self.assertIn("Spark session error", str(context.exception))

    @patch("src.tp_utils.common.get_spark_session")
    @patch("src.tp_utils.common.materialise_path", side_effect=Exception("Path resolution failed"))
    def test_materialise_path_failure(self, mock_materialise_path, mock_get_spark_session):
        mock_get_spark_session.return_value = self.mock_spark
        with self.assertRaises(Exception) as context:
            acn_prod_trans_materialize(self.mock_df, self.run_id)
        self.assertIn("Path resolution failed", str(context.exception))

    @patch("src.tp_utils.common.get_spark_session")
    @patch("src.tp_utils.common.materialise_path")
    def test_write_failure(self, mock_materialise_path, mock_get_spark_session):
        mock_get_spark_session.return_value = self.mock_spark
        mock_materialise_path.return_value = "/mock/path"

        self.mock_df.coalesce.return_value = self.mock_df
        mock_writer = MagicMock()
        self.mock_df.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.options.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.save.side_effect = Exception("Write failed")

        with self.assertRaises(Exception) as context:
            acn_prod_trans_materialize(self.mock_df, self.run_id)
        self.assertIn("Write failed", str(context.exception))

if __name__ == "__main__":
    unittest.main()
