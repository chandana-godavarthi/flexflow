import unittest
from unittest.mock import patch, MagicMock
import subprocess

# Import the function
from src.tp_utils.common import unzip  # Adjust path if needed


class TestUnzipFunction(unittest.TestCase):

    def setUp(self):
        self.file_name = "test_archive.zip"
        self.vol_path = "/mock/path"
        self.target_dir = "/mock/path/test_archive"

    @patch("src.tp_utils.common.subprocess.run")
    @patch("src.tp_utils.common.os.listdir", return_value=["file1.txt", "file2.txt"])
    @patch("src.tp_utils.common.os.makedirs")
    @patch("src.tp_utils.common.os.path.exists", return_value=False)
    def test_unzip_no_existing_folder(self, mock_exists, mock_makedirs, mock_listdir, mock_run):
        mock_run.return_value = MagicMock(returncode=0)

        unzip(self.file_name, self.vol_path)

        mock_makedirs.assert_called_once_with(self.target_dir, exist_ok=True)
        mock_run.assert_called_once_with(
            ["unzip", f"{self.vol_path}/{self.file_name}", "-d", self.target_dir],
            check=True
        )

    @patch("src.tp_utils.common.subprocess.run")
    @patch("src.tp_utils.common.os.listdir", side_effect=[["inner_folder"], ["file1.txt"]])
    @patch("src.tp_utils.common.shutil.move")
    @patch("src.tp_utils.common.shutil.rmtree")
    @patch("src.tp_utils.common.os.path.isdir", return_value=True)
    @patch("src.tp_utils.common.os.makedirs")
    @patch("src.tp_utils.common.os.path.exists", return_value=True)
    def test_unzip_flattens_inner_folder(
        self, mock_exists, mock_makedirs, mock_isdir, mock_rmtree, mock_move, mock_listdir, mock_run
    ):
        mock_run.return_value = MagicMock(returncode=0)

        unzip(self.file_name, self.vol_path)

        mock_rmtree.assert_any_call(self.target_dir)  # Overwrite existing folder
        mock_move.assert_called()  # Files moved from inner folder
        mock_rmtree.assert_called()  # Inner folder removed

    @patch("src.tp_utils.common.subprocess.run", side_effect=subprocess.CalledProcessError(9, "unzip"))
    @patch("src.tp_utils.common.os.makedirs")
    @patch("src.tp_utils.common.shutil.rmtree")
    @patch("src.tp_utils.common.os.path.exists", return_value=True)
    def test_unzip_zipfile_error(self, mock_exists, mock_rmtree, mock_makedirs, mock_run):
        with self.assertRaises(subprocess.CalledProcessError) as context:
            unzip(self.file_name, self.vol_path)
        # Assert actual message from CalledProcessError
        self.assertIn("returned non-zero exit status", str(context.exception))


if __name__ == "__main__":
    unittest.main()