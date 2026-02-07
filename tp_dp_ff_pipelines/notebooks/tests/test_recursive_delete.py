import pytest
from unittest.mock import MagicMock

# Import the function to be tested
from src.tp_utils.common import recursive_delete

def test_recursive_delete_success():
    dbutils = MagicMock()
    dbutils.fs.ls.side_effect = [
        [
            MagicMock(path="/path/file1", isDir=MagicMock(return_value=False)),
            MagicMock(path="/path/folder1", isDir=MagicMock(return_value=True))
        ],
        [
            MagicMock(path="/path/folder1/file2", isDir=MagicMock(return_value=False))
        ]
    ]

    recursive_delete("/path", dbutils)

    # Verify that all files and folders are deleted
    dbutils.fs.rm.assert_any_call("/path/file1")
    dbutils.fs.rm.assert_any_call("/path/folder1/file2")
    dbutils.fs.rm.assert_any_call("/path/folder1")
    dbutils.fs.rm.assert_any_call("/path")

def test_recursive_delete_file_not_found():
    """
    Test that recursive_delete handles FileNotFoundException gracefully.
    Simulates a missing path scenario where dbutils.fs.ls throws a file not found exception.
    """
    dbutils = MagicMock()
    dbutils.fs.ls.side_effect = Exception("java.io.FileNotFoundException: /path")

    recursive_delete("/path", dbutils)

    # Ensure no delete operation is attempted
    dbutils.fs.rm.assert_not_called()

def test_recursive_delete_other_exception():
    """
    Test that recursive_delete handles unexpected exceptions gracefully.
    Simulates an error other than file not found during dbutils.fs.ls.
    """
    dbutils = MagicMock()
    dbutils.fs.ls.side_effect = Exception("Some other error")

    recursive_delete("/path", dbutils)

    # Ensure no delete operation is attempted
    dbutils.fs.rm.assert_not_called()

def test_recursive_delete_empty_directory():
    """
    Test that recursive_delete deletes an empty directory.
    Simulates a directory with no contents.
    """
    dbutils = MagicMock()
    dbutils.fs.ls.return_value = []

    recursive_delete("/empty_path", dbutils)

    # Ensure the empty directory itself is deleted
    dbutils.fs.rm.assert_called_once_with("/empty_path")
