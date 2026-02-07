import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    return dbutils

@pytest.fixture(autouse=True)
def mock_requests():
    with patch("requests.get") as mock_get, \
         patch("requests.post") as mock_post, \
         patch("requests.put") as mock_put:

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        mock_response.text = '{"status": "ok"}'  # Simulate valid JSON string

        mock_get.return_value = mock_response
        mock_post.return_value = mock_response
        mock_put.return_value = mock_response

        yield

@patch("src.tp_utils.common.get_spark_session")
@patch("src.tp_utils.common.derive_base_path", return_value="/mnt/tp-source-data")
def test_work_to_arch_file_found(mock_derive_base_path, mock_get_spark_session, mock_dbutils):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='file2', name_attr='target_file.csv')
    ]
    mock_dbutils.fs.ls.return_value[0].name = 'file1.csv'
    mock_dbutils.fs.ls.return_value[1].name = 'target_file.csv'

    result = common.work_to_arch("target_file.csv", mock_dbutils)

    assert result is True
    mock_dbutils.fs.mv.assert_called_once_with(
        '/mnt/tp-source-data/WORK/target_file.csv',
        '/mnt/tp-source-data/ARCH/target_file.csv'
    )

@patch("src.tp_utils.common.get_spark_session")
@patch("src.tp_utils.common.derive_base_path", return_value="/mnt/tp-source-data")
def test_work_to_arch_file_not_found(mock_derive_base_path, mock_get_spark_session, mock_dbutils):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='file2', name_attr='another_file.csv')
    ]
    mock_dbutils.fs.ls.return_value[0].name = 'file1.csv'
    mock_dbutils.fs.ls.return_value[1].name = 'another_file.csv'

    result = common.work_to_arch("missing_file.csv", mock_dbutils)

    assert result is False
    mock_dbutils.fs.mv.assert_not_called()

@patch("src.tp_utils.common.get_spark_session")
@patch("src.tp_utils.common.derive_base_path", return_value="/mnt/tp-source-data")
def test_work_to_arch_empty_directory(mock_derive_base_path, mock_get_spark_session, mock_dbutils):
    mock_dbutils.fs.ls.return_value = []

    result = common.work_to_arch("any_file.csv", mock_dbutils)

    assert result is False
    mock_dbutils.fs.mv.assert_not_called()

@patch("src.tp_utils.common.get_spark_session")
@patch("src.tp_utils.common.derive_base_path", return_value="/mnt/tp-source-data")
def test_work_to_arch_multiple_files_only_first_match_moves(mock_derive_base_path, mock_get_spark_session, mock_dbutils):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='target', name_attr='target_file.csv'),
        MagicMock(name='file2', name_attr='another_file.csv')
    ]
    for idx, fname in enumerate(['file1.csv', 'target_file.csv', 'another_file.csv']):
        mock_dbutils.fs.ls.return_value[idx].name = fname

    result = common.work_to_arch("target_file.csv", mock_dbutils)

    assert result is True
    mock_dbutils.fs.mv.assert_called_once_with(
        '/mnt/tp-source-data/WORK/target_file.csv',
        '/mnt/tp-source-data/ARCH/target_file.csv'
    )
