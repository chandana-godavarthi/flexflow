import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import t2_load_file

@pytest.fixture
def mock_inputs():
    return {
        "cntrt_id": "C123",
        "dmnsn_name": "product",
        "file_type": "csv",
        "run_id": "R001",
        "notebook_name": "load_notebook",
        "postgres_schema": "public",
        "spark": MagicMock(),
        "ref_db_jdbc_url": "jdbc:postgresql://localhost:5432/test",
        "ref_db_name": "test_db",
        "ref_db_user": "user",
        "ref_db_pwd": "pwd"
    }

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.load_file")
@patch("src.tp_utils.common.load_cntrt_dlmtr_lkp")
@patch("src.tp_utils.common.load_cntrt_file_lkp")
@patch("src.tp_utils.common.load_cntrt_lkp")
def test_t2_load_file_success(mock_lkp, mock_file_lkp, mock_dlmtr_lkp, mock_load_file, mock_logger, mock_inputs):
    # Mock return values
    mock_lkp.return_value.select.return_value.collect.return_value = [MagicMock(file_patrn="vendor_pattern")]
    mock_file_lkp.return_value.select.return_value.collect.return_value = [MagicMock(file_patrn="step_pattern")]
    mock_dlmtr_lkp.return_value.collect.return_value = [MagicMock(dlmtr_val=",")]

    logger_instance = MagicMock()
    mock_logger.return_value = logger_instance

    t2_load_file(**mock_inputs)

    mock_lkp.assert_called_once()
    mock_file_lkp.assert_called_once()
    mock_dlmtr_lkp.assert_called_once()
    mock_load_file.assert_called_once_with(
        mock_inputs["file_type"],
        mock_inputs["run_id"],
        mock_inputs["cntrt_id"],
        "step_pattern",
        mock_inputs["notebook_name"],
        ",",
        mock_inputs["postgres_schema"],
        mock_inputs["spark"],
        mock_inputs["ref_db_jdbc_url"],
        mock_inputs["ref_db_name"],
        mock_inputs["ref_db_user"],
        mock_inputs["ref_db_pwd"]
    )
