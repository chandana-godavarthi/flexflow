import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_cntrt_lkp

@pytest.fixture
def mock_spark():
    return MagicMock()

@pytest.fixture
def mock_read_from_postgres():
    with patch("src.tp_utils.common.read_from_postgres") as mock_func:
        yield mock_func

def test_load_cntrt_lkp_success(mock_spark, mock_read_from_postgres):
    mock_df = MagicMock()
    mock_df.filter.return_value = "filtered_df"
    mock_read_from_postgres.return_value = mock_df

    result = load_cntrt_lkp(
        cntrt_id=101,
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_read_from_postgres.assert_called_once_with(
        "test_schema.mm_cntrt_lkp",
        mock_spark,
        "jdbc:test",
        "test_db",
        "user",
        "pwd"
    )
    mock_df.filter.assert_called_once_with("cntrt_id= 101")
    assert result == "filtered_df"

def test_load_cntrt_lkp_empty_inputs(mock_spark, mock_read_from_postgres):
    mock_df = MagicMock()
    mock_df.filter.return_value = "filtered_df"
    mock_read_from_postgres.return_value = mock_df

    result = load_cntrt_lkp(
        cntrt_id=0,
        postgres_schema="",
        spark=mock_spark,
        ref_db_jdbc_url="",
        ref_db_name="",
        ref_db_user="",
        ref_db_pwd=""
    )

    mock_read_from_postgres.assert_called_once_with(
        ".mm_cntrt_lkp",
        mock_spark,
        "",
        "",
        "",
        ""
    )
    mock_df.filter.assert_called_once_with("cntrt_id= 0")
    assert result == "filtered_df"
