import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_cntrt_col_assign

@pytest.fixture
def mock_spark():
    return MagicMock()

@pytest.fixture
def mock_read_from_postgres():
    with patch("src.tp_utils.common.read_from_postgres") as mock_func:
        yield mock_func

def test_load_cntrt_col_assign_success(mock_spark, mock_read_from_postgres):
    # Setup mock DataFrame
    mock_df = MagicMock()
    mock_df.filter.return_value = "filtered_df"
    mock_read_from_postgres.return_value = mock_df

    result = load_cntrt_col_assign(
        cntrt_id=101,
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_read_from_postgres.assert_called_once_with(
        "test_schema.mm_col_asign_lkp",
        mock_spark,
        "jdbc:test",
        "test_db",
        "user",
        "pwd"
    )
    mock_df.filter.assert_called_once_with("cntrt_id = 101")
    assert result == "filtered_df"

def test_load_cntrt_col_assign_empty_cntrt_id(mock_spark, mock_read_from_postgres):
    mock_df = MagicMock()
    mock_df.filter.return_value = "filtered_df"
    mock_read_from_postgres.return_value = mock_df

    result = load_cntrt_col_assign(
        cntrt_id=0,
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_df.filter.assert_called_once_with("cntrt_id = 0")
    assert result == "filtered_df"

def test_load_cntrt_col_assign_invalid_schema(mock_spark, mock_read_from_postgres):
    mock_df = MagicMock()
    mock_df.filter.return_value = "filtered_df"
    mock_read_from_postgres.return_value = mock_df

    result = load_cntrt_col_assign(
        cntrt_id=999,
        postgres_schema="invalid_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_read_from_postgres.assert_called_once_with(
        "invalid_schema.mm_col_asign_lkp",
        mock_spark,
        "jdbc:test",
        "test_db",
        "user",
        "pwd"
    )
    assert result == "filtered_df"
