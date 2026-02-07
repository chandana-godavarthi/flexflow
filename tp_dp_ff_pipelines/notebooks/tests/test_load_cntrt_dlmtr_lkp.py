import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_cntrt_dlmtr_lkp  

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = "mocked_result_df"
    return spark

@pytest.fixture
def mock_read_from_postgres():
    with patch("src.tp_utils.common.read_from_postgres") as mock_func:
        yield mock_func

def test_load_cntrt_dlmtr_lkp_success(mock_spark, mock_read_from_postgres):
    # Setup mock DataFrames
    mock_df_cntrt_file_lkp = MagicMock()
    mock_df_col_dlmtr_lkp = MagicMock()

    # Configure the mock return values
    mock_read_from_postgres.side_effect = [mock_df_cntrt_file_lkp, mock_df_col_dlmtr_lkp]

    # Mock filter and createOrReplaceTempView
    mock_df_cntrt_file_lkp.filter.return_value = mock_df_cntrt_file_lkp
    # Call the function
    result = load_cntrt_dlmtr_lkp(
        cntrt_id=123,
        dmnsn_name="test_dmnsn",
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    # Assertions
    mock_read_from_postgres.assert_any_call("test_schema.mm_cntrt_file_lkp", mock_spark, "jdbc:test", "test_db", "user", "pwd")
    mock_read_from_postgres.assert_any_call("test_schema.mm_col_dlmtr_lkp", mock_spark, "jdbc:test", "test_db", "user", "pwd")
    mock_df_cntrt_file_lkp.filter.assert_called_once_with("cntrt_id=123 AND dmnsn_name='test_dmnsn'")
    mock_df_cntrt_file_lkp.createOrReplaceTempView.assert_called_once_with("mm_cntrt_file_lkp")
    mock_df_col_dlmtr_lkp.createOrReplaceTempView.assert_called_once_with("mm_col_dlmtr_lkp")
    mock_spark.sql.assert_called_once_with(
        " SELECT dt.dlmtr_val FROM mm_cntrt_file_lkp ct join mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id "
    )
    assert result == "mocked_result_df"

def test_load_cntrt_dlmtr_lkp_empty_filter(mock_spark, mock_read_from_postgres):
    # Setup mock DataFrames
    mock_df_cntrt_file_lkp = MagicMock()
    mock_df_col_dlmtr_lkp = MagicMock()

    # Simulate empty filter result
    mock_df_cntrt_file_lkp.filter.return_value = mock_df_cntrt_file_lkp

    # Configure the mock return values
    mock_read_from_postgres.side_effect = [mock_df_cntrt_file_lkp, mock_df_col_dlmtr_lkp]

    # Call the function
    result = load_cntrt_dlmtr_lkp(
        cntrt_id=0,
        dmnsn_name="",
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    # Assertions
    mock_df_cntrt_file_lkp.filter.assert_called_once_with("cntrt_id=0 AND dmnsn_name=''")
    assert result == "mocked_result_df"

def test_load_cntrt_dlmtr_lkp_invalid_schema(mock_spark, mock_read_from_postgres):
    mock_df_cntrt_file_lkp = MagicMock()
    mock_df_col_dlmtr_lkp = MagicMock()

    mock_df_cntrt_file_lkp.filter.return_value = mock_df_cntrt_file_lkp
    mock_read_from_postgres.side_effect = [mock_df_cntrt_file_lkp, mock_df_col_dlmtr_lkp]

    result = load_cntrt_dlmtr_lkp(
        cntrt_id=999,
        dmnsn_name="invalid",
        postgres_schema="invalid_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_read_from_postgres.assert_any_call("invalid_schema.mm_cntrt_file_lkp", mock_spark, "jdbc:test", "test_db", "user", "pwd")
    assert result == "mocked_result_df"
