import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import load_cntry_lkp

# Scenario 1: Valid country name returns expected DataFrame
def test_load_cntry_lkp_valid_country():
    spark = MagicMock()
    mock_df = MagicMock()
    mock_filtered_df = MagicMock()
    mock_selected_df = MagicMock()

    # Chain mocks for read -> filter -> select
    mock_df.filter.return_value = mock_filtered_df
    mock_filtered_df.select.return_value = mock_selected_df

    with patch("src.tp_utils.common.read_from_postgres", return_value=mock_df):
        result = load_cntry_lkp(spark, "public", "India", "jdbc_url", "ref_db", "user", "pwd")

    mock_df.filter.assert_called_once_with("cntry_name='India'")
    mock_filtered_df.select.assert_called_once_with("cntry_id")
    assert result == mock_selected_df

# Scenario 2: Empty result from read_from_postgres
def test_load_cntry_lkp_empty_result():
    spark = MagicMock()
    mock_df = MagicMock()
    mock_filtered_df = MagicMock()
    mock_selected_df = MagicMock()

    mock_df.filter.return_value = mock_filtered_df
    mock_filtered_df.select.return_value = mock_selected_df

    with patch("src.tp_utils.common.read_from_postgres", return_value=mock_df):
        result = load_cntry_lkp(spark, "public", "UnknownCountry", "jdbc_url", "ref_db", "user", "pwd")

    mock_df.filter.assert_called_once_with("cntry_name='UnknownCountry'")
    mock_filtered_df.select.assert_called_once_with("cntry_id")
    assert result == mock_selected_df

# Scenario 3: Exception during read_from_postgres
def test_load_cntry_lkp_read_failure():
    spark = MagicMock()

    with patch("src.tp_utils.common.read_from_postgres", side_effect=Exception("DB connection failed")):
        with pytest.raises(Exception, match="DB connection failed"):
            load_cntry_lkp(spark, "public", "India", "jdbc_url", "ref_db", "user", "pwd")