# Import necessary modules
from unittest.mock import MagicMock, patch
import pytest
# Import the function to be tested
from src.tp_utils.common import reference_data_load_variables


# Test case: Valid contract ID scenario
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_valid_contract_id(mock_read_query, mock_load_lkp):
    # Mock Spark session and DataFrame row
    mock_spark = MagicMock()
    mock_row = MagicMock()
    mock_row.cntrt_code = "C123"
    mock_row.srce_sys_id = "SYS1"
    mock_row.time_perd_type_code = "MONTHLY"

    # Mock lookup DataFrame
    mock_df_lkp = MagicMock()
    mock_df_lkp.collect.return_value = [mock_row]
    mock_load_lkp.return_value = mock_df_lkp

    # Mock associated DataFrame
    mock_df_assoc = MagicMock()
    mock_read_query.return_value = mock_df_assoc

    # Call the function and assert expected results
    result = reference_data_load_variables(1, "schema", mock_spark, "url", "db", "user", "pwd")
    assert result[0] == "C123"
    assert result[1] == "SYS1"
    assert result[2] == "MONTHLY"
    assert result[3] == mock_df_assoc
    assert result[4] == mock_df_lkp


# Test case: Empty contract lookup should raise IndexError
@patch("src.tp_utils.common.load_cntrt_lkp")
def test_empty_contract_lookup(mock_load_lkp):
    mock_spark = MagicMock()
    mock_df_lkp = MagicMock()
    mock_df_lkp.collect.return_value = []  # Simulate no data
    mock_load_lkp.return_value = mock_df_lkp

    with pytest.raises(IndexError):
        reference_data_load_variables(1, "schema", mock_spark, "url", "db", "user", "pwd")


# Test case: Exception during contract lookup
@patch("src.tp_utils.common.load_cntrt_lkp", side_effect=Exception("DB error"))
def test_lookup_exception(mock_load_lkp):
    mock_spark = MagicMock()
    with pytest.raises(Exception, match="DB error"):
        reference_data_load_variables(1, "schema", mock_spark, "url", "db", "user", "pwd")


# Test case: Exception during query execution
@patch("src.tp_utils.common.load_cntrt_lkp")
@patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("Query failed"))
def test_query_exception(mock_read_query, mock_load_lkp):
    mock_spark = MagicMock()
    mock_row = MagicMock()
    mock_row.cntrt_code = "C123"
    mock_row.srce_sys_id = "SYS1"
    mock_row.time_perd_type_code = "MONTHLY"

    mock_df_lkp = MagicMock()
    mock_df_lkp.collect.return_value = [mock_row]
    mock_load_lkp.return_value = mock_df_lkp

    with pytest.raises(Exception, match="Query failed"):
        reference_data_load_variables(1, "schema", mock_spark, "url", "db", "user", "pwd")
