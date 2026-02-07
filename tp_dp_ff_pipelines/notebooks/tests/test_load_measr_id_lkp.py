import pytest
from unittest.mock import MagicMock, patch, ANY
from src.tp_utils import common

# Scenario 1: Successful query execution
def test_load_measr_id_lkp_success():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        result_df = common.load_measr_id_lkp(
            vendr_id="V123",
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        mock_read.assert_called_once()
        actual_query = mock_read.call_args[0][0].strip()
        expected_query = "SELECT * FROM public.MM_MEASR_ID_LKP WHERE vendr_id='V123'"
        assert actual_query == expected_query
        assert result_df == mock_df

# Scenario 2: Custom schema name
def test_load_measr_id_lkp_custom_schema():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        common.load_measr_id_lkp(
            vendr_id="V456",
            postgres_schema="analytics",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0].strip()
        assert "analytics.MM_MEASR_ID_LKP" in query
        assert "vendr_id='V456'" in query

# Scenario 3: Exception during query execution
def test_load_measr_id_lkp_query_failure():
    with patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            common.load_measr_id_lkp(
                vendr_id="V789",
                postgres_schema="public",
                spark=MagicMock(name="SparkSession"),
                ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
                ref_db_name="refdb",
                ref_db_user="user",
                ref_db_pwd="password"
            )

# Scenario 4: Ensure returned object is a DataFrame
def test_load_measr_id_lkp_returns_dataframe():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df):
        result = common.load_measr_id_lkp(
            vendr_id="V000",
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )
        assert hasattr(result, "select")  # crude check for DataFrame-like object
