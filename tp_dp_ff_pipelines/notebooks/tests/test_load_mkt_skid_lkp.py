import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

# Scenario 1: Successful query execution
def test_load_mkt_skid_lkp_success():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        result_df = common.load_mkt_skid_lkp(
            cntry_id="IN",
            vendr_id=101,
            srce_sys_id=202,
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0]
        assert "public.mm_mkt_skid_lkp" in query
        assert "srce_sys_id=202" in query
        assert "vendr_id=101" in query
        assert "cntry_id='IN'" in query
        assert result_df == mock_df

# Scenario 2: Custom schema name
def test_load_mkt_skid_lkp_custom_schema():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        common.load_mkt_skid_lkp(
            cntry_id="US",
            vendr_id=303,
            srce_sys_id=404,
            postgres_schema="analytics",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0]
        assert "analytics.mm_mkt_skid_lkp" in query
        assert "srce_sys_id=404" in query
        assert "vendr_id=303" in query
        assert "cntry_id='US'" in query

# Scenario 3: Exception during query execution
def test_load_mkt_skid_lkp_query_failure():
    with patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            common.load_mkt_skid_lkp(
                cntry_id="FR",
                vendr_id=505,
                srce_sys_id=606,
                postgres_schema="public",
                spark=MagicMock(name="SparkSession"),
                ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
                ref_db_name="refdb",
                ref_db_user="user",
                ref_db_pwd="password"
            )

# Scenario 4: Ensure returned object is a DataFrame
def test_load_mkt_skid_lkp_returns_dataframe():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df):
        result = common.load_mkt_skid_lkp(
            cntry_id="BR",
            vendr_id=707,
            srce_sys_id=808,
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )
        assert hasattr(result, "select")  # crude check for DataFrame-like object