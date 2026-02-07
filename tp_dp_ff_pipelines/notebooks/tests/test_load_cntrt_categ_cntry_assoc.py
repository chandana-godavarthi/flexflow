import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

# Scenario 1: Successful query execution and result collection
def test_load_cntrt_categ_cntry_assoc_success():
    mock_df = MagicMock(name="DataFrame")
    mock_df.collect.return_value = ["mock_row"]

    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        result = common.load_cntrt_categ_cntry_assoc(
            cntrt_id="C123",
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        assert result == "mock_row"
        query = mock_read.call_args[0][0]
        assert "public.mm_cntrt_lkp" in query
        assert "WHERE c.cntrt_id = 'C123'" in query

# Scenario 2: Custom schema name
def test_load_cntrt_categ_cntry_assoc_custom_schema():
    mock_df = MagicMock(name="DataFrame")
    mock_df.collect.return_value = ["mock_row"]

    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        common.load_cntrt_categ_cntry_assoc(
            cntrt_id="C456",
            postgres_schema="analytics",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0]
        assert "analytics.mm_cntrt_lkp" in query
        assert "WHERE c.cntrt_id = 'C456'" in query

# Scenario 3: Exception during query execution
def test_load_cntrt_categ_cntry_assoc_query_failure():
    with patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            common.load_cntrt_categ_cntry_assoc(
                cntrt_id="C789",
                postgres_schema="public",
                spark=MagicMock(name="SparkSession"),
                ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
                ref_db_name="refdb",
                ref_db_user="user",
                ref_db_pwd="password"
            )