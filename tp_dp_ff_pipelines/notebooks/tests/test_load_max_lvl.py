import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

# Scenario 1: Successful query execution
def test_load_max_lvl_success():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        result_df = common.load_max_lvl(
            categ_id="CAT123",
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0]
        assert "public.mm_categ_strct_assoc" in query
        assert "WHERE csa.categ_id = 'CAT123'" in query
        assert "AND csa.STRCT_NUM = '1'" in query
        assert "AND al.attr_name = 'ITEM'" in query
        assert result_df == mock_df

# Scenario 2: Custom schema name
def test_load_max_lvl_custom_schema():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df) as mock_read:
        common.load_max_lvl(
            categ_id="CAT456",
            postgres_schema="analytics",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )

        query = mock_read.call_args[0][0]
        assert "analytics.mm_categ_strct_assoc" in query
        assert "WHERE csa.categ_id = 'CAT456'" in query

# Scenario 3: Exception during query execution
def test_load_max_lvl_query_failure():
    with patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            common.load_max_lvl(
                categ_id="CAT789",
                postgres_schema="public",
                spark=MagicMock(name="SparkSession"),
                ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
                ref_db_name="refdb",
                ref_db_user="user",
                ref_db_pwd="password"
            )

# Scenario 4: Ensure returned object is a DataFrame
def test_load_max_lvl_returns_dataframe():
    mock_df = MagicMock(name="DataFrame")
    with patch("src.tp_utils.common.read_query_from_postgres", return_value=mock_df):
        result = common.load_max_lvl(
            categ_id="CAT000",
            postgres_schema="public",
            spark=MagicMock(name="SparkSession"),
            ref_db_jdbc_url="jdbc:postgresql://localhost:5432/refdb",
            ref_db_name="refdb",
            ref_db_user="user",
            ref_db_pwd="password"
        )
        assert hasattr(result, "select")  # crude check for DataFrame-like object