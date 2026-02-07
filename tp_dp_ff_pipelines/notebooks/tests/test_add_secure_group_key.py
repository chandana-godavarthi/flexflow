import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from src.tp_utils import common  

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = "final_df"
    return spark

def test_add_secure_group_key_with_contract_match(mock_spark):
    mock_df = MagicMock(spec=DataFrame)
    mock_df.createOrReplaceTempView.return_value = None

    contract_id = "C123"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    contract_sgk = [{"secure_group_key": 42}]
    default_sgk = [{"secure_group_key": 99}]

    with patch("src.tp_utils.common.read_query_from_postgres") as mock_read_query:
        mock_read_query.side_effect = [
            MagicMock(collect=MagicMock(return_value=contract_sgk)),
            MagicMock(collect=MagicMock(return_value=default_sgk))
        ]

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, mock_spark, jdbc_url, dbname, user, pwd
        )

        assert result == "final_df"
        mock_spark.sql.assert_called_once()
        mock_df.createOrReplaceTempView.assert_called_once_with("temp")

def test_add_secure_group_key_with_no_contract_match(mock_spark):
    mock_df = MagicMock(spec=DataFrame)
    mock_df.createOrReplaceTempView.return_value = None

    contract_id = "C999"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    contract_sgk = [{"secure_group_key": None}]
    default_sgk = [{"secure_group_key": 99}]

    with patch("src.tp_utils.common.read_query_from_postgres") as mock_read_query:
        mock_read_query.side_effect = [
            MagicMock(collect=MagicMock(return_value=contract_sgk)),
            MagicMock(collect=MagicMock(return_value=default_sgk))
        ]

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, mock_spark, jdbc_url, dbname, user, pwd
        )

        assert result == "final_df"
        mock_spark.sql.assert_called_once()
        mock_df.createOrReplaceTempView.assert_called_once_with("temp")

def test_add_secure_group_key_empty_contract_result(mock_spark):
    mock_df = MagicMock(spec=DataFrame)
    mock_df.createOrReplaceTempView.return_value = None

    contract_id = "C999"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    contract_sgk = []
    default_sgk = [{"secure_group_key": 88}]

    with patch("src.tp_utils.common.read_query_from_postgres") as mock_read_query:
        mock_read_query.side_effect = [
            MagicMock(collect=MagicMock(return_value=contract_sgk)),
            MagicMock(collect=MagicMock(return_value=default_sgk))
        ]

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, mock_spark, jdbc_url, dbname, user, pwd
        )

        assert result == "final_df"
        mock_spark.sql.assert_called_once()
        mock_df.createOrReplaceTempView.assert_called_once_with("temp")
