from unittest.mock import MagicMock, call
from src.tp_utils import common  # adjust if your module is differently named

def test_read_query_from_postgres_calls_spark_read_correctly():
    # Mock SparkSession and its methods
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_read = MagicMock()

    mock_spark.read = mock_read
    mock_format = mock_read.format.return_value
    # Ensure fluent chaining: every .option() returns same mock_format
    mock_format.option.return_value = mock_format
    mock_format.load.return_value = mock_df

    # Test inputs
    query = "SELECT * FROM test_table"
    ref_db_jdbc_url = "jdbc:postgresql://localhost:5432"
    ref_db_name = "testdb"
    ref_db_user = "testuser"
    ref_db_pwd = "testpwd"

    # Call the function under test
    result_df = common.read_query_from_postgres(
        query, mock_spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd
    )

    # Assert .format("jdbc") called once
    mock_read.format.assert_called_once_with("jdbc")

    # Expected chain of option() calls followed by load()
    expected_calls = [
        call.option("driver", "org.postgresql.Driver"),
        call.option("url", f"{ref_db_jdbc_url}/{ref_db_name}"),
        call.option("query", query),
        call.option("user", ref_db_user),
        call.option("password", ref_db_pwd),
        call.option("ssl", True),
        call.option("sslmode", "require"),
        call.option("sslfactory", "org.postgresql.ssl.NonValidatingFactory"),
        call.load()
    ]

    # Assert call chain in correct order
    mock_format.assert_has_calls(expected_calls)

    # Assert returned DataFrame matches the mock load() return
    assert result_df == mock_df
