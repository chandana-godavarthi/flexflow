from unittest.mock import MagicMock, call
from src.tp_utils import common  # adjust to your actual module name

def test_write_to_postgres_calls_write_correctly():
    # Mock DataFrame
    mock_df = MagicMock()
    mock_write = MagicMock()

    mock_df.write = mock_write
    mock_format = mock_write.format.return_value
    # Ensure fluent chaining: every .option() and .mode() returns same mock_format
    mock_format.option.return_value = mock_format
    mock_format.mode.return_value = mock_format

    # Test inputs
    object_name = "test_table"
    ref_db_jdbc_url = "jdbc:postgresql://localhost:5432"
    ref_db_name = "testdb"
    ref_db_user = "testuser"
    ref_db_pwd = "testpwd"

    # Call the function under test
    common.write_to_postgres(
        mock_df, object_name,  
        ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd
    )

    # Assert .format("jdbc") called once
    mock_write.format.assert_called_once_with("jdbc")

    # Expected chain of option() calls in order, followed by mode("append") and save()
    expected_calls = [
        call.option("url", f"{ref_db_jdbc_url}/{ref_db_name}"),
        call.option("dbtable", f"{object_name}"),
        call.option("user", ref_db_user),
        call.option("password", ref_db_pwd),
        call.mode("append"),
        call.save()
    ]

    # Assert call chain in exact order
    mock_format.assert_has_calls(expected_calls)