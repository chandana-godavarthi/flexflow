import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import update_to_postgres

# ✅ Fixture for valid DB config
@pytest.fixture
def db_config():
    return {
        "query": "UPDATE table SET col = %s WHERE id = %s",
        "params": ("value", 1),
        "ref_db_name": "test_db",
        "ref_db_user": "test_user",
        "ref_db_pwd": "test_pwd",
        "ref_db_hostname": "test_host"
    }

# ✅ Test successful update
@patch("src.tp_utils.common.psycopg2.connect")
def test_update_to_postgres_success(mock_connect, db_config):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    update_to_postgres(**db_config)

    mock_connect.assert_called_once_with(
        dbname="test_db",
        user="test_user",
        password="test_pwd",
        host="test_host",
        sslmode='require'
    )
    mock_cursor.execute.assert_called_once_with(db_config["query"], db_config["params"])
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

# ✅ Test connection failure
@patch("src.tp_utils.common.psycopg2.connect")
def test_update_to_postgres_connection_failure(mock_connect, db_config):
    mock_connect.side_effect = Exception("Connection failed")

    with pytest.raises(Exception, match="Connection failed"):
        update_to_postgres(**db_config)

# ✅ Test query execution failure
@patch("src.tp_utils.common.psycopg2.connect")
def test_update_to_postgres_execute_failure(mock_connect, db_config):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Execution failed")

    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    with pytest.raises(Exception, match="Execution failed"):
        update_to_postgres(**db_config)
