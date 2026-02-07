import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from src.tp_utils.common import tier1_time_map

# Mock Spark session fixture
@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read = MagicMock()
    spark.createDataFrame = MagicMock()
    return spark

# Common mocked config
@pytest.fixture
def mock_config():
    return {
        "vendr_id": "mock_vendor",
        "catalog_name": "mock_catalog",
        "postgres_schema": "mock_schema",
        "ref_db_jdbc_url": "jdbc:mock",
        "ref_db_name": "mock_db",
        "ref_db_user": "mock_user",
        "ref_db_pwd": "mock_pwd",
        "run_id": "mock_run_001"
    }

# Patch DBUtils globally for all tests
@pytest.fixture(autouse=True)
def mock_dbutils(monkeypatch):
    import pyspark.dbutils
    monkeypatch.setattr(pyspark.dbutils, "DBUtils", lambda spark: MagicMock(name="DBUtils"))

# Scenario 1: Successful join and materialization
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.materialize")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_tier1_time_map_success(mock_read_query, mock_materialize, mock_get_logger, mock_spark, mock_config):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_lookup = MagicMock(spec=DataFrame)
    df_source = MagicMock(spec=DataFrame)

    mock_read_query.return_value = df_lookup
    mock_spark.read.parquet.return_value = df_source
    df_source.join.return_value = df_source

    tier1_time_map(**mock_config, spark=mock_spark)

    mock_logger.info.assert_any_call("[tier1_time_map] Function execution started.")
    mock_materialize.assert_called_once()

# Scenario 2: Empty lookup table
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.materialize")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_tier1_time_map_empty_lookup(mock_read_query, mock_materialize, mock_get_logger, mock_spark, mock_config):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_lookup = MagicMock(spec=DataFrame)
    df_lookup.count.return_value = 0
    df_source = MagicMock(spec=DataFrame)

    mock_read_query.return_value = df_lookup
    mock_spark.read.parquet.return_value = df_source
    df_source.join.return_value = df_source

    tier1_time_map(**mock_config, spark=mock_spark)

    mock_materialize.assert_called_once()

# Scenario 3: Mismatched join keys
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.materialize")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_tier1_time_map_mismatched_join(mock_read_query, mock_materialize, mock_get_logger, mock_spark, mock_config):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_lookup = MagicMock(spec=DataFrame)
    df_source = MagicMock(spec=DataFrame)
    df_joined = MagicMock(spec=DataFrame)

    mock_read_query.return_value = df_lookup
    mock_spark.read.parquet.return_value = df_source
    df_source.join.return_value = df_joined

    tier1_time_map(**mock_config, spark=mock_spark)

    mock_materialize.assert_called_once()

# Scenario 4: No source data
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.materialize")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_tier1_time_map_no_srce_mtime(mock_read_query, mock_materialize, mock_get_logger, mock_spark, mock_config):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_lookup = MagicMock(spec=DataFrame)
    df_source = MagicMock(spec=DataFrame)
    df_source.count.return_value = 0
    df_source.join.return_value = df_source
    mock_read_query.return_value = df_lookup
    mock_spark.read.parquet.return_value = df_source

    tier1_time_map(**mock_config, spark=mock_spark)

    mock_materialize.assert_called_once()
