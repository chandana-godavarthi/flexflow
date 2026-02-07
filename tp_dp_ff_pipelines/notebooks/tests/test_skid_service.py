import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.tp_utils.common import skid_service

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

@pytest.fixture
def sample_df(spark):
    data = [("cust1", "proj1", "mkt1", "prod1", "cntrt1")]
    columns = ["cust_id", "project_id", "extrn_mkt_id", "extrn_prod_id", "cntrt_id"]
    return spark.createDataFrame(data, columns)

# Scenario 1: Successful execution
@patch("src.tp_utils.common.get_logger")
def test_skid_service_success(mock_get_logger, spark, sample_df):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = ["mock_client_id", "mock_client_secret"]

    mock_skid_instance = MagicMock()
    mock_skid_instance.assign_surrogate_keys.return_value = sample_df

    result_df = skid_service(
        df=sample_df,
        input_col=["cust_id", "project_id"],
        table_name="market_dim",
        data_provider_code="TP_140_MKT",
        spark=spark,
        dbutils=mock_dbutils,
        SkidClientv2=lambda *args, **kwargs: mock_skid_instance,
        tenant_id="tenant123",
        env="dev",
        publisher_name="publisherX",
        application_key="appKey123"
    )

    assert result_df is not None
    assert result_df.columns == sample_df.columns
    assert mock_logger.info.call_count >= 2
    assert any("[skid_service]" in str(call) for call in mock_logger.info.call_args_list)

# Scenario 2: Empty DataFrame input
@patch("src.tp_utils.common.get_logger")
def test_skid_service_empty_df(mock_get_logger, spark):
    empty_df = spark.createDataFrame([], schema="cust_id STRING, project_id STRING")

    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = ["mock_client_id", "mock_client_secret"]

    mock_skid_instance = MagicMock()
    mock_skid_instance.assign_surrogate_keys.return_value = empty_df

    result_df = skid_service(
        df=empty_df,
        input_col=["cust_id", "project_id"],
        table_name="market_dim",
        data_provider_code="TP_140_MKT",
        spark=spark,
        dbutils=mock_dbutils,
        SkidClientv2=lambda *args, **kwargs: mock_skid_instance,
        tenant_id="tenant123",
        env="mock_env",
        publisher_name="publisherX",
        application_key="appKey123"
    )

    assert result_df.count() == 0
    assert any("Skid Assignment completed" in str(call) for call in mock_logger.info.call_args_list)

# Scenario 3: Invalid columns in input DataFrame
@patch("src.tp_utils.common.get_logger")
def test_skid_service_invalid_columns(mock_get_logger, spark):
    df = spark.createDataFrame([("val1",)], ["wrong_col"])

    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = ["mock_client_id", "mock_client_secret"]

    mock_skid_instance = MagicMock()
    mock_skid_instance.assign_surrogate_keys.side_effect = Exception("Invalid columns")

    with pytest.raises(Exception, match="Invalid columns"):
        skid_service(
            df=df,
            input_col=["cust_id", "project_id"],
            table_name="market_dim",
            data_provider_code="TP_140_MKT",
            spark=spark,
            dbutils=mock_dbutils,
            SkidClientv2=lambda *args, **kwargs: mock_skid_instance,
            tenant_id="tenant123",
            env="dev",
            publisher_name="publisherX",
            application_key="appKey123"
        )

# Scenario 4: Secrets not found
@patch("src.tp_utils.common.get_logger")
def test_skid_service_missing_secrets(mock_get_logger, spark, sample_df):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.side_effect = Exception("Secrets not found")

    with pytest.raises(Exception, match="Secrets not found"):
        skid_service(
            df=sample_df,
            input_col=["cust_id", "project_id"],
            table_name="market_dim",
            data_provider_code="TP_140_MKT",
            spark=spark,
            dbutils=mock_dbutils,
            SkidClientv2=MagicMock(),
            tenant_id="tenant123",
            env="dev",
            publisher_name="publisherX",
            application_key="appKey123"
        )