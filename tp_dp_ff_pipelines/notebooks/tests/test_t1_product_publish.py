import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common

# Fixtures
@pytest.fixture
def mock_spark():
    spark = MagicMock(name="SparkSession")
    df_mock = MagicMock(name="DataFrame")
    spark.read.parquet.return_value = df_mock
    spark.sql.return_value = df_mock
    df_mock.count.return_value = 10
    df_mock.createOrReplaceTempView.return_value = None
    df_mock.show.return_value = None
    return spark, df_mock

@pytest.fixture
def mock_logger(monkeypatch):
    logger = MagicMock(name="Logger")
    monkeypatch.setattr(common, "get_logger", lambda: logger)
    return logger

@pytest.fixture
def mock_semaphore(monkeypatch):
    monkeypatch.setattr(common, "semaphore_acquisition", lambda run_id, path, catalog, spark: path[0])
    monkeypatch.setattr(common, "release_semaphore", MagicMock(name="release_semaphore"))

@pytest.fixture(autouse=True)
def mock_dbutils():
    # Patch DBUtils at its actual import path
    with patch("pyspark.dbutils.DBUtils", MagicMock()) as dbutils_cls:
        dbutils_instance = MagicMock(name="DBUtilsInstance")
        dbutils_cls.return_value = dbutils_instance
        yield dbutils_instance

# Test Scenarios

def test_t1_product_publish_success(mock_spark, mock_logger, mock_semaphore):
    spark, df_mock = mock_spark
    result = common.t1_product_publish(
        cntrt_id="C123",
        run_id="R456",
        srce_sys_id="SYS789",
        spark=spark,
        catalog_name="test_catalog"
    )

    spark.read.parquet.assert_called_once()
    df_mock.createOrReplaceTempView.assert_called_once_with("input")
    spark.sql.assert_called_once()
    df_mock.show.assert_called_once()
    common.release_semaphore.assert_called_once()

def test_t1_product_publish_merge_query(mock_spark, mock_logger, mock_semaphore):
    spark, df_mock = mock_spark
    common.t1_product_publish(
        cntrt_id="C123",
        run_id="R456",
        srce_sys_id="SYS789",
        spark=spark,
        catalog_name="test_catalog"
    )

    merge_query = spark.sql.call_args[0][0]
    assert "MERGE INTO test_catalog.internal_tp.tp_prod_sdim" in merge_query
    assert "USING input temp" in merge_query
    assert "UPDATE SET *" in merge_query
    assert "INSERT *" in merge_query
def test_t1_product_publish_semaphore_handling(mock_spark, mock_logger, mock_semaphore):
    spark, df_mock = mock_spark
    common.t1_product_publish(
        cntrt_id="C123",
        run_id="R456",
        srce_sys_id="SYS789",
        spark=spark,
        catalog_name="test_catalog"
    )

    common.release_semaphore.assert_called_once()
    assert mock_logger.info.call_count > 0

def test_t1_product_publish_merge_exception(mock_spark, mock_logger, mock_semaphore):
    spark, df_mock = mock_spark
    spark.sql.side_effect = Exception("Merge failed")

    with pytest.raises(Exception, match="Merge failed"):
        common.t1_product_publish(
            cntrt_id="C123",
            run_id="R456",
            srce_sys_id="SYS789",
            spark=spark,
            catalog_name="test_catalog"
        )

    common.release_semaphore.assert_called_once()
