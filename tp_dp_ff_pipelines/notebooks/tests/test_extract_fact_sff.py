from unittest.mock import MagicMock, patch
import pytest
from src.tp_utils import common

@pytest.fixture(autouse=True)
def patch_dbutils(monkeypatch):
    # Patch DBUtils constructor globally
    monkeypatch.setattr("pyspark.dbutils.DBUtils", lambda spark: MagicMock(name="DBUtils"))

@patch("src.tp_utils.common.materialize")
def test_extract_fact_sff_basic_flow(mock_materialize):
    mock_df = MagicMock()
    mock_df.columns = [f"col{i}_c" for i in range(1, 104)]
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.selectExpr.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_df.show.return_value = None
    mock_df.join.return_value = mock_df

    mock_spark = MagicMock()
    mock_spark.read.parquet.return_value = mock_df

    result_df = common.extract_fact_sff(mock_df, "123", "456", "789", mock_spark)

    assert result_df is not None
    mock_materialize.assert_called_once()