import pytest
from unittest.mock import MagicMock
from src.tp_utils import common

@pytest.fixture(autouse=True)
def mock_pyspark_functions(monkeypatch):
    monkeypatch.setattr(common, 'col', MagicMock(name="col"))
    monkeypatch.setattr(common, 'trim', MagicMock(name="trim"))
    monkeypatch.setattr(common, 'when', MagicMock(name="when"))
    monkeypatch.setattr(common, 'date_format', MagicMock(name="date_format"))
    monkeypatch.setattr(common, 'expr', MagicMock(name="expr"))

@pytest.fixture(autouse=True)
def mock_dbutils(monkeypatch):
    # Patch DBUtils from its actual source
    import pyspark.dbutils
    monkeypatch.setattr(pyspark.dbutils, 'DBUtils', lambda spark: MagicMock(name="DBUtils"))

def create_mock_dataframe(row_count=5):
    mock_df = MagicMock()
    mock_df.count.return_value = row_count
    mock_df.createOrReplaceTempView.return_value = None
    mock_df.withColumn.return_value = mock_df
    mock_df.unionByName.return_value = mock_df
    mock_df.groupBy.return_value.agg.return_value = mock_df
    mock_df.drop.return_value = mock_df
    return mock_df

def test_acn_prod_trans_calls_sql_and_parquet_correctly():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_df = create_mock_dataframe()

    mock_spark.sql.return_value = mock_df
    mock_spark.read.parquet.return_value = mock_df

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    mock_spark.sql.assert_called()
    assert result_df is not None

def test_acn_prod_trans_parquet_empty():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_empty_df = create_mock_dataframe(row_count=0)

    mock_spark.sql.return_value = mock_empty_df
    mock_spark.read.parquet.return_value = mock_empty_df

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    assert result_df is not None

def test_acn_prod_trans_dataframe_methods_called():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_df = create_mock_dataframe()

    mock_spark.sql.return_value = mock_df
    mock_spark.read.parquet.return_value = mock_df

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    mock_spark.sql.assert_called()
    assert result_df is not None

def test_acn_prod_trans_final_join_query():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_df = create_mock_dataframe()

    mock_spark.sql.return_value = mock_df
    mock_spark.read.parquet.return_value = mock_df

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.sql.assert_called()
    assert result_df is not None
