import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from src.tp_utils.common import t1_generate_product_description

# ✅ Patch DBUtils globally for all tests
@pytest.fixture(autouse=True)
def mock_dbutils(monkeypatch):
    import pyspark.dbutils
    monkeypatch.setattr(pyspark.dbutils, "DBUtils", lambda spark: MagicMock(name="DBUtils"))

# ✅ Main test case
@patch("src.tp_utils.common.get_dbutils")
@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_query_from_postgres")
@patch("src.tp_utils.common.materialize")
@patch("src.tp_utils.common.column_complementer")
def test_t1_generate_product_description(
    mock_column_complementer,
    mock_materialize,
    mock_read_query,
    mock_get_logger,
    mock_get_dbutils
):
    # Mock logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Mock DBUtils
    mock_dbutils = MagicMock()
    mock_get_dbutils.return_value = mock_dbutils

    # Mock Spark session and its methods
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.createDataFrame.return_value = mock_df
    mock_spark.sql.return_value = mock_df

    # Mock DataFrame methods
    mock_df.select.return_value = mock_df
    mock_df.limit.return_value = mock_df
    mock_df.union.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.join.return_value = mock_df
    mock_df.show.return_value = None
    mock_df.where.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    # Mock collect for prod_lvl_id list and attribute columns
    mock_df.collect.side_effect = [
        [Row(prod_lvl_id=1), Row(prod_lvl_id=2)],  # lst_prod_lvls
        [Row(bus_name_1="attr1,attr2")],           # cols1
        [Row(bus_name_2="attr3,attr4")],           # cols2
        [Row(bus_name_1="attr5,attr6")],           # cols1 for second level
        [Row(bus_name_2="attr7,attr8")]            # cols2 for second level
    ]

    # Mock read_query_from_postgres return values
    mock_read_query.side_effect = [mock_df, mock_df]

    # Mock column_complementer return
    mock_column_complementer.return_value = mock_df

    # Call the function
    t1_generate_product_description(
        df_mm_prod_csdim=mock_df,
        postgres_schema="test_schema",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd",
        run_id="123"
    )

    # ✅ Assertions
    assert mock_get_logger.called
    assert mock_get_dbutils.called
    assert mock_read_query.call_count == 2
    assert mock_materialize.call_count == 4
    assert mock_column_complementer.called
    assert mock_logger.info.call_count > 0
