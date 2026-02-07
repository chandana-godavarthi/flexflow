import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from databricks.connect import DatabricksSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.tp_utils.common import extract_product_sff

@pytest.fixture(scope="module")
def spark():
    return DatabricksSession.builder.getOrCreate()

def create_df_raw1(spark, rows):
    schema = StructType([
        StructField("TAG", StringType(), True),
        StructField("SHORT", StringType(), True),
        StructField("LONG", StringType(), True),
        StructField("DISPLAY_ORDER", IntegerType(), True)
    ])
    return spark.createDataFrame(rows, schema)

def create_df_max_lvl(spark, max_lvl):
    schema = StructType([StructField("max_lvl", IntegerType(), True)])
    return spark.createDataFrame([(max_lvl,)], schema)

@patch("src.tp_utils.common.get_logger")
def test_valid_input(mock_logger, spark):
    mock_logger.return_value = MagicMock()
    row = ("P123", "ShortName", "A B C D E F G H I J K", 1)
    df_raw1 = create_df_raw1(spark, [row])
    df_max_lvl = create_df_max_lvl(spark, 10)

    result_df = extract_product_sff(df_raw1, 1, 2, 3, 4, df_max_lvl)
    assert result_df.count() == 1
    assert "prod_match_attr_list" in result_df.columns

@patch("src.tp_utils.common.get_logger")
def test_empty_input(mock_logger, spark):
    mock_logger.return_value = MagicMock()
    df_raw1 = create_df_raw1(spark, [])
    df_max_lvl = create_df_max_lvl(spark, 5)

    result_df = extract_product_sff(df_raw1, 1, 2, 3, 4, df_max_lvl)
    assert result_df.count() == 0

@patch("src.tp_utils.common.get_logger")
def test_missing_columns(mock_logger, spark):
    mock_logger.return_value = MagicMock()
    schema = StructType([StructField("TAG", StringType(), True)])
    df_raw1 = spark.createDataFrame([("P123",)], schema)
    df_max_lvl = create_df_max_lvl(spark, 5)

    with pytest.raises(Exception):
        extract_product_sff(df_raw1, 1, 2, 3, 4, df_max_lvl)

@patch("src.tp_utils.common.get_logger")
def test_lvl_num_equals_max_lvl(mock_logger, spark):
    mock_logger.return_value = MagicMock()
    row = ("P123", "ShortName", "A B C D E F G H I J K", 1)
    df_raw1 = create_df_raw1(spark, [row])
    df_max_lvl = create_df_max_lvl(spark, 10)

    result_df = extract_product_sff(df_raw1, 1, 2, 3, 4, df_max_lvl)
    row = result_df.first()
    assert row is not None
    assert "prod_match_attr_list" in result_df.columns

@patch("src.tp_utils.common.get_logger")
def test_lvl_num_not_equals_max_lvl(mock_logger, spark):
    mock_logger.return_value = MagicMock()
    row = ("P123", "ShortName", "A B C", 1)
    df_raw1 = create_df_raw1(spark, [row])
    df_max_lvl = create_df_max_lvl(spark, 1)

    result_df = extract_product_sff(df_raw1, 1, 2, 3, 4, df_max_lvl)
    row = result_df.first()
    assert row is not None
    assert row["prod_match_attr_list"] == "A B C"
