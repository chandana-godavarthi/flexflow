import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.tp_utils.common import t1_add_row_change_description  # Replace with actual module name

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.tp_utils.common import t1_add_row_change_description  # Adjust import as needed

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

# Common schemas
nactc_schema = StructType([
    StructField("extrn_prod_id", StringType(), True),
    StructField("prod_match_attr_list", StringType(), True),
    StructField("extrn_prod_attr_val_list", StringType(), True),
    StructField("prod_lvl_name", StringType(), True),
    StructField("extrn_prod_name", StringType(), True),
    StructField("row_chng_desc", StringType(), True)
])

csdim_schema = StructType([
    StructField("extrn_prod_id", StringType(), True),
    StructField("prod_match_attr_list", StringType(), True),
    StructField("extrn_prod_attr_val_list", StringType(), True),
    StructField("prod_name", StringType(), True),
    StructField("prod_skid", StringType(), True)
])

@patch("src.tp_utils.common.get_logger")
def test_row_chng_desc_already_present(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_nactc = spark.createDataFrame([
        ("prod1", "MATCH_ATTR", "ATTR_VAL", "ITEM", "Some Name", "EXISTING DESC")
    ], schema=nactc_schema)

    df_mm_prod_csdim = spark.createDataFrame([], schema=csdim_schema)

    result_df = t1_add_row_change_description(df_prod_nactc, df_mm_prod_csdim, spark)
    assert result_df.collect()[0]["row_chng_desc"] == "EXISTING DESC"

@patch("src.tp_utils.common.get_logger")
def test_new_product(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_nactc = spark.createDataFrame([
        ("prod2", "MATCH_ATTR", "ATTR_VAL", "ITEM", "New Name", None)
    ], schema=nactc_schema)

    df_mm_prod_csdim = spark.createDataFrame([], schema=csdim_schema)

    result_df = t1_add_row_change_description(df_prod_nactc, df_mm_prod_csdim, spark)
    assert result_df.collect()[0]["row_chng_desc"] == "NEW PRODUCT"

@patch("src.tp_utils.common.get_logger")
def test_item_name_change(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_nactc = spark.createDataFrame([
        ("prod3", "MATCH_ATTR", "ATTR_VAL", "ITEM", "Old Name", None)
    ], schema=nactc_schema)

    df_mm_prod_csdim = spark.createDataFrame([
        ("prod3", "MATCH_ATTR", "ATTR_VAL", "New Name", "SKID123")
    ], schema=csdim_schema)

    result_df = t1_add_row_change_description(df_prod_nactc, df_mm_prod_csdim, spark)
    assert result_df.collect()[0]["row_chng_desc"] == "ITEM NAME CHANGE"

@patch("src.tp_utils.common.get_logger")
def test_full_match(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_nactc = spark.createDataFrame([
        ("prod4", "MATCH_ATTR", "ATTR_VAL", "ITEM", "Same Name", None)
    ], schema=nactc_schema)

    df_mm_prod_csdim = spark.createDataFrame([
        ("prod4", "MATCH_ATTR", "ATTR_VAL", "Same Name", "SKID456")
    ], schema=csdim_schema)

    result_df = t1_add_row_change_description(df_prod_nactc, df_mm_prod_csdim, spark)
    assert result_df.collect()[0]["row_chng_desc"] == "FULL MATCH"