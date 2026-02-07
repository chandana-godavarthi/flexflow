import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.tp_utils.common import derive_product_assoc  # Replace with actual module name

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

@patch("src.tp_utils.common.get_logger")
def test_derive_product_assoc_basic(mock_get_logger, spark):
    # Mock logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Sample input data
    df_prod_dim = spark.createDataFrame([
        (1, "A B C", 101, 1001, 1,"secure1"),
        (1, "A B", 102, 1002, 2,"secure2")
    ], ["srce_sys_id", "prod_match_attr_list", "prod_skid", "prod_lvl_id", "run_id","secure_group_key"])

    df_strct_lkp = spark.createDataFrame([
        (1001, 2 ),
        (1002, 1 )
    ], ["strct_lvl_id", "lvl_num"])

    # Run function
    result_df = derive_product_assoc(df_prod_dim, df_strct_lkp, spark)

    # Assertions
    assert result_df is not None
    assert "child_prod_match_attr_list" in result_df.columns
    assert "parnt_prod_match_attr_list" in result_df.columns
    assert "secure_group_key" in result_df.columns
    mock_logger.info.assert_called_with("Started deriving product assoc")

@patch("src.tp_utils.common.get_logger")
def test_derive_product_assoc_empty_input(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_dim = spark.createDataFrame([], schema="srce_sys_id INT, prod_match_attr_list STRING, prod_skid INT, prod_lvl_id INT, run_id INT,secure_group_key STRING")
    df_strct_lkp = spark.createDataFrame([], schema="strct_lvl_id INT, lvl_num INT")

    result_df = derive_product_assoc(df_prod_dim, df_strct_lkp, spark)

    assert result_df.count() == 0

@patch("src.tp_utils.common.get_logger")
def test_derive_product_assoc_missing_match(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    df_prod_dim = spark.createDataFrame([
        (1, "X Y Z", 201, 3001, 1,"secureX")
    ], ["srce_sys_id", "prod_match_attr_list", "prod_skid", "prod_lvl_id", "run_id","secure_group_key"])

    df_strct_lkp = spark.createDataFrame([
        (3001, 3)
    ], ["strct_lvl_id", "lvl_num"])

    result_df = derive_product_assoc(df_prod_dim, df_strct_lkp, spark)

    # Since parent match doesn't exist, result should be empty
    assert result_df.count() == 1

