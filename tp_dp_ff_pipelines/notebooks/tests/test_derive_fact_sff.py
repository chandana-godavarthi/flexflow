import sys
import os
import pytest
from unittest.mock import patch, MagicMock

from pyspark.sql import Row
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
from src.tp_utils.common import extract_time_ffs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pytest
from pyspark.sql import SparkSession
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.tp_utils.common import derive_fact_sff
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
 
@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()


@patch("src.tp_utils.common.get_logger")
def test_derive_fact_sff_success(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Create a mock input DataFrame with some non-null fact_amt columns
    data = [("L1", "C1", "S1", "R1", "MKT", "PROD", "TIME", *["1.0"]*100, *["val"]*103)]
    columns = ["line_num", "cntrt_id", "srce_sys_id", "run_id", "mkt_extrn_code", "prod_extrn_code", "time_extrn_code"] + \
              [f"fact_amt_{i}" for i in range(1, 101)] + [f"col{j}" for j in range(1, 104)]
    df_input = spark.createDataFrame(data, columns)

    result_df = derive_fact_sff(df_input, spark)

    assert result_df is not None
    assert "fact_amt_1" in result_df.columns
    mock_logger.info.assert_any_call("[derive_fact_sff] Starting SFF fact derivation")

@patch("src.tp_utils.common.get_logger")
def test_derive_fact_sff_all_null_facts(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
        # Define columns
    columns = ["line_num", "cntrt_id", "srce_sys_id", "run_id", "mkt_extrn_code", "prod_extrn_code", "time_extrn_code"] + \
              [f"fact_amt_{i}" for i in range(1, 101)] + [f"col{j}" for j in range(1, 104)]
    schema = StructType([StructField(col, StringType(), True) for col in columns])
    data = [("L1", "C1", "S1", "R1", "MKT", "PROD", "TIME") + tuple([None]*100) + tuple(["val"]*103)]

    # All fact_amt columns are null

    df_input = spark.createDataFrame(data, schema=schema)
    
    result_df = derive_fact_sff(df_input, spark)

    assert result_df.count() == 0


@patch("src.tp_utils.common.get_logger")
def test_derive_fact_sff_empty_input(mock_get_logger, spark):
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    columns = ["line_num", "cntrt_id", "srce_sys_id", "run_id", "mkt_extrn_code", "prod_extrn_code", "time_extrn_code"] + \
              [f"fact_amt_{i}" for i in range(1, 101)] + [f"col{j}" for j in range(1, 104)]

    schema = StructType([StructField(col, StringType(), True) for col in columns])
    df_input = spark.createDataFrame([], schema=schema)

    result_df = derive_fact_sff(df_input, spark)

    assert result_df.count() == 0