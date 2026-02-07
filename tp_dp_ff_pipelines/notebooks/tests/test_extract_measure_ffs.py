import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from src.tp_utils.common import extract_measure_ffs

# Use Databricks Connect session
@pytest.fixture(scope="module")
def spark():
    return DatabricksSession.builder.getOrCreate()

def test_valid_input(spark):
    data = [Row(value="O1234567890SomeAttributeDataHereSomeDescriptionHere")]
    df_raw = spark.createDataFrame(data)

    result_df = extract_measure_ffs(df_raw)
    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["extrn_code"] == "123456789"  # corrected to match substring logic
    assert "extrn_name" in result[0]

def test_no_vendor_starting_with_O(spark):
    data = [Row(value="X1234567890SomeAttributeDataHereSomeDescriptionHere")]
    df_raw = spark.createDataFrame(data)

    result_df = extract_measure_ffs(df_raw)
    assert result_df.count() == 0

def test_empty_dataframe(spark):
    df_raw = spark.createDataFrame([], schema="value STRING")
    result_df = extract_measure_ffs(df_raw)
    assert result_df.count() == 0

def test_missing_value_column(spark):
    df_raw = spark.createDataFrame([], schema="other_column STRING")
    with pytest.raises(AnalysisException):
        result_df = extract_measure_ffs(df_raw)
        result_df.collect()  # Force evaluation to trigger the error

def test_malformed_value_column(spark):
    data = [Row(value="O1")]  # Too short to extract substrings
    df_raw = spark.createDataFrame(data)

    result_df = extract_measure_ffs(df_raw)
    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["extrn_code"] == "1"  # substring from index 2 to 10
    assert result[0]["extrn_name"] == ""   # substring from index 67 to 80 likely empty