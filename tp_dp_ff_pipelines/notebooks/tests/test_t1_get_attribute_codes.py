import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import t1_get_attribute_codes  # 🔧 Update this path as needed

@pytest.fixture
def spark():
    return MagicMock()

@pytest.fixture
def df_srce_mprod():
    return MagicMock()

@pytest.fixture
def mock_postgres_df():
    df = MagicMock()
    df.filter.return_value = df
    df.withColumnRenamed.return_value = df
    df.withColumn.return_value = df
    df.cast.return_value = df
    df.show.return_value = None
    df.createOrReplaceTempView.return_value = None
    df.count.return_value = 10
    df.collect.return_value = [{"column_name": "attr_color"}]
    return df

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_successful_execution(mock_read_query, mock_get_logger, spark, df_srce_mprod, mock_postgres_df):
    mock_read_query.return_value = mock_postgres_df
    mock_sql_df = MagicMock()
    mock_sql_df.count.return_value = 10
    spark.sql.return_value = mock_sql_df

    df_gan, df_nactc = t1_get_attribute_codes(
        df_srce_mprod, "ref_schema", spark,
        "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
    )

    assert df_gan is not None
    assert df_nactc is not None
    assert df_gan.count() == 10

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_query_from_postgres", side_effect=Exception("Postgres read failed"))
def test_postgres_read_failure(mock_read_query, mock_get_logger, spark, df_srce_mprod):
    with pytest.raises(Exception, match="Postgres read failed"):
        t1_get_attribute_codes(
            df_srce_mprod, "ref_schema", spark,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd"
        )

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_query_from_postgres")
def test_dynamic_sql_failure(mock_read_query, mock_get_logger, spark, df_srce_mprod, mock_postgres_df):
    mock_read_query.return_value = mock_postgres_df
    spark.sql.side_effect = [MagicMock(), Exception("SQL execution failed")]

    with pytest.raises(Exception, match="SQL execution failed"):
        t1_get_attribute_codes(
            df_srce_mprod, "ref_schema", spark,
            "jdbc:postgresql://localhost:5432/refdb", "refdb", "user", "pwd")
