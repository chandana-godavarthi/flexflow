import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import merge_into_tp_data_vldtn_rprt_tbl

# ✅ Patch DBUtils globally to avoid remote Spark session error
@pytest.fixture(autouse=True)
def patch_dbutils(monkeypatch):
    mock_dbutils = MagicMock()
    mock_dbutils.secrets.get.return_value = "host"
    monkeypatch.setattr("pyspark.dbutils.DBUtils", lambda spark: mock_dbutils)

@pytest.fixture
def mock_data():
    return [
        {"run_id": 1, "valdn_name": "Check1", "result": "PASSED", "details": "OK", "validation_type": "TypeA", "validation_path": "/path"},
        {"run_id": 1, "valdn_name": "Check2", "result": "FAILED", "details": "Issue", "validation_type": "TypeB", "validation_path": "/path"}
    ]

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.withColumn.return_value = df
    df.orderBy.return_value = df
    df.createOrReplaceTempView.return_value = None
    df.show.return_value = None
    df.filter.return_value.count.return_value = 1
    df.write.mode.return_value.saveAsTable.return_value = None
    return df

@patch("src.tp_utils.common.get_logger", return_value=MagicMock())
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.write_to_postgres")
@patch("src.tp_utils.common.update_to_postgres")
@patch("src.tp_utils.common.current_timestamp", return_value=MagicMock())
def test_merge_insert_new_record(
    mock_timestamp, mock_update, mock_write, mock_read, mock_logger, mock_data, mock_df
):
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_spark.sql.return_value = mock_df
    mock_read.return_value.filter.return_value.count.return_value = 0

    merge_into_tp_data_vldtn_rprt_tbl(
        run_id=1,
        spark=mock_spark,
        data=mock_data,
        valdn_grp_id="grp1",
        catalog_name="catalog",
        postgres_schema="schema",
        ref_db_jdbc_url="url",
        ref_db_name="db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_write.assert_called_once()
    mock_update.assert_not_called()

@patch("src.tp_utils.common.get_logger", return_value=MagicMock())
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.write_to_postgres")
@patch("src.tp_utils.common.update_to_postgres")
@patch("src.tp_utils.common.current_timestamp", return_value=MagicMock())
def test_merge_update_existing_record(
    mock_timestamp, mock_update, mock_write, mock_read, mock_logger, mock_data, mock_df
):
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_spark.sql.return_value = mock_df
    mock_read.return_value.filter.return_value.count.return_value = 1

    merge_into_tp_data_vldtn_rprt_tbl(
        run_id=2,
        spark=mock_spark,
        data=mock_data,
        valdn_grp_id="grp2",
        catalog_name="catalog",
        postgres_schema="schema",
        ref_db_jdbc_url="url",
        ref_db_name="db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_write.assert_not_called()
    mock_update.assert_called_once_with(
        "UPDATE schema.mm_run_valdn_plc SET fail_ind=%s WHERE run_id=%s AND valdn_grp_id=%s",
        ("Y", 2, "grp2"),
        "db",
        "user",
        "pwd",
        "host"  # ✅ matches patched dbutils.secrets.get return value
    )
import re
import pytest
from unittest.mock import patch, MagicMock
from src.tp_utils.common import merge_into_tp_data_vldtn_rprt_tbl

def normalize_sql(sql):
    """Utility to normalize SQL by collapsing whitespace."""
    return re.sub(r'\s+', ' ', sql.strip())

@patch("src.tp_utils.common.get_logger", return_value=MagicMock())
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.write_to_postgres")
@patch("src.tp_utils.common.update_to_postgres")
@patch("src.tp_utils.common.current_timestamp", return_value=MagicMock())
def test_merge_sql_execution(
    mock_timestamp, mock_update, mock_write, mock_read, mock_logger
):
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.orderBy.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_df.show.return_value = None
    mock_df.filter.return_value.count.return_value = 1
    mock_spark.createDataFrame.return_value = mock_df
    mock_spark.sql.return_value = mock_df
    mock_read.return_value.filter.return_value.count.return_value = 0

    run_id = 3
    mock_data = [
        {"run_id": 3, "valdn_name": "Check1", "result": "PASSED", "details": "OK", "validation_type": "TypeA", "validation_path": "/path"},
        {"run_id": 3, "valdn_name": "Check2", "result": "FAILED", "details": "Issue", "validation_type": "TypeB", "validation_path": "/path"}
    ]

    merge_into_tp_data_vldtn_rprt_tbl(
        run_id=run_id,
        spark=mock_spark,
        data=mock_data,
        valdn_grp_id="grp3",
        catalog_name="catalog",
        postgres_schema="schema",
        ref_db_jdbc_url="url",
        ref_db_name="db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    actual_sql = normalize_sql(mock_spark.sql.call_args[0][0])
    expected_sql = normalize_sql(f"""
        MERGE INTO catalog.internal_tp.tp_valdn_rprt tgt
        USING src_view src
        ON tgt.run_id={run_id} AND src.valdn_name= tgt.valdn_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    assert actual_sql == expected_sql, f"Expected SQL:\n{expected_sql}\n\nActual SQL:\n{actual_sql}"
