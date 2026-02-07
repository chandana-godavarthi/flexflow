import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import run_prttn_dtls

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = "mocked_result_df"
    return spark

@pytest.fixture
def mock_read_from_postgres():
    with patch("src.tp_utils.common.read_from_postgres") as mock_func:
        yield mock_func

def test_run_prttn_dtls_success(mock_spark, mock_read_from_postgres):
    # Setup mock DataFrame
    mock_df = MagicMock()
    mock_df.filter.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_read_from_postgres.return_value = mock_df

    cntrt_id = 123
    catalog_name = "test_catalog"
    postgres_schema = "test_schema"

    expected_sql = f"""
        SELECT 
            plc.time_perd_class_code,
            plc.srce_sys_id,
            plc.cntrt_id,
            plc.mm_time_perd_end_date,
            plc.run_id,
            run.run_sttus_name
        FROM 
            {catalog_name}.gold_tp.tp_run_prttn_plc plc
        JOIN 
            mm_run_plc run ON run.run_id = plc.run_id
        
        WHERE 
                plc.cntrt_id = {cntrt_id}

        """

    result = run_prttn_dtls(
        cntrt_id=cntrt_id,
        catalog_name=catalog_name,
        postgres_schema=postgres_schema,
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="test_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    mock_read_from_postgres.assert_called_once_with(
        f"{postgres_schema}.mm_run_plc",
        mock_spark,
        "jdbc:test",
        "test_db",
        "user",
        "pwd"
    )
    mock_df.filter.assert_called_once_with(f"cntrt_id={cntrt_id}")
    mock_df.createOrReplaceTempView.assert_called_once_with("mm_run_plc")
    mock_spark.sql.assert_called_once_with(expected_sql)
    assert result == "mocked_result_df"