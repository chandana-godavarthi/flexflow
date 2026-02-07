import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import derive_fact_non_sff

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.np")
@patch("src.tp_utils.common.Window")
@patch("src.tp_utils.common.nth_value")
@patch("src.tp_utils.common.expr")
@patch("src.tp_utils.common.lit")
@patch("src.tp_utils.common.col")
@patch("src.tp_utils.common.DecimalType")
def test_derive_fact_non_sff_basic(
    mock_decimal_type, mock_col, mock_lit, mock_expr, mock_nth_value, mock_window, mock_np, mock_get_logger
):
    # Setup logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Setup Spark and input DataFrames
    mock_spark = MagicMock()
    mock_df_mfct = MagicMock()
    mock_df_mmeasr = MagicMock()

    # SQL result mock
    mock_df_sql_result = MagicMock()
    mock_spark.sql.return_value = mock_df_sql_result

    # Columns in SQL result
    mock_df_sql_result.columns = [f"fact_amt_{i}" for i in range(1, 6)] + [
        "line_num", "mkt_extrn_code", "dummy_separator_1", "prod_extrn_code",
        "time_extrn_code", "dummy_separator_2", "mkt_extrn_code2", "dummy_separator_3", "other_fct_data"
    ]

    # Mock missing measures
    mock_df_mmeasr.filter.return_value.select.return_value.collect.return_value = []

    # Chain all withColumn calls to return the same mock
    mock_df_sql_result.withColumn.side_effect = lambda *args, **kwargs: mock_df_sql_result
    mock_df_sql_result.filter.return_value = mock_df_sql_result
    mock_df_sql_result.na.replace.return_value = mock_df_sql_result

    # Add enough fact_amt columns to simulate missing ones
    mock_df_sql_result.columns += [f"fact_amt_{i}" for i in range(6, 101)]

    # Final result
    result = derive_fact_non_sff(
        df_srce_mfct=mock_df_mfct,
        df_srce_mmeasr=mock_df_mmeasr,
        run_id="run123",
        cntrt_id=456,
        srce_sys_id=789,
        spark=mock_spark
    )

    # Assertions
    mock_df_mfct.createOrReplaceTempView.assert_called_once()
    mock_df_mmeasr.createOrReplaceTempView.assert_called_once()
    mock_spark.sql.assert_called()
    assert result == mock_df_sql_result
