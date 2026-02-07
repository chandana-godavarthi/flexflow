import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import merge_tbl  # Adjust import path if needed

def test_merge_tbl_executes_sql_and_logs():
    # Setup
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    catalog_name = "test_catalog"
    tgt_table = "target_table"
    merge_expr = "tgt.id = src.id"

    expected_sql = f"""
            MERGE INTO {catalog_name}.{tgt_table} tgt
            USING df_new src
            ON {merge_expr}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            """

    with patch("src.tp_utils.common.get_logger", return_value=mock_logger) as mock_get_logger:
        merge_tbl(mock_df, catalog_name, tgt_table, merge_expr, mock_spark)

        # Assertions
        mock_df.createOrReplaceTempView.assert_called_once_with("df_new")
        mock_spark.sql.assert_called_once_with(expected_sql)
        mock_logger.info.assert_called_once_with(f"Successfully merged the dataframe into {tgt_table}")
        mock_get_logger.assert_called_once()