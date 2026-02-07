import pytest
import re
from unittest.mock import MagicMock, call
from src.tp_utils import common 


def normalize_sql(sql):
    """Utility to normalize SQL string for reliable assertions."""
    return re.sub(r"\s+", " ", sql.strip())


def test_t2_publish_product_calls_sql_and_union(monkeypatch):
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    prod_dim = "test_prod_dim"

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_select_df = MagicMock()

    mock_spark.sql.side_effect = [mock_select_df, None]
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    assert mock_spark.sql.call_count == 2

    expected_select_query = f"SELECT * FROM {catalog_name}.{schema_name}.{prod_dim} limit 0"
    mock_spark.sql.assert_has_calls([call(expected_select_query)], any_order=False)

    mock_df.unionByName.assert_called_once_with(mock_select_df, allowMissingColumns=True)
    mock_df.createOrReplaceTempView.assert_called_once_with("df_mm_prod_sdim_promo_vw")

    merge_sql_call = normalize_sql(mock_spark.sql.call_args_list[1][0][0])
    assert "MERGE INTO" in merge_sql_call
    assert f"{catalog_name}.{schema_name}.{prod_dim}" in merge_sql_call
    assert "WHEN MATCHED THEN UPDATE SET *" in merge_sql_call
    assert "WHEN NOT MATCHED THEN INSERT *" in merge_sql_call


def test_t2_publish_product_empty_dataframe(monkeypatch):
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    prod_dim = "test_prod_dim"

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_empty_df = MagicMock()

    mock_spark.sql.side_effect = [mock_empty_df, None]
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    mock_df.unionByName.assert_called_once_with(mock_empty_df, allowMissingColumns=True)
    mock_df.createOrReplaceTempView.assert_called_once_with("df_mm_prod_sdim_promo_vw")
    assert mock_spark.sql.call_count == 2


def test_t2_publish_product_merge_sql_query_correct(monkeypatch):
    catalog_name = "catalog"
    schema_name = "schema"
    prod_dim = "prod_dim"

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_target_df = MagicMock()

    mock_spark.sql.side_effect = [mock_target_df, None]
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    merge_sql = normalize_sql(mock_spark.sql.call_args_list[1][0][0])

    assert f"MERGE INTO {catalog_name}.{schema_name}.{prod_dim}" in merge_sql
    assert "USING df_mm_prod_sdim_promo_vw src" in merge_sql
    assert "ON src.prod_skid = tgt.prod_skid AND src.part_srce_sys_id = tgt.part_srce_sys_id" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET *" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT *" in merge_sql