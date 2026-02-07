import pytest
from unittest.mock import MagicMock
from src.tp_utils.common import dq_query_retrieval

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark

def test_dq_query_retrieval_success(mock_spark):
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df
    mock_df.filter.return_value = "filtered_df"

    result = dq_query_retrieval(
        catalog_name="test_catalog",
        validation_name="vldn_1",
        tier="tier_1",
        description="desc_1",
        spark=mock_spark
    )

    mock_spark.sql.assert_called_once_with("select * from test_catalog.internal_tp.tp_valdn_detl_lkp ")
    mock_df.filter.assert_called_once_with(" valdn_grp_name = 'vldn_1' and valdn_name = 'desc_1' and data_tier = 'tier_1'")
    assert result == "filtered_df"

def test_dq_query_retrieval_empty_inputs(mock_spark):
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df
    mock_df.filter.return_value = "filtered_df"

    result = dq_query_retrieval(
        catalog_name="",
        validation_name="",
        tier="",
        description="",
        spark=mock_spark
    )

    mock_spark.sql.assert_called_once_with("select * from .internal_tp.tp_valdn_detl_lkp ")
    mock_df.filter.assert_called_once_with(" valdn_grp_name = '' and valdn_name = '' and data_tier = ''")
    assert result == "filtered_df"

def test_dq_query_retrieval_special_chars(mock_spark):
    mock_df = MagicMock()
    mock_spark.sql.return_value = mock_df
    mock_df.filter.return_value = "filtered_df"

    result = dq_query_retrieval(
        catalog_name="cat$log",
        validation_name="val@name",
        tier="tier#1",
        description="desc!",
        spark=mock_spark
    )

    mock_spark.sql.assert_called_once_with("select * from cat$log.internal_tp.tp_valdn_detl_lkp ")
    mock_df.filter.assert_called_once_with(" valdn_grp_name = 'val@name' and valdn_name = 'desc!' and data_tier = 'tier#1'")
    assert result == "filtered_df"
