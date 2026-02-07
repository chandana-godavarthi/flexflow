import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import publish_valdn_run_strct_lvl

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.sql.return_value.createOrReplaceTempView = MagicMock()
    spark.sql.return_value.write = MagicMock()
    spark.sql.return_value.write.format.return_value.mode.return_value.saveAsTable = MagicMock()
    return spark

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.filter.return_value = df
    df.withColumn.return_value = df
    df.createOrReplaceTempView = MagicMock()
    df.show = MagicMock()
    return df

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.add_secure_group_key")
@patch("src.tp_utils.common.column_complementer")
def test_publish_valdn_run_strct_lvl_srce_sys_3(
    mock_column_complementer,
    mock_add_secure_group_key,
    mock_read_from_postgres,
    mock_get_logger,
    mock_spark,
    mock_df
):
    # Setup
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_read_from_postgres.return_value = mock_df
    mock_add_secure_group_key.return_value = mock_df
    mock_column_complementer.return_value = mock_df
    mock_spark.sql.return_value = mock_df

    # Call function
    result = publish_valdn_run_strct_lvl(
        cntrt_id=123,
        run_id=456,
        srce_sys_id=3,
        postgres_schema="test_schema",
        catalog_name="test_catalog",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="ref_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    # Assertions
    mock_read_from_postgres.assert_called_with(
        "test_schema.mm_valdn_cntrt_strct_lvl_prc_vw",
        mock_spark,
        "jdbc:test",
        "ref_db",
        "user",
        "pwd"
    )
    mock_add_secure_group_key.assert_called()
    mock_column_complementer.assert_called()
    mock_logger.info.assert_called_with("TP_VALDN_RUN_STRCT_LVL_PLC")
    mock_df.show.assert_called()
    assert result == mock_df

@patch("src.tp_utils.common.get_logger")
@patch("src.tp_utils.common.read_from_postgres")
@patch("src.tp_utils.common.add_secure_group_key")
@patch("src.tp_utils.common.column_complementer")
def test_publish_valdn_run_strct_lvl_srce_sys_not_3(
    mock_column_complementer,
    mock_add_secure_group_key,
    mock_read_from_postgres,
    mock_get_logger,
    mock_spark,
    mock_df
):
    # Setup
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_read_from_postgres.return_value = mock_df
    mock_add_secure_group_key.return_value = mock_df
    mock_column_complementer.return_value = mock_df
    mock_spark.sql.return_value = mock_df

    # Call function
    result = publish_valdn_run_strct_lvl(
        cntrt_id=789,
        run_id=1011,
        srce_sys_id=2,
        postgres_schema="test_schema",
        catalog_name="test_catalog",
        spark=mock_spark,
        ref_db_jdbc_url="jdbc:test",
        ref_db_name="ref_db",
        ref_db_user="user",
        ref_db_pwd="pwd"
    )

    # Assertions
    mock_read_from_postgres.assert_called_with(
        "test_schema.mm_valdn_tier2_strct_lvl_prc_vw",
        mock_spark,
        "jdbc:test",
        "ref_db",
        "user",
        "pwd"
    )
    mock_add_secure_group_key.assert_called()
    mock_column_complementer.assert_called()
    mock_logger.info.assert_called_with("TP_VALDN_RUN_STRCT_LVL_PLC")
    mock_df.show.assert_called()
    assert result == mock_df
