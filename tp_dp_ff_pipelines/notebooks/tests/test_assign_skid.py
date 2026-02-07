import pytest
from unittest.mock import MagicMock, patch
from src.tp_utils import common


def test_assign_skid_type_not_in_list():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_ssid = MagicMock()
    mock_cntrt_id = MagicMock()

    result = common.assign_skid(mock_df, 1, 'invalid_type', 'catalog_name', mock_spark, mock_ssid, mock_cntrt_id)
    assert result is None


def test_assign_skid_runid_exists():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_ssid = MagicMock()
    mock_cntrt_id = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    df_sel_mock = MagicMock()
    df_sel_mock.select.return_value = df_sel_mock

    count_mock = MagicMock()
    count_mock.count.return_value = 1

    mock_spark.sql.side_effect = [df_sel_mock, count_mock, df_sel_mock, df_sel_mock]

    with patch.object(mock_df, 'createOrReplaceTempView'), \
         patch.object(df_sel_mock, 'createOrReplaceTempView'):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark, mock_ssid, mock_cntrt_id)

        assert result == df_sel_mock
        mock_df.createOrReplaceTempView.assert_called()
        df_sel_mock.createOrReplaceTempView.assert_called()
        df_sel_mock.select.assert_called()


@patch('src.tp_utils.common.safe_write_with_retry')
@patch('src.tp_utils.common.semaphore_generate_path', return_value='mock_path')
@patch('src.tp_utils.common.semaphore_acquisition', return_value='mock_check_path')
@patch('src.tp_utils.common.release_semaphore')
def test_assign_skid_runid_not_exists(mock_release, mock_acquire, mock_generate_path, mock_safe_write):
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']
    mock_ssid = MagicMock()
    mock_cntrt_id = MagicMock()

    count_mock = MagicMock()
    count_mock.count.return_value = 0

    df_sel_mock = MagicMock()
    result_df = MagicMock()
    result_df.select.return_value = result_df

    mock_spark.sql.side_effect = [
        df_sel_mock,         # SELECT CAST(run_id...) query
        count_mock,          # Check run_id exists
        result_df,           # SELECT skid mapping
        result_df            # Final join
    ]

    with patch.object(mock_df, 'createOrReplaceTempView'), \
         patch.object(result_df, 'createOrReplaceTempView'):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark, mock_ssid, mock_cntrt_id)

        mock_safe_write.assert_called_once_with(
            df_sel_mock,
            'catalog_name.internal_tp.tp_prod_skid_seq',
            'append'
        )
        assert result == result_df


def test_assign_skid_exception():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_ssid = MagicMock()
    mock_cntrt_id = MagicMock()

    mock_spark.sql.side_effect = Exception("mock error")

    with pytest.raises(Exception) as exc_info:
        common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark, mock_ssid, mock_cntrt_id)

    assert "mock error" in str(exc_info.value)