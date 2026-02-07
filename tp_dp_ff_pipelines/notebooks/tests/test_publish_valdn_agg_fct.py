# import pytest
# from unittest.mock import patch, MagicMock, ANY
# from src.tp_utils.common import publish_valdn_agg_fct

# @pytest.fixture
# def common_mocks():
#     mock_df = MagicMock()
#     mock_df.columns = ['time_factr_pp', 'share_su_pct']
#     mock_df.withColumn.return_value = mock_df
#     mock_df.createOrReplaceTempView.return_value = None
#     mock_df.show.return_value = None
#     mock_df.rdd.isEmpty.return_value = False

#     mock_final_df = MagicMock()
#     mock_final_df.columns = ['time_factr_pp', 'share_su_pct', 'part_cntrt_id']
#     mock_final_df.withColumn.return_value = mock_final_df
#     mock_final_df.createOrReplaceTempView.return_value = None
#     mock_final_df.show.return_value = None
#     mock_final_df.rdd.isEmpty.return_value = False

#     # Mock write chain
#     mock_write = MagicMock()
#     mock_partition = MagicMock()
#     mock_mode = MagicMock()
#     mock_save = MagicMock()

#     mock_final_df.write = mock_write
#     mock_write.partitionBy.return_value = mock_partition
#     mock_partition.mode.return_value = mock_save
#     mock_save.saveAsTable.return_value = None

#     with patch("src.tp_utils.common.get_logger", return_value=MagicMock()) as mock_logger, \
#          patch("src.tp_utils.common.column_complementer", return_value=mock_final_df) as mock_complementer, \
#          patch("src.tp_utils.common.add_secure_group_key", return_value=mock_final_df) as mock_secure, \
#          patch("src.tp_utils.common.lit", return_value=MagicMock(name="Column")) as mock_lit, \
#          patch("src.tp_utils.common.semaphore_generate_path", return_value="mock_path") as mock_path, \
#          patch("src.tp_utils.common.semaphore_acquisition", return_value="mock_check_path") as mock_acquire, \
#          patch("src.tp_utils.common.release_semaphore") as mock_release:

#         yield {
#             "mock_logger": mock_logger,
#             "mock_complementer": mock_complementer,
#             "mock_secure": mock_secure,
#             "mock_lit": mock_lit,
#             "mock_path": mock_path,
#             "mock_acquire": mock_acquire,
#             "mock_release": mock_release,
#             "mock_final_df": mock_final_df,
#             "mock_write": mock_write,
#             "mock_partition": mock_partition,
#             "mock_mode": mock_mode,
#             "mock_save": mock_save
#         }

# def test_df_full_flow(common_mocks):
#     mock_df = MagicMock()
#     mock_df.columns = ['time_factr_pp', 'share_su_pct']
#     mock_df.withColumn.return_value = mock_df
#     mock_df.createOrReplaceTempView.return_value = None
#     mock_df.show.return_value = None
#     mock_df.rdd.isEmpty.return_value = False

#     mock_run_id = MagicMock()
#     mock_srce_sys_id = MagicMock()
#     mock_spark = MagicMock()
#     mock_spark.table.return_value = mock_df
#     mock_spark.sql.return_value = mock_df

#     publish_valdn_agg_fct(
#         mock_df,
#         789,
#         "schema",
#         "catalog",
#         mock_spark,
#         "url",
#         "ref_db",
#         "user",
#         "pwd",
#         mock_srce_sys_id,
#         mock_run_id
#     )

#     # Assert on the correct mock object
#     common_mocks["mock_final_df"].withColumn.assert_any_call("part_cntrt_id", ANY)
#     common_mocks["mock_write"].partitionBy.assert_called_once_with("part_cntrt_id")
#     common_mocks["mock_save"].saveAsTable.assert_called_once_with("catalog.gold_tp.tp_valdn_agg_fct")
#     common_mocks["mock_final_df"].show.assert_called_once()