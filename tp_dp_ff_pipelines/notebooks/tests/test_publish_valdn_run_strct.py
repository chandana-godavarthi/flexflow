# import pytest
# from unittest.mock import patch, MagicMock
# from src.tp_utils.common import publish_valdn_run_strct

# @patch('src.tp_utils.common.lit', return_value=MagicMock())
# @patch('src.tp_utils.common.abs', return_value=MagicMock())
# @patch('src.tp_utils.common.get_logger', return_value=MagicMock())
# @patch('src.tp_utils.common.read_from_postgres')
# @patch('src.tp_utils.common.add_secure_group_key')
# @patch('src.tp_utils.common.column_complementer')
# def test_publish_valdn_run_strct_success(
#     mock_column_complementer, mock_add_secure_group_key, mock_read_query,
#     mock_logger, mock_abs, mock_lit
# ):
#     mock_df = MagicMock()
#     mock_df.withColumn.return_value = mock_df
#     mock_df.createOrReplaceTempView.return_value = None
#     mock_df.rdd.isEmpty.return_value = False
#     mock_df.show.return_value = None

#     mock_read_query.return_value = mock_df
#     mock_add_secure_group_key.return_value = mock_df

#     mock_final_df = MagicMock()
#     mock_final_df.rdd.isEmpty.return_value = False
#     mock_final_df.withColumn.return_value = mock_final_df
#     mock_final_df.createOrReplaceTempView.return_value = None
#     mock_final_df.show.return_value = None

#     # Mock write chain
#     mock_write = MagicMock()
#     mock_partition = MagicMock()
#     mock_format = MagicMock()
#     mock_mode = MagicMock()
#     mock_save = MagicMock()

#     mock_final_df.write = mock_write
#     mock_write.partitionBy.return_value = mock_partition
#     mock_partition.format.return_value = mock_mode
#     mock_mode.mode.return_value = mock_save
#     mock_save.saveAsTable.return_value = None

#     mock_column_complementer.return_value = mock_final_df

#     mock_spark = MagicMock()
#     mock_spark.sql.return_value = mock_final_df

#     publish_valdn_run_strct(
#         run_id=1, cntrt_id=100, postgres_schema='schema', catalog_name='catalog',
#         spark=mock_spark, ref_db_jdbc_url='url', ref_db_name='db',
#         ref_db_user='user', ref_db_pwd='pwd'
#     )

#     mock_write.partitionBy.assert_called_once_with("run_id")
#     mock_save.saveAsTable.assert_called_once_with("catalog.gold_tp.tp_valdn_run_strct_plc")
#     mock_final_df.show.assert_called_once()


# @patch('src.tp_utils.common.lit', return_value=MagicMock())
# @patch('src.tp_utils.common.abs', return_value=MagicMock())
# @patch('src.tp_utils.common.get_logger', return_value=MagicMock())
# @patch('src.tp_utils.common.read_from_postgres')
# @patch('src.tp_utils.common.add_secure_group_key')
# @patch('src.tp_utils.common.column_complementer')
# def test_empty_dataframe(
#     mock_column_complementer, mock_add_secure_group_key, mock_read_query,
#     mock_logger, mock_abs, mock_lit
# ):
#     mock_df = MagicMock()
#     mock_df.rdd.isEmpty.return_value = True
#     mock_df.withColumn.return_value = mock_df
#     mock_df.createOrReplaceTempView.return_value = None
#     mock_df.show.return_value = None

#     mock_read_query.return_value = mock_df
#     mock_add_secure_group_key.return_value = mock_df

#     mock_final_df = MagicMock()
#     mock_final_df.rdd.isEmpty.return_value = True
#     mock_final_df.withColumn.return_value = mock_final_df
#     mock_final_df.createOrReplaceTempView.return_value = None
#     mock_final_df.show.return_value = None

#     # Mock write chain
#     mock_write = MagicMock()
#     mock_partition = MagicMock()
#     mock_format = MagicMock()
#     mock_mode = MagicMock()
#     mock_save = MagicMock()

#     mock_final_df.write = mock_write
#     mock_write.partitionBy.return_value = mock_partition
#     mock_partition.format.return_value = mock_mode
#     mock_mode.mode.return_value = mock_save
#     mock_save.saveAsTable.return_value = None

#     mock_column_complementer.return_value = mock_final_df

#     mock_spark = MagicMock()
#     mock_spark.sql.return_value = mock_final_df

#     publish_valdn_run_strct(
#         run_id=1, cntrt_id=100, postgres_schema='schema', catalog_name='catalog',
#         spark=mock_spark, ref_db_jdbc_url='url', ref_db_name='db',
#         ref_db_user='user', ref_db_pwd='pwd'
#     )

#     mock_write.partitionBy.assert_called_once_with("run_id")
#     mock_save.saveAsTable.assert_called_once_with("catalog.gold_tp.tp_valdn_run_strct_plc")
