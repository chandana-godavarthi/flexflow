import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql.types import IntegerType

# Replace this with your actual import path
from src.tp_utils.common import add_partition_columns

class TestAddPartitionColumns(unittest.TestCase):

    def get_chainable_mock_df(self, srce_sys_id_casted="casted_srce_sys_id", mm_time_perd_end_date_casted="casted_mm_time_perd_end_date"):
        mock_df = MagicMock()

        # Mock column objects
        mock_srce_sys_id_col = MagicMock()
        mock_mm_time_perd_end_date_col = MagicMock()
        mock_srce_sys_id_col.cast.return_value = srce_sys_id_casted
        mock_mm_time_perd_end_date_col.cast.return_value = mm_time_perd_end_date_casted

        mock_df.__getitem__.side_effect = lambda col: {
            "srce_sys_id": mock_srce_sys_id_col,
            "mm_time_perd_end_date": mock_mm_time_perd_end_date_col
        }[col]

        # Make withColumn chainable
        mock_df.withColumn.return_value = mock_df

        return mock_df

    @patch("src.tp_utils.common.lit")
    def test_valid_input(self, mock_lit):
        mock_df = self.get_chainable_mock_df()
        mock_lit.return_value = "mocked_lit_value"

        add_partition_columns(mock_df, 456)

        mock_df.withColumn.assert_any_call('part_srce_sys_id', "casted_srce_sys_id")
        mock_df.withColumn.assert_any_call('part_cntrt_id', "mocked_lit_value")
        mock_df.withColumn.assert_any_call('part_mm_time_perd_end_date', "casted_mm_time_perd_end_date")

    @patch("src.tp_utils.common.lit")
    def test_null_values(self, mock_lit):
        mock_df = self.get_chainable_mock_df(srce_sys_id_casted=None, mm_time_perd_end_date_casted=None)
        mock_lit.return_value = "mocked_lit_value"

        add_partition_columns(mock_df, 789)

        mock_df.withColumn.assert_any_call('part_srce_sys_id', None)
        mock_df.withColumn.assert_any_call('part_mm_time_perd_end_date', None)

    @patch("src.tp_utils.common.lit")
    def test_invalid_date_format(self, mock_lit):
        mock_df = self.get_chainable_mock_df(mm_time_perd_end_date_casted="casted_invalid_date")
        mock_lit.return_value = "mocked_lit_value"

        add_partition_columns(mock_df, 101)

        mock_df.withColumn.assert_any_call('part_mm_time_perd_end_date', "casted_invalid_date")

    @patch("src.tp_utils.common.lit")
    def test_non_integer_cntrt_id(self, mock_lit):
        mock_df = self.get_chainable_mock_df()
        mock_lit.return_value = "mocked_lit_value"

        add_partition_columns(mock_df, "123")  # string input

        mock_lit.assert_called_with(123)
        mock_df.withColumn.assert_any_call('part_cntrt_id', "mocked_lit_value")
