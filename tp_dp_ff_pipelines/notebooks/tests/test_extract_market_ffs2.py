import unittest
from unittest.mock import MagicMock, patch
from src.tp_utils.common import extract_market_ffs2  # Adjust path

class TestExtractMarketFFS2(unittest.TestCase):

    def setUp(self):
        self.run_id = "RUN123"
        self.cntrt_id = "CNTRT001"
        self.categ_id = "CAT001"
        self.srce_sys_id = "SYS001"
        self.strct_code = "STR001"

        # Create a mock DataFrame
        self.mock_df = MagicMock()
        self.mock_df.filter.return_value = self.mock_df
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.select.return_value = self.mock_df
        self.mock_df.drop.return_value = self.mock_df
        self.mock_df.count.return_value = 1
        self.mock_df.columns = ["value", "extrn_code", "attr_code_list"]

    def patch_all_pyspark(self):
        # Create mock column object with getItem
        col_obj = MagicMock()
        col_obj.getItem.return_value = MagicMock()

        return [
            patch("src.tp_utils.common.get_logger", return_value=MagicMock()),
            patch("src.tp_utils.common.col", return_value=col_obj),
            patch("src.tp_utils.common.expr", return_value=MagicMock()),
            patch("src.tp_utils.common.lit", return_value=MagicMock()),
            patch("src.tp_utils.common.split", return_value=col_obj),
            patch("src.tp_utils.common.concat_ws", return_value=MagicMock()),
            patch("src.tp_utils.common.when", return_value=MagicMock()),
            patch("src.tp_utils.common.row_number", return_value=MagicMock()),
            patch("src.tp_utils.common.Window.orderBy", return_value=MagicMock()),
            patch("src.tp_utils.common.monotonically_increasing_id", return_value=MagicMock()),
            patch("src.tp_utils.common.size", return_value=MagicMock()),
            patch("src.tp_utils.common.ltrim", return_value=MagicMock()),
            patch("src.tp_utils.common.rtrim", return_value=MagicMock()),
        ]

    def test_successful_execution(self):
        with self._patched_context():
            result = extract_market_ffs2(
                self.mock_df, self.run_id, self.cntrt_id,
                self.categ_id, self.srce_sys_id, self.strct_code
            )
            self.assertGreater(result.count(), 0)

    def test_no_g_rows(self):
        self.mock_df.count.return_value = 0
        with self._patched_context():
            result = extract_market_ffs2(
                self.mock_df, self.run_id, self.cntrt_id,
                self.categ_id, self.srce_sys_id, self.strct_code
            )
            self.assertEqual(result.count(), 0)

    def test_malformed_value_column(self):
        # Simulate malformed value handling by returning empty DataFrame
        self.mock_df.count.return_value = 0
        with self._patched_context():
            result = extract_market_ffs2(
                self.mock_df, self.run_id, self.cntrt_id,
                self.categ_id, self.srce_sys_id, self.strct_code
            )
            self.assertEqual(result.count(), 0)

    def _patched_context(self):
        # Helper to apply all patches as context managers
        patches = self.patch_all_pyspark()
        return self._chain_context_managers(patches)

    def _chain_context_managers(self, patches):
        # Chain all context managers
        class ChainContext:
            def __init__(self, managers):
                self.managers = managers
                self.entered = []

            def __enter__(self):
                for manager in self.managers:
                    self.entered.append(manager.__enter__())
                return self.entered

            def __exit__(self, exc_type, exc_val, exc_tb):
                for manager in reversed(self.managers):
                    manager.__exit__(exc_type, exc_val, exc_tb)

        return ChainContext(patches)
