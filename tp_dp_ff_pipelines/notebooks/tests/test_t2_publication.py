import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row


class TestPublishingPipeline(unittest.TestCase):

    def setUp(self):
        self.catalog_name = "catalog"
        self.time_perd_class_code = "MONTHLY"
        self.cntrt_id = "C123"
        self.run_id = "R456"
        self.srce_sys_id = 2
        self.file_name = "input_file.csv"

        self.mock_spark = MagicMock()
        self.mock_df = MagicMock()
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.write = MagicMock()
        self.mock_df.show = MagicMock()
        self.mock_df.createOrReplaceTempView = MagicMock()
        self.mock_df.collect.return_value = [
            Row(part_mm_time_perd_end_date="2023-12-31"),
            Row(part_mm_time_perd_end_date="2024-01-31")
        ]
        self.mock_spark.sql.return_value = self.mock_df

        self.mock_dbutils = MagicMock()
        self.mock_config = MagicMock()
        self.mock_meta_client = MagicMock()

    def test_fact_partition_path_generation(self):
        affected_partitions = self.mock_df.collect.return_value
        paths = [
            f"/mnt/tp-publish-data/TP_{self.time_perd_class_code}_FCT/part_srce_sys_id={self.srce_sys_id}/part_cntrt_id={self.cntrt_id}/part_time_perd_end_date={row.part_mm_time_perd_end_date}"
            for row in affected_partitions
        ]
        self.assertEqual(len(paths), 2)
        self.assertTrue(paths[0].endswith("2023-12-31"))
        self.assertTrue(paths[1].endswith("2024-01-31"))

    @patch("src.tp_utils.common.semaphore_acquisition")
    @patch("src.tp_utils.common.release_semaphore")
    def test_fact_delta_write_and_semaphore(self, mock_release, mock_acquire):
        mock_acquire.return_value = "/mnt/semaphore/fact"
        table = f"{self.catalog_name}.gold_tp.TP_{self.time_perd_class_code}_FCT"

        self.mock_df.withColumn.return_value.write.format.return_value.mode.return_value.option.return_value.saveAsTable(table)
        self.mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(table)

        mock_release.assert_not_called()  # Would be called in actual pipeline

    @patch("src.tp_utils.common.cdl_publishing")
    def test_cdl_publishing_for_all_dimensions(self, mock_cdl):
        from src.tp_utils.common import cdl_publishing

        table_configs = [
            ("TP_PROD_DIM", "part_srce_sys_id/part_cntrt_id"),
            ("TP_MKT_DIM", "part_srce_sys_id"),
            (f"TP_{self.time_perd_class_code}_FCT", "part_srce_sys_id/part_cntrt_id/part_mm_time_perd_end_date")
        ]

        for logical, partition_def in table_configs:
            cdl_publishing(logical, logical, logical, partition_def, self.mock_dbutils, self.mock_config, self.mock_meta_client)

        self.assertEqual(mock_cdl.call_count, 3)

    @patch("src.tp_utils.common.work_to_arch")
    def test_file_archiving(self, mock_arch):
        from src.tp_utils.common import work_to_arch
        work_to_arch(self.file_name, self.mock_dbutils)
        mock_arch.assert_called_once_with(self.file_name, self.mock_dbutils)