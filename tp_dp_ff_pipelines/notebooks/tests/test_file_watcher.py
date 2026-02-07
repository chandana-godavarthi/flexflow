
import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession


class TestTradepanelFileProcessing(unittest.TestCase):

    def setUp(self):
        # Path to the actual script you want to run
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/file_watcher.py"

    def get_mock_modules(self):
        # --- Mock Spark ---
        mock_spark = MagicMock(spec=SparkSession)
        mock_spark.createDataFrame.return_value = MagicMock()

        # Mock SparkSession builder to return our mock_spark
        spark_session_module = types.ModuleType("pyspark.sql.session")
        spark_session_module.SparkSession = MagicMock(
            builder=MagicMock(appName=MagicMock(return_value=MagicMock(getOrCreate=MagicMock(return_value=mock_spark))))
        )

        spark_sql_module = types.ModuleType("pyspark.sql")
        spark_sql_module.SparkSession = spark_session_module.SparkSession

        # --- Mock tp_utils.common ---
        mock_common = types.ModuleType("tp_utils.common")
        mock_logger = MagicMock()
        mock_logger.info = MagicMock()
        mock_logger.error = MagicMock()
        mock_common.get_logger = MagicMock(return_value=mock_logger)

        mock_dbutils = MagicMock()
        # Secrets usage
        mock_dbutils.secrets.get.side_effect = lambda scope, key: f"{scope}_{key}"
        mock_common.get_dbutils = MagicMock(return_value=mock_dbutils)

        # DB helpers
        mock_common.read_query_from_postgres = MagicMock()
        mock_common.write_to_postgres = MagicMock()
        mock_common.update_to_postgres = MagicMock()
        mock_common.derive_base_path = MagicMock(return_value="/mnt/tp-source-data/")

        return {
            "pyspark": types.ModuleType("pyspark"),
            "pyspark.sql": spark_sql_module,
            "pyspark.sql.session": spark_session_module,
            "tp_utils.common": mock_common
        }, mock_common, mock_spark, mock_dbutils

    def run_script_with_mock(self, mock_common):
        # Compile & exec the script as __main__ so the main logic runs
        with open(self.script_path) as f:
            code = compile(f.read(), "file_watcher.py", 'exec')
            exec(code, {"__name__": "__main__"})

    def test_successful_file_processing(self):
        mock_modules, mock_common, mock_spark, mock_dbutils = self.get_mock_modules()

        # One valid zip file present
        mock_file = MagicMock()
        mock_file.name = 'testfile.zip'
        mock_file.modificationTime = 1000
        mock_file.size = 512 * 1024 ** 2  # 512 MB

        # For the current definition, the code may call ls multiple times; return same list
        mock_dbutils.fs.ls.return_value = [mock_file]

        # Contract lookup returns one match
        df_cntrt_lkp = MagicMock()
        df_cntrt_lkp.count.return_value = 1
        df_cntrt_lkp.first.side_effect = lambda: {
            "cntrt_id": "123",
            "wkf_job_id": "456",
            "wkf_ovwrt_ind": "N"
        }

        # run_id retrieval
        df_run_id = MagicMock()
        df_run_id.first.return_value = {"run_id": "789"}

        # read_query_from_postgres called twice: contract lkp, run_id
        mock_common.read_query_from_postgres.side_effect = [df_cntrt_lkp, df_run_id]

        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common)

        mock_common.write_to_postgres.assert_called_once()
        mock_common.update_to_postgres.assert_called_once()
        mock_dbutils.fs.mv.assert_called_once()

    def test_overwrite_file_processing(self):
        mock_modules, mock_common, mock_spark, mock_dbutils = self.get_mock_modules()

        # One big zip file present to trigger Large cluster and overwrite path
        mock_file = MagicMock()
        mock_file.name = 'testfile.zip'
        mock_file.modificationTime = 1000
        mock_file.size = 4 * 1024 ** 3  # 4 GB

        mock_dbutils.fs.ls.return_value = [mock_file]

        # Contract lookup indicates overwrite
        df_cntrt_lkp = MagicMock()
        df_cntrt_lkp.count.return_value = 1
        df_cntrt_lkp.first.side_effect = lambda: {
            "cntrt_id": "123",
            "wkf_job_id": "456",
            "wkf_ovwrt_ind": "Y"
        }

        # Workflow lookup returns a different job id
        df_wkf_lkp = MagicMock()
        df_wkf_lkp.first.return_value = {"wkf_job_id": "999"}

        # run_id retrieval
        df_run_id = MagicMock()
        df_run_id.first.return_value = {"run_id": "789"}

        # read_query_from_postgres called thrice: contract lkp, wkf lkp, run_id
        mock_common.read_query_from_postgres.side_effect = [df_cntrt_lkp, df_wkf_lkp, df_run_id]

        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common)

        mock_common.write_to_postgres.assert_called_once()
        mock_common.update_to_postgres.assert_called_once()
        mock_dbutils.fs.mv.assert_called_once()

    def test_no_zip_files(self):
        """
        Directory contains files but none are .zip.
        Expect: early exit, no writes/updates/moves, and 'No .zip files...' log emitted.

        IMPORTANT: Your current script calls dbutils.fs.ls(IN_PATH) three times in the
        first iteration. To reach the branch that logs 'No .zip files found in IN_PATH'
        and breaks (before accessing sorted_files[0]), we return:
         - first ls: non-empty (enter loop),
         - second ls: non-empty (files has the non-zip),
         - third ls: empty (so `not sorted_files and not ls(...)` is True).
        """
        mock_modules, mock_common, mock_spark, mock_dbutils = self.get_mock_modules()

        # Non-zip file present
        mock_file = MagicMock()
        mock_file.name = 'not_a_zip.txt'
        mock_file.modificationTime = 1000
        mock_file.size = 512 * 1024 ** 2

        # Drive the control flow: [enter loop], [have non-zip file], [empty on guard]
        mock_dbutils.fs.ls.side_effect = [[mock_file], [mock_file], []]

        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common)

        # No DB or file ops should be performed
        mock_common.write_to_postgres.assert_not_called()
        mock_common.update_to_postgres.assert_not_called()
        mock_dbutils.fs.mv.assert_not_called()

        # Assert the log message about no zip files was emitted
        info_calls = [str(call) for call in mock_common.get_logger().info.mock_calls]
        self.assertTrue(
            any("No .zip files found in IN_PATH" in c for c in info_calls),
            "Expected log message '[INFO] No .zip files found in IN_PATH' was not emitted."
        )

    def test_empty_directory(self):
        """
        Directory is empty (fs.ls returns []).
        With your current definition, the while condition is falsy and the loop
        never executes. The script then logs 'No more files in the IN directory'.
        """
        mock_modules, mock_common, mock_spark, mock_dbutils = self.get_mock_modules()

        # Empty directory listing: loop does not run
        mock_dbutils.fs.ls.return_value = []

        with patch.dict(sys.modules, mock_modules):
            self.run_script_with_mock(mock_common)

        mock_common.write_to_postgres.assert_not_called()
        mock_common.update_to_postgres.assert_not_called()
        mock_dbutils.fs.mv.assert_not_called()

        info_calls = [str(call) for call in mock_common.get_logger().info.mock_calls]
        self.assertTrue(
            any("No more files in the IN directory" in c for c in info_calls),
            "Expected log message '[INFO] No more files in the IN directory' was not emitted."
        )


if __name__ == "__main__":
    unittest.main()
