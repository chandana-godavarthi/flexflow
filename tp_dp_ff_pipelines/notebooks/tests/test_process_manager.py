import sys
import types
import unittest
from unittest.mock import MagicMock

class TestDatabricksJobFunctions(unittest.TestCase):

    def setUp(self):
        self.module_path = "src.process_manager"

        # Mock tp_utils as a package and tp_utils.common as a submodule
        mock_tp_utils = types.ModuleType("tp_utils")
        mock_common = types.ModuleType("tp_utils.common")

        # Mock commonly used functions
        mock_common.get_logger = MagicMock(return_value=MagicMock())
        mock_common.get_dbutils = MagicMock(return_value=MagicMock())
        mock_common.read_query_from_postgres = MagicMock(return_value=MagicMock())
        mock_common.write_to_postgres = MagicMock(return_value=MagicMock())
        mock_common.update_to_postgres = MagicMock(return_value=MagicMock())
        mock_common.read_from_postgres = MagicMock(return_value=MagicMock())
        mock_common.get_database_config = MagicMock(return_value=MagicMock())

        # Inject into sys.modules
        sys.modules["tp_utils"] = mock_tp_utils
        sys.modules["tp_utils.common"] = mock_common

    def tearDown(self):
        sys.modules.pop("tp_utils", None)
        sys.modules.pop("tp_utils.common", None)

    def get_mocked_process_manager(self, post_success=True):
        from src import process_manager

        # Patch global variables
        process_manager.databricks_instance = "https://dummy.databricks.com"
        process_manager.databricks_token = "dummy_token"
        process_manager.wkf_job_run_id = 789

        # Mock requests.post
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"run_id": 123, "repair_id": 456}

        process_manager.requests = MagicMock()
        if post_success:
            process_manager.requests.post = MagicMock(return_value=mock_response)
        else:
            process_manager.requests.post = MagicMock(side_effect=Exception("API error"))

        return process_manager

    def test_trigger_new_job_success(self):
        process_manager = self.get_mocked_process_manager(post_success=True)
        result = process_manager.trigger_new_job(1, 101, 202, "file.csv")
        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["run_id"], 123)


    def test_repair_job_run_success(self):
        process_manager = self.get_mocked_process_manager(post_success=True)
        result = process_manager.repair_job_run(1, 101, 202, None, "file.csv")
        self.assertTrue(result["success"])
        self.assertEqual(result["data"]["repair_id"], 456)


if __name__ == "__main__":
    unittest.main()