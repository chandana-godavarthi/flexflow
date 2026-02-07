import unittest
from unittest.mock import MagicMock, call
from src.tp_utils.common import get_database_config  # Update path if needed

class TestGetDatabaseConfig(unittest.TestCase):

    def setUp(self):
        self.mock_dbutils = MagicMock()

    def test_all_secrets_returned_correctly(self):
        self.mock_dbutils.secrets.get.side_effect = lambda scope, key: f"mocked_{key}"

        config = get_database_config(self.mock_dbutils)

        expected_keys = {
            'ref_db_jdbc_url',
            'ref_db_name',
            'ref_db_user',
            'ref_db_pwd',
            'ref_db_hostname',  # ✅ Added
            'catalog_name',
            'postgres_schema',
            'consol_postgres_schema'
        }
        self.assertEqual(set(config.keys()), expected_keys)

        for key, value in config.items():
            self.assertTrue(value.startswith("mocked_"))

    def test_secrets_get_called_with_correct_arguments(self):
        self.mock_dbutils.secrets.get.side_effect = lambda scope, key: f"mocked_{key}"

        get_database_config(self.mock_dbutils)

        expected_calls = [
            call('tp_dpf2cdl', 'refDBjdbcURL'),
            call('tp_dpf2cdl', 'refDBname'),
            call('tp_dpf2cdl', 'refDBuser'),
            call('tp_dpf2cdl', 'refDBpwd'),
            call('tp_dpf2cdl', 'refDBhostname'),  # ✅ Added
            call('tp_dpf2cdl', 'databricks-catalog-name'),
            call('tp_dpf2cdl', 'database-postgres-schema'),
            call('tp_dpf2cdl', 'database-postgres-schema-console')
        ]

        self.mock_dbutils.secrets.get.assert_has_calls(expected_calls, any_order=False)

    def test_missing_secret_raises_exception(self):
        def side_effect(scope, key):
            if key == 'refDBpwd':
                raise KeyError("Secret not found")
            return f"mocked_{key}"

        self.mock_dbutils.secrets.get.side_effect = side_effect

        with self.assertRaises(KeyError) as context:
            get_database_config(self.mock_dbutils)

        self.assertIn("Secret not found", str(context.exception))

    def test_partial_secret_values(self):
        def side_effect(scope, key):
            return "" if key == 'refDBuser' else f"mocked_{key}"

        self.mock_dbutils.secrets.get.side_effect = side_effect

        config = get_database_config(self.mock_dbutils)

        self.assertEqual(config['ref_db_user'], "")
        self.assertTrue(config['ref_db_jdbc_url'].startswith("mocked_"))
        self.assertTrue(config['consol_postgres_schema'].startswith("mocked_"))
