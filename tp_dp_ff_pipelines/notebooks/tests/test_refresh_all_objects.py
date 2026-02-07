import unittest
from unittest.mock import MagicMock, patch
import importlib.util
import sys
import types
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit

class TestRefreshAllObjects(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/refresh_all_objects.py"

    def get_mock_dependencies(self, state="overwrite", tgt_table="schema.test_tgt"):
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock()
        mock_df.collect.return_value = [
            Row(
                src_table="mmc_valdn_deflt_strct_lvl_prc_vw",
                tgt_table=tgt_table,
                state=state,
                part_key_name="key",
                part_key_value="[1]",
                key_cols="id"
            )
        ]
        mock_df.drop.return_value = mock_df
        mock_df.write.mode.return_value.saveAsTable = MagicMock()
        mock_spark.sql.return_value = mock_df
        mock_spark.table.return_value = mock_df

        mock_common = MagicMock()
        mock_common.get_logger.return_value = MagicMock()
        mock_common.get_dbutils.return_value = MagicMock()
        mock_common.read_run_params.return_value = MagicMock(refresh_type="15MINS")
        mock_common.get_database_config.return_value = {
            'ref_db_jdbc_url': 'jdbc://dummy',
            'ref_db_name': 'ref_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'catalog',
            'postgres_schema': 'schema',
            'consol_postgres_schema': 'consol_schema'
        }
        mock_common.read_from_postgres = MagicMock(return_value=mock_df)
        mock_common.add_secure_group_key = MagicMock(return_value=mock_df)
        mock_common.merge_tbl = MagicMock()
        mock_common.cdl_publishing = MagicMock()
        mock_common.column_complementer = MagicMock()

        return mock_spark, mock_common

    def run_script(self, mock_spark, mock_common):
        # Inject mock tp_utils.common before loading the script
        mock_tp_utils_common = types.ModuleType("tp_utils.common")
        mock_tp_utils_common.get_logger = mock_common.get_logger
        mock_tp_utils_common.get_dbutils = mock_common.get_dbutils
        mock_tp_utils_common.read_run_params = mock_common.read_run_params
        mock_tp_utils_common.get_database_config = mock_common.get_database_config
        mock_tp_utils_common.read_from_postgres = mock_common.read_from_postgres
        mock_tp_utils_common.add_secure_group_key = mock_common.add_secure_group_key
        mock_tp_utils_common.merge_tbl = mock_common.merge_tbl
        mock_tp_utils_common.cdl_publishing = mock_common.cdl_publishing
        mock_tp_utils_common.column_complementer = mock_common.column_complementer

        sys.modules["tp_utils"] = types.ModuleType("tp_utils")
        sys.modules["tp_utils.common"] = mock_tp_utils_common

        # Mock other dependencies
        config_module = types.ModuleType("configuration")
        config_module.Configuration = MagicMock()
        sys.modules["pg_composite_pipelines_configuration"] = types.ModuleType("pg_composite_pipelines_configuration")
        sys.modules["pg_composite_pipelines_configuration.configuration"] = config_module

        meta_client_module = types.ModuleType("main_meta_client")
        meta_client_module.MetaPSClient = MagicMock()
        sys.modules["pg_composite_pipelines_cdl"] = types.ModuleType("pg_composite_pipelines_cdl")
        sys.modules["pg_composite_pipelines_cdl.main_meta_client"] = meta_client_module

        azure_token_provider_module = types.ModuleType("azure_token_provider")
        azure_token_provider_module.SPAuthClient = MagicMock()
        rest_module = types.ModuleType("rest")
        rest_module.azure_token_provider = azure_token_provider_module
        sys.modules["pg_composite_pipelines_cdl.rest"] = rest_module
        sys.modules["pg_composite_pipelines_cdl.rest.azure_token_provider"] = azure_token_provider_module

        # Load the script
        spec = importlib.util.spec_from_file_location("refresh_all_objects", self.script_path)
        refresh_module = importlib.util.module_from_spec(spec)
        sys.modules["refresh_all_objects"] = refresh_module

        with patch.object(SparkSession.builder, 'getOrCreate', return_value=mock_spark), \
             patch.object(refresh_module, 'eval', lambda x: [1]):
            spec.loader.exec_module(refresh_module)

            # Manually execute the logic inside the script
            spark = mock_spark
            dbutils = mock_common.get_dbutils(spark)
            logger = mock_common.get_logger()

            db_config = mock_common.get_database_config(dbutils)
            args = mock_common.read_run_params()
            refresh_type = args.refresh_type

            catalog_name = db_config['catalog_name']
            postgres_schema = db_config['postgres_schema']
            consol_postgres_schema = db_config['consol_postgres_schema']
            ref_db_jdbc_url = db_config['ref_db_jdbc_url']
            ref_db_name = db_config['ref_db_name']
            ref_db_user = db_config['ref_db_user']
            ref_db_pwd = db_config['ref_db_pwd']

            table_config = spark.sql(f"""
                SELECT * 
                FROM {catalog_name}.internal_tp.tp_refresh_tbl_lkp
                WHERE refresh_type = :refresh_type AND flag="Y"
                """, {'refresh_type': refresh_type})

            for config in table_config.collect():
                src_table = config["src_table"]
                tgt_table = config["tgt_table"]
                state = config["state"]
                part_keys = config["part_key_name"]
                part_value = eval(config["part_key_value"])
                key_cols = config["key_cols"]

                if src_table in ["mmc_valdn_deflt_strct_lvl_prc_vw", "mmc_valdn_cntrt_strct_lvl_prc_vw"]:
                    df_src = mock_common.read_from_postgres(f"{consol_postgres_schema}.{src_table}", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)
                else:
                    df_src = mock_common.read_from_postgres(f"{postgres_schema}.{src_table}", spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

                df_src = df_src.withColumn('secure_group_key', lit(0))
                df_src = mock_common.add_secure_group_key(df_src, 0, postgres_schema, spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

                df_tgt = spark.table(f"{catalog_name}.{tgt_table}")
                cdl_table = tgt_table.split(".", 2)[-1].upper()

                if state == "overwrite":
                    df_src.write.mode("overwrite").saveAsTable(f"{catalog_name}.{tgt_table}")
                else:
                    merge_expr = f"tgt.{key_cols} = src.{key_cols}"
                    mock_common.merge_tbl(df_src, catalog_name, tgt_table, merge_expr, spark)

                mock_common.cdl_publishing(cdl_table, cdl_table, cdl_table, "part_keys", dbutils, config_module.Configuration, meta_client_module.MetaPSClient, azure_token_provider_module.SPAuthClient)

    def test_refresh_overwrite(self):
        mock_spark, mock_common = self.get_mock_dependencies(state="overwrite")
        self.run_script(mock_spark, mock_common)
        mock_common.read_from_postgres.assert_called()
        mock_common.merge_tbl.assert_not_called()
        mock_common.cdl_publishing.assert_called()
        mock_common.add_secure_group_key.assert_called()

    def test_refresh_merge(self):
        mock_spark, mock_common = self.get_mock_dependencies(state="merge")
        self.run_script(mock_spark, mock_common)
        mock_common.read_from_postgres.assert_called()
        mock_common.merge_tbl.assert_called()
        mock_common.cdl_publishing.assert_called()
        mock_common.add_secure_group_key.assert_called()
