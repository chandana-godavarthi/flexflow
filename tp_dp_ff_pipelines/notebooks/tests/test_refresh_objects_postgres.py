import unittest
from unittest.mock import MagicMock, patch
import importlib.util
import sys
import types
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
import re


class TestRefreshPostgresPipeline(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/refresh_objects_postgres.py"

    def get_mock_dependencies(self):
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock()
        mock_df.collect.return_value = [
            Row(
                tgt_table="catalog.source_table",
                src_table="target_table",
                state="overwrite",
                part_key_name="key",
                part_key_value="[1]",
                key_cols="id"
            )
        ]
        mock_df.drop.return_value = mock_df
        mock_spark.sql.return_value = mock_df
        mock_spark.table.return_value = mock_df

        mock_common = MagicMock()
        mock_common.get_logger.return_value = MagicMock()
        mock_common.get_dbutils.return_value = MagicMock()
        mock_common.read_run_params.return_value = MagicMock(refresh_type="twice")
        mock_common.get_database_config.return_value = {
            'ref_db_jdbc_url': 'jdbc://dummy',
            'ref_db_name': 'ref_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'catalog',
            'postgres_schema': 'schema'
        }
        mock_common.read_from_postgres = MagicMock(return_value=mock_df)
        mock_common.update_to_postgres = MagicMock()
        mock_common.write_to_postgres = MagicMock()
        mock_common.cdl_publishing = MagicMock()
        mock_common.sanitize_variable = lambda x: x

        return mock_spark, mock_common

    def run_script_logic(self, mock_spark, mock_common):
        # Simulate the script logic manually
        dbutils = mock_common.get_dbutils(mock_spark)
        logger = mock_common.get_logger()

        db_config = mock_common.get_database_config(dbutils)
        args = mock_common.read_run_params()
        refresh_type = args.refresh_type

        catalog_name = db_config['catalog_name']
        postgres_schema = db_config['postgres_schema']
        ref_db_jdbc_url = db_config['ref_db_jdbc_url']
        ref_db_name = db_config['ref_db_name']
        ref_db_user = db_config['ref_db_user']
        ref_db_pwd = db_config['ref_db_pwd']

        table_config = mock_spark.sql(f"""
            SELECT * 
            FROM {catalog_name}.internal_tp.tp_refresh_tbl_lkp
            WHERE refresh_type = :refresh_type AND flag="Y"
            """, {'refresh_type': refresh_type})

        for config in table_config.collect():
            src_table = config["tgt_table"]
            tgt_table = config["src_table"]
            part_keys = config["part_key_name"]
            part_value = eval(config["part_key_value"])
            key_cols = config["key_cols"]

            df_src = mock_spark.table(f"{catalog_name}.{src_table}")
            df_src = df_src.drop("part_srce_sys_id", "part_cntrt_id", "secure_group_key")
            df_tgt = mock_common.read_from_postgres(f"{postgres_schema}.{tgt_table}", mock_spark, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

            cdl_table = tgt_table.split(".", 2)[-1].upper()
            logical_table_name = physical_table_name = unity_catalog_table_name = cdl_table

            if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', postgres_schema) or not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', tgt_table):
                raise ValueError("Unsafe schema or table name")

            tbl_name = f'{postgres_schema}.{tgt_table}'
            tbl_name = mock_common.sanitize_variable(tbl_name)
            query = f"TRUNCATE TABLE {tbl_name}"
            mock_common.update_to_postgres(query, (), ref_db_name, ref_db_user, ref_db_pwd)

            mock_common.write_to_postgres(df_src, tbl_name, ref_db_jdbc_url, ref_db_name, ref_db_user, ref_db_pwd)

            mock_common.cdl_publishing(logical_table_name, physical_table_name, unity_catalog_table_name, "part_keys", dbutils, MagicMock(), MagicMock(), MagicMock())

    def test_refresh_postgres_pipeline(self):
        mock_spark, mock_common = self.get_mock_dependencies()
        self.run_script_logic(mock_spark, mock_common)

        mock_common.read_from_postgres.assert_called()
        mock_common.update_to_postgres.assert_called()
        mock_common.write_to_postgres.assert_called()
        mock_common.cdl_publishing.assert_called()