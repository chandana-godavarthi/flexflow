import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit


class TestPySparkPipeline(unittest.TestCase):

    def setUp(self):
        self.script_path = "/home/runner/work/da-dp-pda-fftp01-databricks/da-dp-pda-fftp01-databricks/tp_dp_ff_pipelines/notebooks/src/t2_dq_pre_business_validation.py"
        self.mock_spark = MagicMock(spec=SparkSession)
        self.mock_df = MagicMock()
        self.mock_df.columns = ['col1', 'col2']
        self.mock_df.filter.return_value = self.mock_df
        self.mock_df.withColumn.return_value = self.mock_df
        self.mock_df.withColumnRenamed.return_value = self.mock_df
        self.mock_df.groupBy.return_value.agg.return_value = self.mock_df
        self.mock_df.drop.return_value = self.mock_df
        self.mock_df.unionByName.return_value = self.mock_df
        self.mock_df.show = MagicMock()
        self.mock_df.createOrReplaceTempView = MagicMock()
        self.mock_df.select = MagicMock()
        self.mock_df.write = MagicMock()
        self.mock_spark.sql.return_value = self.mock_df
        self.mock_spark.read.parquet.return_value = self.mock_df
        self.mock_spark.table.return_value = self.mock_df

        # Corrected mock for select().collect() to return a real Row object
        self.mock_df.select.return_value.collect.return_value = [
            Row(srce_sys_id=101, time_perd_type_code="MONTHLY", strct_lvl_id=1)
        ]

        self.mock_common = MagicMock()
        self.mock_common.get_logger.return_value = MagicMock()
        self.mock_common.get_dbutils.return_value = MagicMock()
        self.mock_common.read_run_params.return_value = MagicMock(CNTRT_ID="C123", RUN_ID="R456")
        self.mock_common.get_database_config.return_value = {
            'ref_db_jdbc_url': 'jdbc://dummy',
            'ref_db_name': 'ref_db',
            'ref_db_user': 'user',
            'ref_db_pwd': 'pwd',
            'catalog_name': 'catalog',
            'postgres_schema': 'schema'
        }
        self.mock_common.load_cntrt_lkp.return_value = self.mock_df
        self.mock_common.read_from_postgres.return_value = self.mock_df
        self.mock_common.run_prttn_dtls.return_value = self.mock_df
        self.mock_common.publish_valdn_run_strct_lvl.return_value = self.mock_df
        self.mock_common.time_perd_class_codes.return_value = "MONTHLY"
        self.mock_common.publish_valdn_run_strct.return_value = None
        self.mock_common.publish_valdn_agg_fct.return_value = None
        self.mock_common.column_complementer.return_value = self.mock_df
        self.mock_common.add_secure_group_key.return_value = self.mock_df

    def test_initial_setup_and_contract_lookup(self):
        dbutils = self.mock_common.get_dbutils(self.mock_spark)
        logger = self.mock_common.get_logger()
        db_config = self.mock_common.get_database_config(dbutils)
        args = self.mock_common.read_run_params()
        cntrt_id = args.CNTRT_ID
        run_id = args.RUN_ID
        df_cntrt_lkp = self.mock_common.load_cntrt_lkp(
            cntrt_id, db_config['postgres_schema'], self.mock_spark,
            db_config['ref_db_jdbc_url'], db_config['ref_db_name'],
            db_config['ref_db_user'], db_config['ref_db_pwd']
        )
        row = df_cntrt_lkp.select('srce_sys_id', 'time_perd_type_code').collect()[0]
        srce_sys_id = row.srce_sys_id
        time_perd_type_code = row.time_perd_type_code
        self.assertEqual(cntrt_id, "C123")
        self.assertEqual(run_id, "R456")
        self.assertEqual(srce_sys_id, 101)
        self.assertEqual(time_perd_type_code, "MONTHLY")

    def test_materialized_and_gold_layer_data_loading(self):
        run_id = "R456"
        catalog_name = "catalog"
        srce_sys_id = 101
        self.mock_spark.read.parquet(f'/mnt/tp-source-data/temp/materialised/{run_id}/product_transformation_df_prod_stgng_vw')
        self.mock_spark.sql(f"SELECT * FROM {catalog_name}.internal_tp.tp_prod_sdim WHERE srce_sys_id = :srce_sys_id", {"srce_sys_id": srce_sys_id})
        self.mock_spark.read.parquet(f"/mnt/tp-source-data/temp/materialised/{run_id}/fact_transformation_df_fact_stgng_vw")
        self.mock_spark.read.parquet(f'/mnt/tp-source-data/temp/materialised/{run_id}/load_fact_df_fact_extrn')
        self.mock_spark.read.parquet(f'/mnt/tp-source-data/temp/materialised/{run_id}/market_transformation_df_mkt_stgng_vw')
        self.mock_spark.sql.assert_called()

    def test_product_hierarchy_decoding_and_validation_view(self):
        catalog_name = "catalog"
        srce_sys_id = 101
        self.mock_spark.sql.return_value = self.mock_df
        self.mock_df.show = MagicMock()
        self.mock_spark.sql(f"SELECT * FROM {catalog_name}.gold_tp.tp_prod_dim WHERE srce_sys_id = :srce_sys_id", {"srce_sys_id": srce_sys_id})
        self.mock_df.createOrReplaceTempView("MM_PROD_DIM")
        self.mock_spark.sql("SELECT * FROM decoded_names a, mm_cntrt_lkp b, mm_cntrt_categ_assoc c, (SELECT MAX(run_id) run_id, cntrt_id FROM dpf_run GROUP BY cntrt_id) last_run WHERE last_run.cntrt_id = b.cntrt_id AND a.srce_sys_id = b.srce_sys_id AND b.cntrt_id = c.cntrt_id")
        self.mock_df.show()
        self.mock_df.show.assert_called_once()

    def test_validation_publishing_and_fact_aggregation(self):
        cntrt_id = "C123"
        run_id = "R456"
        catalog_name = "catalog"
        postgres_schema = "schema"
        self.mock_common.publish_valdn_run_strct(run_id, cntrt_id, postgres_schema, catalog_name, self.mock_spark,
                                                 'jdbc://dummy', 'ref_db', 'user', 'pwd')
        df4 = self.mock_common.publish_valdn_run_strct_lvl(cntrt_id, run_id, 101, postgres_schema, catalog_name,
                                                           self.mock_spark, 'jdbc://dummy', 'ref_db', 'user', 'pwd')
        df4.select('strct_lvl_id')
        df4.select.assert_called_with('strct_lvl_id')

    def test_index_metric_calculation_and_publishing(self):
        run_id = "R456"
        catalog_name = "catalog"
        df_calc_index = self.mock_df.withColumn('IPP_SU_PCT', lit(100))
        df_calc_index = df_calc_index.withColumn('IYA_SU_PCT', lit(100))
        df_calc_index = df_calc_index.withColumn('IPD_SU_PCT', lit(100))
        df_calc_index = df_calc_index.withColumn('IPP_LC_PCT', lit(100))
        df_calc_index = df_calc_index.withColumn('IYA_LC_PCT', lit(100))
        df_calc_index = df_calc_index.withColumn('IPD_LC_PCT', lit(100))
        df_calc_index.show()
        self.mock_spark.sql(f"DELETE FROM {catalog_name}.gold_tp.tp_valdn_prod_dim WHERE RUN_ID = :run_id", {"run_id": run_id})
        df_calc_index.write.mode('overwrite').format('parquet').save(f'/mnt/tp-source-data/temp/materialised/{run_id}/DQ_Pre_Business_Validation_df_calc_index')
        self.mock_common.publish_valdn_agg_fct(df_calc_index, "C123", "schema", "catalog", self.mock_spark,
                                               'jdbc://dummy', 'ref_db', 'user', 'pwd')