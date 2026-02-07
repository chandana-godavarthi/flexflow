from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Tradepanel").getOrCreate()
from tp_utils.common import unzip
from tp_utils.common import get_dbutils, read_run_params
import argparse
from tp_utils.common import get_logger

if __name__ == "__main__":
    dbutils = get_dbutils(spark)
    logger = get_logger()
    args = read_run_params()
    file_name = args.FILE_NAME
    cntrt_id = args.CNTRT_ID
    run_id = args.RUN_ID

    catalog_name= dbutils.secrets.get('tp_dpf2cdl', 'databricks-catalog-name')
    # Define the path to the directory where the zip file is located
    vol_path = f'/Volumes/{catalog_name}/internal_tp/tp-source-data/WORK/'
    logger.info(f"Starting Unzipping the file: {vol_path}/{file_name}")
    unzip(file_name, vol_path)
    logger.info("Unzipping Finished")