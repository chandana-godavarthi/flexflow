import logging

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Locate/Create a sparksession using databricks-connect if local, or sparksession if in
    """
    # yes this is ugly, yes this is how databricks cli repo recommends doing it
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def get_dbutils() -> DBUtils:
    spark = get_spark()
    return DBUtils(spark)


def get_logger():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # disable console noise from py4j in databricks runner
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # return the logger for this module
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger
