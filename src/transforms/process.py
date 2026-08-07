import logging

from pyspark.sql import SparkSession

from src.config.schemas import SCHEMAS
from src.config.tables import TABLES
from src.utils import logger

logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("Retail Data Platform")
    .getOrCreate()
)


def process_table(table_name):

    logger.info(f"Processing table: {table_name}")

    try:

        df = (
            spark.read
            .option("header", True)
            .schema(SCHEMAS[table_name])
            .csv(f"data/landing/{table_name}.csv")
        )

        df.write.mode("overwrite").parquet(
            f"data/processed/{table_name}.parquet"
        )

        logger.info(f"{table_name} saved as Parquet")

    except Exception:
        logger.exception(f"Error processing table: {table_name}")
        raise


if __name__ == "__main__":

    for table in TABLES:
        process_table(table)