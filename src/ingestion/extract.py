import logging

import pandas as pd

from src.aws.s3 import upload_file_to_s3
from src.config.database import get_connection
from src.config.tables import TABLES
from src.utils import logger

logger = logging.getLogger(__name__)



def extract_table(table_name):

    logger.info(f"Starting extraction of table: {table_name}")

    conn = None

    try:
        conn = get_connection()

        df = pd.read_sql(
            f"SELECT * FROM {table_name}",
            conn
        )

        df.to_csv(
            f"data/landing/{table_name}.csv",
            index=False
        )

        logger.info(f"{table_name}.csv created successfully")

    except Exception as error:
        logger.error(
            f"Error extracting {table_name}: {error}"
        )
        return

    try:
        upload_file_to_s3(
            f"data/landing/{table_name}.csv",
            f"retail/landing/{table_name}.csv"
        )

        logger.info(
            f"{table_name}.csv uploaded to S3."
        )

    except Exception as error:
        logger.error(
            f"Error uploading {table_name}.csv to S3: {error}"
        )
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":

    for table in TABLES:
        extract_table(table)