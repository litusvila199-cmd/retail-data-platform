import logging

import pandas as pd

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

        logger.error(f"Error extracting {table_name}: {error}")

    finally:

        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":

    for table in TABLES:
        extract_table(table)