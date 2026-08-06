import os
import logging
import psycopg2
from dotenv import load_dotenv

from src.utils import logger

load_dotenv()

logger = logging.getLogger(__name__)


def get_connection():

    try:

        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            port=os.getenv("DB_PORT"),
            password=os.getenv("DB_PASSWORD"),
        )

        logger.info("Connected to PostgreSQL")

        return conn

    except psycopg2.Error as error:

        logger.error(f"Database connection failed: {error}")

        raise