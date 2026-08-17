import logging
import os

import boto3
from dotenv import load_dotenv
from src.utils import logger


logger = logging.getLogger(__name__)

load_dotenv()

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def upload_file_to_s3(local_path, s3_key):

    try:

        s3.upload_file(
            local_path,
            BUCKET_NAME,
            s3_key
        )

        logger.info(
            f"File uploaded successfully to "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

    except Exception as error:

        logger.error(
            f"Error uploading {local_path} to S3: {error}"
        )

        raise


def upload_directory_to_s3(local_dir, s3_prefix):

    try:

        for root, directories, files in os.walk(local_dir):

            for file_name in files:

                local_path = os.path.join(
                    root,
                    file_name
                )

                relative_path = os.path.relpath(
                    local_path,
                    local_dir
                )

                s3_key = (
                    f"{s3_prefix.rstrip('/')}/"
                    f"{relative_path.replace(os.sep, '/')}"
                )

                s3.upload_file(
                    local_path,
                    BUCKET_NAME,
                    s3_key
                )

                logger.info(
                    f"File uploaded  successfully to "
                    f"s3://{BUCKET_NAME}/{s3_key}"
                )

    except Exception as error:

        logger.error(
            f"Error uploading directory "
            f"{local_dir} to S3: {error}"
        )

        raise                