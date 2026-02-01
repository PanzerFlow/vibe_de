import json
import logging
from typing import Any, Dict

import boto3

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def upload_json_to_s3(
    data: Dict[str, Any], bucket: str, key: str, metadata: Dict[str, str] | None = None
) -> bool:
    """Upload JSON data to S3 from memory with metadata and error handling"""
    try:
        extra_args = {"ContentType": "application/json"}

        if metadata:
            extra_args["Metadata"] = metadata

        s3.put_object(
            Bucket=bucket, Key=key, Body=json.dumps(data, indent=2), **extra_args
        )

        logger.info(f"Uploaded {key} to s3://{bucket}/{key}")
        if metadata:
            logger.debug(f"Metadata: {metadata}")
        return True

    except Exception as e:
        logger.error(f"Error uploading {key} to {bucket}: {e}")
        return False
