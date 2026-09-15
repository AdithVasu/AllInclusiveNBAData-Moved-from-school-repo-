from __future__ import annotations

import json
import os
from typing import Any, Dict

import boto3

from .config import AWS_REGION, BRONZE_BUCKET


def get_s3_client() -> boto3.client:
    """Create a boto3 S3 client using environment-configured credentials."""
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )


def object_exists(s3_client: boto3.client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_raw_payload(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    payload: Dict[str, Any],
) -> None:
    raw_json_string = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw_json_string,
        ContentType="application/json",
    )
