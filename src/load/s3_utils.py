from __future__ import annotations

import os
from typing import List

import boto3

from .config import SILVER_BUCKET


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )


def list_silver_csvs(s3_client: boto3.client) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: List[str] = []

    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix="silver/play_by_play/"):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".csv"):
                keys.append(item["Key"])

    return sorted(keys)
