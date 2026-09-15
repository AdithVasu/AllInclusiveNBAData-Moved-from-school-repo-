from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List

import boto3

from .config import BRONZE_BUCKET, SILVER_BUCKET


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )


def list_json_objects(s3_client: boto3.client, prefix: str) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: List[str] = []

    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".json"):
                keys.append(item["Key"])

    return sorted(keys)


def load_json_from_s3(s3_client: boto3.client, key: str) -> Dict[str, Any]:
    response = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def write_csv_to_s3(s3_client: boto3.client, seasonal_frame: Any, season: str) -> None:
    output_key = f"silver/play_by_play/season={season}/play_by_play_data.csv"
    s3_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=output_key,
        Body=seasonal_frame.to_csv(index=False, quoting=csv.QUOTE_MINIMAL),
        ContentType="text/csv",
    )
