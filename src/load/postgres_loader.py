from __future__ import annotations

import gc
import logging
import os
from io import StringIO
from typing import Any, Dict, List

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

logger = logging.getLogger(__name__)
SILVER_BUCKET = os.getenv("NBA_SILVER_BUCKET", "nba-pipeline-silver")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
REDSHIFT_POSTGRES_URL = os.getenv(
    "REDSHIFT_POSTGRES_URL",
    os.getenv("RDS_POSTGRES_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/nba_pipeline"),
)
TABLE_NAME = os.getenv("PLAY_BY_PLAY_TABLE", "play_by_play")
CHUNKSIZE = int(os.getenv("PANDAS_CHUNKSIZE", "10000"))


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
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


def clean_column_names(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]
    return frame


def load_csv_to_redshift(key: str, engine: Any) -> Dict[str, Any]:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
    csv_text = response["Body"].read().decode("utf-8")

    chunks = pd.read_csv(StringIO(csv_text), chunksize=CHUNKSIZE)
    chunk_count = 0

    for chunk in chunks:
        chunk = clean_column_names(chunk)
        chunk.to_sql(
            TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        chunk_count += 1
        logger.info("Loaded chunk %s for %s", chunk_count, key)

    return {
        "s3_key": key,
        "chunks_loaded": chunk_count,
    }


def load_csv_to_postgres(key: str, engine: Any) -> Dict[str, Any]:
    return load_csv_to_redshift(key, engine)


def load_all_silver_to_redshift() -> Dict[str, Any]:
    s3_client = get_s3_client()
    engine = create_engine(REDSHIFT_POSTGRES_URL)

    csv_keys = list_silver_csvs(s3_client)
    if not csv_keys:
        raise FileNotFoundError("No Silver CSV files were found to load into Amazon Redshift.")

    results = []

    for key in csv_keys:
        try:
            results.append(load_csv_to_redshift(key, engine))
        except Exception as exc:
            logger.exception("Failed to load S3 key %s into Amazon Redshift", key)
            raise exc
        finally:
            gc.collect()

    return {"files_processed": len(results), "results": results}


def load_all_silver_to_rds() -> Dict[str, Any]:
    return load_all_silver_to_redshift()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_all_silver_to_redshift()
