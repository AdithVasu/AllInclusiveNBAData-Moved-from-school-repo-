from __future__ import annotations

import gc
import logging
from io import StringIO
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import create_engine

from ..validation import validate_silver_frame_for_load
from .config import CHUNKSIZE, REDSHIFT_POSTGRES_URL, SILVER_BUCKET, TABLE_NAME
from .s3_utils import get_s3_client, list_silver_csvs

logger = logging.getLogger(__name__)


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
        validated_chunk, rows_in_chunk = validate_silver_frame_for_load(chunk)
        validated_chunk.to_sql(
            TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        chunk_count += 1
        logger.info("Loaded chunk %s for %s (%s rows)", chunk_count, key, rows_in_chunk)

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

    results: List[Dict[str, Any]] = []

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
