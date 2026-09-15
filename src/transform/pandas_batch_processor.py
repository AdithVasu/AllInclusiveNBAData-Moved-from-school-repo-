from __future__ import annotations

import csv
import gc
import json
import logging
import os
from datetime import datetime, timezone
from io import StringIO
from typing import Any, Dict, List

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
BRONZE_BUCKET = os.getenv("NBA_BRONZE_BUCKET", "nba-pipeline-bronze")
SILVER_BUCKET = os.getenv("NBA_SILVER_BUCKET", "nba-pipeline-silver")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "2026.09.11")


def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
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


def parse_play_by_play_frames(payload: Dict[str, Any]) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []

    result_sets = payload.get("resultSets", [])
    for result_set in result_sets:
        name = result_set.get("name", "")
        if name != "PlayByPlay":
            continue

        headers = result_set.get("headers", [])
        row_set = result_set.get("rowSet", [])
        if not headers or not row_set:
            continue

        frame = pd.DataFrame(row_set, columns=headers)
        frames.append(frame)

    return frames


def normalize_frame(frame: pd.DataFrame, season: str) -> pd.DataFrame:
    frame = frame.copy()

    if "GAME_ID" in frame.columns:
        frame["GAME_ID"] = frame["GAME_ID"].astype("string")
    if "PLAYER_ID" in frame.columns:
        frame["PLAYER_ID"] = frame["PLAYER_ID"].astype("string")

    for description_column in ["HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"]:
        if description_column in frame.columns:
            frame[description_column] = frame[description_column].fillna("").astype(str).str.strip()

    frame["transformed_at_utc"] = datetime.now(timezone.utc).isoformat()
    frame["pipeline_version"] = PIPELINE_VERSION
    frame["season"] = season

    return frame


def write_csv_to_s3(s3_client: boto3.client, seasonal_frame: pd.DataFrame, season: str) -> None:
    output_key = f"silver/play_by_play/season={season}/play_by_play_data.csv"
    csv_buffer = StringIO()
    seasonal_frame.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_MINIMAL)
    s3_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=output_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    logger.info("Wrote CSV to s3://%s/%s", SILVER_BUCKET, output_key)


def transform_season(season: str) -> Dict[str, Any]:
    s3_client = get_s3_client()
    prefix = f"bronze/play_by_play/season={season}/"
    object_keys = list_json_objects(s3_client, prefix)

    if not object_keys:
        raise FileNotFoundError(f"No Bronze JSON objects found for season {season}.")

    all_frames: List[pd.DataFrame] = []

    for key in object_keys:
        payload = load_json_from_s3(s3_client, key)
        parsed_frames = parse_play_by_play_frames(payload)
        for frame in parsed_frames:
            all_frames.append(normalize_frame(frame, season))

    if not all_frames:
        raise RuntimeError(f"No PlayByPlay frames were parsed for season {season}.")

    season_df = pd.concat(all_frames, ignore_index=True, sort=False)
    season_df = season_df.rename(columns=lambda column: str(column).strip().lower())
    season_df = season_df.reset_index(drop=True)

    rows_written = len(season_df)
    write_csv_to_s3(s3_client, season_df, season)

    del all_frames
    del season_df
    gc.collect()

    return {
        "season": season,
        "rows_written": rows_written,
        "silver_prefix": f"silver/play_by_play/season={season}/play_by_play_data.csv",
        "pipeline_version": PIPELINE_VERSION,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    seasons = [f"{year}-{str(year + 1)[-2:]}" for year in range(2000, 2026)]

    for season in seasons:
        transform_season(season)
