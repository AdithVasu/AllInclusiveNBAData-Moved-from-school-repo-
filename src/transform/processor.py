from __future__ import annotations

import gc
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

from ..validation import validate_payload_structure, validate_season_label, validate_transformed_frame
from .config import PIPELINE_VERSION
from .s3_utils import get_s3_client, list_json_objects, load_json_from_s3, write_csv_to_s3

logger = logging.getLogger(__name__)


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


def transform_season(season: str) -> Dict[str, Any]:
    validated_season = validate_season_label(season)

    s3_client = get_s3_client()
    prefix = f"bronze/play_by_play/season={validated_season}/"
    object_keys = list_json_objects(s3_client, prefix)

    if not object_keys:
        raise FileNotFoundError(f"No Bronze JSON objects found for season {validated_season}.")

    all_frames: List[pd.DataFrame] = []

    for key in object_keys:
        payload = load_json_from_s3(s3_client, key)
        validate_payload_structure(payload)
        parsed_frames = parse_play_by_play_frames(payload)
        for frame in parsed_frames:
            normalized_frame = normalize_frame(frame, validated_season)
            validated_frame = validate_transformed_frame(normalized_frame, validated_season)
            all_frames.append(validated_frame)

    if not all_frames:
        raise RuntimeError(f"No PlayByPlay frames were parsed for season {validated_season}.")

    season_df = pd.concat(all_frames, ignore_index=True, sort=False)
    season_df = season_df.rename(columns=lambda column: str(column).strip().lower())
    season_df = season_df.reset_index(drop=True)

    rows_written = len(season_df)
    write_csv_to_s3(s3_client, season_df, validated_season)

    del all_frames
    del season_df
    gc.collect()

    return {
        "season": validated_season,
        "rows_written": rows_written,
        "silver_prefix": f"silver/play_by_play/season={validated_season}/play_by_play_data.csv",
        "pipeline_version": PIPELINE_VERSION,
    }
