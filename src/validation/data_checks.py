from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import pandas as pd


def validate_season_label(season: str) -> str:
    season_pattern = r"^\d{4}-\d{2}$"
    if not re.match(season_pattern, season):
        raise ValueError(f"Invalid season label '{season}'. Expected format YYYY-YY.")

    start_year = int(season[:4])
    end_year = int(season[-2:])
    if end_year != (start_year % 100) + 1:
        raise ValueError(f"Invalid season label '{season}'. End year must be next season year.")

    return season


def validate_payload_structure(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("Bronze payload must be a JSON object.")

    result_sets = payload.get("resultSets")
    if not isinstance(result_sets, list):
        raise ValueError("Bronze payload must contain a 'resultSets' list.")

    play_by_play_sets = []
    for result_set in result_sets:
        if not isinstance(result_set, dict):
            raise ValueError("Each result set must be a JSON object.")

        name = result_set.get("name")
        headers = result_set.get("headers")
        row_set = result_set.get("rowSet")

        if name == "PlayByPlay":
            if not isinstance(headers, list) or not isinstance(row_set, list):
                raise ValueError("PlayByPlay result set must include headers and rowSet.")
            if not headers:
                raise ValueError("PlayByPlay result set cannot have empty headers.")
            play_by_play_sets.append(result_set)

    if not play_by_play_sets:
        raise ValueError("No PlayByPlay resultSets were found in the bronze payload.")

    return play_by_play_sets


def validate_transformed_frame(frame: pd.DataFrame, season: str) -> pd.DataFrame:
    if frame.empty:
        raise ValueError(f"Transformed frame for season {season} is empty.")

    required_columns = {"season", "transformed_at_utc", "pipeline_version"}
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(
            f"Transformed frame for season {season} is missing required columns: {sorted(missing_columns)}"
        )

    if frame.isna().all().all():
        raise ValueError(f"Transformed frame for season {season} contains only missing values.")

    return frame


def validate_silver_frame_for_load(frame: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if frame.empty:
        raise ValueError("Silver CSV frame is empty.")

    if frame.shape[0] == 0:
        raise ValueError("Silver CSV frame has zero rows.")

    required_columns = {"season", "transformed_at_utc", "pipeline_version"}
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(f"Silver frame is missing required columns: {sorted(missing_columns)}")

    rows = int(frame.shape[0])
    if rows < 1:
        raise ValueError("Silver frame must contain at least one row.")

    return frame, rows
