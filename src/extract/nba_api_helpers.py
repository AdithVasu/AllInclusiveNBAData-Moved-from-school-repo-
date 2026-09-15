from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Dict, List, Optional

from nba_api.stats.endpoints import leaguegamefinder

from .config import DEFAULT_HEADERS, S3_PLAY_BY_PLAY_PREFIX

logger = logging.getLogger(__name__)


def _sleep_for_akamai_delay() -> None:
    """Inject a randomized delay between API requests to reduce request clustering."""
    time.sleep(random.uniform(2.5, 4.0))


def _sleep_after_failed_api_call() -> None:
    """Pause before re-raising so Airflow can retry the task."""
    time.sleep(10)


def _endpoint_payload(endpoint: Any) -> Dict[str, Any]:
    """Normalize nba_api endpoint responses into a Python dictionary."""
    candidate_methods = ["get_dict", "get_json", "get_data", "get_response"]

    for method_name in candidate_methods:
        method = getattr(endpoint, method_name, None)
        if callable(method):
            try:
                value = method()
                if isinstance(value, dict):
                    return value
                if isinstance(value, str):
                    return json.loads(value)
            except Exception as exc:
                logger.debug("Endpoint method %s failed: %s", method_name, exc)

    data_attr = getattr(endpoint, "data", None)
    if isinstance(data_attr, dict):
        return data_attr

    if isinstance(data_attr, str):
        try:
            return json.loads(data_attr)
        except Exception:
            return {}

    raise RuntimeError("Unable to extract JSON payload from nba_api endpoint response.")


def _game_s3_key(season: str, game_id: str) -> str:
    return f"{S3_PLAY_BY_PLAY_PREFIX}/season={season}/game_{game_id}.json"


def _build_game_id_list(season: str, headers: Optional[Dict[str, str]] = None) -> List[str]:
    """Fetch all regular-season game IDs for a season through nba_api."""
    logger.info("Fetching game IDs for season %s", season)
    _sleep_for_akamai_delay()

    try:
        finder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable="00",
            season_nullable=season,
            season_type_nullable="Regular Season",
            headers=headers or DEFAULT_HEADERS,
        )
        payload = _endpoint_payload(finder)
    except Exception as exc:
        logger.exception("Failed to fetch season game IDs for %s", season)
        _sleep_after_failed_api_call()
        raise exc

    result_sets = payload.get("resultSets", [])
    game_header_set = next(
        (item for item in result_sets if item.get("name") == "GameHeader"),
        None,
    )

    if game_header_set is None:
        raise RuntimeError(f"GameHeader result set not found for season {season}.")

    row_set = game_header_set.get("rowSet", [])
    game_ids = []
    for row in row_set:
        if not row:
            continue
        game_ids.append(str(row[0]))

    unique_game_ids = list(dict.fromkeys(game_ids))
    logger.info("Fetched %s game IDs for season %s", len(unique_game_ids), season)
    return unique_game_ids
