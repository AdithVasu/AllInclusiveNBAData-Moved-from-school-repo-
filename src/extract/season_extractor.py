from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from nba_api.stats.endpoints import playbyplayv2

from .config import BRONZE_BUCKET, DEFAULT_HEADERS, PIPELINE_VERSION
from .nba_api_helpers import (
    _build_game_id_list,
    _endpoint_payload,
    _game_s3_key,
    _sleep_after_failed_api_call,
    _sleep_for_akamai_delay,
)
from .s3_utils import get_s3_client, object_exists, upload_raw_payload

logger = logging.getLogger(__name__)


def extract_play_by_play_for_game(
    season: str,
    game_id: str,
    bronze_bucket: str,
    s3_client: Any,
    headers: Optional[Dict[str, str]] = None,
) -> bool:
    """Check S3 idempotency then fetch and store raw play-by-play JSON."""
    key = _game_s3_key(season, game_id)

    if object_exists(s3_client, bronze_bucket, key):
        logger.info("Skipping already existing game payload: %s", key)
        return False

    _sleep_for_akamai_delay()

    try:
        endpoint = playbyplayv2.PlayByPlayV2(
            game_id=game_id,
            headers=headers or DEFAULT_HEADERS,
        )
        payload = _endpoint_payload(endpoint)
    except Exception as exc:
        logger.exception("API call failed for game %s in season %s", game_id, season)
        _sleep_after_failed_api_call()
        raise exc

    upload_raw_payload(s3_client, bronze_bucket, key, payload)
    return True


def extract_season(
    season: str,
    bronze_bucket: str = BRONZE_BUCKET,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Pull and store all regular-season game payloads for a single season."""
    logger.info("Starting extraction for season %s", season)
    s3_client = get_s3_client()

    try:
        game_ids = _build_game_id_list(
            season=season,
            headers=headers or DEFAULT_HEADERS,
        )
    except Exception as exc:
        logger.exception("Failed to build game ID list for season %s", season)
        raise exc

    extracted_count = 0
    skipped_count = 0

    for game_id in game_ids:
        try:
            result = extract_play_by_play_for_game(
                season=season,
                game_id=game_id,
                bronze_bucket=bronze_bucket,
                s3_client=s3_client,
                headers=headers or DEFAULT_HEADERS,
            )
            if result:
                extracted_count += 1
            else:
                skipped_count += 1
        except Exception as exc:
            logger.exception("Season extraction aborted on game %s for season %s", game_id, season)
            raise exc

    summary = {
        "season": season,
        "bronze_bucket": bronze_bucket,
        "game_ids_found": len(game_ids),
        "games_extracted": extracted_count,
        "games_skipped": skipped_count,
        "pipeline_version": PIPELINE_VERSION,
    }
    logger.info("Finished extraction for season %s: %s", season, summary)
    return summary
