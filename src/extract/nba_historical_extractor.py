from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional

import boto3
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2

logger = logging.getLogger(__name__)

BRONZE_BUCKET = os.getenv("NBA_BRONZE_BUCKET", "nba-pipeline-bronze")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "2026.09.11")
S3_PLAY_BY_PLAY_PREFIX = "bronze/play_by_play"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "X-Requested-With": "XMLHttpRequest",
}


def get_s3_client() -> boto3.client:
    """Create a boto3 S3 client using environment-configured credentials."""
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )


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
    headers = game_header_set.get("headers", [])
    if not headers:
        headers = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME", "GAME_DATE", "MATCHUP", "WL"]

    game_ids = []
    for row in row_set:
        if not row:
            continue
        game_ids.append(str(row[0]))

    unique_game_ids = list(dict.fromkeys(game_ids))
    logger.info("Fetched %s game IDs for season %s", len(unique_game_ids), season)
    return unique_game_ids


def _game_s3_key(season: str, game_id: str) -> str:
    return f"{S3_PLAY_BY_PLAY_PREFIX}/season={season}/game_{game_id}.json"


def _object_exists(s3_client: boto3.client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _upload_raw_payload(
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
    logger.info("Uploaded %s to s3://%s/%s", key, bucket, key)


def extract_play_by_play_for_game(
    season: str,
    game_id: str,
    bronze_bucket: str,
    s3_client: boto3.client,
    headers: Optional[Dict[str, str]] = None,
) -> bool:
    """Check S3 idempotency then fetch and store raw play-by-play JSON."""
    key = _game_s3_key(season, game_id)

    if _object_exists(s3_client, bronze_bucket, key):
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

    _upload_raw_payload(s3_client, bronze_bucket, key, payload)
    return True


def extract_season(
    season: str,
    bronze_bucket: str = BRONZE_BUCKET,
    aws_region: str = AWS_REGION,
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    seasons = [f"{year}-{str(year + 1)[-2:]}" for year in range(2000, 2026)]
    for season in seasons:
        extract_season(season=season)




