from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

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
