from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

BRONZE_BUCKET = os.getenv("NBA_BRONZE_BUCKET", "nba-pipeline-bronze")
SILVER_BUCKET = os.getenv("NBA_SILVER_BUCKET", "nba-pipeline-silver")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "2026.09.11")
