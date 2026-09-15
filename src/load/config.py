from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

SILVER_BUCKET = os.getenv("NBA_SILVER_BUCKET", "nba-pipeline-silver")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
REDSHIFT_POSTGRES_URL = os.getenv(
    "REDSHIFT_POSTGRES_URL",
    os.getenv("RDS_POSTGRES_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/nba_pipeline"),
)
TABLE_NAME = os.getenv("PLAY_BY_PLAY_TABLE", "play_by_play")
CHUNKSIZE = int(os.getenv("PANDAS_CHUNKSIZE", "10000"))
