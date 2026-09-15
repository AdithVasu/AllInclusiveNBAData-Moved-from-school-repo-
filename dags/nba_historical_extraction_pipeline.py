from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from extract.nba_historical_extractor import extract_season

BRONZE_BUCKET = os.getenv("NBA_BRONZE_BUCKET", "nba-pipeline-bronze")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SEASONS = [f"{year}-{str(year + 1)[-2:]}" for year in range(2000, 2026)]


def _run_season_extraction(**context):
    season = context["op_kwargs"]["season"]
    return extract_season(
        season=season,
        bronze_bucket=BRONZE_BUCKET,
        aws_region=AWS_REGION,
    )


with DAG(
    dag_id="nba_historical_extraction_pipeline",
    description="Extract NBA play-by-play data for historical seasons into the Bronze S3 layer.",
    start_date=datetime(2026, 9, 11),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["nba", "bronze", "historical"],
) as dag:

    previous_task = None

    for season in SEASONS:
        task = PythonOperator(
            task_id=f"extract_season_{season.replace('-', '_')}",
            python_callable=_run_season_extraction,
            op_kwargs={"season": season},
            provide_context=True,
        )

        if previous_task is not None:
            previous_task >> task

        previous_task = task
