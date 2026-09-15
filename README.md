# AllInclusiveNBAData

This repository is copied from my original private, school repository after getting instructor permission

This project is a data engineering pipeline for extracting, transforming, and loading NBA play-by-play data using open-source Python tooling and AWS services. It was built as a class project to explore modern data engineering workflows, data architecture, and cloud-based analytics patterns.

## Project status

This project was originally designed to run against AWS services, including Amazon S3 and Amazon Redshift, but the pipeline was ultimately shut down because the team did not have access to the AWS credits that were expected to be provided through the class. The project was still completed as a data engineering initiative for the course and received recognition for its scale and creativity, including:

- an award for most data ingested (a rough estimate of roughly 10–50 GB of raw and transformed NBA event data across multiple seasons, depending on exactly how many seasons and game files were ingested in the class environment)
- recognition for the strongest proposed use cases, including historical trend analysis, player performance analysis, and game-state exploration

## What this project does

The project follows a bronze/silver/load architecture:

1. Extract
   - Pulls NBA play-by-play data from the nba_api client
   - Stores raw JSON payloads in S3 (bronze layer)

2. Transform
   - Parses raw JSON files into normalized tabular data
   - Writes cleaned CSV outputs to S3 (silver layer)

3. Load
   - Loads the silver CSVs into a queryable warehouse target
   - The project was updated to prefer Amazon Redshift as the load destination

## Architecture overview

```text
nba_api / nba.com
    |
    v
[Extract layer: src/extract]
    |
    +--> bronze/raw JSON stored in S3
    |
    v
[Transform layer: src/transform]
    |
    +--> silver/normalized CSV stored in S3
    |
    v
[Load layer: src/load]
    |
    +--> Amazon Redshift (preferred target)
```

## Repository structure

```text
.
├── .env.example
├── dags/
│   └── nba_historical_extraction_pipeline.py
├── src/
│   ├── extract/
│   │   ├── config.py
│   │   ├── nba_api_helpers.py
│   │   ├── s3_utils.py
│   │   └── season_extractor.py
│   ├── load/
│   │   ├── config.py
│   │   ├── loader.py
│   │   ├── postgres_loader.py
│   │   ├── s3_utils.py
│   │   └── __init__.py
│   └── transform/
│       ├── config.py
│       ├── processor.py
│       ├── s3_utils.py
│       └── __init__.py
├── requirements.txt
├── README.md
└── .venv/
```

## Tech stack

- Python
- nba_api
- pandas
- boto3
- SQLAlchemy
- Apache Airflow
- Amazon S3
- Amazon Redshift
- python-dotenv

## Getting started

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy the example environment file and update the values for your AWS and Redshift setup:

```bash
cp .env.example .env
```

Then edit `.env` with your actual credentials and connection details.

### 4. Run the pipeline

The project is organized by ETL stage and can be run through the Airflow DAG or by invoking the stage-specific Python modules.

Example:

```bash
python -m src.extract.season_extractor
```

Or run the DAG through Airflow if you have it configured in your environment.

## Environment configuration

This project supports a `.env` file through `python-dotenv`. The existing config files load environment variables automatically, so you can keep settings centralized in `.env` and avoid hardcoding secrets in the code.

### Example `.env`

```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
NBA_BRONZE_BUCKET=nba-pipeline-bronze
NBA_SILVER_BUCKET=nba-pipeline-silver
REDSHIFT_POSTGRES_URL=postgresql+psycopg2://username:password@redshift-host:5439/dev?sslmode=require
PLAY_BY_PLAY_TABLE=play_by_play
PANDAS_CHUNKSIZE=10000
PIPELINE_VERSION=2026.09.12
```

## Data organization

### Bronze layer
- Stores raw NBA JSON responses
- Organized by season and game
- Example path pattern:

```text
s3://nba-pipeline-bronze/bronze/play_by_play/season=2023-24/game_1234567890.json
```

### Silver layer
- Stores normalized CSV outputs for downstream analytics
- Example path pattern:

```text
s3://nba-pipeline-silver/silver/play_by_play/season=2023-24/play_by_play_data.csv
```

### Load layer
- Reads the silver CSVs
- Loads them into Redshift using SQLAlchemy connection handling
- The loader is intentionally organized so it can be extended for staging, transformation, or incremental updates

## Suggested use cases

This project was designed to support a range of NBA analytics ideas, including:

- historical player trend analysis
- team play-style comparison over time
- game-state and possession analysis
- advanced performance metrics
- predictive modeling for player or team outcomes

## Notes on AWS and class constraints

The original goal of the project was to fully deploy and operate the pipeline on AWS with real data volumes and cloud-native analytics tooling. However, the team had to stop the project when class-provided AWS credits were not available. Despite that, the project still served as a strong demonstration of data engineering fundamentals, cloud architecture thinking, and data pipeline design.

## Awards and recognition

This project was recognized in class for:

- most data ingested (a rough estimate of roughly 10–50 GB of raw and transformed NBA event data across multiple seasons, depending on exactly how many seasons and game files were ingested in the class environment)
- best proposed use cases, especially for historical analysis and practical downstream analytics applications

## Future improvements

Potential next steps include:

- adding incremental load logic
- adding schema validation and data quality checks
- introducing a gold layer for curated analytics tables
- integrating Athena, Glue, or Iceberg for lakehouse-style querying
- adding CI/CD and test coverage

## License

This project is intended for educational and portfolio use.
