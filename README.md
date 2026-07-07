# Weather Data ETL Pipeline

A simple, production-style ETL (Extract, Transform, Load) pipeline that pulls live weather
data for multiple global cities from the free [Open-Meteo API](https://open-meteo.com/),
cleans and validates it, and loads it into a SQLite database.

## Why this project

This project is designed to demonstrate core data engineering skills in a small, readable
codebase:

- **Extract**: calling a real REST API, handling network failures per-source without
  crashing the whole run
- **Transform**: reshaping nested JSON into a tidy tabular format, deduplication, and
  basic data quality checks with pandas
- **Load**: idempotent inserts into a relational database (no duplicate rows on reruns)
- **Observability**: structured logging to both console and file
- **Modularity**: each stage is an independent, testable module

## Architecture

```
config.py    --> city list, API URL, DB path
extract.py   --> extract_weather_data()  : API -> raw JSON
transform.py --> transform_data()        : raw JSON -> clean DataFrame
load.py      --> load_to_db()            : DataFrame -> SQLite
main.py      --> orchestrates the run, logging, error handling
```

Data flow:

```
Open-Meteo API --> extract.py --> transform.py --> load.py --> weather_data.db
```

## Setup

```bash
pip install -r requirements.txt
python main.py
```

Each run appends new readings to `weather_data.db` (created automatically) and writes
logs to `pipeline.log`. Duplicate readings for the same city and timestamp are skipped.

## Querying the results

```bash
sqlite3 weather_data.db "SELECT city, temperature_c, weather_time FROM weather_readings ORDER BY ingested_at DESC LIMIT 10;"
```

## Possible extensions (good talking points in an interview)

- Swap SQLite for PostgreSQL/MySQL and use SQLAlchemy
- Orchestrate scheduled runs with **Airflow** or **Prefect** instead of a manual script
- Add a **dbt** layer on top for downstream transformations/aggregations
- Containerize with **Docker** and deploy on a schedule (cron, AWS Lambda, GitHub Actions)
- Add unit tests with `pytest` for `transform.py`'s logic
- Add a data quality framework like **Great Expectations**
- Stream instead of batch, using **Kafka**

## Resume bullet point examples

- Built an end-to-end ETL pipeline in Python that extracts data from a public REST API,
  applies validation/deduplication logic with pandas, and loads it into a relational
  database with idempotent writes
- Designed a modular data pipeline architecture (extract/transform/load) with structured
  logging and per-source error handling to ensure partial failures don't halt the pipeline
- Implemented data quality checks (null handling, deduplication) to ensure clean,
  analysis-ready data in downstream storage
