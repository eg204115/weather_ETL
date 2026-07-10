# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Raw Ingestion
# MAGIC Calls the Open-Meteo API for each configured city and lands the raw JSON
# MAGIC response, untouched, into a Bronze Delta table. Bronze = raw source of
# MAGIC truth; no cleaning or validation happens here, so we can always
# MAGIC reprocess from scratch if downstream logic changes.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import Row

# COMMAND ----------

def fetch_raw_weather(cities):
    """Call the API for each city; return list of Rows with raw JSON + metadata."""
    records = []
    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current_weather": "true",
            "hourly": "relativehumidity_2m",
            "timezone": "auto",
        }
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            records.append(
                Row(
                    city=city["name"],
                    raw_json=json.dumps(response.json()),
                    extracted_at=datetime.now(timezone.utc),
                )
            )
        except requests.exceptions.RequestException as e:
            print(f"WARNING: failed to fetch data for {city['name']}: {e}")
            continue
    return records

# COMMAND ----------

raw_records = fetch_raw_weather(CITIES)

if not raw_records:
    raise RuntimeError("No records fetched from API for any city. Aborting bronze ingest.")

bronze_df = spark.createDataFrame(raw_records)

display(bronze_df)

# COMMAND ----------

# Append-only write: Bronze should never delete or overwrite history.
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

# Register/refresh the table in the metastore so it's queryable by name
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print(f"Bronze ingest complete: {bronze_df.count()} raw records written to {BRONZE_TABLE}")
