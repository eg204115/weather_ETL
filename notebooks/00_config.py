# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Config
# MAGIC Shared configuration for the Bronze / Silver / Gold weather pipeline.
# MAGIC Run this notebook first via `%run` from the other notebooks, or as the
# MAGIC first task in the Databricks Job.

# COMMAND ----------

# Widgets let you parameterize the pipeline from the Databricks UI or Jobs API
dbutils.widgets.text("storage_account", "yourstorageaccount", "ADLS Storage Account Name")
dbutils.widgets.text("container", "weather", "ADLS Container Name")
dbutils.widgets.text("catalog", "hive_metastore", "Unity Catalog Name")
dbutils.widgets.text("schema", "weather_pipeline", "Schema/Database Name")

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# ADLS Gen2 paths (using abfss:// protocol). Auth is handled via a Databricks
# secret scope holding a service principal or storage account access key -
# see README for setup. This assumes the storage account is already
# accessible to the cluster (mounted, or via Spark config / Unity Catalog
# external location).
BRONZE_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/weather_readings"
SILVER_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/weather_readings"
GOLD_PATH = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/weather_daily_summary"

BRONZE_TABLE = f"{catalog}.{schema}.bronze_weather_readings"
SILVER_TABLE = f"{catalog}.{schema}.silver_weather_readings"
GOLD_TABLE = f"{catalog}.{schema}.gold_weather_daily_summary"

# COMMAND ----------

CITIES = [
    {"name": "Colombo", "lat": 6.9271, "lon": 79.8612},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# COMMAND ----------

# Ensure the target schema exists (safe to run every time)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"Config loaded. Bronze -> {BRONZE_TABLE}, Silver -> {SILVER_TABLE}, Gold -> {GOLD_TABLE}")
