# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: Cleaned & Validated
# MAGIC Parses the raw JSON from Bronze into a proper typed schema, applies data
# MAGIC quality checks (nulls, duplicates), and merges into Silver using Delta's
# MAGIC `MERGE INTO` so reruns are idempotent (no duplicate rows on reprocessing).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
)
from delta.tables import DeltaTable

# COMMAND ----------

# Schema for the fields we care about inside the raw JSON blob.
# Only current_weather + the hourly humidity array are needed.
weather_schema = StructType([
    StructField("current_weather", StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("time", StringType()),
    ])),
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType())),
        StructField("relativehumidity_2m", ArrayType(DoubleType())),
    ])),
])

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

parsed_df = (
    bronze_df
    .withColumn("parsed", F.from_json(F.col("raw_json"), weather_schema))
    .withColumn("weather_time", F.to_timestamp(F.col("parsed.current_weather.time")))
    .withColumn("temperature_c", F.col("parsed.current_weather.temperature"))
    .withColumn("windspeed_kmh", F.col("parsed.current_weather.windspeed"))
    # Match the current_weather timestamp to its position in the hourly arrays
    # to pull out the corresponding humidity reading
    .withColumn(
        "humidity_idx",
        F.array_position(F.col("parsed.hourly.time"), F.col("parsed.current_weather.time")) - 1
    )
    .withColumn(
        "humidity_pct",
        F.when(
            F.col("humidity_idx") >= 0,
            F.col("parsed.hourly.relativehumidity_2m")[F.col("humidity_idx")]
        )
    )
    .select("city", "temperature_c", "windspeed_kmh", "humidity_pct", "weather_time", "extracted_at")
)

# COMMAND ----------

# Data quality checks
silver_df = (
    parsed_df
    .filter(F.col("city").isNotNull() & F.col("temperature_c").isNotNull())
    .dropDuplicates(["city", "weather_time"])
    .withColumnRenamed("extracted_at", "ingested_at")
)

display(silver_df)

# COMMAND ----------

# Idempotent upsert: create the table on first run, MERGE on every run after
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.city = source.city AND target.weather_time = source.weather_time"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f"Silver merge complete. {SILVER_TABLE} now has "
      f"{spark.read.format('delta').load(SILVER_PATH).count()} total rows.")
