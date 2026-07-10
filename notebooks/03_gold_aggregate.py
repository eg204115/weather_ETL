# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Daily Summary
# MAGIC Aggregates Silver readings into a daily per-city summary table, ready for
# MAGIC BI tools (Power BI, Tableau) or ad-hoc SQL analytics.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)

gold_df = (
    silver_df
    .withColumn("reading_date", F.to_date("weather_time"))
    .groupBy("city", "reading_date")
    .agg(
        F.round(F.avg("temperature_c"), 1).alias("avg_temperature_c"),
        F.round(F.max("temperature_c"), 1).alias("max_temperature_c"),
        F.round(F.min("temperature_c"), 1).alias("min_temperature_c"),
        F.round(F.avg("windspeed_kmh"), 1).alias("avg_windspeed_kmh"),
        F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
        F.count("*").alias("reading_count"),
    )
)

display(gold_df)

# COMMAND ----------

# Gold is fully recomputed from Silver each run (small aggregate table,
# so a full overwrite is simpler and cheap here; for larger datasets you'd
# window this to only recompute recent partitions).
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
    USING DELTA
    LOCATION '{GOLD_PATH}'
""")

print(f"Gold aggregation complete: {gold_df.count()} city/day summary rows written to {GOLD_TABLE}")
