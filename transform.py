"""
Transform module: cleans and reshapes raw API data into a tidy DataFrame.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


def transform_data(raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert raw API JSON responses into a clean, flat DataFrame.

    Args:
        raw_records: list of raw JSON dicts from the extract step.

    Returns:
        A pandas DataFrame with one row per city containing:
        city, temperature_c, windspeed_kmh, humidity_pct, weather_time, ingested_at
    """
    rows = []

    for record in raw_records:
        try:
            current = record["current_weather"]

            # Grab the humidity reading matching the current weather timestamp
            hourly_times = record.get("hourly", {}).get("time", [])
            hourly_humidity = record.get("hourly", {}).get("relativehumidity_2m", [])
            humidity = None
            if current["time"] in hourly_times:
                idx = hourly_times.index(current["time"])
                humidity = hourly_humidity[idx]

            rows.append({
                "city": record["city_name"],
                "temperature_c": current["temperature"],
                "windspeed_kmh": current["windspeed"],
                "humidity_pct": humidity,
                "weather_time": current["time"],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })

        except (KeyError, ValueError, IndexError) as e:
            logger.warning(f"Skipping malformed record for {record.get('city_name', 'unknown')}: {e}")
            continue

    df = pd.DataFrame(rows)

    # Basic data quality checks
    df = df.dropna(subset=["city", "temperature_c"])
    df = df.drop_duplicates(subset=["city", "weather_time"])

    logger.info(f"Transformed {len(df)} clean records out of {len(raw_records)} raw records")

    return df
