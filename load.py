"""
Load module: writes the transformed DataFrame into a SQLite database.
"""

import logging
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT NOT NULL,
    temperature_c REAL,
    windspeed_kmh REAL,
    humidity_pct REAL,
    weather_time TEXT,
    ingested_at TEXT,
    UNIQUE(city, weather_time)
);
"""


def load_to_db(df: pd.DataFrame, db_path: str) -> int:
    """
    Load a DataFrame into the weather_readings table, skipping duplicates.

    Args:
        df: cleaned DataFrame from the transform step.
        db_path: path to the SQLite database file.

    Returns:
        Number of new rows inserted.
    """
    if df.empty:
        logger.warning("No data to load; DataFrame is empty.")
        return 0

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)

        inserted = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO weather_readings
                        (city, temperature_c, windspeed_kmh, humidity_pct, weather_time, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["city"],
                        row["temperature_c"],
                        row["windspeed_kmh"],
                        row["humidity_pct"],
                        row["weather_time"],
                        row["ingested_at"],
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate (city, weather_time) reading already exists
                continue

        conn.commit()
        logger.info(f"Loaded {inserted} new rows into {db_path}")
        return inserted

    finally:
        conn.close()
