"""
Load module: writes the transformed DataFrame into a PostgreSQL database.
"""

import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_readings (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    temperature_c REAL,
    windspeed_kmh REAL,
    humidity_pct REAL,
    weather_time TIMESTAMP,
    ingested_at TIMESTAMP,
    UNIQUE(city, weather_time)
);
"""

INSERT_SQL = """
INSERT INTO weather_readings
    (city, temperature_c, windspeed_kmh, humidity_pct, weather_time, ingested_at)
VALUES %s
ON CONFLICT (city, weather_time) DO NOTHING;
"""


def load_to_db(df: pd.DataFrame, pg_config: dict) -> int:
    """
    Load a DataFrame into the weather_readings table in PostgreSQL,
    skipping rows that already exist (same city + weather_time).

    Args:
        df: cleaned DataFrame from the transform step.
        pg_config: dict with host, port, dbname, user, password.

    Returns:
        Number of new rows inserted.
    """
    if df.empty:
        logger.warning("No data to load; DataFrame is empty.")
        return 0

    conn = None
    try:
        conn = psycopg2.connect(**pg_config)
        cursor = conn.cursor()

        cursor.execute(CREATE_TABLE_SQL)

        records = [
            (
                row["city"],
                row["temperature_c"],
                row["windspeed_kmh"],
                row["humidity_pct"],
                row["weather_time"],
                row["ingested_at"],
            )
            for _, row in df.iterrows()
        ]

        # Count rows before/after to figure out how many were actually new,
        # since ON CONFLICT DO NOTHING doesn't report affected row counts per-row.
        cursor.execute("SELECT COUNT(*) FROM weather_readings;")
        before_count = cursor.fetchone()[0]

        execute_values(cursor, INSERT_SQL, records)

        cursor.execute("SELECT COUNT(*) FROM weather_readings;")
        after_count = cursor.fetchone()[0]

        conn.commit()
        inserted = after_count - before_count
        logger.info(f"Loaded {inserted} new rows into PostgreSQL ({pg_config['dbname']})")
        return inserted

    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to PostgreSQL: {e}")
        raise

    finally:
        if conn is not None:
            conn.close()
