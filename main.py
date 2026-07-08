"""
Main entry point: orchestrates the extract -> transform -> load pipeline.

Run with:
    python main.py
"""

import logging
import sys

from config import CITIES, POSTGRES_CONFIG, LOG_FILE
from extract import extract_weather_data
from transform import transform_data
from load import load_to_db


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout),
        ],
    )


def run_pipeline():
    logger = logging.getLogger("pipeline")
    logger.info("=== Pipeline run started ===")

    try:
        raw_data = extract_weather_data(CITIES)
        clean_df = transform_data(raw_data)
        rows_inserted = load_to_db(clean_df, POSTGRES_CONFIG)

        logger.info(f"=== Pipeline run finished successfully. {rows_inserted} new rows added. ===")

    except Exception as e:
        logger.exception(f"Pipeline run failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    run_pipeline()
