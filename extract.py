"""
Extract module: pulls raw weather data from the Open-Meteo API for a list of cities.
"""

import logging
import requests
from typing import List, Dict, Any

from config import API_BASE_URL

logger = logging.getLogger(__name__)


def extract_weather_data(cities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fetch current weather data for each city.

    Args:
        cities: list of dicts with keys 'name', 'lat', 'lon'.

    Returns:
        List of raw API response dicts, each tagged with the city name.
        Cities that fail to fetch are skipped (and logged) rather than
        crashing the whole pipeline.
    """
    raw_records = []

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
            data = response.json()
            data["city_name"] = city["name"]
            raw_records.append(data)
            logger.info(f"Successfully extracted data for {city['name']}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to extract data for {city['name']}: {e}")
            continue

    if not raw_records:
        raise RuntimeError("Extraction failed for all cities. Aborting pipeline.")

    return raw_records
