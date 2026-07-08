"""
Configuration for the Weather ETL Pipeline.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Cities to pull data for: name, latitude, longitude
CITIES = [
    {"name": "Colombo", "lat": 6.9271, "lon": 79.8612},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
]

# API Configuration
API_BASE_URL = os.getenv(
    "API_BASE_URL",
    "https://api.open-meteo.com/v1/forecast"
)

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "weather_etl"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# Logging
LOG_FILE = os.getenv("LOG_FILE", "pipeline.log")