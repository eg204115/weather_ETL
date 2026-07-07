"""
Configuration for the Weather ETL Pipeline.
"""

# Cities to pull data for: name, latitude, longitude
CITIES = [
    {"name": "Colombo", "lat": 6.9271, "lon": 79.8612},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
]

# Open-Meteo requires no API key
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# SQLite database file
DATABASE_PATH = "weather_data.db"

# Logging
LOG_FILE = "pipeline.log"
