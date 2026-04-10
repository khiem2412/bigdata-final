"""
Batch ingestion for weather data updates.
Fetches latest weather from Open-Meteo and appends to Bronze Delta table.
"""
import logging
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable

from src.config import (
    FOREST_CENTER_LAT, FOREST_CENTER_LON,
    S3_BRONZE_PATH, DELTA_STORAGE_OPTIONS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WEATHER_PATH = f"{S3_BRONZE_PATH}/weather_raw"


def fetch_recent_weather(hours: int = 48) -> pd.DataFrame:
    """Fetch recent weather data from Open-Meteo forecast API."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": FOREST_CENTER_LAT,
        "longitude": FOREST_CENTER_LON,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation",
        "past_hours": hours,
        "forecast_hours": 0,
        "timezone": "Asia/Ho_Chi_Minh",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        hourly = data["hourly"]
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(hourly["time"]),
            "temperature_2m": hourly["temperature_2m"],
            "relativehumidity_2m": hourly["relativehumidity_2m"],
            "precipitation": hourly["precipitation"],
            "latitude": FOREST_CENTER_LAT,
            "longitude": FOREST_CENTER_LON,
            "source": "open-meteo",
        })
        df["year_month"] = df["timestamp"].dt.strftime("%Y-%m")
        log.info(f"Fetched {len(df)} recent weather records")
        return df
    except Exception as e:
        log.error(f"Failed to fetch weather: {e}")
        return pd.DataFrame()


def main():
    """Fetch and append recent weather to Bronze."""
    log.info("=== Weather Batch Ingestion ===")
    df = fetch_recent_weather()
    if df.empty:
        log.warning("No weather data fetched. Skipping.")
        return

    write_deltalake(
        WEATHER_PATH,
        df,
        mode="append",
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("Weather batch ingestion complete")


if __name__ == "__main__":
    main()
