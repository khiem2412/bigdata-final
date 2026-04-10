"""
Fetch weather data from Open-Meteo API (free, no API key needed).
Supports both historical fetch and synthetic fallback.
"""
import logging
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from deltalake import write_deltalake

from src.config import (
    FOREST_CENTER_LAT, FOREST_CENTER_LON,
    HISTORICAL_START, HISTORICAL_END,
    S3_BRONZE_PATH, DELTA_STORAGE_OPTIONS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WEATHER_PATH = f"{S3_BRONZE_PATH}/weather_raw"


def fetch_historical_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch historical hourly weather from Open-Meteo archive API."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": FOREST_CENTER_LAT,
        "longitude": FOREST_CENTER_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation",
        "timezone": "Asia/Ho_Chi_Minh",
    }

    log.info(f"Fetching weather data: {start_date} to {end_date}")
    try:
        resp = requests.get(url, params=params, timeout=60)
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
        log.info(f"Fetched {len(df)} hourly weather records from API")
        return df

    except Exception as e:
        log.warning(f"API fetch failed: {e}. Using synthetic fallback.")
        return generate_synthetic_weather(start_date, end_date)


def generate_synthetic_weather(start_date: str, end_date: str) -> pd.DataFrame:
    """Generate synthetic weather data as fallback."""
    timestamps = pd.date_range(start_date, end_date, freq="h")
    n = len(timestamps)
    doy = timestamps.dayofyear.values.astype(float)
    hour = timestamps.hour.values.astype(float)

    np.random.seed(42)

    temp_seasonal = 27.0 + 7.0 * np.sin(2 * np.pi * (doy - 80) / 365)
    temp_daily = 5.0 * np.sin(2 * np.pi * (hour - 6) / 24)
    temperature = temp_seasonal + temp_daily + np.random.normal(0, 1.5, n)

    humidity = 78 - 10 * np.sin(2 * np.pi * (doy - 80) / 365) + np.random.normal(0, 4, n)
    humidity = np.clip(humidity, 35, 100)

    # Precipitation: more in rainy season (Oct-Dec)
    rain_prob = 0.15 + 0.35 * np.clip(np.sin(2 * np.pi * (doy - 200) / 365), 0, 1)
    rain_mask = np.random.random(n) < rain_prob
    precipitation = np.zeros(n)
    precipitation[rain_mask] = np.random.exponential(3.0, rain_mask.sum())
    precipitation = np.round(precipitation, 1)

    df = pd.DataFrame({
        "timestamp": timestamps,
        "temperature_2m": np.round(temperature, 1),
        "relativehumidity_2m": np.round(humidity, 0).astype(int),
        "precipitation": precipitation,
        "latitude": FOREST_CENTER_LAT,
        "longitude": FOREST_CENTER_LON,
        "source": "synthetic",
    })
    log.info(f"Generated {len(df)} synthetic weather records")
    return df


def write_weather_to_bronze(df: pd.DataFrame):
    """Write weather DataFrame to Bronze Delta table on MinIO."""
    df["year_month"] = df["timestamp"].dt.strftime("%Y-%m")
    log.info(f"Writing {len(df)} weather records to {WEATHER_PATH}")
    write_deltalake(
        WEATHER_PATH,
        df,
        mode="overwrite",
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("Weather data written to Bronze successfully")


def main():
    df = fetch_historical_weather(HISTORICAL_START, HISTORICAL_END)
    write_weather_to_bronze(df)


if __name__ == "__main__":
    main()
