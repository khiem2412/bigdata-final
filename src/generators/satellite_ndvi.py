"""
Generate synthetic NDVI satellite data.
Temporal resolution: every 5-10 days per plot.
Simulates Sentinel-2 NDVI time series for acacia forest.
"""
import logging
import sys
import numpy as np
import pandas as pd
from deltalake import write_deltalake

from src.config import (
    HISTORICAL_START, HISTORICAL_END,
    S3_BRONZE_PATH, DELTA_STORAGE_OPTIONS,
)
from src.generators.plots import PLOTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NDVI_PATH = f"{S3_BRONZE_PATH}/satellite_raw"

# Disease outbreak config: affects specific plots during a period
DISEASE_PLOTS = ["plot_02_03", "plot_02_04", "plot_03_03", "plot_03_04"]
DISEASE_START_DOY = 300  # late October
DISEASE_END_DOY = 350    # mid December

# Drought stress: Jun-Jul
DROUGHT_START_DOY = 150
DROUGHT_END_DOY = 210


def generate_ndvi_series() -> pd.DataFrame:
    """Generate NDVI time series for all 25 plots."""
    np.random.seed(123)
    start = pd.Timestamp(HISTORICAL_START)
    end = pd.Timestamp(HISTORICAL_END)

    records = []

    for plot in PLOTS:
        # Base NDVI for healthy acacia: 0.55-0.75
        base_ndvi = 0.65 + np.random.uniform(-0.05, 0.05)

        # Generate observation dates (every 5-10 days, with cloud gaps)
        current = start
        while current <= end:
            doy = current.timetuple().tm_yday

            # Seasonal component: slightly higher in rainy season
            seasonal = 0.05 * np.sin(2 * np.pi * (doy - 250) / 365)

            ndvi = base_ndvi + seasonal + np.random.normal(0, 0.02)

            # Drought stress: Jun-Jul → NDVI drops
            if DROUGHT_START_DOY <= doy <= DROUGHT_END_DOY:
                drought_severity = 0.5 * (
                    1 + np.cos(np.pi * (doy - DROUGHT_START_DOY)
                               / (DROUGHT_END_DOY - DROUGHT_START_DOY))
                )
                ndvi -= 0.12 * drought_severity + np.random.normal(0, 0.02)

            # Disease outbreak for affected plots
            if plot["plot_id"] in DISEASE_PLOTS:
                if DISEASE_START_DOY <= doy <= DISEASE_END_DOY:
                    progress = (doy - DISEASE_START_DOY) / (
                        DISEASE_END_DOY - DISEASE_START_DOY
                    )
                    ndvi -= 0.20 * progress + np.random.normal(0, 0.02)

            ndvi = round(np.clip(ndvi, 0.1, 0.95), 4)

            records.append({
                "plot_id": plot["plot_id"],
                "date": current.strftime("%Y-%m-%d"),
                "ndvi": ndvi,
                "latitude": plot["latitude"],
                "longitude": plot["longitude"],
                "source": "synthetic_sentinel2",
                "cloud_cover": round(np.random.uniform(0, 0.4), 2),
            })

            # Next observation: 5-10 days
            gap = np.random.randint(5, 11)
            # Simulate cloud-affected gaps (20% chance of extra delay)
            if np.random.random() < 0.2:
                gap += np.random.randint(3, 8)
            current += pd.Timedelta(days=int(gap))

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    log.info(f"Generated {len(df)} NDVI observations for {len(PLOTS)} plots")
    return df


def write_ndvi_to_bronze(df: pd.DataFrame):
    """Write NDVI data to Bronze Delta table on MinIO."""
    df["year_month"] = df["date"].dt.strftime("%Y-%m")
    log.info(f"Writing {len(df)} NDVI records to {NDVI_PATH}")
    write_deltalake(
        NDVI_PATH,
        df,
        mode="overwrite",
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("NDVI data written to Bronze successfully")


def main():
    # Try fetching real Sentinel-2 data first
    try:
        from src.ingestion.satellite_ingest import fetch_historical, write_to_bronze

        log.info("Attempting to fetch real Sentinel-2 NDVI from Planetary Computer...")
        df = fetch_historical()
        if not df.empty:
            write_to_bronze(df, mode="overwrite")
            log.info(f"Successfully ingested {len(df)} real satellite NDVI records")
            return
        log.warning("No real satellite data returned. Falling back to synthetic.")
    except Exception as e:
        log.warning(f"Real satellite fetch failed: {e}")
        log.info("Falling back to synthetic NDVI generation...")

    # Fallback: generate synthetic data
    df = generate_ndvi_series()
    write_ndvi_to_bronze(df)


if __name__ == "__main__":
    main()
