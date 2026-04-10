"""
Generate 1 year of historical sensor data (5-minute averages).
75 sensors × 288 points/day × 365 days ≈ 7.9M rows.
Generated month-by-month to manage memory.

Includes:
- Seasonal patterns (Central Vietnam climate)
- Daily temperature cycles
- Drought periods (Jun-Jul)
- Disease-correlated anomalies (Oct-Dec for affected plots)
- Random sensor spikes and dropouts
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
from src.generators.plots import SENSORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SENSOR_PATH = f"{S3_BRONZE_PATH}/sensor_stream"

# Plots affected by disease outbreak (Oct-Dec)
DISEASE_PLOTS = {"plot_02_03", "plot_02_04", "plot_03_03", "plot_03_04"}


def generate_month(year: int, month: int, sensors: list) -> pd.DataFrame:
    """Generate sensor data for one month."""
    start = pd.Timestamp(year=year, month=month, day=1)
    if month == 12:
        end = pd.Timestamp(year=year + 1, month=1, day=1) - pd.Timedelta(minutes=5)
    else:
        end = pd.Timestamp(year=year, month=month + 1, day=1) - pd.Timedelta(minutes=5)

    # Clip to historical range
    hist_start = pd.Timestamp(HISTORICAL_START)
    hist_end = pd.Timestamp(HISTORICAL_END)
    start = max(start, hist_start)
    end = min(end, hist_end)
    if start > end:
        return pd.DataFrame()

    timestamps = pd.date_range(start, end, freq="5min")
    n_times = len(timestamps)
    n_sensors = len(sensors)
    total_rows = n_times * n_sensors

    log.info(f"  Generating {year}-{month:02d}: {n_times} timestamps × {n_sensors} sensors = {total_rows:,} rows")

    # Vectorized time features
    doy = np.tile(timestamps.dayofyear.values.astype(float), n_sensors)
    hour = np.tile(
        (timestamps.hour + timestamps.minute / 60.0).values, n_sensors
    )

    # Repeat sensor info
    sensor_ids = np.repeat([s["sensor_id"] for s in sensors], n_times)
    plot_ids = np.repeat([s["plot_id"] for s in sensors], n_times)
    lats = np.repeat([s["latitude"] for s in sensors], n_times)
    lons = np.repeat([s["longitude"] for s in sensors], n_times)
    ts_rep = np.tile(timestamps.values, n_sensors)

    # Per-sensor offset (deterministic based on sensor index)
    sensor_offset = np.repeat(
        np.array([hash(s["sensor_id"]) % 200 / 200.0 - 0.5 for s in sensors]),
        n_times,
    )

    np.random.seed(year * 100 + month)

    # === Temperature ===
    temp_seasonal = 27.0 + 7.0 * np.sin(2 * np.pi * (doy - 80) / 365)
    temp_daily = 5.0 * np.sin(2 * np.pi * (hour - 6) / 24)
    temp_noise = np.random.normal(0, 1.0, total_rows)
    temperature = temp_seasonal + temp_daily + temp_noise + sensor_offset

    # === Humidity ===
    hum_seasonal = 78.0 - 10.0 * np.sin(2 * np.pi * (doy - 80) / 365)
    hum_daily = 3.0 * np.sin(2 * np.pi * (hour - 14) / 24)
    hum_noise = np.random.normal(0, 2.5, total_rows)
    humidity = np.clip(hum_seasonal + hum_daily + hum_noise, 30, 100)

    # === Soil Moisture ===
    sm_seasonal = 0.37 - 0.12 * np.sin(2 * np.pi * (doy - 100) / 365)
    sm_noise = np.random.normal(0, 0.03, total_rows)
    soil_moisture = np.clip(sm_seasonal + sm_noise + sensor_offset * 0.02, 0.05, 0.80)

    # === Drought stress: Jun-Jul (doy 150-210) ===
    drought_mask = (doy >= 150) & (doy <= 210)
    if drought_mask.any():
        drought_factor = np.sin(np.pi * (doy[drought_mask] - 150) / 60)
        temperature[drought_mask] += 4.0 * drought_factor
        humidity[drought_mask] -= 12.0 * drought_factor
        soil_moisture[drought_mask] -= 0.15 * drought_factor

    # === Disease plots: elevated temp, lower moisture (Oct-Dec, doy 275-355) ===
    disease_plot_set = DISEASE_PLOTS
    for i, s in enumerate(sensors):
        if s["plot_id"] in disease_plot_set:
            idx_start = i * n_times
            idx_end = (i + 1) * n_times
            plot_doy = doy[idx_start:idx_end]
            disease_mask = (plot_doy >= 275) & (plot_doy <= 355)
            if disease_mask.any():
                temperature[idx_start:idx_end][disease_mask] += 1.5
                soil_moisture[idx_start:idx_end][disease_mask] -= 0.05

    # === Random anomalies: spikes ===
    spike_mask = np.random.random(total_rows) < 0.002  # 0.2% chance
    temperature[spike_mask] += np.random.uniform(8, 15, spike_mask.sum())

    dropout_mask = np.random.random(total_rows) < 0.001  # 0.1% chance
    soil_moisture[dropout_mask] = np.random.uniform(0.03, 0.08, dropout_mask.sum())

    df = pd.DataFrame({
        "sensor_id": sensor_ids,
        "plot_id": plot_ids,
        "timestamp": ts_rep,
        "temperature": np.round(temperature, 2),
        "humidity": np.round(humidity, 2),
        "soil_moisture": np.round(soil_moisture, 3),
        "latitude": lats,
        "longitude": lons,
    })

    return df


def main():
    """Generate full historical sensor data and write to Bronze."""
    log.info("=== Historical Sensor Data Generation ===")
    log.info(f"Period: {HISTORICAL_START} to {HISTORICAL_END}")
    log.info(f"Sensors: {len(SENSORS)}")

    start = pd.Timestamp(HISTORICAL_START)
    end = pd.Timestamp(HISTORICAL_END)

    # Iterate month by month
    current = start.to_period("M")
    end_period = end.to_period("M")
    total_rows = 0
    first_write = True

    while current <= end_period:
        df = generate_month(current.year, current.month, SENSORS)
        if df.empty:
            current += 1
            continue

        df["year_month"] = df["timestamp"].dt.strftime("%Y-%m")

        mode = "overwrite" if first_write else "append"
        write_deltalake(
            SENSOR_PATH,
            df,
            mode=mode,
            partition_by=["year_month"],
            storage_options=DELTA_STORAGE_OPTIONS,
        )
        total_rows += len(df)
        first_write = False

        log.info(f"  Written {len(df):,} rows for {current}. Total: {total_rows:,}")
        current += 1

        # Free memory
        del df

    log.info(f"=== Historical sensor data complete: {total_rows:,} total rows ===")


if __name__ == "__main__":
    main()
