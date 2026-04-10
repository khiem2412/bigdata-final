"""
Plot and sensor definitions for the 1-hectare acacia forest.
5x5 grid of plots, each with 3 sensors.
Center: 15.3350, 108.2528
"""
from src.config import (
    FOREST_CENTER_LAT, FOREST_CENTER_LON,
    NUM_PLOTS_X, NUM_PLOTS_Y, SENSORS_PER_PLOT,
)

# 1 hectare = 100m x 100m
# Each plot = 20m x 20m
PLOT_SIZE_M = 20.0
FOREST_SIZE_M = 100.0

# Approximate degree per meter at this latitude
DEG_PER_M_LAT = 1.0 / 111_320.0
DEG_PER_M_LON = 1.0 / (111_320.0 * 0.9659)  # cos(15.335°) ≈ 0.9659


def generate_plots():
    """Generate plot definitions with center coordinates."""
    plots = []
    half = FOREST_SIZE_M / 2.0

    for row in range(1, NUM_PLOTS_Y + 1):
        for col in range(1, NUM_PLOTS_X + 1):
            plot_id = f"plot_{row:02d}_{col:02d}"

            # Offset from center (meters)
            offset_x = (col - 1) * PLOT_SIZE_M - half + PLOT_SIZE_M / 2
            offset_y = (row - 1) * PLOT_SIZE_M - half + PLOT_SIZE_M / 2

            lat = FOREST_CENTER_LAT + offset_y * DEG_PER_M_LAT
            lon = FOREST_CENTER_LON + offset_x * DEG_PER_M_LON

            plots.append({
                "plot_id": plot_id,
                "row": row,
                "col": col,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
            })
    return plots


def generate_sensors(plots=None):
    """Generate sensor definitions (3 per plot)."""
    if plots is None:
        plots = generate_plots()

    sensors = []
    for plot in plots:
        for s in range(1, SENSORS_PER_PLOT + 1):
            sensor_id = f"sensor_{plot['plot_id'].replace('plot_', '')}_{s:02d}"
            # Slight offset within plot
            offset_lat = (s - 2) * 3 * DEG_PER_M_LAT
            offset_lon = (s - 2) * 2 * DEG_PER_M_LON

            sensors.append({
                "sensor_id": sensor_id,
                "plot_id": plot["plot_id"],
                "sensor_num": s,
                "latitude": round(plot["latitude"] + offset_lat, 6),
                "longitude": round(plot["longitude"] + offset_lon, 6),
            })
    return sensors


# Pre-computed for convenience
PLOTS = generate_plots()
SENSORS = generate_sensors(PLOTS)
PLOT_IDS = [p["plot_id"] for p in PLOTS]
SENSOR_IDS = [s["sensor_id"] for s in SENSORS]
