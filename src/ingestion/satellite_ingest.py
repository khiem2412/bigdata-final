"""
Real Sentinel-2 NDVI ingestion from Microsoft Planetary Computer.

Fetches Sentinel-2 L2A bands (B04 Red, B08 NIR) and computes NDVI
for each forest monitoring plot.

Usage:
    python -m src.ingestion.satellite_ingest --mode historical
    python -m src.ingestion.satellite_ingest --mode batch --days 7
"""
import logging
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pystac_client
import planetary_computer
import rasterio
from rasterio.env import Env as RioEnv
from rasterio.warp import transform as warp_transform, transform_bounds
from rasterio.transform import rowcol
from rasterio.windows import from_bounds
from deltalake import write_deltalake

from src.config import (
    FOREST_CENTER_LAT, FOREST_CENTER_LON,
    HISTORICAL_START, HISTORICAL_END,
    S3_BRONZE_PATH, DELTA_STORAGE_OPTIONS,
)
from src.generators.plots import PLOTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

NDVI_PATH = f"{S3_BRONZE_PATH}/satellite_raw"

# ── Configuration ──────────────────────────────────────────
STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION = "sentinel-2-l2a"
S2_SCALE = 1.0 / 10000.0        # Sentinel-2 L2A reflectance scale
MAX_CLOUD_COVER = 30             # max cloud cover %
MIN_INTERVAL_DAYS = 4            # min days between selected scenes
SEARCH_BUFFER_DEG = 0.01         # ~1 km buffer for STAC search bbox
WINDOW_BUFFER_DEG = 0.002        # ~200 m buffer for pixel read window


def _search_bbox():
    """Bounding box for STAC catalog search [lon_min, lat_min, lon_max, lat_max]."""
    return [
        FOREST_CENTER_LON - SEARCH_BUFFER_DEG,
        FOREST_CENTER_LAT - SEARCH_BUFFER_DEG,
        FOREST_CENTER_LON + SEARCH_BUFFER_DEG,
        FOREST_CENTER_LAT + SEARCH_BUFFER_DEG,
    ]


def _ndvi_from_scene(item) -> list[dict]:
    """
    Read B04 & B08 for the forest window, compute per-plot NDVI.
    Returns list of dicts matching the existing Bronze satellite_raw schema.
    """
    records = []
    scene_date = item.datetime.strftime("%Y-%m-%d")
    cloud_pct = round(item.properties.get("eo:cloud_cover", 0) / 100.0, 2)

    try:
        b04_href = item.assets["B04"].href
        b08_href = item.assets["B08"].href
    except KeyError as exc:
        log.warning("  Scene %s missing band: %s", item.id, exc)
        return records

    try:
        # Window covering entire forest + buffer (~400m x 400m)
        bbox = [
            FOREST_CENTER_LON - WINDOW_BUFFER_DEG,
            FOREST_CENTER_LAT - WINDOW_BUFFER_DEG,
            FOREST_CENTER_LON + WINDOW_BUFFER_DEG,
            FOREST_CENTER_LAT + WINDOW_BUFFER_DEG,
        ]

        with rasterio.open(b04_href) as red_ds:
            crs = red_ds.crs
            proj_bounds = transform_bounds("EPSG:4326", crs, *bbox)
            window = from_bounds(*proj_bounds, transform=red_ds.transform)
            red = red_ds.read(1, window=window).astype(np.float64) * S2_SCALE
            win_tf = red_ds.window_transform(window)

        with rasterio.open(b08_href) as nir_ds:
            nir = nir_ds.read(1, window=window).astype(np.float64) * S2_SCALE

        # Full-window NDVI
        denom = nir + red
        ndvi_arr = np.where(denom > 0, (nir - red) / denom, np.nan)

        # Convert all plot centres to scene CRS in one call
        lons = [p["longitude"] for p in PLOTS]
        lats = [p["latitude"] for p in PLOTS]
        xs, ys = warp_transform("EPSG:4326", crs, lons, lats)

        for idx, plot in enumerate(PLOTS):
            try:
                r, c = rowcol(win_tf, xs[idx], ys[idx])

                # 2×2 patch (≈ 20 m plot at 10 m resolution)
                r0 = max(0, int(r) - 1)
                r1 = min(ndvi_arr.shape[0], int(r) + 1)
                c0 = max(0, int(c) - 1)
                c1 = min(ndvi_arr.shape[1], int(c) + 1)

                patch = ndvi_arr[r0:r1, c0:c1]
                valid = patch[~np.isnan(patch)]
                if valid.size == 0:
                    continue

                ndvi_val = round(float(np.clip(np.nanmean(valid), 0.0, 1.0)), 4)

                records.append({
                    "plot_id": plot["plot_id"],
                    "date": scene_date,
                    "ndvi": ndvi_val,
                    "latitude": plot["latitude"],
                    "longitude": plot["longitude"],
                    "source": "sentinel2_l2a",
                    "cloud_cover": cloud_pct,
                })
            except Exception:
                continue

        log.info(
            "  [%s] %d plots OK  (cloud %.0f%%)",
            scene_date, len(records), cloud_pct * 100,
        )
    except Exception as exc:
        log.warning("  Scene %s failed: %s", item.id, exc)

    return records


# ── Public API ─────────────────────────────────────────────

def fetch_sentinel2_ndvi(start_date: str, end_date: str) -> pd.DataFrame:
    """Query Planetary Computer STAC and compute per-plot NDVI."""
    log.info("Querying Sentinel-2 L2A: %s → %s", start_date, end_date)
    log.info(
        "Area centre (%.4f, %.4f)  cloud < %d%%",
        FOREST_CENTER_LAT, FOREST_CENTER_LON, MAX_CLOUD_COVER,
    )

    catalog = pystac_client.Client.open(
        STAC_API_URL,
        modifier=planetary_computer.sign_inplace,
    )

    search = catalog.search(
        collections=[COLLECTION],
        bbox=_search_bbox(),
        datetime=f"{start_date}/{end_date}",
        query={"eo:cloud_cover": {"lt": MAX_CLOUD_COVER}},
        sortby=[{"field": "datetime", "direction": "asc"}],
    )

    items = list(search.items())
    log.info("Found %d scenes total", len(items))

    if not items:
        log.warning("No Sentinel-2 scenes found for the given date range and area")
        return pd.DataFrame()

    # Keep ≈ 1 scene every MIN_INTERVAL_DAYS (avoid orbit-overlap duplicates)
    selected: list = []
    last_dt = None
    for item in items:
        dt = item.datetime.date()
        if last_dt is None or (dt - last_dt).days >= MIN_INTERVAL_DAYS:
            selected.append(item)
            last_dt = dt

    log.info("Selected %d scenes (~every %d+ days)", len(selected), MIN_INTERVAL_DAYS)

    # Process scenes with GDAL optimisations for COG access
    all_records: list[dict] = []
    with RioEnv(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
        GDAL_HTTP_MULTIPLEX="YES",
        GDAL_HTTP_VERSION=2,
    ):
        for i, item in enumerate(selected, 1):
            log.info(
                "Processing %d/%d: %s (%s)",
                i, len(selected), item.id,
                item.datetime.strftime("%Y-%m-%d"),
            )
            all_records.extend(_ndvi_from_scene(item))

    if not all_records:
        log.warning("No NDVI values computed from any scene")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    log.info(
        "Total: %d NDVI records, %d unique dates, %d plots",
        len(df), df["date"].nunique(), df["plot_id"].nunique(),
    )
    return df


def fetch_historical(start: str = None, end: str = None) -> pd.DataFrame:
    """Fetch 1 year of historical Sentinel-2 NDVI."""
    return fetch_sentinel2_ndvi(start or HISTORICAL_START, end or HISTORICAL_END)


def fetch_recent(days: int = 7) -> pd.DataFrame:
    """Fetch recent NDVI for batch processing."""
    end = datetime.now()
    start = end - timedelta(days=days + 2)  # +2 day buffer for processing delay
    return fetch_sentinel2_ndvi(
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )


def write_to_bronze(df: pd.DataFrame, mode: str = "overwrite"):
    """Write NDVI DataFrame to Bronze Delta table in MinIO."""
    if df.empty:
        log.warning("No data to write")
        return

    df["year_month"] = df["date"].dt.strftime("%Y-%m")
    log.info("Writing %d records → %s (mode=%s)", len(df), NDVI_PATH, mode)
    write_deltalake(
        NDVI_PATH,
        df,
        mode=mode,
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("✓ Satellite NDVI → Bronze")


def main():
    """CLI: python -m src.ingestion.satellite_ingest [--mode historical|batch] [--days N]"""
    import argparse

    p = argparse.ArgumentParser(description="Fetch real Sentinel-2 NDVI")
    p.add_argument(
        "--mode", choices=["historical", "batch"], default="historical",
        help="historical = full year; batch = last N days (append)",
    )
    p.add_argument(
        "--days", type=int, default=7,
        help="Look-back window for batch mode (default: 7)",
    )
    args = p.parse_args()

    if args.mode == "historical":
        log.info("=== Historical Sentinel-2 NDVI Ingestion (1 Year) ===")
        df = fetch_historical()
        write_to_bronze(df, mode="overwrite")
    else:
        log.info("=== Batch Sentinel-2 NDVI (%d days) ===", args.days)
        df = fetch_recent(days=args.days)
        write_to_bronze(df, mode="append")


if __name__ == "__main__":
    main()
