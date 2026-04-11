"""
Check if Delta Lake tables exist in MinIO.
Used by run.sh and Airflow DAGs to skip data generation when data already exists.

Usage:
    python -m src.check_data [--layer bronze|silver|gold] [--table TABLE_NAME]

Exit codes:
    0 = data exists
    1 = data does NOT exist
"""
import sys
import logging
from deltalake import DeltaTable
from src.config import (
    S3_BRONZE_PATH, S3_SILVER_PATH, S3_GOLD_PATH,
    DELTA_STORAGE_OPTIONS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BRONZE_TABLES = {
    "sensor_stream": f"{S3_BRONZE_PATH}/sensor_stream",
    "weather_raw": f"{S3_BRONZE_PATH}/weather_raw",
    "satellite_raw": f"{S3_BRONZE_PATH}/satellite_raw",
    "ocr_text": f"{S3_BRONZE_PATH}/ocr_text",
    "image_metadata": f"{S3_BRONZE_PATH}/image_metadata",
}

SILVER_TABLES = {
    "sensor_clean": f"{S3_SILVER_PATH}/sensor_clean",
    "weather_clean": f"{S3_SILVER_PATH}/weather_clean",
    "satellite_clean": f"{S3_SILVER_PATH}/satellite_clean",
    "ocr_processed": f"{S3_SILVER_PATH}/ocr_processed",
    "image_processed": f"{S3_SILVER_PATH}/image_processed",
}

GOLD_TABLES = {
    "feature_store": f"{S3_GOLD_PATH}/feature_store",
    "forest_health_index": f"{S3_GOLD_PATH}/forest_health_index",
    "plot_summary": f"{S3_GOLD_PATH}/plot_summary",
}

LAYERS = {
    "bronze": BRONZE_TABLES,
    "silver": SILVER_TABLES,
    "gold": GOLD_TABLES,
}


def table_exists(path: str) -> bool:
    try:
        dt = DeltaTable(path, storage_options=DELTA_STORAGE_OPTIONS)
        df = dt.to_pandas()
        return len(df) > 0
    except Exception:
        return False


def check_layer(layer: str) -> bool:
    tables = LAYERS.get(layer, {})
    if not tables:
        log.error(f"Unknown layer: {layer}")
        return False

    all_exist = True
    for name, path in tables.items():
        exists = table_exists(path)
        status = "✓ exists" if exists else "✗ missing"
        log.info(f"  {layer}/{name}: {status}")
        if not exists:
            all_exist = False

    return all_exist


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Check Delta table existence")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], default=None)
    parser.add_argument("--table", type=str, default=None)
    args = parser.parse_args()

    if args.table:
        # Check specific table
        path = None
        for layer_tables in LAYERS.values():
            if args.table in layer_tables:
                path = layer_tables[args.table]
                break
        if not path:
            log.error(f"Unknown table: {args.table}")
            sys.exit(1)
        exists = table_exists(path)
        log.info(f"{args.table}: {'exists' if exists else 'missing'}")
        sys.exit(0 if exists else 1)

    if args.layer:
        log.info(f"Checking {args.layer} layer...")
        exists = check_layer(args.layer)
        sys.exit(0 if exists else 1)

    # Check all layers
    all_ok = True
    for layer in ["bronze", "silver", "gold"]:
        log.info(f"Checking {layer} layer...")
        if not check_layer(layer):
            all_ok = False

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
