"""
Data validation for the Forest Lakehouse.
Checks data completeness, quality, and consistency across layers.
"""
import logging
import sys
from deltalake import DeltaTable
from src.config import S3_BRONZE_PATH, S3_SILVER_PATH, S3_GOLD_PATH, DELTA_STORAGE_OPTIONS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


TABLES = {
    "bronze": [
        ("sensor_stream", f"{S3_BRONZE_PATH}/sensor_stream"),
        ("weather_raw", f"{S3_BRONZE_PATH}/weather_raw"),
        ("satellite_raw", f"{S3_BRONZE_PATH}/satellite_raw"),
        ("ocr_text", f"{S3_BRONZE_PATH}/ocr_text"),
        ("image_metadata", f"{S3_BRONZE_PATH}/image_metadata"),
    ],
    "silver": [
        ("sensor_clean", f"{S3_SILVER_PATH}/sensor_clean"),
        ("weather_clean", f"{S3_SILVER_PATH}/weather_clean"),
        ("satellite_clean", f"{S3_SILVER_PATH}/satellite_clean"),
        ("ocr_processed", f"{S3_SILVER_PATH}/ocr_processed"),
        ("image_processed", f"{S3_SILVER_PATH}/image_processed"),
    ],
    "gold": [
        ("feature_store", f"{S3_GOLD_PATH}/feature_store"),
        ("forest_health_index", f"{S3_GOLD_PATH}/forest_health_index"),
        ("plot_summary", f"{S3_GOLD_PATH}/plot_summary"),
    ],
}


def validate_table(name: str, path: str) -> dict:
    """Validate a single Delta table."""
    result = {"name": name, "path": path, "status": "UNKNOWN"}
    try:
        dt = DeltaTable(path, storage_options=DELTA_STORAGE_OPTIONS)
        df = dt.to_pandas()
        row_count = len(df)
        col_count = len(df.columns)
        null_pct = (df.isnull().sum().sum() / (row_count * col_count) * 100) if row_count > 0 else 0

        result["status"] = "OK" if row_count > 0 else "EMPTY"
        result["rows"] = row_count
        result["columns"] = col_count
        result["null_pct"] = round(null_pct, 2)
        result["schema"] = list(df.columns)

        if "plot_id" in df.columns:
            result["unique_plots"] = df["plot_id"].nunique()
        if "date" in df.columns or "timestamp" in df.columns:
            date_col = "date" if "date" in df.columns else "timestamp"
            result["date_min"] = str(df[date_col].min())
            result["date_max"] = str(df[date_col].max())

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result


def main():
    log.info("========================================")
    log.info("  DATA VALIDATION")
    log.info("========================================")

    all_ok = True
    total_rows = 0

    for layer, tables in TABLES.items():
        log.info(f"\n--- {layer.upper()} Layer ---")

        for name, path in tables:
            result = validate_table(name, path)
            status = result["status"]

            if status == "OK":
                rows = result["rows"]
                total_rows += rows
                log.info(
                    f"  ✓ {name}: {rows:,} rows, {result['columns']} cols, "
                    f"null={result['null_pct']}%"
                )
                if "unique_plots" in result:
                    log.info(f"    plots={result['unique_plots']}, "
                             f"range={result.get('date_min', 'N/A')} → {result.get('date_max', 'N/A')}")
            elif status == "EMPTY":
                log.warning(f"  ⚠ {name}: EMPTY (0 rows)")
                all_ok = False
            else:
                log.error(f"  ✗ {name}: {result.get('error', 'Unknown error')}")
                all_ok = False

    log.info(f"\n{'='*40}")
    log.info(f"Total rows across all tables: {total_rows:,}")

    if all_ok:
        log.info("VALIDATION PASSED ✓")
        return 0
    else:
        log.warning("VALIDATION COMPLETED WITH WARNINGS")
        return 1


if __name__ == "__main__":
    sys.exit(main())
