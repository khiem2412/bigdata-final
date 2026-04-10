"""
Generate synthetic image metadata for field inspection photos.
2-5 images per week, associated with plots.
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
from src.generators.plots import PLOT_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

IMAGE_PATH = f"{S3_BRONZE_PATH}/image_metadata"

DESCRIPTION_TEMPLATES = [
    "Anh tong quan lo {plot_id}, cay phat trien {status}.",
    "Can canh la cay keo tai {plot_id}. Trang thai: {status}.",
    "Anh chup mat dat lo {plot_id}. Do am dat: {moisture}.",
    "Kiem tra tan la {plot_id}. Mau sac: {color}.",
    "Anh goc cay keo {plot_id}. Tinh trang vo cay: {bark}.",
    "Toan canh lo {plot_id} tu goc {direction}.",
]

STATUSES = ["tot", "binh thuong", "hoi yeu", "yeu", "rat yeu"]
COLORS = ["xanh dam", "xanh nhat", "vang nhe", "vang nau", "nau"]
BARKS = ["binh thuong", "co vet nut", "bi boc", "co nam", "tot"]
DIRECTIONS = ["Dong", "Tay", "Nam", "Bac", "Dong Bac", "Tay Nam"]
MOISTURES = ["cao", "vua", "thap", "rat thap"]

TAGS_MAP = {
    "tot": ["healthy", "growth"],
    "binh thuong": ["normal"],
    "hoi yeu": ["stress", "monitoring"],
    "yeu": ["stress", "disease"],
    "rat yeu": ["severe", "disease", "urgent"],
}


def generate_image_metadata() -> pd.DataFrame:
    """Generate 1 year of image metadata."""
    np.random.seed(789)
    start = pd.Timestamp(HISTORICAL_START)
    end = pd.Timestamp(HISTORICAL_END)

    records = []
    img_id = 1
    current_week = start

    while current_week <= end:
        n_images = np.random.randint(2, 6)

        for _ in range(n_images):
            day_offset = np.random.randint(0, 7)
            ts = current_week + pd.Timedelta(days=day_offset)
            if ts > end:
                break

            hour = np.random.randint(8, 16)
            ts = ts.replace(hour=hour, minute=np.random.randint(0, 60))

            plot_id = np.random.choice(PLOT_IDS)
            status = np.random.choice(STATUSES, p=[0.4, 0.3, 0.15, 0.10, 0.05])

            template = np.random.choice(DESCRIPTION_TEMPLATES)
            description = template.format(
                plot_id=plot_id,
                status=status,
                moisture=np.random.choice(MOISTURES),
                color=np.random.choice(COLORS),
                bark=np.random.choice(BARKS),
                direction=np.random.choice(DIRECTIONS),
            )

            date_str = ts.strftime("%Y%m%d_%H%M")
            filename = f"{plot_id}_{date_str}.jpg"

            records.append({
                "image_id": f"img_{img_id:04d}",
                "plot_id": plot_id,
                "timestamp": ts,
                "filename": filename,
                "description": description,
                "tags": ",".join(TAGS_MAP.get(status, ["unknown"])),
            })
            img_id += 1

        current_week += pd.Timedelta(weeks=1)

    df = pd.DataFrame(records)
    log.info(f"Generated {len(df)} image metadata records")
    return df


def write_images_to_bronze(df: pd.DataFrame):
    """Write image metadata to Bronze Delta table on MinIO."""
    df["year_month"] = df["timestamp"].dt.strftime("%Y-%m")
    write_deltalake(
        IMAGE_PATH,
        df,
        mode="overwrite",
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("Image metadata written to Bronze successfully")


def main():
    df = generate_image_metadata()
    write_images_to_bronze(df)


if __name__ == "__main__":
    main()
