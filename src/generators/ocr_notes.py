"""
Generate synthetic OCR field notes (already OCR-processed text).
Content: disease reports, drought observations, pest sightings, damage assessments.
2-5 notes per week over 1 year.
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

OCR_PATH = f"{S3_BRONZE_PATH}/ocr_text"

AUTHORS = [
    "Nguyen Van A", "Tran Thi B", "Le Van C",
    "Pham Minh D", "Hoang Thi E",
]

# Templates with keywords embedded
TEMPLATES = {
    "disease": [
        "Phat hien benh phan trang tren la cay keo tai lo {plot_id}. Khoang {pct}% dien tich bi anh huong. Can phun thuoc diet nam.",
        "Cay bi nam re, heo vang la o lo {plot_id}. So cay bi benh: {count}. De xuat xu ly thuoc diet nam.",
        "Benh dom la xuat hien tren cay keo {plot_id}. Muc do: {severity}. Da bao cao ban quan ly.",
        "Lo {plot_id} co dau hieu benh tham canh. La cay bi dom nau va rung som. Can kiem tra them.",
    ],
    "pest": [
        "Sau duc than xuat hien nhieu tai lo {plot_id}. Gay hai khoang {count} cay. Can phun thuoc tru sau.",
        "Phat hien moi tan cong goc cay tai {plot_id}, dac biet o khu vuc am thap. Khoang {count} cay bi anh huong.",
        "Lo {plot_id} bi ray pha hai la non. Muc do {severity}. Da xu ly thuoc bao ve thuc vat.",
        "Bo canh cung xuat hien tai lo {plot_id}. An la va vo cay. So luong: {severity}.",
    ],
    "drought": [
        "Cay bi stress nuoc nghiem trong tai lo {plot_id}. La kho heo. Can tuoi bo sung khan cap.",
        "Tinh trang han han keo dai. Dat nut ne o lo {plot_id}. NDVI giam manh. Cay co dau hieu chet kho.",
        "Lo {plot_id} thieu nuoc tram trong. Do am dat rat thap. Can he thong tuoi nham.",
        "Canh bao han tai lo {plot_id}. Nhiet do cao bat thuong. Cay non bi anh huong nang.",
    ],
    "damage": [
        "Bao gay ganh nhieu cay tai lo {plot_id}. Can don dep va trong bo sung. So cay hong: {count}.",
        "Gio lon gay do {count} cay keo tai {plot_id}. Thiet hai uoc tinh: {severity}.",
        "Sat lo dat nho anh huong lo {plot_id}. Mat khoang {pct}% dien tich trong. Can khac phuc.",
        "Lo {plot_id} bi ngap nuoc sau mua lon. {count} cay bi ung re. Can thoat nuoc gap.",
    ],
    "healthy": [
        "Lo {plot_id} phat trien tot. Cay xanh tot, khong co sau benh. NDVI on dinh.",
        "Kiem tra dinh ky lo {plot_id}. Cay sinh truong binh thuong. Khong phat hien bat thuong.",
        "Lo {plot_id}: cay keo {age} thang tuoi phat trien deu. Tan la xanh tot. Dat am vua phai.",
    ],
}

# Seasonal event probabilities (by month, 1-indexed)
MONTHLY_WEIGHTS = {
    "disease":  [0.05, 0.05, 0.08, 0.08, 0.10, 0.15, 0.15, 0.12, 0.10, 0.20, 0.25, 0.15],
    "pest":     [0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.18, 0.15, 0.10, 0.08, 0.08, 0.06],
    "drought":  [0.02, 0.05, 0.08, 0.10, 0.20, 0.35, 0.40, 0.25, 0.10, 0.03, 0.02, 0.02],
    "damage":   [0.05, 0.03, 0.03, 0.05, 0.05, 0.05, 0.05, 0.08, 0.15, 0.25, 0.20, 0.10],
    "healthy":  [0.60, 0.60, 0.55, 0.50, 0.40, 0.30, 0.25, 0.35, 0.45, 0.40, 0.35, 0.50],
}


def _fill_template(template: str, plot_id: str) -> str:
    return template.format(
        plot_id=plot_id,
        pct=np.random.randint(5, 40),
        count=np.random.randint(3, 25),
        severity=np.random.choice(["nhe", "trung binh", "nang", "rat nang"]),
        age=np.random.randint(6, 48),
    )


def generate_ocr_notes() -> pd.DataFrame:
    """Generate 1 year of OCR field notes."""
    np.random.seed(456)
    start = pd.Timestamp(HISTORICAL_START)
    end = pd.Timestamp(HISTORICAL_END)

    records = []
    note_id = 1
    current_week = start

    while current_week <= end:
        n_notes = np.random.randint(2, 6)  # 2-5 notes per week

        for _ in range(n_notes):
            day_offset = np.random.randint(0, 7)
            ts = current_week + pd.Timedelta(days=day_offset)
            if ts > end:
                break

            hour = np.random.randint(7, 17)
            ts = ts.replace(hour=hour, minute=np.random.randint(0, 60))

            month_idx = ts.month - 1
            plot_id = np.random.choice(PLOT_IDS)

            # Choose note type based on seasonal weights
            types = list(MONTHLY_WEIGHTS.keys())
            weights = [MONTHLY_WEIGHTS[t][month_idx] for t in types]
            weights = np.array(weights) / sum(weights)
            note_type = np.random.choice(types, p=weights)

            template = np.random.choice(TEMPLATES[note_type])
            text = _fill_template(template, plot_id)

            # Extract keyword flags
            keywords = []
            if note_type != "healthy":
                keywords.append(note_type)
            text_lower = text.lower()
            for kw in ["disease", "benh", "nam"]:
                if kw in text_lower and "disease" not in keywords:
                    keywords.append("disease")
            for kw in ["pest", "sau", "moi", "ray", "bo"]:
                if kw in text_lower and "pest" not in keywords:
                    keywords.append("pest")
            for kw in ["drought", "han", "kho", "stress nuoc"]:
                if kw in text_lower and "drought" not in keywords:
                    keywords.append("drought")
            for kw in ["damage", "gay", "do", "sat lo", "ngap"]:
                if kw in text_lower and "damage" not in keywords:
                    keywords.append("damage")

            records.append({
                "note_id": f"note_{note_id:04d}",
                "plot_id": plot_id,
                "timestamp": ts,
                "text": text,
                "author": np.random.choice(AUTHORS),
                "note_type": note_type,
                "keywords": ",".join(keywords) if keywords else "healthy",
            })
            note_id += 1

        current_week += pd.Timedelta(weeks=1)

    df = pd.DataFrame(records)
    log.info(f"Generated {len(df)} OCR notes")
    return df


def write_ocr_to_bronze(df: pd.DataFrame):
    """Write OCR notes to Bronze Delta table on MinIO."""
    df["year_month"] = df["timestamp"].dt.strftime("%Y-%m")
    write_deltalake(
        f"{S3_BRONZE_PATH}/ocr_text",
        df,
        mode="overwrite",
        partition_by=["year_month"],
        storage_options=DELTA_STORAGE_OPTIONS,
    )
    log.info("OCR notes written to Bronze successfully")


def main():
    df = generate_ocr_notes()
    write_ocr_to_bronze(df)


if __name__ == "__main__":
    main()
