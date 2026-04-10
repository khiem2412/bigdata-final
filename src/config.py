"""
Shared configuration for the Forest Lakehouse system.
"""
import os

# === MinIO ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

# === Kafka ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_SENSOR = os.getenv("KAFKA_TOPIC_SENSOR", "sensor-readings")

# === Forest Layout ===
FOREST_CENTER_LAT = float(os.getenv("FOREST_CENTER_LAT", "15.3350"))
FOREST_CENTER_LON = float(os.getenv("FOREST_CENTER_LON", "108.2528"))
NUM_PLOTS_X = int(os.getenv("NUM_PLOTS_X", "5"))
NUM_PLOTS_Y = int(os.getenv("NUM_PLOTS_Y", "5"))
SENSORS_PER_PLOT = int(os.getenv("SENSORS_PER_PLOT", "3"))

# === Date Range ===
HISTORICAL_START = os.getenv("HISTORICAL_START", "2024-04-10")
HISTORICAL_END = os.getenv("HISTORICAL_END", "2025-04-09")

# === S3A Paths ===
BRONZE_PATH = f"s3a://{MINIO_BUCKET}/bronze"
SILVER_PATH = f"s3a://{MINIO_BUCKET}/silver"
GOLD_PATH = f"s3a://{MINIO_BUCKET}/gold"
CHECKPOINT_PATH = "s3a://checkpoints"

# === S3 Paths (for deltalake Python library) ===
S3_BRONZE_PATH = f"s3://{MINIO_BUCKET}/bronze"
S3_SILVER_PATH = f"s3://{MINIO_BUCKET}/silver"
S3_GOLD_PATH = f"s3://{MINIO_BUCKET}/gold"

# === Delta Lake Storage Options (for deltalake Python library) ===
DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# === Spark ===
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
SPARK_CORES = int(os.getenv("SPARK_CORES", "2"))
