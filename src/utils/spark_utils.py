"""
Spark session utilities for the Forest Lakehouse.
"""
from pyspark.sql import SparkSession
from src.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    SPARK_DRIVER_MEMORY, SPARK_CORES,
)


def get_spark_session(app_name: str = "ForestLakehouse") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{SPARK_CORES}]")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning for low-resource environment
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.memory.fraction", "0.7")
        .config("spark.memory.storageFraction", "0.3")
        # Reduce logging noise
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
