"""
Bronze → Silver transformations using PySpark + Delta Lake.

Transforms:
1. Sensor data: clean, standardize, detect anomalies
2. Weather data: clean, standardize
3. Satellite NDVI: clean, compute anomalies
4. OCR notes: extract keyword flags
5. Image metadata: clean
"""
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, lit, when, avg, stddev,
    abs as spark_abs, lag, lower, array_contains, split,
    year, month, dayofmonth, concat_ws, lpad,
    expr, window, count, sum as spark_sum, max as spark_max,
    min as spark_min, first, last, regexp_extract,
    current_timestamp, date_format,
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType

from src.config import BRONZE_PATH, SILVER_PATH
from src.utils.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def transform_sensor(spark: SparkSession):
    """Clean sensor data + detect anomalies."""
    log.info("--- Transforming sensor data ---")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/sensor_stream")
    log.info("  Loaded sensor records from Bronze (detecting anomalies...)")

    # Basic cleaning
    clean = (
        df
        .filter(col("temperature").isNotNull())
        .filter(col("humidity").isNotNull())
        .filter(col("soil_moisture").isNotNull())
        .filter(col("temperature").between(-10, 60))
        .filter(col("humidity").between(0, 100))
        .filter(col("soil_moisture").between(0, 1))
        .withColumn("date", to_date(col("timestamp")))
    )

    # Compute global stats per sensor via aggregation (much faster than rolling window)
    sensor_stats = clean.groupBy("sensor_id").agg(
        avg("temperature").alias("temp_avg"),
        stddev("temperature").alias("temp_std"),
        avg("soil_moisture").alias("sm_avg"),
        stddev("soil_moisture").alias("sm_std"),
    )

    # Join stats back and compute anomaly flags
    with_stats = clean.join(sensor_stats, on="sensor_id", how="left")

    anomalies = (
        with_stats
        .withColumn(
            "is_temp_anomaly",
            when(col("temperature") > 40, True)
            .when(
                spark_abs(col("temperature") - col("temp_avg"))
                > 3 * col("temp_std"),
                True,
            )
            .otherwise(False),
        )
        .withColumn(
            "is_moisture_anomaly",
            when(col("soil_moisture") < 0.10, True)
            .when(
                spark_abs(col("soil_moisture") - col("sm_avg"))
                > 3 * col("sm_std"),
                True,
            )
            .otherwise(False),
        )
        .withColumn(
            "is_anomaly",
            col("is_temp_anomaly") | col("is_moisture_anomaly"),
        )
        .withColumnRenamed("temp_avg", "temp_rolling_avg")
        .withColumnRenamed("sm_avg", "sm_rolling_avg")
    )

    # Select final columns
    silver_sensor = (
        anomalies
        .select(
            "sensor_id", "plot_id", "timestamp", "date",
            "temperature", "humidity", "soil_moisture",
            "latitude", "longitude",
            "is_temp_anomaly", "is_moisture_anomaly", "is_anomaly",
            "temp_rolling_avg", "sm_rolling_avg",
        )
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    silver_sensor.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{SILVER_PATH}/sensor_clean")

    # Read back from written Delta table to avoid recomputing
    written = spark.read.format("delta").load(f"{SILVER_PATH}/sensor_clean")
    count = written.count()
    anomaly_count = written.filter(col("is_anomaly")).count()
    log.info(f"  Silver sensor: {count:,} rows, {anomaly_count:,} anomalies")


def transform_weather(spark: SparkSession):
    """Clean and standardize weather data."""
    log.info("--- Transforming weather data ---")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/weather_raw")

    clean = (
        df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("date", to_date(col("timestamp")))
        .filter(col("temperature_2m").isNotNull())
        .dropDuplicates(["timestamp"])
        .withColumnRenamed("temperature_2m", "temperature")
        .withColumnRenamed("relativehumidity_2m", "humidity")
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    clean.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{SILVER_PATH}/weather_clean")

    log.info(f"  Silver weather: {clean.count():,} rows")


def transform_ndvi(spark: SparkSession):
    """Clean NDVI data and compute anomaly flags."""
    log.info("--- Transforming NDVI data ---")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/satellite_raw")

    clean = (
        df
        .withColumn("date", to_date(col("date")))
        .filter(col("ndvi").isNotNull())
        .filter(col("ndvi").between(0, 1))
    )

    # Compute 30-day rolling average NDVI per plot
    w30 = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )
    w_prev = Window.partitionBy("plot_id").orderBy("date")

    with_stats = (
        clean
        .withColumn("ndvi_30d_avg", avg("ndvi").over(w30))
        .withColumn("ndvi_prev", lag("ndvi", 1).over(w_prev))
        .withColumn(
            "ndvi_change_pct",
            when(
                col("ndvi_prev").isNotNull(),
                (col("ndvi") - col("ndvi_prev")) / col("ndvi_prev") * 100,
            ),
        )
        .withColumn(
            "is_ndvi_anomaly",
            when(
                col("ndvi_30d_avg").isNotNull(),
                (col("ndvi") < col("ndvi_30d_avg") * 0.80),
            ).otherwise(False),
        )
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    with_stats.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{SILVER_PATH}/satellite_clean")

    count = with_stats.count()
    anomaly_count = with_stats.filter(col("is_ndvi_anomaly")).count()
    log.info(f"  Silver NDVI: {count:,} rows, {anomaly_count:,} anomalies")


def transform_ocr(spark: SparkSession):
    """Process OCR notes, extract keyword flags."""
    log.info("--- Transforming OCR notes ---")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/ocr_text")

    processed = (
        df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("text_lower", lower(col("text")))
        .withColumn(
            "has_disease",
            col("text_lower").contains("benh")
            | col("text_lower").contains("nam")
            | col("text_lower").contains("disease"),
        )
        .withColumn(
            "has_pest",
            col("text_lower").contains("sau")
            | col("text_lower").contains("moi")
            | col("text_lower").contains("ray")
            | col("text_lower").contains("pest"),
        )
        .withColumn(
            "has_drought",
            col("text_lower").contains("han")
            | col("text_lower").contains("kho heo")
            | col("text_lower").contains("stress nuoc")
            | col("text_lower").contains("drought"),
        )
        .withColumn(
            "has_damage",
            col("text_lower").contains("gay")
            | col("text_lower").contains("do cay")
            | col("text_lower").contains("sat lo")
            | col("text_lower").contains("ngap")
            | col("text_lower").contains("damage"),
        )
        .drop("text_lower")
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    processed.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{SILVER_PATH}/ocr_processed")

    log.info(f"  Silver OCR: {processed.count():,} rows")


def transform_images(spark: SparkSession):
    """Clean image metadata."""
    log.info("--- Transforming image metadata ---")

    df = spark.read.format("delta").load(f"{BRONZE_PATH}/image_metadata")

    clean = (
        df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    clean.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{SILVER_PATH}/image_processed")

    log.info(f"  Silver images: {clean.count():,} rows")


def main():
    log.info("========================================")
    log.info("  BRONZE → SILVER TRANSFORMATION")
    log.info("========================================")

    spark = get_spark_session("BronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    try:
        transform_sensor(spark)
        transform_weather(spark)
        transform_ndvi(spark)
        transform_ocr(spark)
        transform_images(spark)
        log.info("=== All Bronze → Silver transformations complete ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
