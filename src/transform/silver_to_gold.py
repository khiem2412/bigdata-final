"""
Silver → Gold transformations using PySpark + Delta Lake.

Produces:
1. Forest Health Index (FHI) - per plot, daily
2. Feature Store - per plot, daily grain (AI-ready)
3. Plot Summary - aggregated overview
"""
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit, when, avg, stddev, count, sum as spark_sum,
    max as spark_max, min as spark_min, first, last,
    datediff, expr, coalesce, greatest, least,
    date_format, broadcast, round as spark_round,
)
from pyspark.sql.window import Window

from src.config import SILVER_PATH, GOLD_PATH
from src.utils.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_feature_store(spark: SparkSession):
    """
    Build daily feature store per plot.

    Primary key: (plot_id, date)

    Features:
    - ndvi, ndvi_7d_slope, ndvi_30d_slope
    - rainfall_7d_sum, temperature_7d_avg
    - soil_moisture_avg
    - anomaly_count_7d
    - has_disease_flag, has_drought_flag
    """
    log.info("--- Building Feature Store ---")

    # 1. Daily sensor aggregations per plot
    sensor = spark.read.format("delta").load(f"{SILVER_PATH}/sensor_clean")

    sensor_daily = (
        sensor
        .groupBy("plot_id", "date")
        .agg(
            spark_round(avg("temperature"), 2).alias("temperature_avg"),
            spark_round(avg("humidity"), 2).alias("humidity_avg"),
            spark_round(avg("soil_moisture"), 3).alias("soil_moisture_avg"),
            spark_round(spark_max("temperature"), 2).alias("temperature_max"),
            spark_round(spark_min("temperature"), 2).alias("temperature_min"),
            count(when(col("is_anomaly"), True)).alias("anomaly_count"),
            count(when(col("is_temp_anomaly"), True)).alias("temp_anomaly_count"),
            count(when(col("is_moisture_anomaly"), True)).alias("moisture_anomaly_count"),
        )
    )

    # 2. Rolling window features
    w7 = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )
    w30 = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )

    sensor_features = (
        sensor_daily
        .withColumn("temperature_7d_avg", spark_round(avg("temperature_avg").over(w7), 2))
        .withColumn("soil_moisture_7d_avg", spark_round(avg("soil_moisture_avg").over(w7), 3))
        .withColumn("anomaly_count_7d", spark_sum("anomaly_count").over(w7))
    )

    # 3. Weather daily aggregations
    weather = spark.read.format("delta").load(f"{SILVER_PATH}/weather_clean")

    weather_daily = (
        weather
        .withColumn("date", to_date(col("timestamp")))
        .groupBy("date")
        .agg(
            spark_round(avg("temperature"), 2).alias("weather_temp_avg"),
            spark_round(spark_sum("precipitation"), 1).alias("rainfall_daily"),
        )
    )

    # Weather rolling
    w7_date = (
        Window.orderBy(col("date").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )
    weather_features = (
        weather_daily
        .withColumn("rainfall_7d_sum", spark_round(spark_sum("rainfall_daily").over(w7_date), 1))
    )

    # 4. NDVI data (sparse - forward fill to daily)
    ndvi = spark.read.format("delta").load(f"{SILVER_PATH}/satellite_clean")

    # NDVI slopes
    w_ndvi_prev = Window.partitionBy("plot_id").orderBy("date")
    w_ndvi_7d = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )
    w_ndvi_30d = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )

    ndvi_features = (
        ndvi
        .select("plot_id", "date", "ndvi", "is_ndvi_anomaly")
        .withColumn("ndvi_7d_avg", spark_round(avg("ndvi").over(w_ndvi_7d), 4))
        .withColumn("ndvi_30d_avg", spark_round(avg("ndvi").over(w_ndvi_30d), 4))
        .withColumn(
            "ndvi_7d_slope",
            spark_round(
                (col("ndvi") - first("ndvi").over(w_ndvi_7d))
                / greatest(
                    lit(1),
                    datediff(col("date"), first("date").over(w_ndvi_7d)),
                ),
                6,
            ),
        )
        .withColumn(
            "ndvi_30d_slope",
            spark_round(
                (col("ndvi") - first("ndvi").over(w_ndvi_30d))
                / greatest(
                    lit(1),
                    datediff(col("date"), first("date").over(w_ndvi_30d)),
                ),
                6,
            ),
        )
    )

    # 5. OCR flags per plot per day
    ocr = spark.read.format("delta").load(f"{SILVER_PATH}/ocr_processed")

    w_ocr_7d = (
        Window.partitionBy("plot_id")
        .orderBy(col("date").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )

    ocr_daily = (
        ocr
        .groupBy("plot_id", "date")
        .agg(
            spark_max(col("has_disease").cast("int")).alias("has_disease_flag"),
            spark_max(col("has_drought").cast("int")).alias("has_drought_flag"),
            spark_max(col("has_pest").cast("int")).alias("has_pest_flag"),
            spark_max(col("has_damage").cast("int")).alias("has_damage_flag"),
            count("*").alias("report_count"),
        )
    )

    # 6. Join all features
    # Start with sensor_features as base (has most daily records)
    features = (
        sensor_features
        .join(
            broadcast(weather_features.select("date", "rainfall_daily", "rainfall_7d_sum")),
            on="date",
            how="left",
        )
    )

    # Join NDVI (sparse - left join)
    features = features.join(
        ndvi_features.select(
            "plot_id", "date", "ndvi", "ndvi_7d_avg", "ndvi_30d_avg",
            "ndvi_7d_slope", "ndvi_30d_slope", "is_ndvi_anomaly",
        ),
        on=["plot_id", "date"],
        how="left",
    )

    # Join OCR flags
    features = features.join(
        ocr_daily,
        on=["plot_id", "date"],
        how="left",
    )

    # Fill nulls for flags
    features = (
        features
        .fillna(0, subset=[
            "has_disease_flag", "has_drought_flag",
            "has_pest_flag", "has_damage_flag",
            "report_count", "anomaly_count_7d",
        ])
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    features.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{GOLD_PATH}/feature_store")

    log.info(f"  Feature store: {features.count():,} rows")


def build_forest_health_index(spark: SparkSession):
    """
    Compute Forest Health Index (FHI) per plot per day.

    FHI = weighted combination of:
    - NDVI trend (40%)
    - Sensor anomaly rate (30%)
    - Report severity (30%)

    Scale: 0-100 (100 = healthiest)
    """
    log.info("--- Building Forest Health Index ---")

    features = spark.read.format("delta").load(f"{GOLD_PATH}/feature_store")

    fhi = (
        features
        .withColumn(
            "ndvi_score",
            when(col("ndvi").isNotNull(),
                 least(lit(100), greatest(lit(0), (col("ndvi") - 0.2) / 0.6 * 100)))
            .otherwise(lit(50)),
        )
        .withColumn(
            "anomaly_score",
            greatest(lit(0), lit(100) - col("anomaly_count_7d") * 5),
        )
        .withColumn(
            "report_score",
            greatest(
                lit(0),
                lit(100)
                - col("has_disease_flag") * 25
                - col("has_drought_flag") * 20
                - col("has_pest_flag") * 15
                - col("has_damage_flag") * 20,
            ),
        )
        .withColumn(
            "fhi",
            spark_round(
                col("ndvi_score") * 0.4
                + col("anomaly_score") * 0.3
                + col("report_score") * 0.3,
                1,
            ),
        )
        .withColumn(
            "health_status",
            when(col("fhi") >= 80, "Healthy")
            .when(col("fhi") >= 60, "Moderate")
            .when(col("fhi") >= 40, "Stressed")
            .when(col("fhi") >= 20, "Critical")
            .otherwise("Severe"),
        )
        .select(
            "plot_id", "date", "fhi", "health_status",
            "ndvi_score", "anomaly_score", "report_score",
            "ndvi", "temperature_avg", "soil_moisture_avg",
            "anomaly_count_7d", "has_disease_flag", "has_drought_flag",
        )
        .withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    )

    fhi.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(f"{GOLD_PATH}/forest_health_index")

    log.info(f"  FHI records: {fhi.count():,}")

    # Summary stats
    summary = (
        fhi
        .groupBy("health_status")
        .agg(count("*").alias("count"))
        .orderBy("health_status")
    )
    log.info("  FHI distribution:")
    summary.show(truncate=False)


def build_plot_summary(spark: SparkSession):
    """Build overall plot summary for dashboard overview."""
    log.info("--- Building Plot Summary ---")

    fhi = spark.read.format("delta").load(f"{GOLD_PATH}/forest_health_index")

    # Latest FHI per plot
    w_latest = Window.partitionBy("plot_id").orderBy(col("date").desc())

    latest = (
        fhi
        .withColumn("rn", expr("row_number() over (partition by plot_id order by date desc)"))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Overall stats per plot
    stats = (
        fhi
        .groupBy("plot_id")
        .agg(
            spark_round(avg("fhi"), 1).alias("avg_fhi"),
            spark_round(spark_min("fhi"), 1).alias("min_fhi"),
            spark_round(spark_max("fhi"), 1).alias("max_fhi"),
            spark_sum(col("has_disease_flag").cast("int")).alias("total_disease_reports"),
            spark_sum(col("has_drought_flag").cast("int")).alias("total_drought_reports"),
            spark_sum("anomaly_count_7d").alias("total_anomalies"),
        )
    )

    summary = (
        latest
        .select(
            "plot_id", col("date").alias("latest_date"),
            col("fhi").alias("latest_fhi"),
            col("health_status").alias("latest_status"),
            "ndvi",
        )
        .join(stats, on="plot_id")
    )

    summary.write.format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/plot_summary")

    log.info(f"  Plot summary: {summary.count():,} rows")
    summary.show(5, truncate=False)


def main():
    log.info("========================================")
    log.info("  SILVER → GOLD TRANSFORMATION")
    log.info("========================================")

    spark = get_spark_session("SilverToGold")
    spark.sparkContext.setLogLevel("WARN")

    try:
        build_feature_store(spark)
        build_forest_health_index(spark)
        build_plot_summary(spark)
        log.info("=== All Silver → Gold transformations complete ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
