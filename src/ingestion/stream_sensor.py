"""
Spark Structured Streaming: Kafka sensor data → Bronze Delta Lake on MinIO.
Reads from Kafka topic 'sensor-readings', writes to Bronze layer.
"""
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, year, month, dayofmonth,
    concat_ws, lpad, current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from src.config import (
    KAFKA_BROKER, KAFKA_TOPIC_SENSOR,
    BRONZE_PATH, CHECKPOINT_PATH,
)
from src.utils.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("plot_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("soil_moisture", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])


def main():
    log.info("=== Starting Sensor Streaming Ingestion ===")
    log.info(f"Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC_SENSOR}")

    spark = get_spark_session("SensorStreaming")
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC_SENSOR)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingest_time", current_timestamp())
        .withColumn(
            "year_month",
            concat_ws(
                "-",
                year(col("timestamp")).cast("string"),
                lpad(month(col("timestamp")).cast("string"), 2, "0"),
            ),
        )
    )

    output_path = f"{BRONZE_PATH}/sensor_stream"
    checkpoint = f"{CHECKPOINT_PATH}/sensor_stream"

    log.info(f"Writing to: {output_path}")
    log.info(f"Checkpoint: {checkpoint}")

    # Write to Delta
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("year_month")
        .trigger(processingTime="30 seconds")
        .start(output_path)
    )

    log.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
