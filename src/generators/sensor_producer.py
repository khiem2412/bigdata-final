"""
Real-time sensor data producer for Kafka.
Generates sensor readings every 10 seconds for 75 sensors.
"""
import json
import time
import logging
import sys
import numpy as np
from datetime import datetime, timezone

from kafka import KafkaProducer
from src.config import KAFKA_BROKER, KAFKA_TOPIC_SENSOR
from src.generators.plots import SENSORS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

SEND_INTERVAL = 10  # seconds


def _seasonal_base(dt: datetime):
    """Compute seasonal base values for Central Vietnam."""
    doy = dt.timetuple().tm_yday
    hour = dt.hour + dt.minute / 60.0

    # Temperature: peaks in Jun-Jul (~day 172)
    temp_seasonal = 27.0 + 7.0 * np.sin(2 * np.pi * (doy - 80) / 365)
    temp_daily = 5.0 * np.sin(2 * np.pi * (hour - 6) / 24)
    temperature = temp_seasonal + temp_daily

    # Humidity: higher in rainy season (Sep-Jan, doy ~250-365,1-31)
    hum_seasonal = 78.0 - 10.0 * np.sin(2 * np.pi * (doy - 80) / 365)
    humidity = hum_seasonal + 3.0 * np.sin(2 * np.pi * (hour - 14) / 24)

    # Soil moisture: follows rainfall pattern with lag
    sm_seasonal = 0.37 - 0.12 * np.sin(2 * np.pi * (doy - 100) / 365)
    soil_moisture = sm_seasonal

    return temperature, humidity, soil_moisture


def generate_reading(sensor: dict, dt: datetime) -> dict:
    """Generate a single sensor reading with realistic noise."""
    temp_base, hum_base, sm_base = _seasonal_base(dt)

    # Add per-sensor variation + noise
    np.random.seed(hash(sensor["sensor_id"] + str(dt.timestamp())) % (2**31))
    sensor_offset = hash(sensor["sensor_id"]) % 100 / 100.0

    temperature = round(temp_base + np.random.normal(0, 1.2) + sensor_offset, 2)
    humidity = round(np.clip(hum_base + np.random.normal(0, 3.0), 30, 100), 2)
    soil_moisture = round(np.clip(sm_base + np.random.normal(0, 0.04), 0.05, 0.80), 3)

    # Rare anomaly: temperature spike (0.5% chance)
    if np.random.random() < 0.005:
        temperature = round(temperature + np.random.uniform(8, 15), 2)

    # Rare anomaly: soil moisture drop (0.3% chance)
    if np.random.random() < 0.003:
        soil_moisture = round(np.random.uniform(0.05, 0.12), 3)

    return {
        "sensor_id": sensor["sensor_id"],
        "plot_id": sensor["plot_id"],
        "timestamp": dt.isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "soil_moisture": soil_moisture,
        "latitude": sensor["latitude"],
        "longitude": sensor["longitude"],
    }


def main():
    log.info(f"Connecting to Kafka broker: {KAFKA_BROKER}")
    log.info(f"Topic: {KAFKA_TOPIC_SENSOR}, Sensors: {len(SENSORS)}")

    retry = 0
    producer = None
    while retry < 30:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                retries=3,
            )
            log.info("Connected to Kafka successfully")
            break
        except Exception as e:
            retry += 1
            log.warning(f"Kafka connection attempt {retry}/30 failed: {e}")
            time.sleep(5)

    if producer is None:
        log.error("Failed to connect to Kafka after 30 attempts")
        sys.exit(1)

    log.info("Starting sensor data streaming...")
    batch_count = 0

    try:
        while True:
            now = datetime.now(timezone.utc)
            for sensor in SENSORS:
                reading = generate_reading(sensor, now)
                producer.send(KAFKA_TOPIC_SENSOR, value=reading)

            producer.flush()
            batch_count += 1

            if batch_count % 6 == 0:  # Log every minute
                log.info(
                    f"Sent batch #{batch_count} ({len(SENSORS)} readings) "
                    f"at {now.isoformat()}"
                )

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        log.info("Shutting down sensor producer...")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
