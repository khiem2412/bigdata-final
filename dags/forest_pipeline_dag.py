"""
Forest Lakehouse - Full Pipeline DAG

Generates historical data, transforms Bronze -> Silver -> Gold,
and validates the results. Data generators run in parallel,
followed by sequential Spark transformations.

DAG Graph:
  [sensors, weather, ndvi, ocr, images] >> bronze_to_silver >> silver_to_gold >> validate
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "forest-lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="forest_lakehouse_pipeline",
    default_args=default_args,
    description="Forest Health Monitoring - Full Data Pipeline (Generate -> Transform -> Validate)",
    schedule=None,
    start_date=datetime(2024, 4, 10),
    catchup=False,
    tags=["forest", "lakehouse", "pipeline"],
    doc_md=__doc__,
) as dag:

    # ── Data Generation (parallel) ────────────────────────────────
    generate_sensors = BashOperator(
        task_id="generate_sensor_data",
        bash_command="cd /app && python -m src.generators.historical_data",
    )

    generate_weather = BashOperator(
        task_id="generate_weather_data",
        bash_command="cd /app && python -m src.generators.weather_fetcher",
    )

    generate_ndvi = BashOperator(
        task_id="generate_ndvi_data",
        bash_command="cd /app && python -m src.generators.satellite_ndvi",
    )

    generate_ocr = BashOperator(
        task_id="generate_ocr_notes",
        bash_command="cd /app && python -m src.generators.ocr_notes",
    )

    generate_images = BashOperator(
        task_id="generate_image_metadata",
        bash_command="cd /app && python -m src.generators.image_metadata",
    )

    # ── Spark Transformations (sequential) ─────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /app && python -m src.transform.bronze_to_silver",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="cd /app && python -m src.transform.silver_to_gold",
    )

    # ── Validation ─────────────────────────────────────────────────
    validate = BashOperator(
        task_id="validate_data",
        bash_command="cd /app && python -m src.validate",
    )

    # ── Dependencies ───────────────────────────────────────────────
    (
        [generate_sensors, generate_weather, generate_ndvi,
         generate_ocr, generate_images]
        >> bronze_to_silver
        >> silver_to_gold
        >> validate
    )
