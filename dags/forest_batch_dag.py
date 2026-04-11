"""
Forest Lakehouse - Batch Processing DAG

Periodic batch processing (every 5 days):
  - Fetch latest Sentinel-2 NDVI from Planetary Computer
  - Fetch latest weather from Open-Meteo
  - Re-run transformations to update Silver & Gold layers

DAG Graph:
  [batch_satellite_ingest, batch_weather_ingest] >> bronze_to_silver >> silver_to_gold
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "forest-lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="forest_batch_processing",
    default_args=default_args,
    description="Every-5-day batch: Satellite NDVI + Weather ingestion + Transform refresh",
    schedule=timedelta(days=5),
    start_date=datetime(2024, 4, 10),
    catchup=False,
    tags=["forest", "lakehouse", "batch", "satellite"],
    doc_md=__doc__,
) as dag:

    ingest_satellite = BashOperator(
        task_id="batch_satellite_ingest",
        bash_command="cd /app && python -m src.ingestion.satellite_ingest --mode batch --days 7",
        execution_timeout=timedelta(hours=1),
    )

    ingest_weather = BashOperator(
        task_id="batch_weather_ingest",
        bash_command="cd /app && python -m src.ingestion.batch_ingest",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /app && python -m src.transform.bronze_to_silver",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="cd /app && python -m src.transform.silver_to_gold",
    )

    [ingest_satellite, ingest_weather] >> bronze_to_silver >> silver_to_gold
