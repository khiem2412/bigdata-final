"""
Forest Lakehouse - Batch Processing DAG

Periodic batch processing: fetch latest weather data,
then re-run transformations to update Gold layer.

DAG Graph:
  batch_weather >> bronze_to_silver >> silver_to_gold
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "forest-lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="forest_batch_processing",
    default_args=default_args,
    description="Periodic batch processing - Weather ingestion + Transform refresh",
    schedule="@daily",
    start_date=datetime(2024, 4, 10),
    catchup=False,
    tags=["forest", "lakehouse", "batch"],
    doc_md=__doc__,
) as dag:

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

    ingest_weather >> bronze_to_silver >> silver_to_gold
