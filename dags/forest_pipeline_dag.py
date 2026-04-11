"""
Forest Lakehouse - Full Pipeline DAG

Generates historical data, transforms Bronze -> Silver -> Gold,
and validates the results. Data generators run in parallel,
followed by sequential Spark transformations.

Idempotent: checks if Bronze data exists before generating.
Use the 'force_regenerate' param to override.

DAG Graph:
  check_bronze >> [sensors, weather, ndvi, ocr, images] >> bronze_to_silver >> silver_to_gold >> validate
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

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
    params={"force_regenerate": False},
    doc_md=__doc__,
) as dag:

    def _check_bronze_data(**context):
        """Skip generation if Bronze data already exists (unless force_regenerate)."""
        import subprocess
        force = context["params"].get("force_regenerate", False)
        if force:
            return "generate_sensor_data"
        result = subprocess.run(
            ["python", "-m", "src.check_data", "--layer", "bronze"],
            cwd="/app", capture_output=True,
        )
        if result.returncode == 0:
            return "skip_generation"
        return "generate_sensor_data"

    check_bronze = BranchPythonOperator(
        task_id="check_bronze_exists",
        python_callable=_check_bronze_data,
    )

    skip_generation = EmptyOperator(
        task_id="skip_generation",
    )

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

    generation_done = EmptyOperator(
        task_id="generation_done",
        trigger_rule="none_failed_min_one_success",
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
    check_bronze >> [generate_sensors, skip_generation]

    generate_sensors >> [generate_weather, generate_ndvi,
                         generate_ocr, generate_images]

    [generate_images, generate_weather, generate_ndvi,
     generate_ocr, skip_generation] >> generation_done

    generation_done >> bronze_to_silver >> silver_to_gold >> validate
