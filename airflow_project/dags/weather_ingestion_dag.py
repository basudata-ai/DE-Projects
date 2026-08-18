"""
Airflow DAG: Hourly Weather Ingestion
======================================

This file is the ORCHESTRATION layer — it does not fetch data or write to
MinIO itself. It simply tells Airflow's scheduler:
  - WHAT to run  -> run_batch_ingestion() from ingest_weather_to_minio.py
  - WHEN to run  -> every hour
  - HOW to behave on failure -> retry twice, 5 minutes apart

The actual ingestion logic stays in ingest_weather_to_minio.py, unchanged.
This DAG just imports and calls it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Because this file lives in the `dags/` folder, and ingest_weather_to_minio.py
# also lives in `dags/`, Airflow's scheduler can import it directly — the
# dags folder is automatically on the Python import path.
from ingest_weather_to_minio import run_batch_ingestion

default_args = {
    "owner": "basalingamma",
    "retries": 2,                        # retry a failed run twice...
    "retry_delay": timedelta(minutes=5),  # ...waiting 5 minutes between tries
}

with DAG(
    dag_id="weather_ingestion_dag",
    description="Hourly batch ingestion: Open-Meteo API -> MinIO landing zone",
    default_args=default_args,
    schedule="@hourly",                 # cron-like schedule; @hourly = every hour
    start_date=datetime(2026, 1, 1),    # DAG won't run for dates before this
    catchup=False,                      # don't backfill every missed hour since start_date
    tags=["ingestion", "portfolio", "minio"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_weather_to_minio",
        python_callable=run_batch_ingestion,  # <-- calls your existing function directly
    )

    # Single-task DAG for now. If you add a data-quality check function later,
    # you'd chain it like:
    #   ingest_task >> validate_task
