"""
Batch Ingestion DAG

Orchestrates the full batch ingestion pipeline:
  mapping → cleaning → transformation → analysis

Schedule: daily at 01:00 UTC (processes previous day's uploaded files).
Replaces NiFi batch ingestion flow.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BUCKET_NAME = "pulse-bucket-1"

# Paths inside the python/api container (volumes are mounted at /app)
MAPPING_CMD = f"cd /app && python -m mapping.run_mapping --bucket-name {BUCKET_NAME}"
CLEANING_CMD = f"cd /app && python -m cleaning.data_cleaning --bucket-name {BUCKET_NAME}"
TRANSFORMATION_CMD = f"cd /app && python -m transformation.transformation --bucket-name {BUCKET_NAME}"
ANALYSIS_CMD = f"cd /app && python -m analysis.analysis --bucket-name {BUCKET_NAME}"

default_args = {
    "owner": "pulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_ingestion_pipeline",
    default_args=default_args,
    description="Daily batch ingestion: mapping → cleaning → transformation → analysis",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "ingestion", "etl"],
) as dag:

    run_mapping = BashOperator(
        task_id="run_mapping",
        bash_command=MAPPING_CMD,
    )

    run_cleaning = BashOperator(
        task_id="run_cleaning",
        bash_command=CLEANING_CMD,
    )

    run_transformation = BashOperator(
        task_id="run_transformation",
        bash_command=TRANSFORMATION_CMD,
    )

    run_analysis = BashOperator(
        task_id="run_analysis",
        bash_command=ANALYSIS_CMD,
    )

    run_mapping >> run_cleaning >> run_transformation >> run_analysis
