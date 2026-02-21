"""
ML Training DAG

Trains all ML models (general + specific) on a weekly schedule.
Replaces machine-learning/scheduled_ml_training.py (which used the `schedule`
Python library in a blocking loop).

Schedule: every Sunday at 02:00 UTC — matches the original default in
          scheduled_ml_training.py.

The PythonOperator imports and calls train_all.main() directly so that
Airflow manages retries, alerting, and execution history.
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_NAME = "pulse-bucket-1"


def train_all_models(bucket_name: str = BUCKET_NAME, **kwargs) -> None:
    """
    Import and run the existing train_all entry point.
    Path handling covers both container and local dev layouts.
    """
    try:
        from machine_learning.train_all import main as _train
    except ImportError:
        ml_path = os.path.join(os.path.dirname(__file__), "..", "..", "machine-learning")
        sys.path.insert(0, os.path.abspath(ml_path))
        from train_all import main as _train  # type: ignore[import]

    _train(bucket_name)


default_args = {
    "owner": "pulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ml_weekly_training",
    default_args=default_args,
    description="Weekly ML model retraining (general + specific models)",
    schedule_interval="0 2 * * 0",  # every Sunday at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training", "weekly"],
) as dag:

    train_models = PythonOperator(
        task_id="train_all_models",
        python_callable=train_all_models,
        op_kwargs={"bucket_name": BUCKET_NAME},
    )
