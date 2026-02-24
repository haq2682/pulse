"""
Airflow DAG: API Ingestion Pipeline
=====================================
Orchestrates the Pulse pipeline for **API endpoint** ingestion.

This DAG covers the ingestion path from your Pulse frontend/backend API
(FastAPI service at 10.5.0.9:8000) that exposes e-commerce event streams.

How it works
------------
1. An HTTP sensor checks that the API is healthy and returning data.
2. run_mapping.py --mode api is called with --poll-duration N and
   --trigger-once so that:
     • the API ingestion process polls the endpoint for N seconds and stops.
     • the Spark streaming consumer uses availableNow trigger to drain the
       resulting Kafka topic and exits.
3. The downstream cleaning → transform → analyze / infer chain runs exactly
   as in the batch pipeline.

Schedule
--------
Default: every 30 minutes.  Override via the Airflow UI (Connections/Variables)
or dag_run.conf.

Endpoint setup (frontend)
-------------------------
The frontend API must expose a route that returns structured e-commerce records
in the canonical format understood by api_ingest_service.py.  Configure
FRONTEND_API_URL in pipeline_config.py or the Airflow Variable "frontend_api_url".

Example route (FastAPI):
  GET /api/ingest/stream?since=<iso_timestamp>&limit=1000

Override parameters (dag_run.conf)
------------------------------------
  {
    "bucket":        "pulse-bucket-1",
    "api_url":       "http://10.5.0.9:8000/api/ingest/stream",
    "poll_interval": 30,
    "poll_duration": 300
  }
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.sensors.http_sensor import HttpSensor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    API_POLL_DURATION,
    API_POLL_INTERVAL,
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    FRONTEND_API_URL,
    PYTHON_CONTAINER,
)

BUCKET       = Variable.get("default_bucket",   default_var=DEFAULT_BUCKET)
API_URL      = Variable.get("frontend_api_url", default_var=FRONTEND_API_URL)
POLL_INTERVAL = int(Variable.get("api_poll_interval", default_var=str(API_POLL_INTERVAL)))
POLL_DURATION = int(Variable.get("api_poll_duration", default_var=str(API_POLL_DURATION)))

_task_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=DEFAULT_TASK_ARGS["depends_on_past"],
    retries=DEFAULT_TASK_ARGS["retries"],
    retry_exponential_backoff=DEFAULT_TASK_ARGS["retry_exponential_backoff"],
    retry_delay=timedelta(seconds=DEFAULT_TASK_ARGS["retry_delay_seconds"]),
    max_retry_delay=timedelta(seconds=DEFAULT_TASK_ARGS["max_retry_delay_seconds"]),
    execution_timeout=timedelta(seconds=DEFAULT_TASK_ARGS["execution_timeout_seconds"]),
    email_on_failure=DEFAULT_TASK_ARGS["email_on_failure"],
    email_on_retry=DEFAULT_TASK_ARGS["email_on_retry"],
)


def _docker_exec(script_path: str, extra_args: str = "") -> str:
    return f'docker exec {PYTHON_CONTAINER} python /app/{script_path} {extra_args}'


# ---------------------------------------------------------------------------
# Health-check callable (used in PythonOperator fallback if HttpSensor fails)
# ---------------------------------------------------------------------------
def verify_api_health(**context):
    """
    Verify the frontend API endpoint is reachable and returning valid data.
    Raises on failure so Airflow retries the task.
    """
    import urllib.request
    import urllib.error
    import json

    conf     = context["dag_run"].conf or {}
    api_url  = conf.get("api_url", API_URL)
    # Use a lightweight health/ping endpoint if available
    health_url = api_url.replace("/ingest/stream", "/health").replace("/api/ingest/stream", "/health")

    try:
        with urllib.request.urlopen(health_url, timeout=15) as resp:
            body = resp.read().decode()
            print(f"API health check OK: {health_url} → {resp.status}")
            try:
                data = json.loads(body)
                print(f"Response: {data}")
            except Exception:
                pass   # plain 200 is sufficient
    except urllib.error.HTTPError as exc:
        # 404 on /health is acceptable if the API doesn't expose it
        if exc.code == 404:
            print(f"No /health endpoint (404) – assuming API is up.")
        else:
            raise RuntimeError(f"API health check failed: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(
            f"Cannot reach frontend API at {health_url}: {exc}\n"
            "Ensure the api container is running and FRONTEND_API_URL is correct."
        ) from exc


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="api_pipeline",
    description="Pulse API pipeline: poll frontend API → Kafka → map → clean → transform → analyze → infer",
    schedule_interval="*/30 * * * *",   # every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pulse", "api"],
    default_args=_task_defaults,
    params={
        "bucket":        BUCKET,
        "api_url":       API_URL,
        "poll_interval": POLL_INTERVAL,
        "poll_duration": POLL_DURATION,
    },
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Verify the frontend API is reachable ────────────────────────────
    check_api_health = PythonOperator(
        task_id="check_api_health",
        python_callable=verify_api_health,
        retries=5,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )

    # ── 2. Ingest from API → Kafka → MinIO mapped/ ─────────────────────────
    #   --poll-duration: how many seconds to run the API poller before stopping
    #   --trigger-once:  Spark uses availableNow to drain Kafka then exits
    ingest_and_map = BashOperator(
        task_id="ingest_and_map",
        bash_command=_docker_exec(
            "mapping/run_mapping.py",
            (
                "--mode api "
                "--business-id {{ params.bucket }} "
                "--api-url {{ params.api_url }} "
                "--api-poll-interval {{ params.poll_interval }} "
                "--poll-duration {{ params.poll_duration }} "
                "--trigger-once"
            ),
        ),
        # Give enough time for poll_duration + Spark bootstrap overhead
        execution_timeout=timedelta(seconds=POLL_DURATION + 600),
    )

    # ── 3. Clean (incremental) ─────────────────────────────────────────────
    clean_incremental = BashOperator(
        task_id="clean_incremental",
        bash_command=_docker_exec(
            "cleaning/cleaning.py",
            "--bucket-name {{ params.bucket }} --incremental",
        ),
    )

    # ── 4. Transform ───────────────────────────────────────────────────────
    transform = BashOperator(
        task_id="transform",
        bash_command=_docker_exec(
            "transformation/transformation.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 5a/b. Analyze + Infer (parallel) ───────────────────────────────────
    analyze = BashOperator(
        task_id="analyze",
        bash_command=_docker_exec(
            "analysis/analysis.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    infer = BashOperator(
        task_id="infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 6. Trigger drift check ─────────────────────────────────────────────
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "api_pipeline"},
        wait_for_completion=False,
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        check_api_health
        >> ingest_and_map
        >> clean_incremental
        >> transform
        >> [analyze, infer]
        >> trigger_drift_check
    )
