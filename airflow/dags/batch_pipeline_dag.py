"""
Airflow DAG: Batch Pipeline
===========================
Orchestrates the full Pulse data pipeline for BATCH ingestion.

Trigger
-------
* Scheduled (default: daily at 02:00 UTC).
* Can also be triggered manually from the Airflow UI or via the REST API.
* Triggered automatically by NiFi after it deposits files in MinIO ingested/.

Flow
----
  [wait_for_ingested_files]        ← MinIONewFileSensor
          │
  [map_batch]                      ← run_mapping.py --mode batch
          │
  [clean]                          ← cleaning.py
          │
  [transform]                      ← transformation.py
          │
  ┌───────┴───────┐
  [analyze]     [infer]            ← analysis.py / infer_all.py (parallel)
  └───────┬───────┘
          │
  [trigger_drift_check]            ← TriggerDagRunOperator → ml_retrain_dag

Crash / retry policy
--------------------
Every task retries 3 times with exponential back-off (1 min → 2 min → 4 min).
On permanent failure the DAG is marked FAILED and an Airflow alert is raised
(configure email / Slack via Airflow connections).

Bucket / business-id
--------------------
Resolved from the Airflow Variable "default_bucket" (defaults to
pipeline_config.DEFAULT_BUCKET).  Override per-run via dag_run.conf:
  {"bucket": "my-bucket"}
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    PYTHON_CONTAINER,
)
from plugins.sensors.minio_sensor import MinIONewFileSensor

# ---------------------------------------------------------------------------
# Resolve runtime bucket from Variable (UI-configurable) or config default
# ---------------------------------------------------------------------------
BUCKET = Variable.get("default_bucket", default_var=DEFAULT_BUCKET)

# Task default arguments (retries, timeouts, …)
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


# ---------------------------------------------------------------------------
# Helper: build docker exec command for the python container
# ---------------------------------------------------------------------------
def _docker_exec(script_path: str, extra_args: str = "") -> str:
    """Return a bash command that runs *script_path* inside the python container."""
    return (
        f'docker exec {PYTHON_CONTAINER} '
        f'python /app/{script_path} {extra_args}'
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="batch_pipeline",
    description="Pulse batch pipeline: ingest → map → clean → transform → analyze → infer",
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC; override in UI
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,               # prevent overlapping batch runs
    tags=["pulse", "batch"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},       # overridable via dag_run.conf
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Wait for NiFi to land files in ingested/ ────────────────────────
    wait_for_ingested_files = MinIONewFileSensor(
        task_id="wait_for_ingested_files",
        bucket="{{ params.bucket }}",
        prefix="ingested/",
        min_objects=1,
        poke_interval=60,            # check every 60 s
        timeout=7200,                # give up after 2 h
        mode="reschedule",           # free worker slot between pokes
        soft_fail=False,
    )

    # ── 2. Mapping ─────────────────────────────────────────────────────────
    map_batch = BashOperator(
        task_id="map_batch",
        bash_command=_docker_exec(
            "mapping/run_mapping.py",
            "--mode batch --business-id {{ params.bucket }}",
        ),
        # On success NiFi files are in mapped/; retry is safe (idempotent)
    )

    # ── 3. Cleaning ────────────────────────────────────────────────────────
    clean = BashOperator(
        task_id="clean",
        bash_command=_docker_exec(
            "cleaning/cleaning.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 4. Transformation ──────────────────────────────────────────────────
    transform = BashOperator(
        task_id="transform",
        bash_command=_docker_exec(
            "transformation/transformation.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 5a. Analysis (parallel with inference) ─────────────────────────────
    analyze = BashOperator(
        task_id="analyze",
        bash_command=_docker_exec(
            "analysis/analysis.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 5b. ML Inference (parallel with analysis) ──────────────────────────
    infer = BashOperator(
        task_id="infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 6. Trigger KS drift-check + conditional retraining ────────────────
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "batch_pipeline"},
        wait_for_completion=False,   # fire-and-forget; retrain runs asynchronously
        reset_dag_run=False,
    )

    # ── Task dependencies ──────────────────────────────────────────────────
    (
        wait_for_ingested_files
        >> map_batch
        >> clean
        >> transform
        >> [analyze, infer]
        >> trigger_drift_check
    )
