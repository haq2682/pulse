"""
Airflow DAG: DB / Debezium CDC Pipeline
========================================
Orchestrates the Pulse pipeline for real-time **database CDC** ingestion via
Debezium → Kafka → Spark Structured Streaming.

Architecture
------------
Debezium (already running as a docker service) continuously captures changes
from the source database and publishes them to Kafka topics (ecom.*).

This DAG runs on a schedule (default: every 15 minutes) to flush accumulated
CDC events through the pipeline in bounded micro-batches using
``--trigger-once`` (Spark availableNow trigger).  The connector itself is
managed via Debezium's REST API; if it is not yet deployed, the first task
creates it.

Flow
----
  [check_debezium_connector]       ← ensure connector is deployed/healthy
          │
  [map_db]                         ← run_mapping.py --mode db --trigger-once
          │
  [clean_incremental]              ← cleaning.py --incremental
          │
  [transform]                      ← transformation.py
          │
  ┌───────┴───────┐
  [analyze]     [infer]            ← parallel
  └───────┬───────┘
          │
  [trigger_drift_check]            ← ml_retrain_dag

Override parameters per-run (dag_run.conf)
------------------------------------------
  {
    "bucket":     "pulse-bucket-1",
    "db_uri":     "postgresql://user:pass@host:5432/db",
    "db_tables":  "orders,payments,inventory"
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEBEZIUM_URL,
    DEFAULT_BUCKET,
    DEFAULT_DB_TABLES,
    DEFAULT_DB_URI,
    DEFAULT_TASK_ARGS,
    PYTHON_CONTAINER,
)

BUCKET     = Variable.get("default_bucket", default_var=DEFAULT_BUCKET)
DB_URI     = Variable.get("cdc_db_uri",     default_var=DEFAULT_DB_URI)
DB_TABLES  = Variable.get("cdc_db_tables",  default_var=",".join(DEFAULT_DB_TABLES))

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
# Helper: verify / deploy Debezium connector via REST API
# ---------------------------------------------------------------------------
def check_or_deploy_debezium_connector(**context):
    """
    Check that the Debezium connector is healthy.
    If it is not present, deploy it by calling run_mapping.py in a subprocess
    (which internally uses DebeziumConnectorManager).

    This runs inside the Airflow container (not the python container), so we
    use plain HTTP calls to the Debezium REST API.
    """
    import subprocess
    import urllib.request
    import json

    conf    = context["dag_run"].conf or {}
    bucket  = conf.get("bucket", BUCKET)
    db_uri  = conf.get("db_uri",  DB_URI)
    tables  = conf.get("db_tables", DB_TABLES)

    connector_name = f"pulse-{bucket}-connector"
    status_url     = f"{DEBEZIUM_URL}/connectors/{connector_name}/status"

    try:
        with urllib.request.urlopen(status_url, timeout=10) as resp:
            status = json.loads(resp.read())
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            if connector_state == "RUNNING":
                print(f"Debezium connector '{connector_name}' is RUNNING.")
                return
            print(f"Connector state: {connector_state}. Re-deploying…")
    except Exception as exc:
        print(f"Could not reach connector status ({exc}). Will deploy now.")

    # Deploy via python container (DebeziumConnectorManager handles the REST call)
    cmd = [
        "docker", "exec", PYTHON_CONTAINER,
        "python", "/app/mapping/run_mapping.py",
        "--mode", "db",
        "--business-id", bucket,
        "--db-uri", db_uri,
        "--db-tables", tables,
        "--trigger-once",   # deploy connector then immediately exit
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Debezium deploy failed (rc={result.returncode}):\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}"
        )
    print("Debezium connector deployed successfully.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="db_pipeline",
    description="Pulse CDC pipeline: Debezium → Kafka → map (trigger-once) → clean → transform → analyze → infer",
    schedule_interval="*/15 * * * *",   # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                  # never run two CDC flushes simultaneously
    tags=["pulse", "db", "cdc"],
    default_args=_task_defaults,
    params={
        "bucket":    BUCKET,
        "db_uri":    DB_URI,
        "db_tables": DB_TABLES,
    },
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Ensure Debezium connector is healthy ────────────────────────────
    check_debezium = PythonOperator(
        task_id="check_debezium_connector",
        python_callable=check_or_deploy_debezium_connector,
    )

    # ── 2. Consume pending CDC events from Kafka (bounded, trigger-once) ───
    #   --trigger-once uses Spark availableNow trigger: processes all queued
    #   Kafka messages then exits.  Safe to re-run if a previous run failed.
    map_db = BashOperator(
        task_id="map_db",
        bash_command=_docker_exec(
            "mapping/run_mapping.py",
            (
                "--mode db "
                "--business-id {{ params.bucket }} "
                "--db-uri {{ params.db_uri }} "
                "--db-tables {{ params.db_tables }} "
                "--trigger-once"
            ),
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── 3. Clean (incremental: only process new files) ─────────────────────
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
        conf={"bucket": "{{ params.bucket }}", "source_dag": "db_pipeline"},
        wait_for_completion=False,
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        check_debezium
        >> map_db
        >> clean_incremental
        >> transform
        >> [analyze, infer]
        >> trigger_drift_check
    )
