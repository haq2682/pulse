"""
Airflow DAG: DB / Debezium Streaming Supervisor
================================================
Manages the CONTINUOUS, long-running DB ingestion pipeline for database
CDC (Change Data Capture) mode.

Architecture
------------
Debezium listens to the source database's transaction log and publishes
every INSERT/UPDATE/DELETE as a Kafka message (ecom.* topics).
``run_mapping.py --mode db`` runs Spark Structured Streaming that:
  - Reads from those Kafka topics in micro-batches
  - Runs the 7-algorithm column-mapping pipeline on every micro-batch
  - Writes normalised Parquet files to MinIO  mapped/

The downstream pipeline (cleaning → transformation → analysis → inference)
is handled by the SEPARATE ``streaming_downstream`` DAG which runs on a
10-minute schedule over whatever has accumulated in mapped/.

When does this DAG run?
-----------------------
ONCE — triggered manually or by the frontend when the user submits their
database URI during onboarding (POST /onboarding/start-mapping, mode=db).

  Frontend  →  POST /api/v1/dags/db_streaming/dagRuns   (Airflow REST API)
                 conf: {
                   "bucket":    "pulse-bucket-1",
                   "db_uri":    "postgresql://user:pass@host:5432/dbname",
                   "db_tables": "orders,payments,inventory"
                 }

Do NOT trigger this DAG multiple times for the same tenant — it would
start a second competing streaming job. Use max_active_runs=1 to guard
against accidental double-triggers.

Why not scheduled?
------------------
The streaming job is designed to run 24/7.  Scheduling it would either
be a no-op (if already running) or would start duplicate jobs.

About "mapping runs only once"
-------------------------------
The Spark Structured Streaming job does run the mapping algorithm on
EVERY micro-batch — but that is intentional (new CDC events need to be
normalised).  What runs "only once" is the user interaction: the user
reviews the mapping results and fixes any missing columns in the
onboarding UI.  That approved mapping configuration is stored in Redis
(key: manual_mappings:{bucket}) and is re-read automatically by every
subsequent micro-batch via foreachBatch.  No further user action is
needed.

Crash handling & auto-restart
------------------------------
``run_db_streaming`` is a long-running task (never exits normally).
If the Spark job or Debezium connection crashes:
  - The docker-exec command exits with non-zero
  - Airflow marks the task FAILED
  - Airflow waits ``retry_delay`` (1 min) then restarts the task
  - retries=9999 means effectively infinite restarts

Spark Structured Streaming uses a MinIO-backed checkpoint
(s3a://pulse-checkpoints/normalize-stream) so restarts pick up exactly
where they left off — no duplicate or lost events.

Override parameters (dag_run.conf)
-----------------------------------
  {
    "bucket":    "pulse-bucket-1",
    "db_uri":    "postgresql://user:pass@host:5432/dbname",
    "db_tables": "orders,payments,inventory,customers,products"
  }
"""

from __future__ import annotations

import os
import sys
import urllib.request
import urllib.error
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEBEZIUM_URL,
    DEFAULT_BUCKET,
    DEFAULT_DB_TABLES,
    DEFAULT_DB_URI,
    DEFAULT_TASK_ARGS,
    PYTHON_CONTAINER,
)

BUCKET    = Variable.get("default_bucket", default_var=DEFAULT_BUCKET)
DB_URI    = Variable.get("cdc_db_uri",     default_var=DEFAULT_DB_URI)
DB_TABLES = Variable.get("cdc_db_tables",  default_var=",".join(DEFAULT_DB_TABLES))

# Streaming tasks need different defaults from normal batch tasks:
# - no execution_timeout  (runs forever)
# - retries=9999          (infinite restart on crash)
# - retry_exponential_backoff=False (constant 1-min restart delay, no escalation)
_batch_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=False,
    retries=3,
    retry_delay=timedelta(seconds=DEFAULT_TASK_ARGS["retry_delay_seconds"]),
    execution_timeout=timedelta(minutes=10),
    email_on_failure=DEFAULT_TASK_ARGS["email_on_failure"],
    email_on_retry=DEFAULT_TASK_ARGS["email_on_retry"],
)

_streaming_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=False,
    retries=9999,                          # restart on every crash
    retry_exponential_backoff=False,       # constant delay — don't wait longer each time
    retry_delay=timedelta(minutes=1),      # wait 1 min before restarting
    execution_timeout=None,                # NEVER timeout — this runs forever
    email_on_failure=False,
    email_on_retry=False,
)


def _docker_exec(script_path: str, extra_args: str = "") -> str:
    return f"docker exec {PYTHON_CONTAINER} python /app/{script_path} {extra_args}"


# ---------------------------------------------------------------------------
# Connector health-check / deploy
# ---------------------------------------------------------------------------
def check_or_deploy_debezium(**context):
    """
    Verify the Debezium connector for this tenant is deployed and RUNNING.
    If it is missing or in a failed state, redeploy it by calling
    run_mapping.py with --trigger-once (which deploys the connector and
    immediately returns without starting the streaming query).
    """
    import subprocess

    conf      = context["dag_run"].conf or {}
    bucket    = conf.get("bucket",    BUCKET)
    db_uri    = conf.get("db_uri",    DB_URI)
    db_tables = conf.get("db_tables", DB_TABLES)

    connector_name = f"pulse-{bucket}-connector"
    status_url     = f"{DEBEZIUM_URL}/connectors/{connector_name}/status"

    try:
        with urllib.request.urlopen(status_url, timeout=10) as resp:
            status          = json.loads(resp.read())
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")

            if connector_state == "RUNNING":
                print(f"Connector '{connector_name}' is RUNNING — nothing to do.")
                return

            print(f"Connector state is '{connector_state}' — redeploying.")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(f"Connector '{connector_name}' not found — deploying for the first time.")
        else:
            raise
    except Exception as exc:
        print(f"Could not reach Debezium ({exc}) — proceeding to deploy.")

    # Deploy: run_mapping --mode db --trigger-once only deploys the connector
    # and then exits (availableNow trigger on zero Kafka messages → immediate exit).
    cmd = [
        "docker", "exec", PYTHON_CONTAINER,
        "python", "/app/mapping/run_mapping.py",
        "--mode", "db",
        "--business-id", bucket,
        "--db-uri",    db_uri,
        "--db-tables", db_tables,
        "--trigger-once",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            f"Debezium deploy failed (rc={result.returncode}).\n"
            f"STDOUT: {result.stdout[-2000:]}\nSTDERR: {result.stderr[-2000:]}"
        )
    print("Debezium connector deployed successfully.")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="db_streaming",
    description=(
        "Manages the continuous Debezium CDC → Spark Streaming → MinIO mapped/ pipeline. "
        "Triggered ONCE when the user connects their database. Auto-restarts on crash."
    ),
    schedule_interval=None,      # USER-TRIGGERED — never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,           # guard against accidental double-starts
    tags=["pulse", "db", "cdc", "streaming"],
    default_args=_batch_defaults,
    params={
        "bucket":    BUCKET,
        "db_uri":    DB_URI,
        "db_tables": DB_TABLES,
    },
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Deploy / verify Debezium connector ─────────────────────────────
    # Finite task: checks the connector REST API and deploys if needed.
    deploy_debezium = PythonOperator(
        task_id="deploy_debezium_connector",
        python_callable=check_or_deploy_debezium,
        **_batch_defaults,
    )

    # ── 2. Start the continuous mapping stream ────────────────────────────
    # This task runs FOREVER (until the Spark job crashes or is stopped).
    # On crash:  Airflow waits 1 min → restarts → Spark resumes from checkpoint.
    # On DAG pause/cancellation: docker exec is terminated → streaming stops.
    #
    # NOTE: does NOT use --trigger-once here.  This is the 24/7 streaming job.
    run_db_streaming = BashOperator(
        task_id="run_db_mapping_stream",
        bash_command=_docker_exec(
            "mapping/run_mapping.py",
            (
                "--mode db "
                "--business-id {{ params.bucket }} "
                "--db-uri {{ params.db_uri }} "
                "--db-tables {{ params.db_tables }} "
                "--enable-downstream"
                # No --trigger-once: this must run indefinitely
            ),
        ),
        **_streaming_defaults,
    )

    deploy_debezium >> run_db_streaming
