"""
Airflow DAG: API Streaming Supervisor
======================================
Manages the CONTINUOUS, long-running API ingestion pipeline for API mode.

Architecture
------------
``run_mapping.py --mode api`` starts two parallel processes inside the
python container:

  Process 1 – API Ingestion Service
    Polls the USER-PROVIDED external API endpoint every ``api_poll_interval``
    seconds, validates the response, and publishes records to Kafka
    (ecom.* topics).

  Process 2 – Spark Structured Streaming Consumer
    Reads from those Kafka topics in micro-batches, runs the 7-algorithm
    column-mapping pipeline, and writes normalised Parquet files to
    MinIO  mapped/.

The downstream pipeline (cleaning → transformation → analysis → inference)
is handled by the SEPARATE ``streaming_downstream`` DAG which runs on a
10-minute schedule over whatever has accumulated in mapped/.

When does this DAG run?
-----------------------
ONCE — triggered automatically from the onboarding confirm-mapping step
after the user confirms their column mappings. The API backend calls the
Airflow REST API:

  POST /api/v1/dags/api_streaming/dagRuns
       conf: {
         "bucket":        "<business_id>",
         "api_url":       "https://api.example.com/ecommerce/data",
         "poll_interval": 30
       }

The ``api_url`` MUST be the user's own external REST endpoint that returns
e-commerce records in the canonical format expected by
``api_ingest_service.py``.

Required response format from the user's API:
  { "tables": [{ "table_name": "orders", "data": [{...}, ...] }] }

About "mapping runs only once"
-------------------------------
The API ingestion service polls continuously (e.g., every 30 seconds) and
the Spark consumer processes each batch.  The column-mapping algorithm runs
on every batch, but the mapping CONFIGURATION (which source columns map to
which canonical schema columns) is fixed after the user completes the
onboarding mapping step and is stored in Redis.  Subsequent polls reuse
that configuration automatically — no further user action is needed.

Crash handling & auto-restart
------------------------------
``run_api_streaming`` runs forever.  On crash:
  - docker exec exits non-zero
  - Airflow marks task FAILED, waits 1 min, restarts
  - retries=9999 means effectively infinite restarts
  - Spark checkpoint persists in MinIO so no data is lost

Required dag_run.conf parameters
----------------------------------
  {
    "bucket":        "<business_id>",   # MinIO bucket / business ID
    "api_url":       "<user_api_url>",  # User's external API endpoint (REQUIRED)
    "poll_interval": 30                 # Seconds between polls (optional, default 30)
  }
"""

from __future__ import annotations

import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    API_POLL_INTERVAL,
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    PYTHON_CONTAINER,
)

BUCKET        = Variable.get("default_bucket",    default_var=DEFAULT_BUCKET)
# api_url has NO system-wide default — it must come from dag_run.conf per run.
POLL_INTERVAL = int(Variable.get("api_poll_interval", default_var=str(API_POLL_INTERVAL)))

_batch_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=False,
    retries=5,
    retry_delay=timedelta(seconds=30),
    execution_timeout=timedelta(minutes=5),
    email_on_failure=DEFAULT_TASK_ARGS["email_on_failure"],
    email_on_retry=DEFAULT_TASK_ARGS["email_on_retry"],
)

_streaming_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=False,
    retries=9999,
    retry_exponential_backoff=False,
    retry_delay=timedelta(minutes=1),
    execution_timeout=None,               # runs forever
    email_on_failure=False,
    email_on_retry=False,
)


def _docker_exec(script_path: str, extra_args: str = "") -> str:
    return f"docker exec {PYTHON_CONTAINER} python /app/{script_path} {extra_args}"


# ---------------------------------------------------------------------------
# Health check callable
# ---------------------------------------------------------------------------
def verify_api_health(**context):
    """
    Check that the user-provided API endpoint is reachable before starting
    the continuous polling process.  Raises on failure so Airflow retries.
    """
    conf    = context["dag_run"].conf or {}
    api_url = conf.get("api_url", "").strip()

    if not api_url:
        raise RuntimeError(
            "dag_run.conf must include 'api_url' — the user's external API endpoint. "
            "Example: POST dagRuns with conf={\"bucket\": \"<id>\", "
            "\"api_url\": \"https://api.example.com/data\", \"poll_interval\": 30}"
        )

    # Try a lightweight /health endpoint first; fall back to the main URL.
    for url in [
        api_url.rsplit("/", 1)[0] + "/health",   # e.g. https://host/health
        api_url,
    ]:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                print(f"API reachable at {url} (HTTP {resp.status})")
                return   # success
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue   # endpoint doesn't exist, try next
            raise RuntimeError(f"API health check failed: {exc}") from exc
        except Exception:
            continue

    raise RuntimeError(
        f"Could not reach the user API at {api_url}. "
        "Ensure the endpoint is running and accessible from within the Docker network."
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="api_streaming",
    description=(
        "Manages the continuous frontend-API polling → Kafka → Spark Streaming → "
        "MinIO mapped/ pipeline. Triggered ONCE when the user connects their API endpoint."
    ),
    schedule_interval=None,      # USER-TRIGGERED — never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pulse", "api", "streaming"],
    default_args=_batch_defaults,
    params={
        "bucket":        BUCKET,
        "api_url":       "",          # REQUIRED — provided per run via dag_run.conf
        "poll_interval": POLL_INTERVAL,
    },
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Verify the frontend API is reachable ────────────────────────────
    check_api_health = PythonOperator(
        task_id="check_api_health",
        python_callable=verify_api_health,
        **_batch_defaults,
    )

    # ── 2. Start the continuous API ingestion + mapping stream ────────────
    # run_mapping.py --mode api starts TWO processes:
    #   • API ingestion service  → polls every poll_interval seconds
    #   • Spark streaming consumer → drains Kafka → writes to MinIO mapped/
    # Both run until crash.  Airflow restarts the whole command on failure.
    #
    # NOTE: no --trigger-once / --poll-duration here.
    # The job polls indefinitely at poll_interval seconds per cycle.
    run_api_streaming = BashOperator(
        task_id="run_api_mapping_stream",
        bash_command=_docker_exec(
            "mapping/run_mapping.py",
            (
                "--mode api "
                "--business-id {{ params.bucket }} "
                "--api-url {{ params.api_url }} "
                "--api-poll-interval {{ params.poll_interval }} "
                "--enable-downstream"
                # No --trigger-once: must run indefinitely
                # No --poll-duration: must poll forever
            ),
        ),
        **_streaming_defaults,
    )

    check_api_health >> run_api_streaming
