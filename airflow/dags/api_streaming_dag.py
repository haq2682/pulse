"""
Airflow DAG: API Streaming Supervisor
======================================
Manages the CONTINUOUS, long-running API ingestion pipeline for API mode.

Architecture
------------
``run_mapping.py --mode api`` starts two parallel processes inside the
task pod:

  Process 1 – API Ingestion Service
    Polls the USER-PROVIDED external API endpoint every ``api_poll_interval``
    seconds, validates the response, and publishes records to Kafka
    (ecom.* topics).

  Process 2 – Spark Structured Streaming Consumer
    Reads from those Kafka topics in micro-batches, runs the 7-algorithm
    column-mapping pipeline, and writes normalised Parquet files to
    MinIO  mapped/.

This DAG is responsible ONLY for ingestion + mapping (streaming layer).
All downstream processing (cleaning → transformation → analysis → ML)
runs separately in the ``scheduled_batch`` DAG every 10 minutes.

When does this DAG run?
-----------------------
ONCE -- triggered automatically from the onboarding confirm-mapping step
after the user confirms their column mappings.
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
onboarding mapping step. Runtime reads from Redis first and falls back to the
onboarding database record for the same business_id when Redis keys are
missing/expired. Subsequent polls reuse that configuration automatically --
no further user action is needed.

Crash handling & auto-restart
------------------------------
``run_api_streaming`` runs forever.  On crash:
  - The task pod's container exits non-zero
  - Airflow marks task FAILED, waits 1 min, restarts
  - retries=9999 means effectively infinite restarts
  - Spark checkpoint persists in MinIO so no data is lost
  - The exited pod is deleted (matching the old auto_remove="force"
    DockerOperator behaviour) and a fresh one is created on retry.

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
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    API_POLL_INTERVAL,
    API_STREAM_POD_LABELS,
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    POD_NAMESPACE,
    PYTHON_IMAGE,
    STREAM_POD_RESOURCES,
    TASK_SERVICE_ACCOUNT,
    POSTGRES_SERVER,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    k8s_pipeline_env,
)

BUCKET        = Variable.get("default_bucket",    default_var=DEFAULT_BUCKET)
# api_url has NO system-wide default -- it must come from dag_run.conf per run.
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




# ---------------------------------------------------------------------------
# Health check callable
# ---------------------------------------------------------------------------
def _resolve_runtime_api_conf(bucket: str, api_url: str, poll_interval: int):
    """
    Resolve runtime API streaming config with business-specific onboarding values.

    Priority:
      1) Explicit dag_run.conf values (api_url / poll_interval)
      2) Latest onboarding row for this business (api_url)
      3) Airflow params/variables defaults
    """
    resolved_url = (api_url or "").strip()
    try:
        resolved_poll = int(poll_interval)
    except Exception:
        resolved_poll = int(API_POLL_INTERVAL)

    source = "dag_run_conf_or_airflow_defaults"
    if resolved_url:
        return bucket, resolved_url, resolved_poll, source

    try:
        import psycopg2 as _psycopg2

        conn = _psycopg2.connect(
            host=POSTGRES_SERVER,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT api_url
                    FROM onboarding
                    WHERE business_id = %s
                    ORDER BY is_completed DESC, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (bucket,),
                )
                row = cur.fetchone()
                if row and (row[0] or "").strip():
                    resolved_url = (row[0] or "").strip()
                    source = "onboarding"
    except Exception as exc:
        print(f"Warning: Could not resolve onboarding API config for {bucket}: {exc}")

    return bucket, resolved_url, resolved_poll, source


def verify_api_health(**context):
    """
    Check that the user-provided API endpoint is reachable before starting
    the continuous polling process.  Raises on failure so Airflow retries.
    """
    conf = context["dag_run"].conf or {}

    # IMPORTANT: mirror db_streaming precedence behavior.
    # Use only explicit dag_run.conf values for first-pass resolution.
    # If omitted, resolver pulls onboarding values; only then we apply
    # Airflow defaults.
    explicit_bucket = conf.get("bucket")
    explicit_api_url = conf.get("api_url")
    explicit_poll_interval = conf.get("poll_interval")

    bucket = explicit_bucket or BUCKET
    api_url = explicit_api_url
    poll_interval = explicit_poll_interval if explicit_poll_interval is not None else POLL_INTERVAL

    bucket, api_url, poll_interval, cfg_source = _resolve_runtime_api_conf(
        bucket,
        api_url,
        poll_interval,
    )

    print(f"Resolved api_streaming config source: {cfg_source}")
    print(f"Resolved API URL: {api_url}")
    print(f"Resolved poll interval: {poll_interval}s")

    if not api_url:
        raise RuntimeError(
            "dag_run.conf must include 'api_url' -- the user's external API endpoint. "
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
                return {
                    "bucket": bucket,
                    "api_url": api_url,
                    "poll_interval": poll_interval,
                    "config_source": cfg_source,
                }
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue   # endpoint doesn't exist, try next
            raise RuntimeError(f"API health check failed: {exc}") from exc
        except Exception:
            continue

    raise RuntimeError(
        f"Could not reach the user API at {api_url}. "
        "Ensure the endpoint is running and accessible from within the cluster."
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
    schedule_interval=None,      # USER-TRIGGERED -- never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=25,             # one perpetual run per active tenant; matches db_streaming_dag
    default_args=_batch_defaults,
    params={
        "bucket":        BUCKET,
        "api_url":       "",          # REQUIRED -- provided per run via dag_run.conf
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
    #
    # This pod also needs internet egress (the user's api_url is an
    # arbitrary external host) - see the pulse-api-stream-netpol entry in
    # networkpolicy.yaml, which grants that the same way pulse-api-netpol
    # does for Gemini/Google/SMTP.
    run_api_streaming = KubernetesPodOperator(
        task_id="run_api_mapping_stream",
        name="pulse-api-stream",
        namespace=POD_NAMESPACE,
        image=PYTHON_IMAGE,
        cmds=["python3", "/app/mapping/run_mapping.py"],
        arguments=[
            "--mode", "api",
            # dag_run.conf overrides; params are fallback for manual UI runs.
            "--business-id",     "{{ (ti.xcom_pull(task_ids='check_api_health') or {}).get('bucket') or dag_run.conf.get('bucket') or params.bucket }}",
            "--api-url",         "{{ (ti.xcom_pull(task_ids='check_api_health') or {}).get('api_url') or dag_run.conf.get('api_url') or params.api_url }}",
            "--api-poll-interval","{{ (ti.xcom_pull(task_ids='check_api_health') or {}).get('poll_interval') or dag_run.conf.get('poll_interval') or params.poll_interval }}",
            # No --trigger-once: must run indefinitely.
            # No --poll-duration: must poll forever.
            # No --enable-downstream: downstream is a scheduled Airflow batch job.
        ],
        env_vars=k8s_pipeline_env(),
        service_account_name=TASK_SERVICE_ACCOUNT,
        labels=API_STREAM_POD_LABELS,
        container_resources=STREAM_POD_RESOURCES(),
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,   # matches the old auto_remove="force"
        **_streaming_defaults,
    )

    check_api_health >> run_api_streaming
