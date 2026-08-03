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

This DAG is responsible ONLY for ingestion + mapping (streaming layer).
All downstream processing (cleaning → transformation → analysis → ML)
runs separately in the ``scheduled_batch`` DAG every 10 minutes.

When does this DAG run?
-----------------------
ONCE -- triggered manually or by the frontend when the user submits their
database URI during onboarding (POST /onboarding/start-mapping, mode=db).

  Frontend  →  POST /api/v1/dags/db_streaming/dagRuns   (Airflow REST API)
                 conf: {
                   "bucket":    "pulse-bucket-1",
                   "db_uri":    "postgresql://user:pass@host:5432/dbname",
                   "db_tables": "orders,payments,inventory"
                 }

Do NOT trigger this DAG multiple times for the same tenant -- it would
start a second competing streaming job for the same bucket.  The API
layer (POST /onboarding/start-mapping) is responsible for checking
whether a run is already active for a given business_id before firing.

Why not scheduled?
------------------
The streaming job is designed to run 24/7.  Scheduling it would either
be a no-op (if already running) or would start duplicate jobs.

About "mapping runs only once"
-------------------------------
The Spark Structured Streaming job does run the mapping algorithm on
EVERY micro-batch -- but that is intentional (new CDC events need to be
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
  - The task pod's container exits with non-zero
  - Airflow marks the task FAILED
  - Airflow waits ``retry_delay`` (1 min) then restarts the task
  - retries=9999 means effectively infinite restarts
  - The exited pod is deleted (matching the old auto_remove="force"
    DockerOperator behaviour) and a fresh one is created on retry.

Spark Structured Streaming uses a MinIO-backed checkpoint
(s3a://pulse-checkpoints/normalize-stream) so restarts pick up exactly
where they left off -- no duplicate or lost events.

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
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DB_STREAM_POD_LABELS,
    DEBEZIUM_URL,
    DEFAULT_BUCKET,
    DEFAULT_DB_TABLES,
    DEFAULT_DB_URI,
    DEFAULT_TASK_ARGS,
    POD_NAMESPACE,
    PYTHON_IMAGE,
    STREAM_POD_RESOURCES,
    TASK_SERVICE_ACCOUNT,
    POSTGRES_SERVER,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    k8s_pipeline_env_templated,
    k8s_pipeline_pod_ip_runtime_env,
    run_k8s_task_pod,
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
    retry_exponential_backoff=False,       # constant delay -- don't wait longer each time
    retry_delay=timedelta(minutes=1),      # wait 1 min before restarting
    execution_timeout=None,                # NEVER timeout -- this runs forever
    email_on_failure=False,
    email_on_retry=False,
)




# ---------------------------------------------------------------------------
# Connector health-check / deploy
# ---------------------------------------------------------------------------
def _resolve_runtime_db_conf(bucket: str, db_uri: str, db_tables: str):
    """
    Resolve runtime DB streaming config with business-specific onboarding values.

    Priority:
      1) Explicit dag_run.conf values (db_uri / db_tables) when provided
      2) Latest completed onboarding row for the business
      3) Airflow defaults / variables
    """
    resolved_uri = (db_uri or "").strip() if isinstance(db_uri, str) else ""
    if isinstance(db_tables, list):
        resolved_tables = ",".join([str(t).strip() for t in db_tables if str(t).strip()])
    else:
        resolved_tables = (db_tables or "").strip() if isinstance(db_tables, str) else ""

    # If explicit values are already provided, keep them.
    source = "dag_run_conf_or_airflow_defaults"
    if resolved_uri and resolved_tables:
        return bucket, resolved_uri, resolved_tables, source

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
                    SELECT db_uri, db_tables
                    FROM onboarding
                    WHERE business_id = %s
                    ORDER BY is_completed DESC, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (bucket,),
                )
                row = cur.fetchone()
                if row:
                    onboarding_uri = (row[0] or "").strip()
                    onboarding_tables = (row[1] or "").strip()
                    if not resolved_uri and onboarding_uri:
                        resolved_uri = onboarding_uri
                        source = "onboarding"
                    if not resolved_tables and onboarding_tables:
                        resolved_tables = onboarding_tables
                        source = "onboarding"
    except Exception as exc:
        print(f"Warning: Could not resolve onboarding DB config for {bucket}: {exc}")

    return bucket, resolved_uri, resolved_tables, source


def check_or_deploy_debezium(**context):
    """
    Verify the Debezium connector for this tenant is deployed and RUNNING.
    If it is missing or in a failed state, redeploy it by calling
    run_mapping.py with --deploy-connector-only, which deploys (or updates)
    the Debezium connector with snapshot.mode=no_data and exits immediately.

    IMPORTANT: we must NOT use --trigger-once here because that flag now
    triggers a full JDBC initial load → batch mapping pipeline.  The
    health-check task should only ensure the connector is alive; it must
    never re-snapshot the database.
    """

    conf      = context["dag_run"].conf or {}
    bucket    = conf.get("bucket", BUCKET)

    # IMPORTANT: pass only EXPLICIT dag_run.conf values into resolver.
    # If we pass Airflow defaults here, onboarding lookup is skipped and
    # stale default credentials/table lists may be used.
    explicit_db_uri = conf.get("db_uri")
    explicit_db_tables = conf.get("db_tables")

    bucket, db_uri, db_tables, cfg_source = _resolve_runtime_db_conf(
        bucket,
        explicit_db_uri,
        explicit_db_tables,
    )

    # Final fallback to Airflow variables only if neither conf nor onboarding
    # provided values.
    if not db_uri:
        db_uri = DB_URI
        cfg_source = "airflow_defaults"
    if not db_tables:
        db_tables = DB_TABLES
        if cfg_source != "airflow_defaults":
            cfg_source = f"{cfg_source}+airflow_defaults"

    if not db_uri:
        raise RuntimeError(
            f"No db_uri provided for bucket '{bucket}'. Pass dag_run.conf.db_uri or ensure onboarding.db_uri exists."
        )
    if not db_tables:
        raise RuntimeError(
            f"No db_tables provided for bucket '{bucket}'. Pass dag_run.conf.db_tables or ensure onboarding.db_tables exists."
        )

    masked_db_uri = db_uri
    if "@" in db_uri and "://" in db_uri:
        scheme, rest = db_uri.split("://", 1)
        if "@" in rest:
            creds, host_part = rest.split("@", 1)
            user = creds.split(":", 1)[0] if creds else ""
            masked_db_uri = f"{scheme}://{user}:***@{host_part}"
    print(f"Resolved db_streaming config source: {cfg_source}")
    print(f"Resolved DB URI: {masked_db_uri}")
    if isinstance(db_tables, str):
        _tbl_list = [t.strip() for t in db_tables.split(",") if t.strip()]
    else:
        _tbl_list = [str(t).strip() for t in (db_tables or []) if str(t).strip()]
    print(f"Resolved DB tables count: {len(_tbl_list)}")
    print(f"Resolved DB tables: {_tbl_list}")

    connector_name = f"pulse-{bucket}-connector"
    status_url     = f"{DEBEZIUM_URL}/connectors/{connector_name}/status"

    try:
        with urllib.request.urlopen(status_url, timeout=10) as resp:
            status          = json.loads(resp.read())
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")

            if connector_state == "RUNNING":
                print(f"Connector '{connector_name}' is RUNNING -- nothing to do.")
                return {
                    "bucket": bucket,
                    "db_uri": db_uri,
                    "db_tables": db_tables,
                }

            print(f"Connector state is '{connector_state}' -- redeploying.")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(f"Connector '{connector_name}' not found -- deploying for the first time.")
        else:
            raise
    except Exception as exc:
        print(f"Could not reach Debezium ({exc}) -- proceeding to deploy.")

    # Deploy / verify with --deploy-connector-only so we get snapshot.mode=no_data.
    # This ensures a connector restart NEVER triggers a full database re-snapshot.
    # Runs in its own Kubernetes Pod (see run_k8s_task_pod) rather than
    # exec'ing into a shared container over docker.sock.
    _cmd = [
        "python3", "/app/mapping/run_mapping.py",
        "--mode", "db",
        "--business-id", bucket,
        "--db-uri",    db_uri,
        "--db-tables", db_tables,
        "--deploy-connector-only",   # deploy connector only -- NO JDBC re-load
    ]
    exit_code, logs = run_k8s_task_pod(
        name="pulse-debezium-deploy",
        command=_cmd,
        timeout_seconds=120,
    )
    if exit_code != 0:
        raise RuntimeError(
            f"Debezium deploy failed (rc={exit_code}).\nOutput: {logs[-2000:]}"
        )
    print("Debezium connector deployed successfully.")
    return {
        "bucket": bucket,
        "db_uri": db_uri,
        "db_tables": db_tables,
    }



# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="db_streaming",
    description=(
        "Manages the continuous Debezium CDC → Spark Streaming → MinIO mapped/ pipeline. "
        "Triggered ONCE when the user connects their database. Auto-restarts on crash."
    ),
    schedule_interval=None,      # USER-TRIGGERED -- never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=25,          # one perpetual run per active tenant; no hard cap needed
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
    # On DAG pause/cancellation: the task pod is deleted → streaming stops.
    # NOTE: does NOT use --trigger-once here.  This is the 24/7 streaming job.
    run_db_streaming = KubernetesPodOperator(
        task_id="run_db_mapping_stream",
        name="pulse-db-stream",
        namespace=POD_NAMESPACE,
        image=PYTHON_IMAGE,
        # PYTHON_IMAGE is tagged :latest, which Kubernetes defaults to
        # imagePullPolicy=Always for - silently re-pulling from Docker Hub
        # over any locally-built/freshly-pushed image otherwise. Same fix
        # already applied to the long-lived Deployments.
        image_pull_policy="IfNotPresent",
        cmds=["python3", "/app/mapping/run_mapping.py"],
        arguments=[
            "--mode", "db",
            # dag_run.conf overrides; params are fallback for manual UI runs.
            "--business-id", "{{ (ti.xcom_pull(task_ids='deploy_debezium_connector') or {}).get('bucket') or dag_run.conf.get('bucket') or params.bucket }}",
            "--db-uri",      "{{ (ti.xcom_pull(task_ids='deploy_debezium_connector') or {}).get('db_uri') or dag_run.conf.get('db_uri') or params.db_uri }}",
            "--db-tables",   "{{ (ti.xcom_pull(task_ids='deploy_debezium_connector') or {}).get('db_tables') or dag_run.conf.get('db_tables') or params.db_tables }}",
            # No --trigger-once: this must run indefinitely.
            # No --enable-downstream: downstream runs as a scheduled Airflow batch job.
        ],
        env_vars=k8s_pipeline_env_templated(),
        pod_runtime_info_envs=k8s_pipeline_pod_ip_runtime_env(),
        service_account_name=TASK_SERVICE_ACCOUNT,
        labels=DB_STREAM_POD_LABELS,
        container_resources=STREAM_POD_RESOURCES(),
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,   # matches the old auto_remove="force"
        **_streaming_defaults,
    )

    deploy_debezium >> run_db_streaming
