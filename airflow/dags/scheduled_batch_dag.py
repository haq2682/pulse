"""
Airflow DAG: Scheduled Batch Pipeline (Streaming Tenants)
==========================================================
Runs the downstream batch pipeline for ALL db-mode and api-mode streaming
tenants every 10 minutes.

Architecture
------------
This DAG sits at the boundary between the streaming and batch layers:

  Streaming layer  (db_streaming / api_streaming DAGs)
    Source DB / API  →  Kafka  →  StreamingNormalization  →  mapped/ (Delta)

  Batch layer  (THIS DAG — runs every 10 min)
    mapped/  →  Cleaning  →  Transformation  →  Analysis  →  ML  →  Dashboard

Why batch-only downstream?
--------------------------
Running clean → ML inline after every Spark micro-batch wastes resources:
the pipeline is re-run for potentially a handful of new CDC rows, and the
cleaning / ML stages have significant fixed start-up costs (Spark session,
model loading).  A 10-minute batch cadence amortises those costs while still
delivering near-real-time analytics for end-users.

Cleaning uses the IncrementalCleaner (incremental=True by default) so only
the NEW Delta partitions written since the last clean run are re-processed.
Historical data is not re-cleaned on every cycle.

Steps (per tenant bucket, sequential)
--------------------------------------
  1. clean               (incremental — only new mapped/ partitions)
  2. transform
  3. analyze
  4. specific_model_training  (always retrained on current-cycle data)
  5. ml_infer
  6. trigger_drift_check      (async — fires ml_retrain DAG for general model KS drift)

Multi-tenant
------------
All active streaming tenants are discovered at runtime from PostgreSQL
(businesses where onboarding.ingestion_type IN ('db', 'api') and the
pipeline has completed at least once).

Multiple buckets are processed in parallel — each tenant runs as an independent
Airflow task instance via expand() — so N tenants do not serialise into N ×
pipeline_time.

Per-tenant queue / concurrency
-------------------------------
max_active_runs=1 on this DAG ensures no two scheduled runs overlap.
Each tenant's steps are sequential within a run; different tenants fan out
in parallel via ThreadPoolExecutor.

If the previous run has not finished when the 10-minute trigger fires,
Airflow queues the new run (max_active_runs=1 → it waits).  This is the
correct behaviour: better to let the active run finish than to start a
concurrent run that would contend for Spark / MinIO resources.

Relation to other DAGs
-----------------------
  batch_downstream_dag  : user-triggered after onboarding (batch mode +
                          first streaming confirm-mapping).  NOT scheduled.
  THIS DAG              : 10-min schedule for ongoing streaming tenants.
  ml_retrain_dag        : async KS-test + optional retrain; triggered from
                          batch_downstream_dag and from this DAG.

Manual rerun from the dashboard
--------------------------------
POST /pipeline/trigger-streaming  calls  pipeline_service.start_pipeline()
which triggers a new batch_downstream Airflow DAG run for that single tenant.
This is the "rerun pipeline" button on the streaming-indicator in the dashboard.

Per-tenant parallelism model
-----------------------------
All discovered tenants start their pipelines concurrently (one thread each).
Business-level isolation is enforced by a pre-check inside
_run_batch_pipeline_for_bucket: if pipeline_status already has a 'running' row
for that business_id (whether from this DAG or from a user-triggered
batch_downstream run), the scheduled run for that tenant is skipped silently.

Spark-level resource contention is handled naturally by the Spark standalone
master: when submitted cores exceed available workers, extra jobs queue and run
as soon as cores free up.  Jobs never fail due to resource contention — they
only fail if a step exits non-zero or times out after _STEP_TIMEOUT_SECONDS.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    POSTGRES_SERVER,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    run_k8s_task_pod,
)

# ---------------------------------------------------------------------------
# Airflow Variables (override in Admin → Variables to tune without deploys)
# ---------------------------------------------------------------------------
BUCKET   = Variable.get("default_bucket",          default_var=DEFAULT_BUCKET)
SCHEDULE = Variable.get("scheduled_batch_interval", default_var="*/10 * * * *")

# Hard timeout per step per bucket (15 minutes).
_STEP_TIMEOUT_SECONDS = 900


# ---------------------------------------------------------------------------
# Helper: write pipeline_status for the scheduled batch run of a bucket
# ---------------------------------------------------------------------------

def _update_pipeline_status_for_bucket(
    bucket: str,
    status: str,
    step: str = "",
    error: str = "",
) -> None:
    """
    Upsert a ``pipeline_status`` row for the scheduled batch run of *bucket*.

    Uses a deterministic ``pipeline_id`` of ``scheduled-{bucket}`` so there
    is always at most one scheduled-batch row per tenant in the table.

    This serves two purposes:
      1. The ``discover_active_streaming_buckets`` SQL guard
         (NOT EXISTS status='running') correctly skips tenants whose
         scheduled batch is still in progress, preventing duplicate runs.
      2. The API's ``/pipeline/status`` endpoint returns ``'running'`` while
         the batch is active, so the frontend shows the rotating spinner on
         the Database / API tag.

    The function is best-effort: a DB failure is logged and does NOT abort
    the pipeline.
    """
    import psycopg2 as _psycopg2

    pipeline_id = f"scheduled-{bucket}"
    try:
        conn = _psycopg2.connect(
            host=POSTGRES_SERVER,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn:
            with conn.cursor() as cur:
                # Look up the business owner so we satisfy the FK constraint.
                cur.execute(
                    "SELECT user_id FROM businesses WHERE business_id = %s LIMIT 1",
                    (bucket,),
                )
                row = cur.fetchone()
                if not row:
                    print(
                        f"  [{bucket}] ⚠️  Cannot find user_id for bucket — "
                        "pipeline_status update skipped."
                    )
                    return
                user_id = row[0]

                if status == "running":
                    cur.execute(
                        """
                        INSERT INTO pipeline_status
                            (pipeline_id, business_id, user_id, status,
                             current_step, progress_percentage, started_at,
                             completed_at, error_message, process_ids)
                        VALUES (%s, %s, %s, 'running', %s, 0, NOW(),
                                NULL, NULL,
                                '{"dag_id": "scheduled_batch"}')
                        ON CONFLICT (pipeline_id) DO UPDATE
                        SET status              = 'running',
                            current_step        = EXCLUDED.current_step,
                            progress_percentage = 0,
                            started_at          = NOW(),
                            completed_at        = NULL,
                            error_message       = NULL
                        """,
                        (
                            pipeline_id, bucket, user_id,
                            step or "Scheduled batch pipeline starting",
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE pipeline_status
                        SET status              = %s,
                            current_step        = %s,
                            progress_percentage = %s,
                            completed_at        = NOW(),
                            error_message       = %s
                        WHERE pipeline_id = %s
                        """,
                        (
                            status,
                            step or (
                                "Pipeline completed successfully"
                                if status == "completed"
                                else "Pipeline failed"
                            ),
                            100 if status == "completed" else 0,
                            error or None,
                            pipeline_id,
                        ),
                    )
        conn.close()
        print(f"  [{bucket}] pipeline_status → '{status}'")
    except Exception as exc:
        # Non-fatal: a status-write failure must never abort the pipeline.
        print(f"  [{bucket}] ⚠️  Could not update pipeline_status: {exc}")

_task_defaults = dict(
    owner=DEFAULT_TASK_ARGS["owner"],
    depends_on_past=False,
    retries=DEFAULT_TASK_ARGS["retries"],
    retry_exponential_backoff=DEFAULT_TASK_ARGS["retry_exponential_backoff"],
    retry_delay=timedelta(seconds=DEFAULT_TASK_ARGS["retry_delay_seconds"]),
    max_retry_delay=timedelta(seconds=DEFAULT_TASK_ARGS["max_retry_delay_seconds"]),
    execution_timeout=timedelta(seconds=DEFAULT_TASK_ARGS["execution_timeout_seconds"]),
    email_on_failure=DEFAULT_TASK_ARGS["email_on_failure"],
    email_on_retry=DEFAULT_TASK_ARGS["email_on_retry"],
)


# ---------------------------------------------------------------------------
# Helper: ensure specific ML models exist for a bucket
# ---------------------------------------------------------------------------

# (specific model training is now step 4 inside _run_batch_pipeline_for_bucket,
#  using the freshly transformed data from the current cycle)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def discover_active_streaming_buckets(**context) -> list:
    """
    Query PostgreSQL for all business IDs that:
      • use db or api ingestion mode
      • have had at least one completed pipeline run (the downstream stages
        have run at least once after onboarding, so cleaned/transformed data
        exists in MinIO)

    Results are pushed to XCom under key ``active_buckets``.
    Raises on DB error (lets Airflow retry the task) rather than falling back
    to a default bucket, which would silently skip all other tenants.
    Returns an empty list when no tenants qualify — DAG completes with 0
    task instances, which is correct and not an error.
    """
    import psycopg2

    buckets: list = []
    try:
        conn = psycopg2.connect(
            host=POSTGRES_SERVER,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn.cursor() as cur:
            # Select streaming tenants that have completed the pipeline at
            # least once — i.e., cleaned and transformed data exists.
            cur.execute(
                """
                SELECT DISTINCT o.business_id
                FROM onboarding o
                WHERE o.ingestion_type IN ('db', 'api')
                  AND o.is_completed = true
                  AND o.business_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM pipeline_status ps
                      WHERE ps.business_id = o.business_id
                        AND ps.status = 'completed'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM pipeline_status ps2
                      WHERE ps2.business_id = o.business_id
                        AND ps2.status = 'running'
                        -- Ignore stale 'running' rows older than 60 minutes.
                        -- A row this old means the worker was killed (SIGKILL /
                        -- host reboot) and never wrote a terminal status.
                        -- Without this guard, one crashed run permanently
                        -- excludes the tenant from all future scheduled batches.
                        AND ps2.started_at > NOW() - INTERVAL '60 minutes'
                  )
                ORDER BY o.business_id
                """
            )
            buckets = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception as exc:
        raise RuntimeError(
            f"Could not query active streaming buckets from PostgreSQL: {exc}. "
            "Check DB connectivity and retry."
        ) from exc

    if not buckets:
        print("No active streaming buckets found — nothing to process this cycle.")

    print(f"Active streaming buckets ({len(buckets)}): {buckets}")
    context["ti"].xcom_push(key="active_buckets", value=buckets)
    return buckets


# (ensure_specific_models_trained removed — specific model training now runs
#  as step 4 inside _run_batch_pipeline_for_bucket, using current-cycle data)


def _run_batch_pipeline_for_bucket(bucket: str) -> tuple:
    """
    Run the full batch pipeline for a single tenant bucket via the Docker SDK.

    Steps run sequentially (each depends on the previous output):
      clean (incremental) → transform → analyze → specific_model_training → ml_infer

    Specific model training runs here — after the latest data has been
    cleaned, transformed, and analyzed — so the models always train on
    the current cycle's data before inference runs.

    Includes a per-tenant lock: if ``pipeline_status.status = 'running'``
    already exists for this bucket (set by a concurrent run of this function
    OR by an active ``batch_downstream`` DAG run triggered from the
    frontend), the function returns immediately without running any steps.

    Returns (bucket, success: bool).
    """
    # ── Atomic per-tenant lock via PostgreSQL advisory lock ─────────────
    #
    # pg_try_advisory_lock(key) is a session-level exclusive lock.  Only one
    # PostgreSQL session can hold a given key at a time.  Because it is
    # session-level it is held until explicitly released or the connection
    # is closed — which means it spans the entire pipeline run below.
    #
    # This replaces the old SELECT … INSERT two-step (TOCTOU race) with a
    # single atomic operation:
    #   • Two scheduled_batch threads for the same bucket race → only one
    #     obtains the advisory lock; the other is skipped immediately.
    #   • A batch_downstream DAG run does NOT hold an advisory lock, so we
    #     perform a pipeline_status check INSIDE the advisory lock to handle
    #     that case (still a single serialised check, no race).
    import psycopg2 as _psycopg2

    _lock_conn = None
    _lock_cur  = None
    try:
        _lock_conn = _psycopg2.connect(
            host=POSTGRES_SERVER,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        _lock_conn.autocommit = True
        _lock_cur = _lock_conn.cursor()

        # Attempt to acquire an exclusive advisory lock for this bucket.
        # hashtext() maps the bucket UUID string to a stable 32-bit int key.
        _lock_cur.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))", (bucket,)
        )
        _lock_acquired = _lock_cur.fetchone()[0]

        if not _lock_acquired:
            print(
                f"  [{bucket}] ⏭  Advisory lock already held — another scheduled"
                " batch thread is running this tenant. Skipping."
            )
            return bucket, True  # Not a failure; intentionally skipped.

        # Lock acquired.  Now safely check whether a batch_downstream-triggered
        # pipeline is already running for this tenant (it does not use advisory
        # locks so we must query pipeline_status).
        _lock_cur.execute(
            "SELECT 1 FROM pipeline_status "
            "WHERE business_id = %s AND status = 'running' LIMIT 1",
            (bucket,),
        )
        if _lock_cur.fetchone():
            print(
                f"  [{bucket}] ⏭  pipeline_status shows a running pipeline "
                "(batch_downstream or another process) — skipping scheduled batch."
            )
            _lock_cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (bucket,))
            return bucket, True  # Not a failure; intentionally skipped.

    except Exception as _exc:
        print(f"  [{bucket}] ⚠️  Could not acquire advisory lock: {_exc}")
        # Fall through — best-effort; do not block the pipeline on a lock failure.

    # ── Mark this tenant as running ───────────────────────────────────────
    _update_pipeline_status_for_bucket(bucket, "running", "Scheduled batch pipeline starting")

    steps = [
        # (step_name, script_path, extra_args, timeout_override_seconds)
        # timeout_override_seconds=None uses the default _STEP_TIMEOUT_SECONDS.
        (
            "cleaning",
            "cleaning/cleaning.py",
            # incremental=True by default in cleaning.py;
            # --incremental flag makes it explicit and visible in logs.
            f"--bucket-name {bucket} --incremental",
            None,
        ),
        (
            "transformation",
            "transformation/transformation.py",
            f"--bucket-name {bucket}",
            None,
        ),
        (
            "analysis",
            "analysis/analysis.py",
            f"--bucket-name {bucket}",
            None,
        ),
        # (
        #     # Specific models always retrain on every cycle using the freshly
        #     # transformed data produced above.  They are per-tenant and cannot
        #     # be shared across businesses.  Training runs here (inside the
        #     # advisory lock) so a failure aborts this bucket and Airflow retries
        #     # it without affecting any other tenant's pipeline.
        #     "specific_model_training",
        #     "machine-learning/specific/train.py",
        #     f"--bucket-name {bucket}",
        #     _STEP_TIMEOUT_SECONDS * 2,   # training is slower than other steps
        # ),
        # (
        #     "ml_inference",
        #     "machine-learning/infer_all.py",
        #     f"--bucket-name {bucket}",
        #     None,
        # ),
    ]

    print(f"\n{'='*60}\n[{bucket}] Starting batch pipeline\n{'='*60}")

    # Wrap all step execution in try/finally so the advisory lock is released
    # on every exit path — success, step failure, timeout, or unexpected exception.
    _result = (bucket, False)
    try:
        for step_name, script, args_str, timeout_override in steps:
            step_timeout = timeout_override if timeout_override else _STEP_TIMEOUT_SECONDS
            print(f"  [{bucket}] ▶ {step_name}")
            _step_cmd = ["python3", f"/app/{script}"] + args_str.split()

            # run_k8s_task_pod creates a fresh Kubernetes Pod for this one
            # step, waits for it to finish (or the timeout to expire),
            # returns its exit code + combined log output, and always
            # deletes the pod - the Kubernetes-native replacement for what
            # exec_run() into a shared long-running container used to do.
            try:
                _returncode, _log_output = run_k8s_task_pod(
                    name=f"pulse-sched-{step_name}",
                    command=_step_cmd,
                    timeout_seconds=step_timeout,
                )
            except TimeoutError:
                print(
                    f"  [{bucket}] ⏱  {step_name} timed out after {step_timeout}s "
                    "— aborting this bucket."
                )
                _update_pipeline_status_for_bucket(
                    bucket, "failed",
                    step=f"Timed out at: {step_name}",
                    error=f"{step_name} timed out after {step_timeout}s",
                )
                return bucket, False

            log_tail = (_log_output or "")[-800:]

            if _returncode == 0:
                print(f"  [{bucket}] ✅ {step_name} completed")
            else:
                err_msg = (
                    f"{step_name} exited with code {_returncode}. "
                    + (log_tail if log_tail else "")
                )
                print(
                    f"  [{bucket}] ⚠️  {step_name} exited with code {_returncode}"
                    + (f"\n     log: {log_tail}" if log_tail else "")
                )
                # Abort the remaining steps for this bucket so we don't
                # transform/analyze stale or partially-cleaned data.
                print(f"  [{bucket}] Aborting remaining steps.")
                _update_pipeline_status_for_bucket(
                    bucket, "failed",
                    step=f"Failed at: {step_name}",
                    error=err_msg[:500],
                )
                return bucket, False

        print(f"\n  [{bucket}] ✅ Batch pipeline completed successfully.")
        _update_pipeline_status_for_bucket(
            bucket, "completed", step="Pipeline completed successfully"
        )
        _result = (bucket, True)
        return bucket, True

    finally:
        # Always release the advisory lock regardless of how the function exits:
        # success, step failure, timeout, or unexpected exception.  PostgreSQL
        # also releases session-level locks automatically when the connection
        # closes, but explicit unlock is preferred so the lock is freed as soon
        # as possible for the next scheduled run.
        try:
            if _lock_cur and _lock_conn and not _lock_conn.closed:
                _lock_cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (bucket,))
                _lock_cur.close()
                _lock_conn.close()
        except Exception:
            pass


def _trigger_drift_check_for_bucket(bucket: str) -> None:
    """
    Fire the ml_retrain DAG asynchronously for a single tenant bucket.

    Called immediately after that tenant's pipeline succeeds so each
    business gets its drift check as soon as its own data is fresh —
    with no dependency on any other tenant finishing.

    Non-fatal: a failure here is logged but never raises, so it cannot
    prevent the task instance from being marked as successful.
    """
    import urllib.request
    import json as _json
    import base64

    airflow_base     = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    airflow_user     = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")
    url              = f"{airflow_base}/api/v1/dags/ml_retrain/dagRuns"
    encoded = base64.b64encode(
        f"{airflow_user}:{airflow_password}".encode()
    ).decode()

    payload = _json.dumps(
        {"conf": {"bucket": bucket, "source_dag": "scheduled_batch"}}
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {encoded}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode(errors="replace")
            print(
                f"  [{bucket}] ml_retrain triggered "
                f"(HTTP {resp.status}): {body[:200]}"
            )
    except Exception as exc:
        print(f"  [{bucket}] ⚠️  Could not trigger ml_retrain: {exc}")


def run_pipeline_for_single_bucket(bucket: str, **context) -> None:
    """
    Airflow task callable for dynamic task mapping (expand()).

    Wraps _run_batch_pipeline_for_bucket so that each tenant bucket becomes
    an independent Airflow task instance with its own:
      • log stream
      • retry counter  (up to DEFAULT_TASK_ARGS["retries"])
      • success / failure state visible in the Airflow UI grid view

    On success, immediately triggers the ml_retrain drift-check DAG for
    this bucket — no waiting for other tenants to finish first.

    Raises RuntimeError on failure so Airflow marks this task instance as
    failed (and retries it) without affecting other tenant task instances.
    """
    _, success = _run_batch_pipeline_for_bucket(bucket)
    if not success:
        raise RuntimeError(
            f"Batch pipeline failed for tenant '{bucket}'. "
            "See task log for step-level details."
        )
    # ML forecasting/prediction is temporarily disabled project-wide (see
    # ml_retrain_dag.py) - firing this would just trigger a DAG with no
    # active tasks, so it's commented out rather than left calling into it.
    # Trigger drift check immediately — this tenant's data is fresh right now.
    # Does not wait for any other tenant; does not block on failure.
    # _trigger_drift_check_for_bucket(bucket)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="scheduled_batch",
    description=(
        "10-minute scheduled batch pipeline for db/api streaming tenants: "
        "clean (incremental) → transform → analyze → ml_infer → drift_check. "
        "Processes all active streaming tenants discovered from PostgreSQL."
    ),
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # max_active_runs=1 ensures no two scheduled runs overlap.
    # If the previous 10-min run is still in progress, the new run is queued
    # and starts immediately after the active run finishes.
    max_active_runs=1,
    tags=["pulse", "batch", "scheduled", "streaming"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Discover all active streaming tenant buckets ───────────────────
    discover = PythonOperator(
        task_id="discover_buckets",
        python_callable=discover_active_streaming_buckets,
    )

    # ── 2. Run batch pipeline — one task instance per tenant ─────────────
    # Dynamic task mapping (Airflow 2.3+): expand() creates one independent
    # task instance per bucket returned by discover_buckets.
    #
    # Each instance gets its own:
    #   • log stream     — visible at the per-bucket row in the Grid view
    #   • retry counter  — a failing tenant is retried without re-running others
    #   • status cell    — green/red per tenant, not aggregate
    #
    # execution_timeout is per task instance (i.e. per tenant).
    # The advisory lock inside run_pipeline_for_single_bucket ensures that
    # even if Airflow schedules two instances for the same bucket concurrently
    # (e.g. during a manual backfill), only one proceeds.
    run_pipeline = PythonOperator.partial(
        task_id="run_pipeline_for_bucket",
        python_callable=run_pipeline_for_single_bucket,
        execution_timeout=timedelta(hours=2),
    ).expand(op_args=discover.output.map(lambda b: [b]))

    # ── Sequential dependencies ────────────────────────────────────────────
    # Each tenant's full pipeline (clean → transform → analyze →
    # specific_train → infer → drift_check) runs as an independent task
    # instance.  No separate pre-training task exists; specific model training
    # runs inside each bucket's task using freshly transformed data.
    #
    #   discover_buckets
    #       ├─ run_pipeline_for_bucket[0]  (train+infer+drift_check for A)
    #       ├─ run_pipeline_for_bucket[1]  (train+infer+drift_check for B)
    #       └─ run_pipeline_for_bucket[N]  (train+infer+drift_check for N)
    #
    # A failing or retrying tenant never delays any other tenant.
    discover >> run_pipeline
