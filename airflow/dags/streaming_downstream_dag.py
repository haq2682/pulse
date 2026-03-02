"""
Airflow DAG: Streaming Downstream Pipeline
==========================================
Processes data that has been mapped and written to MinIO  mapped/  by
the continuous db_streaming or api_streaming DAGs.

Reduced-interval fallback
--------------------------
The primary downstream processing now runs INLINE within the streaming
job itself (via ``--enable-downstream``), executing the downstream
pipeline (clean → transform → analyze → ML inference) in a background
thread immediately after each Spark micro-batch.  This achieves ~10 s –
2 min end-to-end latency.

This DAG remains as a FALLBACK / catch-up mechanism that runs every
2 minutes (configurable).  It ensures no data is left unprocessed if
the inline downstream was temporarily unable to run (e.g. resource
contention, script error, or the streaming job restarting).

Because cleaning uses ``--incremental``, duplicate processing is
avoided — files already cleaned by the inline downstream are skipped.

Flow (sequential)
-----------------
  clean_incremental
    → transform
    → analyze
    → ensure_specific_models_trained   (trains specific models on first cycle; fast no-op thereafter)
    → ml_infer
    → trigger_drift_check   (fires ml_retrain asynchronously if drift found)

Relation to db_streaming / api_streaming
-----------------------------------------
  db_streaming DAG   ─┐
  api_streaming DAG  ─┤──→  MinIO mapped/  ──→  [inline downstream]  ──→  dashboard
                      │                     ──→  [THIS DAG fallback]  ──→  dashboard
"""

from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PYTHON_CONTAINER,
    SPECIFIC_MODELS,
)

BUCKET   = Variable.get("default_bucket",              default_var=DEFAULT_BUCKET)
SCHEDULE = Variable.get("streaming_downstream_interval", default_var="*/2 * * * *")

# Per-step hard timeout for downstream subprocess calls (15 minutes).
_STEP_TIMEOUT_SECONDS = 900

# Maximum number of tenant buckets processed in parallel inside a single
# Airflow task.  Each parallel worker spawns its own ``docker exec`` chain
# (clean → transform → analyze → ML), so set this to a value that the host
# can sustain without OOM.  Override with DOWNSTREAM_MAX_PARALLEL_BUCKETS
# in the environment / Airflow Variables.
_MAX_PARALLEL_BUCKETS = int(
    Variable.get("downstream_max_parallel_buckets", default_var="8")
)

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
# Task callable: first-run specific model training
# ---------------------------------------------------------------------------

def _ensure_specific_models_for_bucket(bucket: str) -> None:
    """
    Guarantee that specific ML models are trained for a single bucket.
    Called once per active bucket by ensure_specific_models_trained.
    """
    import subprocess
    from minio import Minio
    from minio.error import S3Error

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    missing = []
    for model in SPECIFIC_MODELS:
        key = f"models/drift_baselines/{model}/baseline.json"
        try:
            client.stat_object(bucket, key)
        except S3Error:
            missing.append(model)

    if not missing:
        print(
            f"All {len(SPECIFIC_MODELS)} specific model baselines already exist "
            f"in bucket '{bucket}'. Skipping initial training."
        )
        return

    print(
        f"Missing baselines for: {missing}. "
        f"Running initial specific model training for bucket '{bucket}'..."
    )

    result = subprocess.run(
        [
            "docker", "exec", PYTHON_CONTAINER,
            "python3", "/app/machine-learning/specific/train.py",
            "--bucket-name", bucket,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"specific/train.py failed (exit {result.returncode}). "
            "See task logs for details."
        )

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from utils.model_baseline import save_baselines_for_all_models

    results = save_baselines_for_all_models(bucket=bucket, model_names=SPECIFIC_MODELS)
    failed  = [m for m, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(f"Failed to save baselines for specific models: {failed}")

    print(
        f"Initial specific model training complete. "
        f"Baselines saved for {len(SPECIFIC_MODELS)} models in bucket '{bucket}'."
    )


def ensure_specific_models_trained(**context):
    """
    Guarantee that specific ML models are trained for EVERY active tenant
    bucket discovered by get_active_business_ids.

    Buckets are processed in parallel (up to _MAX_PARALLEL_BUCKETS concurrent
    threads) so that N-tenant first-run training does not take N × training_time.
    """
    buckets = (
        context["ti"].xcom_pull(task_ids="get_active_buckets", key="active_buckets")
        or [BUCKET]
    )

    def _train_one(bucket):
        print(f"\n{'='*60}\nEnsuring specific models for bucket: {bucket}\n{'='*60}")
        try:
            _ensure_specific_models_for_bucket(bucket)
            return bucket, None
        except Exception as exc:
            return bucket, exc

    with ThreadPoolExecutor(max_workers=min(len(buckets), _MAX_PARALLEL_BUCKETS)) as pool:
        futures = {pool.submit(_train_one, b): b for b in buckets}
        for fut in as_completed(futures):
            bucket, err = fut.result()
            if err:
                # Non-fatal: log and continue so other tenants are not blocked.
                print(f"  ⚠️  ensure_specific_models failed for {bucket}: {err}")


def trigger_drift_check_for_all_buckets(**context):
    """
    Trigger the ml_retrain DAG for every active tenant bucket so that KS
    drift detection runs for each one — not only the default bucket.
    """
    import urllib.request
    import urllib.error
    import json as _json
    import base64

    buckets = (
        context["ti"].xcom_pull(task_ids="get_active_buckets", key="active_buckets")
        or [BUCKET]
    )

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")
    url = f"{airflow_base}/api/v1/dags/ml_retrain/dagRuns"
    encoded = base64.b64encode(f"{airflow_user}:{airflow_password}".encode()).decode()

    for bucket in buckets:
        payload = _json.dumps({
            "conf": {"bucket": bucket, "source_dag": "streaming_downstream"}
        }).encode()
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
                print(f"ml_retrain triggered for bucket '{bucket}' (HTTP {resp.status}): {body[:200]}")
        except Exception as exc:
            # Non-fatal: log and continue to the next bucket
            print(f"⚠️  Could not trigger ml_retrain for bucket '{bucket}': {exc}")


def get_active_business_ids(**context):
    """
    Query PostgreSQL for all business_ids that have completed onboarding.
    Results are pushed to XCom so downstream tasks can iterate over them.
    Falls back to the single BUCKET variable if the DB query fails.
    """
    import psycopg2
    buckets = []
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_SERVER", "postgresql"),
            database=os.getenv("POSTGRES_DATABASE_NAME", "pulse"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT business_id FROM onboarding WHERE is_completed = true AND business_id IS NOT NULL"
            )
            buckets = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception as exc:
        print(f"⚠️  Could not query active business IDs from DB: {exc}")

    if not buckets:
        print(f"No active business IDs found — falling back to default bucket '{BUCKET}'")
        buckets = [BUCKET]

    print(f"Active business IDs: {buckets}")
    context["ti"].xcom_push(key="active_buckets", value=buckets)
    return buckets


def _run_downstream_for_bucket(bucket: str) -> tuple[str, bool]:
    """
    Execute the full downstream pipeline for a single tenant bucket.

    Steps run sequentially (each depends on the previous one's output),
    but multiple buckets run this function concurrently via ThreadPoolExecutor.

    Returns (bucket, success_bool).
    """
    import subprocess

    _steps = [
        ("cleaning",       "cleaning/cleaning.py",            f"--bucket-name {bucket} --incremental"),
        ("transformation", "transformation/transformation.py", f"--bucket-name {bucket}"),
        ("analysis",       "analysis/analysis.py",            f"--bucket-name {bucket}"),
        ("ml_inference",   "machine-learning/infer_all.py",   f"--bucket-name {bucket}"),
    ]

    print(f"\n{'='*60}\nProcessing downstream for bucket: {bucket}\n{'='*60}")

    for step_name, script, args_str in _steps:
        args = args_str.split()
        cmd = ["docker", "exec", PYTHON_CONTAINER, "python3", f"/app/{script}"] + args
        print(f"  [{bucket}] ▶ {step_name}: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=_STEP_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired:
            print(f"  [{bucket}] ⏱  {step_name} timed out after {_STEP_TIMEOUT_SECONDS}s — aborting bucket.")
            return bucket, False

        if result.returncode == 0:
            print(f"  [{bucket}] ✅ {step_name} completed")
        else:
            stderr_tail = (result.stderr or "")[-600:]
            print(
                f"  [{bucket}] ⚠️  {step_name} failed (exit {result.returncode})"
                + (f"\n     stderr: {stderr_tail}" if stderr_tail else "")
            )
            print(f"  [{bucket}] ⏭  Skipping remaining steps to avoid processing stale data.")
            return bucket, False

    return bucket, True


def run_downstream_for_all_buckets(**context):
    """
    Run the full downstream pipeline for every active tenant bucket **in
    parallel** — up to _MAX_PARALLEL_BUCKETS concurrent pipelines.

    Each bucket's pipeline (clean → transform → analyze → ML) runs
    sequentially inside its own thread so that inter-step dependencies are
    preserved, while different tenants do not block each other.

    With the sequential implementation a single slow tenant (e.g. cold
    Spark start on a large dataset) would delay ALL subsequent tenants;
    this parallel fan-out keeps the fallback DAG within its 2-minute
    schedule even with hundreds of active tenants.
    """
    buckets = (
        context["ti"].xcom_pull(task_ids="get_active_buckets", key="active_buckets")
        or [BUCKET]
    )

    print(f"Starting parallel downstream for {len(buckets)} bucket(s), "
          f"max_workers={min(len(buckets), _MAX_PARALLEL_BUCKETS)}")

    failed_buckets = []
    with ThreadPoolExecutor(
        max_workers=min(len(buckets), _MAX_PARALLEL_BUCKETS),
        thread_name_prefix="downstream-bucket",
    ) as pool:
        futures = {pool.submit(_run_downstream_for_bucket, b): b for b in buckets}
        for fut in as_completed(futures):
            bucket, ok = fut.result()
            if not ok:
                failed_buckets.append(bucket)

    if failed_buckets:
        # Log individually but do not raise — partial success is acceptable;
        # the next scheduled run and the inline downstream will catch up.
        print(f"\n⚠️  {len(failed_buckets)} bucket(s) had downstream errors: {failed_buckets}")
    else:
        print(f"\n✅ All {len(buckets)} bucket(s) processed successfully.")


with DAG(
    dag_id="streaming_downstream",
    description=(
        "Fallback downstream processing for streaming: "
        "clean (incremental) → transform → analyze → ml_infer. "
        "Primary processing is inline; this DAG catches up every 2 min. "
        "Processes all active tenant buckets discovered from the onboarding table."
    ),
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,           # never overlap runs
    tags=["pulse", "streaming", "downstream"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Discover all active tenant buckets ─────────────────────────────
    fetch_buckets = PythonOperator(
        task_id="get_active_buckets",
        python_callable=get_active_business_ids,
    )

    # ── 2. Ensure specific models trained for ALL active buckets ──────────
    # Must run BEFORE ml_infer (inside run_downstream) so that specific model
    # files exist in the business bucket on the very first cycle.
    # Fast no-op per bucket once baselines exist.
    ensure_specific = PythonOperator(
        task_id="ensure_specific_models_trained",
        python_callable=ensure_specific_models_trained,
        execution_timeout=timedelta(hours=2),
    )

    # ── 3. Run full downstream pipeline for ALL discovered buckets ─────────
    # Sequential per-bucket processing: clean (incremental) → transform → analyze → ml_infer
    # Specific models are guaranteed trained by the previous step.
    run_downstream = PythonOperator(
        task_id="run_downstream_all_buckets",
        python_callable=run_downstream_for_all_buckets,
        execution_timeout=timedelta(hours=2),
    )

    # ── 4. Trigger KS drift check for ALL active buckets (async) ──────────
    trigger_drift_check = PythonOperator(
        task_id="trigger_drift_check",
        python_callable=trigger_drift_check_for_all_buckets,
    )

    # ── Sequential dependencies ────────────────────────────────────────────
    fetch_buckets >> ensure_specific >> run_downstream >> trigger_drift_check
