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
    → ensure_specific_models_trained   (trains specific models on first cycle)
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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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


def _docker_exec(script_path: str, extra_args: str = "") -> str:
    return f"docker exec {PYTHON_CONTAINER} python /app/{script_path} {extra_args}"


# ---------------------------------------------------------------------------
# Task callable: first-run specific model training
# ---------------------------------------------------------------------------

def ensure_specific_models_trained(**context):
    """
    Guarantee that specific ML models are trained for this business bucket
    before inference runs.

    Logic:
      1. Check MinIO for each specific model's drift baseline JSON.
         (Baseline existence ≡ model has been trained at least once.)
      2. If ALL baselines exist → skip (fast path, completes in seconds).
      3. If ANY baseline is missing → run specific/train.py inside the
         python container, then save fresh baselines for all specific models.

    This runs every 10 minutes with the DAG, but after the first successful
    training the MinIO check short-circuits in milliseconds.
    """
    import subprocess
    from minio import Minio
    from minio.error import S3Error

    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", BUCKET)

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
            "python", "/app/machine-learning/specific/train.py",
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


with DAG(
    dag_id="streaming_downstream",
    description=(
        "Fallback downstream processing for streaming: "
        "clean (incremental) → transform → analyze → ml_infer. "
        "Primary processing is inline; this DAG catches up every 2 min."
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

    # ── 1. Clean (incremental) ─────────────────────────────────────────────
    # --incremental: cleaning.py checks Redis for which mapped/ files it has
    # already processed and skips them.  Only new files are cleaned.
    # If nothing is new, this step completes in seconds.
    clean_incremental = BashOperator(
        task_id="clean_incremental",
        bash_command=_docker_exec(
            "cleaning/cleaning.py",
            "--bucket-name {{ params.bucket }} --incremental",
        ),
    )

    # ── 2. Transform ───────────────────────────────────────────────────────
    transform = BashOperator(
        task_id="transform",
        bash_command=_docker_exec(
            "transformation/transformation.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 3. Analyze ─────────────────────────────────────────────────────────
    analyze = BashOperator(
        task_id="analyze",
        bash_command=_docker_exec(
            "analysis/analysis.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 4. Ensure specific models are trained for this business ───────────
    # Fast no-op when baselines already exist; runs specific/train.py only
    # on the very first streaming cycle where transformed data is available.
    ensure_specific = PythonOperator(
        task_id="ensure_specific_models_trained",
        python_callable=ensure_specific_models_trained,
        execution_timeout=timedelta(hours=2),
    )

    # ── 5. ML Inference ────────────────────────────────────────────────────
    ml_infer = BashOperator(
        task_id="ml_infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 6. Trigger KS drift check (async, weekly DAG handles retraining) ──
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "streaming_downstream"},
        wait_for_completion=False,
    )

    # ── Sequential dependencies ────────────────────────────────────────────
    clean_incremental >> transform >> analyze >> ensure_specific >> ml_infer >> trigger_drift_check
