"""
Airflow DAG: Batch Downstream Pipeline
=======================================
Runs the post-mapping pipeline stages for BATCH ingestion mode.

When does this run?
-------------------
ONLY after the user has completed the onboarding mapping step (including any
manual column corrections).  It is NOT scheduled — it must be triggered
explicitly, either:

  1. By the Pulse API when the user clicks "Confirm Mapping" on the frontend
     (via Airflow REST API:  POST /api/v1/dags/batch_downstream/dagRuns).
  2. Manually from the Airflow UI (Trigger DAG).

Why not scheduled?
------------------
The batch pipeline is entirely user-driven: the user uploads files, reviews
the mapping results, optionally fixes missing columns, then kicks off the
rest of the pipeline.  A schedule would be meaningless here.

General vs Specific ML models
------------------------------
General models are pre-trained globally (across all businesses) and only
need inference here.  Specific models are per-business: they must be trained
on THIS business's own data before inference can run.

  • ensure_specific_models_trained  checks MinIO for the 10 specific model
    drift baselines stored under  models/drift_baselines/<model>/baseline.json
    in the business bucket.  If any baseline is missing (= model not yet
    trained for this business), it runs  specific/train.py --bucket-name
    inside the python container, then saves fresh baselines.  Subsequent DAG
    runs skip training in seconds (baselines already exist).

  • From then on, ml_retrain handles retraining via KS-test drift detection.

Flow (fully sequential — no parallel branches)
----------------------------------------------
  clean
    → transform
    → analyze
    → ensure_specific_models_trained   (trains specific models on first run)
    → ml_infer
    → trigger_drift_check   (fires ml_retrain DAG asynchronously)

Crash / retry
-------------
Every task retries 3 times with exponential back-off.  If all retries
are exhausted the DAG is marked FAILED and the Airflow UI shows the
exact task and log that failed.  Use "Retry pipeline" on the frontend
(POST /pipeline/retry) or re-trigger from the Airflow UI to resume.

Override bucket per-run
-----------------------
  dag_run.conf = {"bucket": "my-other-bucket"}
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

BUCKET = Variable.get("default_bucket", default_var=DEFAULT_BUCKET)

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

    Subsequent DAG runs always take the fast path once the first training
    succeeds.  All future retraining is handled by ml_retrain (KS-test driven).
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

    # Save drift baselines so the next run takes the fast path and
    # ml_retrain has a baseline to compare against.
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
    dag_id="batch_downstream",
    description=(
        "Post-mapping batch pipeline: clean → transform → analyze → ml_infer. "
        "Triggered by the frontend after the user confirms column mapping."
    ),
    schedule_interval=None,      # USER-TRIGGERED ONLY — never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pulse", "batch"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Clean ───────────────────────────────────────────────────────────
    clean = BashOperator(
        task_id="clean",
        bash_command=_docker_exec(
            "cleaning/cleaning.py",
            "--bucket-name {{ params.bucket }}",
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
    # on the very first batch run for a new business.
    ensure_specific = PythonOperator(
        task_id="ensure_specific_models_trained",
        python_callable=ensure_specific_models_trained,
        execution_timeout=timedelta(hours=2),  # training can be slow on large datasets
    )

    # ── 5. ML Inference ────────────────────────────────────────────────────
    # Runs infer_all.py (general + specific models).
    # General models are pre-trained globally; specific models are guaranteed
    # trained by the previous step.
    ml_infer = BashOperator(
        task_id="ml_infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 6. Trigger KS drift check + conditional retraining ────────────────
    # Runs asynchronously so this DAG completes immediately after firing.
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "batch_downstream"},
        wait_for_completion=False,
    )

    # ── Sequential dependencies (no parallel branches) ────────────────────
    clean >> transform >> analyze >> ensure_specific >> ml_infer >> trigger_drift_check
