"""
Airflow DAG: ML Model Retraining with KS-Test Drift Detection
==============================================================
Detects data drift using Kolmogorov-Smirnov (KS) tests and conditionally
retrains all affected ML models.

This DAG is designed to be triggered in two ways:
  1. Automatically by batch_pipeline / db_pipeline / api_pipeline after each
     successful run (via TriggerDagRunOperator).
  2. On a weekly schedule for proactive drift monitoring.
  3. Manually from the Airflow UI at any time.

Flow
----
  [load_current_features]          ← validate transformed/ data in MinIO exists
          │
  [run_ks_tests]                   ← PythonOperator: scipy KS-2-sample per model/feature
          │
  [evaluate_drift_report]          ← PythonOperator: push retrain list to XCom
          │
  [should_retrain]                 ← ShortCircuitOperator: skip if no drift
          │
  ┌───────┴───────┐
  [retrain_general]  [retrain_specific]   ← parallel BashOperators
  └───────┬───────┘
          │
  [save_new_baselines]             ← PythonOperator: save post-training distributions
          │
  [run_inference_after_retrain]    ← infer_all.py with new models

KS-test logic
-------------
For each model, KS-2-sample is applied to every numeric feature using the
distributions saved during the previous training run (the "baseline").

  • A feature is considered drifted if its KS p-value < alpha (default 0.05).
  • A model is flagged for retraining if the fraction of drifted features
    exceeds drift_ratio_trigger (default 0.20 = 20 %).
  • If no baseline exists for a model (first run), a new baseline is saved
    and the model is NOT retrained (there is nothing to compare against).

XCom keys produced by run_ks_tests
-----------------------------------
  "ks_report"          → full dict from drift_detection.run_ks_tests_all_models()
  "models_to_retrain"  → list[str] of model names requiring retraining
  "any_drift"          → bool
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    GENERAL_MODELS,
    KS_ALPHA,
    KS_MIN_SAMPLE_SIZE,
    PYTHON_CONTAINER,
    SPECIFIC_MODELS,
)

BUCKET = Variable.get("default_bucket", default_var=DEFAULT_BUCKET)

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
# Task callables
# ---------------------------------------------------------------------------

def load_current_features(**context):
    """
    Sanity-check that transformed/ data exists in MinIO before running KS tests.
    Raises if the bucket/prefix is empty (the upstream pipeline hasn't run yet).
    """
    from minio import Minio
    from minio.error import S3Error

    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", BUCKET)

    endpoint   = os.getenv("MINIO_ENDPOINT",   "10.5.0.4:9000").lstrip("http://").lstrip("https://")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    client  = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    prefix  = "transformed/"

    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    except S3Error as exc:
        if exc.code == "NoSuchBucket":
            raise RuntimeError(
                f"Bucket '{bucket}' does not exist. "
                "Run the batch/db/api pipeline at least once before drift detection."
            ) from exc
        raise

    parquet_count = sum(1 for o in objects if o.object_name.endswith(".parquet"))
    if parquet_count == 0:
        raise RuntimeError(
            f"No parquet files found at {bucket}/{prefix}. "
            "The transformation stage has not produced output yet."
        )

    print(f"Found {parquet_count} parquet files in {bucket}/{prefix}. Proceeding with KS tests.")
    context["task_instance"].xcom_push(key="bucket", value=bucket)


def run_ks_tests(**context):
    """
    Run KS-2-sample drift tests for every model.
    Pushes results and the list of models requiring retraining to XCom.
    """
    ti     = context["task_instance"]
    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", ti.xcom_pull(task_ids="load_current_features", key="bucket") or BUCKET)

    # Import is done here so we run inside the Airflow container (has scipy)
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from utils.drift_detection import run_ks_tests_all_models

    print(f"Running KS drift tests for bucket: {bucket}")
    report = run_ks_tests_all_models(
        bucket=bucket,
        alpha=KS_ALPHA,
        min_samples=KS_MIN_SAMPLE_SIZE,
    )

    models_to_retrain = report["models_with_drift"]
    any_drift         = report["any_drift_detected"]

    # Persist to XCom
    ti.xcom_push(key="ks_report",         value=report)
    ti.xcom_push(key="models_to_retrain", value=models_to_retrain)
    ti.xcom_push(key="any_drift",         value=any_drift)
    ti.xcom_push(key="bucket",            value=bucket)

    # Print a human-readable summary
    print("\n" + "=" * 60)
    print("KS DRIFT DETECTION SUMMARY")
    print("=" * 60)
    print(f"Models with drift    : {report['models_with_drift']}")
    print(f"Models without drift : {report['models_without_drift']}")
    print(f"Models with errors   : {report['models_with_errors']}")
    print(f"Any drift detected   : {any_drift}")
    print("=" * 60)

    for model_name, model_report in report["per_model"].items():
        if model_report.get("error"):
            print(f"  [{model_name}] ERROR: {model_report['error']}")
            continue
        drifted = model_report["drifted_features"]
        ratio   = model_report["drift_ratio"]
        flag    = "RETRAIN" if model_report["drift_detected"] else "ok"
        print(
            f"  [{model_name}] {flag}  drift_ratio={ratio:.2f}  "
            f"drifted_features={drifted}"
        )

    print("=" * 60 + "\n")


def evaluate_drift_report(**context):
    """
    Log the drift report and split models into general vs specific for
    the parallel retrain tasks downstream.
    """
    ti                = context["task_instance"]
    models_to_retrain = ti.xcom_pull(task_ids="run_ks_tests", key="models_to_retrain") or []
    any_drift         = ti.xcom_pull(task_ids="run_ks_tests", key="any_drift")

    general_retrain  = [m for m in models_to_retrain if m in GENERAL_MODELS]
    specific_retrain = [m for m in models_to_retrain if m in SPECIFIC_MODELS]

    ti.xcom_push(key="general_retrain",  value=general_retrain)
    ti.xcom_push(key="specific_retrain", value=specific_retrain)

    print(f"General models to retrain:  {general_retrain}")
    print(f"Specific models to retrain: {specific_retrain}")

    return any_drift  # passed to ShortCircuitOperator


def should_retrain_callable(**context):
    """Return True if any drift was detected, causing downstream tasks to run."""
    ti        = context["task_instance"]
    any_drift = ti.xcom_pull(task_ids="run_ks_tests", key="any_drift")
    if not any_drift:
        print("No drift detected. Skipping retraining.")
    else:
        print("Drift detected. Proceeding with retraining.")
    return bool(any_drift)


def save_new_baselines(**context):
    """
    After retraining completes, save fresh feature baselines so the NEXT
    drift-check run compares against the newly trained model's distribution.
    """
    ti     = context["task_instance"]
    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", ti.xcom_pull(task_ids="run_ks_tests", key="bucket") or BUCKET)
    models_to_retrain = ti.xcom_pull(task_ids="run_ks_tests", key="models_to_retrain") or []

    if not models_to_retrain:
        print("No models were retrained – no baselines to update.")
        return

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from utils.model_baseline import save_baselines_for_all_models

    results = save_baselines_for_all_models(bucket=bucket, model_names=models_to_retrain)

    print("\nBaseline save results:")
    for model, success in results.items():
        status = "OK" if success else "FAILED"
        print(f"  {model}: {status}")

    failed = [m for m, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(f"Failed to save baselines for: {failed}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ml_retrain",
    description="KS-test drift detection + conditional ML model retraining",
    schedule_interval="0 3 * * 0",    # weekly on Sunday 03:00 UTC (+ on-demand triggers)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pulse", "ml", "drift", "retrain"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Verify transformed data exists ──────────────────────────────────
    load_features = PythonOperator(
        task_id="load_current_features",
        python_callable=load_current_features,
    )

    # ── 2. KS-2-sample drift tests ─────────────────────────────────────────
    ks_tests = PythonOperator(
        task_id="run_ks_tests",
        python_callable=run_ks_tests,
        execution_timeout=timedelta(minutes=30),
    )

    # ── 3. Evaluate & split models by type ────────────────────────────────
    evaluate_drift = PythonOperator(
        task_id="evaluate_drift_report",
        python_callable=evaluate_drift_report,
    )

    # ── 4. Short-circuit: skip if no drift ────────────────────────────────
    gate_retrain = ShortCircuitOperator(
        task_id="should_retrain",
        python_callable=should_retrain_callable,
        ignore_downstream_trigger_rules=True,
    )

    # ── 5a. Retrain general models ─────────────────────────────────────────
    #   Passes the full bucket; train_all.py decides which models to run.
    #   The bash command pipes the drift report so train scripts can filter
    #   to only retrain drifted models (future enhancement).
    retrain_general = BashOperator(
        task_id="retrain_general",
        bash_command=_docker_exec(
            "machine-learning/general/train.py",
            "--bucket-name {{ params.bucket }}",
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 5b. Retrain specific models (parallel) ─────────────────────────────
    retrain_specific = BashOperator(
        task_id="retrain_specific",
        bash_command=_docker_exec(
            "machine-learning/specific/train.py",
            "--bucket-name {{ params.bucket }}",
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 6. Save updated baselines ─────────────────────────────────────────
    save_baselines = PythonOperator(
        task_id="save_new_baselines",
        python_callable=save_new_baselines,
        trigger_rule="all_done",   # run even if one retrain branch was skipped
    )

    # ── 7. Run inference with newly trained models ─────────────────────────
    infer_after_retrain = BashOperator(
        task_id="run_inference_after_retrain",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
        trigger_rule="all_done",
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        load_features
        >> ks_tests
        >> evaluate_drift
        >> gate_retrain
        >> [retrain_general, retrain_specific]
        >> save_baselines
        >> infer_after_retrain
    )
