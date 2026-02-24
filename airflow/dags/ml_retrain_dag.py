"""
Airflow DAG: ML Model Retraining with KS-Test Drift Detection
==============================================================
Detects data drift using Kolmogorov-Smirnov (KS) tests and conditionally
retrains all affected ML models.

Triggers
--------
  1. Automatically by batch_downstream / streaming_downstream after each
     successful run (TriggerDagRunOperator, fire-and-forget).
  2. Weekly schedule (Sunday 03:00 UTC) for proactive drift monitoring.
  3. Manually from the Airflow UI — use  force_retrain=true  for the
     INITIAL training run before any models exist.

dag_run.conf options
---------------------
  {
    "bucket":       "pulse-bucket-1",   # optional, defaults to Variable
    "force_retrain": true               # skip KS tests, retrain everything
                                        # USE THIS FOR FIRST-TIME TRAINING
  }

First-time training
-------------------
The very first time you deploy the system, there are no trained models in
MinIO and no drift baselines.  Inference (infer_all.py) will fail until at
least one training run completes.

To perform initial training:
  1. Run batch_downstream (or streaming_downstream) at least once so that
     cleaned / transformed data exists in MinIO.
  2. Trigger this DAG manually with  conf = {"force_retrain": true}.
     The KS-test step is skipped, all models are trained, baselines are
     saved.  Subsequent runs then compare against those baselines.

Auto-detection of missing baselines
------------------------------------
If ≥ 80 % of models have no saved baseline (typically first run or after a
full reset), the DAG automatically treats it as force_retrain=True even if
you did not set that flag explicitly.

KS-test logic
-------------
For each model, KS-2-sample is applied to every numeric feature defined in
MODEL_FEATURE_MAP:

  • A feature is "drifted" when its KS p-value < alpha  (default 0.05).
  • A model is flagged for retraining when the fraction of drifted features
    ≥ drift_ratio_trigger  (default 0.20 = 20 %).
  • Models with no baseline are excluded from the retrain list (they have
    nothing to compare against; their baseline is saved after training).

Flow
----
  load_current_features            validate transformed/ data exists
    → run_ks_tests                 scipy KS-2-sample per model/feature
    → evaluate_drift_report        split models → general vs specific
    → should_retrain               ShortCircuit: skip if no drift
    → [retrain_general | retrain_specific]   parallel
    → save_new_baselines           update MinIO drift baselines
    → run_inference_after_retrain  run infer_all with new models
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
    MODEL_FEATURE_MAP,
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
# Task callables
# ---------------------------------------------------------------------------

def load_current_features(**context):
    """
    Verify that transformed/ data exists in MinIO.
    Raises if the bucket/prefix is empty (upstream pipeline hasn't run yet).
    """
    from minio import Minio
    from minio.error import S3Error

    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", BUCKET)

    endpoint   = os.getenv("MINIO_ENDPOINT",   "10.5.0.4:9000").lstrip("http://").lstrip("https://")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    try:
        objects = list(client.list_objects(bucket, prefix="transformed/", recursive=True))
    except S3Error as exc:
        if exc.code == "NoSuchBucket":
            raise RuntimeError(
                f"Bucket '{bucket}' does not exist. "
                "Run the batch_downstream pipeline at least once before drift detection."
            ) from exc
        raise

    parquet_count = sum(1 for o in objects if o.object_name.endswith(".parquet"))
    if parquet_count == 0:
        raise RuntimeError(
            f"No parquet files at {bucket}/transformed/. "
            "Run the downstream pipeline (batch_downstream or streaming_downstream) first."
        )

    print(f"Found {parquet_count} parquet files in {bucket}/transformed/.")
    context["task_instance"].xcom_push(key="bucket", value=bucket)


def run_ks_tests(**context):
    """
    Run KS-2-sample drift tests for every model.

    If force_retrain=True (passed via dag_run.conf), skip all KS tests
    and immediately mark every model as needing retraining.

    Pushes to XCom:
      ks_report, models_to_retrain, any_drift, bucket
    """
    ti     = context["task_instance"]
    conf   = context["dag_run"].conf or {}
    bucket = conf.get("bucket", ti.xcom_pull(task_ids="load_current_features", key="bucket") or BUCKET)
    force  = conf.get("force_retrain", False)

    ti.xcom_push(key="bucket", value=bucket)

    if force:
        all_models = list(MODEL_FEATURE_MAP.keys())
        print(f"force_retrain=True — marking all {len(all_models)} models for retraining.")
        ti.xcom_push(key="models_to_retrain", value=all_models)
        ti.xcom_push(key="any_drift",         value=True)
        ti.xcom_push(key="ks_report",         value={
            "models_with_drift":    all_models,
            "models_without_drift": [],
            "models_with_errors":   [],
            "any_drift_detected":   True,
            "per_model":            {m: {"drift_detected": True, "drift_ratio": 1.0,
                                         "drifted_features": [], "details": {}, "error": None}
                                     for m in all_models},
        })
        return

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

    # ── Auto-detect first-run / missing baselines ──────────────────────────
    # If the vast majority of models have errors (= no baseline exists), treat
    # it as a forced full retrain so we bootstrap the system automatically.
    total        = len(MODEL_FEATURE_MAP)
    error_count  = len(report["models_with_errors"])
    if error_count > total * 0.8:
        print(
            f"\nNo baseline found for {error_count}/{total} models. "
            "This looks like a first-time run — forcing full initial training."
        )
        models_to_retrain = list(MODEL_FEATURE_MAP.keys())
        any_drift         = True
        report["models_with_drift"]  = models_to_retrain
        report["any_drift_detected"] = True

    ti.xcom_push(key="ks_report",         value=report)
    ti.xcom_push(key="models_to_retrain", value=models_to_retrain)
    ti.xcom_push(key="any_drift",         value=any_drift)

    # ── Human-readable summary ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("KS DRIFT DETECTION SUMMARY")
    print("=" * 60)
    print(f"Models with drift    : {report['models_with_drift']}")
    print(f"Models without drift : {report['models_without_drift']}")
    print(f"Models with errors   : {report['models_with_errors']}")
    print(f"Any drift / retrain  : {any_drift}")
    print("=" * 60)
    for name, r in report.get("per_model", {}).items():
        if r.get("error"):
            print(f"  [{name}] ERROR: {r['error']}")
            continue
        flag = "RETRAIN" if r["drift_detected"] else "ok"
        print(f"  [{name}] {flag}  drift_ratio={r['drift_ratio']:.2f}  "
              f"drifted={r['drifted_features']}")
    print("=" * 60 + "\n")


def evaluate_drift_report(**context):
    """
    Split models needing retraining into general vs specific buckets
    for the parallel retrain tasks downstream.
    """
    ti                = context["task_instance"]
    models_to_retrain = ti.xcom_pull(task_ids="run_ks_tests", key="models_to_retrain") or []
    any_drift         = ti.xcom_pull(task_ids="run_ks_tests", key="any_drift")

    general_retrain  = [m for m in models_to_retrain if m in GENERAL_MODELS]
    specific_retrain = [m for m in models_to_retrain if m in SPECIFIC_MODELS]

    ti.xcom_push(key="general_retrain",  value=general_retrain)
    ti.xcom_push(key="specific_retrain", value=specific_retrain)

    print(f"General models to retrain : {general_retrain}")
    print(f"Specific models to retrain: {specific_retrain}")

    return bool(any_drift)


def should_retrain_callable(**context):
    """
    ShortCircuit: return True to proceed with retraining, False to skip.
    Reads the decision made by evaluate_drift_report via its return value.
    """
    ti        = context["task_instance"]
    any_drift = ti.xcom_pull(task_ids="run_ks_tests", key="any_drift")
    if not any_drift:
        print("No drift detected and force_retrain not set. Skipping retraining.")
    else:
        print("Drift detected (or forced). Proceeding with retraining.")
    return bool(any_drift)


def save_new_baselines(**context):
    """
    After retraining, save fresh feature baselines to MinIO so the NEXT
    drift-check run compares against the newly trained model's distribution.
    """
    ti                = context["task_instance"]
    conf              = context["dag_run"].conf or {}
    bucket            = conf.get("bucket", ti.xcom_pull(task_ids="run_ks_tests", key="bucket") or BUCKET)
    models_to_retrain = ti.xcom_pull(task_ids="run_ks_tests", key="models_to_retrain") or []

    if not models_to_retrain:
        print("No models were retrained — no baselines to update.")
        return

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from utils.model_baseline import save_baselines_for_all_models

    results = save_baselines_for_all_models(bucket=bucket, model_names=models_to_retrain)

    print("\nBaseline save results:")
    for model, success in results.items():
        print(f"  {model}: {'OK' if success else 'FAILED'}")

    failed = [m for m, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(f"Failed to save baselines for: {failed}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ml_retrain",
    description=(
        "KS-test drift detection + conditional ML model retraining. "
        "Use dag_run.conf={'force_retrain': true} for initial training."
    ),
    schedule_interval="0 3 * * 0",   # weekly Sunday 03:00 UTC + on-demand triggers
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

    # ── 2. KS drift tests (or force_retrain bypass) ────────────────────────
    ks_tests = PythonOperator(
        task_id="run_ks_tests",
        python_callable=run_ks_tests,
        execution_timeout=timedelta(minutes=30),
    )

    # ── 3. Evaluate & split models by category ────────────────────────────
    evaluate_drift = PythonOperator(
        task_id="evaluate_drift_report",
        python_callable=evaluate_drift_report,
    )

    # ── 4. Gate: skip all downstream if no drift and not force_retrain ────
    gate_retrain = ShortCircuitOperator(
        task_id="should_retrain",
        python_callable=should_retrain_callable,
        ignore_downstream_trigger_rules=True,
    )

    # ── 5a. Retrain general models ─────────────────────────────────────────
    retrain_general = BashOperator(
        task_id="retrain_general",
        bash_command=_docker_exec(
            "machine-learning/general/train.py",
            "--bucket-name {{ params.bucket }}",
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 5b. Retrain specific models (parallel with general) ────────────────
    retrain_specific = BashOperator(
        task_id="retrain_specific",
        bash_command=_docker_exec(
            "machine-learning/specific/train.py",
            "--bucket-name {{ params.bucket }}",
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 6. Save updated drift baselines ───────────────────────────────────
    save_baselines = PythonOperator(
        task_id="save_new_baselines",
        python_callable=save_new_baselines,
        trigger_rule="all_done",   # run even if one retrain branch was skipped
    )

    # ── 7. Run inference with the freshly trained models ──────────────────
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
