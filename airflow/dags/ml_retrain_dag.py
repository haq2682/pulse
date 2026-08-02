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
        → evaluate_drift_report        filter to general models needing retrain
        → should_retrain               ShortCircuit: skip if no general drift
        → retrain_general              retrain affected general models
        → save_new_baselines           update MinIO drift baselines

NOTE: Specific models are NOT managed here. They are per-tenant and are
retrained unconditionally on every scheduled_batch pipeline run.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
# DockerOperator/docker_pipeline_env/docker_app_mounts no longer exist
# (task execution moved to KubernetesPodOperator - see
# docs/CLOUD_DEPLOYMENT_GUIDE.md and pipeline_config.py) and this whole
# DAG's registration is commented out below along with the rest of ML, so
# these stay commented rather than updated - re-enable both together.
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    GENERAL_MODELS,
    KS_ALPHA,
    KS_MIN_SAMPLE_SIZE,
    MODEL_FEATURE_MAP,
    PYTHON_IMAGE,
    # SPARK_NETWORK,           # removed along with Docker-based task execution
    # docker_pipeline_env,     # removed along with Docker-based task execution
    # docker_app_mounts,       # removed along with Docker-based task execution
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
        all_models = list(GENERAL_MODELS)   # only general models: specific retrain unconditionally in scheduled_batch
        print(f"force_retrain=True — marking all {len(all_models)} general models for retraining.")
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
        model_names=GENERAL_MODELS,   # only test general models; specific models retrain unconditionally
        alpha=KS_ALPHA,
        min_samples=KS_MIN_SAMPLE_SIZE,
    )

    models_to_retrain = report["models_with_drift"]
    any_drift         = report["any_drift_detected"]

    # ── Auto-detect first-run / missing baselines ──────────────────────────
    # If the vast majority of GENERAL models have errors (= no baseline exists),
    # treat it as a forced full retrain to bootstrap the system automatically.
    total        = len(GENERAL_MODELS)
    error_count  = len(report["models_with_errors"])
    if error_count > total * 0.8:
        print(
            f"\nNo baseline found for {error_count}/{total} general models. "
            "This looks like a first-time run — forcing full initial training."
        )
        models_to_retrain = list(GENERAL_MODELS)
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
    Filter models needing retraining to general models only.

    Specific models are retrained unconditionally on every pipeline run by
    scheduled_batch_dag — they must not be drift-triggered here because they
    are per-tenant and trained with tenant-specific data.
    """
    ti                = context["task_instance"]
    models_to_retrain = ti.xcom_pull(task_ids="run_ks_tests", key="models_to_retrain") or []

    general_retrain = [m for m in models_to_retrain if m in GENERAL_MODELS]

    ti.xcom_push(key="general_retrain", value=general_retrain)

    print(f"General models to retrain : {general_retrain}")
    print(f"(Specific models excluded — retrained unconditionally by scheduled_batch_dag)")

    return bool(general_retrain)


def should_retrain_callable(**context):
    """
    ShortCircuit: proceed only if at least one general model needs retraining.
    Specific models are excluded — they retrain unconditionally each pipeline run.
    """
    ti              = context["task_instance"]
    general_retrain = ti.xcom_pull(task_ids="evaluate_drift_report", key="general_retrain") or []
    if not general_retrain:
        print("No general model drift detected. Skipping general model retraining.")
    else:
        print(f"General models flagged for retraining: {general_retrain}")
    return bool(general_retrain)


def save_new_baselines(**context):
    """
    After retraining, save fresh feature baselines to MinIO so the NEXT
    drift-check run compares against the newly trained model's distribution.
    """
    ti                = context["task_instance"]
    conf              = context["dag_run"].conf or {}
    bucket            = conf.get("bucket", ti.xcom_pull(task_ids="run_ks_tests", key="bucket") or BUCKET)
    models_to_retrain = ti.xcom_pull(task_ids="evaluate_drift_report", key="general_retrain") or []

    if not models_to_retrain:
        print("No general models were retrained — no baselines to update.")
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
# ML forecasting/prediction is temporarily disabled project-wide. The entire
# DAG registration below is commented out (not deleted) so no "ml_retrain"
# DAG is registered with Airflow at all while this is off - the helper
# callables above remain intact and ready to wire back in via the exact
# block below when ML is re-enabled.
#
# with DAG(
#     dag_id="ml_retrain",
#     description=(
#         "KS-test drift detection + conditional ML model retraining. "
#         "Use dag_run.conf={'force_retrain': true} for initial training."
#     ),
#     schedule_interval="0 3 * * 0",   # weekly Sunday 03:00 UTC + on-demand triggers
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=["pulse", "ml", "drift", "retrain"],
#     default_args=_task_defaults,
#     params={"bucket": BUCKET},
#     render_template_as_native_obj=True,
# ) as dag:
#
#     # ── 1. Verify transformed data exists ──────────────────────────────────
#     load_features = PythonOperator(
#         task_id="load_current_features",
#         python_callable=load_current_features,
#     )
#
#     # ── 2. KS drift tests (or force_retrain bypass) ────────────────────────
#     ks_tests = PythonOperator(
#         task_id="run_ks_tests",
#         python_callable=run_ks_tests,
#         execution_timeout=timedelta(minutes=30),
#     )
#
#     # ── 3. Evaluate & split models by category ────────────────────────────
#     evaluate_drift = PythonOperator(
#         task_id="evaluate_drift_report",
#         python_callable=evaluate_drift_report,
#     )
#
#     # ── 4. Gate: skip retraining if no general model drift detected ───────
#     gate_retrain = ShortCircuitOperator(
#         task_id="should_retrain",
#         python_callable=should_retrain_callable,
#         ignore_downstream_trigger_rules=True,
#     )
#
#     # ── 5. Retrain general models ──────────────────────────────────────────
#     # General models intentionally train on ALL tenant buckets (no --bucket-name arg).
#     # The triggering bucket only determines WHICH drift was detected; the retrain
#     # always uses the full cross-tenant dataset so the shared model stays accurate.
#     retrain_general = DockerOperator(
#         task_id="retrain_general",
#         image=PYTHON_IMAGE,
#         command=["python3", "/app/machine-learning/general/train.py"],
#         environment=docker_pipeline_env(),
#         network_mode=SPARK_NETWORK,
#         mounts=docker_app_mounts(),
#         auto_remove="force",
#         do_xcom_push=False,
#         mount_tmp_dir=False,
#         execution_timeout=timedelta(hours=4),
#     )
#
#     # ── 6. Save updated drift baselines for retrained general models ───────
#     save_baselines = PythonOperator(
#         task_id="save_new_baselines",
#         python_callable=save_new_baselines,
#     )
#
#     # ── Dependencies ───────────────────────────────────────────────────────
#     (
#         load_features
#         >> ks_tests
#         >> evaluate_drift
#         >> gate_retrain
#         >> retrain_general
#         >> save_baselines
#     )
