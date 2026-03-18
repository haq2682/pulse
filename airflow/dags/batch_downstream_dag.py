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

General vs Specific ML Models
------------------------------
General models are pre-trained globally across all businesses and their
trained model files already live in the shared  pulse-bucket-1  bucket.
General inference reads those global models and writes per-business results
to the business bucket — no per-business training is needed or performed.

Specific models are per-business mini-models trained on THIS business's own
cleaned data.  The  ml_train  step below calls  specific/train.py  to do this.
After onboarding, specific models are retrained unconditionally on every
subsequent scheduled_batch run so they always reflect the latest tenant data.
General models are retrained only when KS-test drift is detected (ml_retrain DAG).

Flow (fully sequential — no parallel branches)
----------------------------------------------
  clean
    → transform
    → analyze
    → ml_train        (trains specific models for this business bucket)
    → ml_infer        (general inference uses global models; specific uses trained above)
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    PYTHON_IMAGE,
    SPARK_NETWORK,
    docker_pipeline_env,
    docker_app_mounts,
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



with DAG(
    dag_id="batch_downstream",
    description=(
        "Post-mapping batch pipeline: clean → transform → analyze → ml_infer. "
        "Triggered by the frontend after the user confirms column mapping."
    ),
    schedule_interval=None,      # USER-TRIGGERED ONLY — never runs on a schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=10,   # one run per tenant allowed concurrently
    tags=["pulse", "batch"],
    default_args=_task_defaults,
    params={"bucket": BUCKET},
    render_template_as_native_obj=True,
) as dag:

    # ── 1. Clean ───────────────────────────────────────────────────────────
    clean = DockerOperator(
        task_id="clean",
        image=PYTHON_IMAGE,
        command=[
            "python3", "/app/cleaning/cleaning.py",
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        environment=docker_pipeline_env(),
        network_mode=SPARK_NETWORK,
        mounts=docker_app_mounts(),
        auto_remove="force",
        do_xcom_push=False,
        mount_tmp_dir=False,
    )

    # ── 2. Transform ───────────────────────────────────────────────────────
    transform = DockerOperator(
        task_id="transform",
        image=PYTHON_IMAGE,
        command=[
            "python3", "/app/transformation/transformation.py",
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        environment=docker_pipeline_env(),
        network_mode=SPARK_NETWORK,
        mounts=docker_app_mounts(),
        auto_remove="force",
        do_xcom_push=False,
        mount_tmp_dir=False,
    )

    # ── 3. Analyze ─────────────────────────────────────────────────────────
    analyze = DockerOperator(
        task_id="analyze",
        image=PYTHON_IMAGE,
        command=[
            "python3", "/app/analysis/analysis.py",
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        environment=docker_pipeline_env(),
        network_mode=SPARK_NETWORK,
        mounts=docker_app_mounts(),
        auto_remove="force",
        do_xcom_push=False,
        mount_tmp_dir=False,
    )

    # ── 4. Train specific ML models for this business bucket ──────────────
    # General models are pre-trained globally (pulse-bucket-1) and require no
    # per-business training.  Specific models are per-business and must be
    # trained once on this business's own cleaned data before inference runs.
    ml_train = DockerOperator(
        task_id="ml_train",
        image=PYTHON_IMAGE,
        command=[
            "python3", "/app/machine-learning/specific/train.py",
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        environment=docker_pipeline_env(),
        network_mode=SPARK_NETWORK,
        mounts=docker_app_mounts(),
        auto_remove="force",
        do_xcom_push=False,
        mount_tmp_dir=False,
        execution_timeout=timedelta(hours=2),
    )

    # ── 5. ML Inference ────────────────────────────────────────────────────
    # Runs infer_all.py (general + specific models).
    # General models load from pulse-bucket-1 (global); specific models load
    # from this business bucket (guaranteed trained by the previous step).
    ml_infer = DockerOperator(
        task_id="ml_infer",
        image=PYTHON_IMAGE,
        command=[
            "python3", "/app/machine-learning/infer_all.py",
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        environment=docker_pipeline_env(),
        network_mode=SPARK_NETWORK,
        mounts=docker_app_mounts(),
        auto_remove="force",
        do_xcom_push=False,
        mount_tmp_dir=False,
    )

    # ── 6. Trigger KS drift check + conditional retraining ────────────────
    # Runs asynchronously so this DAG completes immediately after firing.
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ dag_run.conf.get('bucket') or params.bucket }}", "source_dag": "batch_downstream"},
        wait_for_completion=False,
    )

    # ── Sequential dependencies (no parallel branches) ────────────────────
    clean >> transform >> analyze >> ml_train >> ml_infer >> trigger_drift_check
