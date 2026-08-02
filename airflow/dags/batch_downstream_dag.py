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

ML forecasting/prediction is temporarily disabled project-wide
-----------------------------------------------------------------
The ml_train / ml_infer / trigger_drift_check steps below are commented out,
not deleted — see the matching note in ml_retrain_dag.py. The general/
specific model background in the section below still describes the intended
design for when ML is re-enabled.

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

Flow
----
  clean → transform → analyze
  (ml_train → ml_infer → trigger_drift_check when ML is re-enabled)

Each step runs in its own Kubernetes Pod via KubernetesPodOperator (the
pulse-python image, RBAC via the pulse-airflow ServiceAccount — see
rbac.yaml) rather than a Docker container, so there's no docker.sock
dependency. See docs/CLOUD_DEPLOYMENT_GUIDE.md for the history.

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
# Only used by trigger_drift_check, which is commented out along with ML.
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import (
    DEFAULT_BUCKET,
    DEFAULT_TASK_ARGS,
    POD_NAMESPACE,
    PYTHON_IMAGE,
    TASK_POD_LABELS,
    TASK_POD_RESOURCES,
    TASK_SERVICE_ACCOUNT,
    k8s_pipeline_env,
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

# Shared kwargs every KubernetesPodOperator task below needs - defined once
# so each task only has to specify what's different about it (name, script,
# args).
_pod_task_defaults = dict(
    namespace=POD_NAMESPACE,
    image=PYTHON_IMAGE,
    env_vars=k8s_pipeline_env(),
    service_account_name=TASK_SERVICE_ACCOUNT,
    labels=TASK_POD_LABELS,
    container_resources=TASK_POD_RESOURCES(),
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,   # clean up the pod once the task finishes
    **_task_defaults,
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
    clean = KubernetesPodOperator(
        task_id="clean",
        name="pulse-batch-clean",
        cmds=["python3", "/app/cleaning/cleaning.py"],
        arguments=[
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        **_pod_task_defaults,
    )

    # ── 2. Transform ───────────────────────────────────────────────────────
    transform = KubernetesPodOperator(
        task_id="transform",
        name="pulse-batch-transform",
        cmds=["python3", "/app/transformation/transformation.py"],
        arguments=[
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        **_pod_task_defaults,
    )

    # ── 3. Analyze ─────────────────────────────────────────────────────────
    analyze = KubernetesPodOperator(
        task_id="analyze",
        name="pulse-batch-analyze",
        cmds=["python3", "/app/analysis/analysis.py"],
        arguments=[
            "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
        ],
        **_pod_task_defaults,
    )

    # ── 4. Train specific ML models for this business bucket ──────────────
    # ML forecasting/prediction is temporarily disabled project-wide — see
    # the matching note in ml_retrain_dag.py. Commented out, not deleted;
    # nothing else in this DAG references these tasks. Left in the older
    # DockerOperator form on purpose — no point migrating operator syntax
    # for code that isn't running; convert it the same way as clean/
    # transform/analyze above when ML is re-enabled.
    # General models are pre-trained globally (pulse-bucket-1) and require no
    # per-business training.  Specific models are per-business and must be
    # trained once on this business's own cleaned data before inference runs.
    # ml_train = DockerOperator(
    #     task_id="ml_train",
    #     image=PYTHON_IMAGE,
    #     command=[
    #         "python3", "/app/machine-learning/specific/train.py",
    #         "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
    #     ],
    #     environment=docker_pipeline_env(),
    #     network_mode=SPARK_NETWORK,
    #     mounts=docker_app_mounts(),
    #     auto_remove="force",
    #     do_xcom_push=False,
    #     mount_tmp_dir=False,
    #     execution_timeout=timedelta(hours=2),
    # )

    # ── 5. ML Inference ────────────────────────────────────────────────────
    # Runs infer_all.py (general + specific models).
    # General models load from pulse-bucket-1 (global); specific models load
    # from this business bucket (guaranteed trained by the previous step).
    # ml_infer = DockerOperator(
    #     task_id="ml_infer",
    #     image=PYTHON_IMAGE,
    #     command=[
    #         "python3", "/app/machine-learning/infer_all.py",
    #         "--bucket-name", "{{ dag_run.conf.get('bucket') or params.bucket }}",
    #     ],
    #     environment=docker_pipeline_env(),
    #     network_mode=SPARK_NETWORK,
    #     mounts=docker_app_mounts(),
    #     auto_remove="force",
    #     do_xcom_push=False,
    #     mount_tmp_dir=False,
    # )

    # ── 6. Trigger KS drift check + conditional retraining ────────────────
    # Disabled along with the rest of ML — there is nothing to retrain while
    # ml_retrain_dag.py is fully commented out, and firing it would just
    # trigger a DAG with no active tasks.
    # trigger_drift_check = TriggerDagRunOperator(
    #     task_id="trigger_drift_check",
    #     trigger_dag_id="ml_retrain",
    #     conf={"bucket": "{{ dag_run.conf.get('bucket') or params.bucket }}", "source_dag": "batch_downstream"},
    #     wait_for_completion=False,
    # )

    # ── Sequential dependencies (no parallel branches) ────────────────────
    # Full chain when ML is re-enabled:
    #   clean >> transform >> analyze >> ml_train >> ml_infer >> trigger_drift_check
    clean >> transform >> analyze
