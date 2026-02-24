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

Note on ML Training
-------------------
The normal batch flow only runs INFERENCE (infer_all.py), not training.
The first time you run (no models in MinIO yet), trigger ml_retrain with
  dag_run.conf = {"force_retrain": true}
BEFORE running this DAG.  After that, ml_retrain handles retraining
automatically via KS-test drift detection on a weekly schedule.

Flow (fully sequential — no parallel branches)
----------------------------------------------
  clean
    → transform
    → analyze
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.pipeline_config import DEFAULT_BUCKET, DEFAULT_TASK_ARGS, PYTHON_CONTAINER

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

    # ── 4. ML Inference ────────────────────────────────────────────────────
    # Runs infer_all.py (general + specific models).
    # If no models are trained yet, trigger the ml_retrain DAG with
    # {"force_retrain": true} first.
    ml_infer = BashOperator(
        task_id="ml_infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 5. Trigger KS drift check + conditional retraining ────────────────
    # Runs asynchronously so this DAG completes immediately after firing.
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "batch_downstream"},
        wait_for_completion=False,
    )

    # ── Sequential dependencies (no parallel branches) ────────────────────
    clean >> transform >> analyze >> ml_infer >> trigger_drift_check
