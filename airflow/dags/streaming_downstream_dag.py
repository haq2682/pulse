"""
Airflow DAG: Streaming Downstream Pipeline
==========================================
Processes data that has been mapped and written to MinIO  mapped/  by
the continuous db_streaming or api_streaming DAGs.

This is the "batch over streaming" pattern
------------------------------------------
The upstream streaming jobs (Debezium CDC or API polling) continuously
produce normalised Parquet files in MinIO  mapped/.  Instead of running
separate streaming versions of cleaning/transformation/analysis/inference,
we run the same BATCH scripts on a short schedule with incremental mode:

  • cleaning.py --incremental  processes only files it hasn't seen yet
    (tracks state in Redis via incremental_cleaner.py)
  • transformation.py, analysis.py, infer_all.py  process whatever is
    in cleaned/, transformed/, etc.

Advantages
----------
  • Re-uses the exact same, well-tested batch scripts
  • Incremental cleaning ensures no duplicate processing
  • Simple crash handling via Airflow retries (no complex streaming state)
  • Acceptable latency: ~10 min from Kafka event to ML inference result

Schedule
--------
Default: every 10 minutes.  Adjust via the Airflow Variable
``streaming_downstream_interval`` (cron syntax) or the docker-compose env.

max_active_runs=1 prevents overlapping runs — if the previous run is still
processing, the next scheduled run is queued and starts immediately after.

Flow (sequential)
-----------------
  clean_incremental
    → transform
    → analyze
    → ml_infer
    → trigger_drift_check   (fires ml_retrain asynchronously if drift found)

This DAG does NOT run ML training — that is handled exclusively by
ml_retrain_dag.py (weekly KS-test driven or force_retrain=true for
the initial training run).

Relation to db_streaming / api_streaming
-----------------------------------------
  db_streaming DAG   ─┐
  api_streaming DAG  ─┤──→  MinIO mapped/  ──→  THIS DAG  ──→  MinIO cleaned/
                      │                                     transformed/
                                                            analytics/
                                                            ml-predictions/

Both source DAGs feed the same mapped/ prefix; this DAG processes all of
them regardless of which source produced the data.
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

BUCKET   = Variable.get("default_bucket",              default_var=DEFAULT_BUCKET)
SCHEDULE = Variable.get("streaming_downstream_interval", default_var="*/10 * * * *")

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
    dag_id="streaming_downstream",
    description=(
        "Incremental batch processing over streaming output: "
        "clean (incremental) → transform → analyze → ml_infer. "
        "Runs every 10 min by default."
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

    # ── 4. ML Inference ────────────────────────────────────────────────────
    ml_infer = BashOperator(
        task_id="ml_infer",
        bash_command=_docker_exec(
            "machine-learning/infer_all.py",
            "--bucket-name {{ params.bucket }}",
        ),
    )

    # ── 5. Trigger KS drift check (async, weekly DAG handles retraining) ──
    trigger_drift_check = TriggerDagRunOperator(
        task_id="trigger_drift_check",
        trigger_dag_id="ml_retrain",
        conf={"bucket": "{{ params.bucket }}", "source_dag": "streaming_downstream"},
        wait_for_completion=False,
    )

    # ── Sequential dependencies ────────────────────────────────────────────
    clean_incremental >> transform >> analyze >> ml_infer >> trigger_drift_check
