"""
Streaming Downstream Runner
============================
Runs the downstream pipeline (clean → transform → analyze → ML inference)
inline after each Spark Structured Streaming micro-batch, replacing the
10-minute Airflow cron schedule with near-real-time processing.

Architecture
------------
After the mapping micro-batch writes normalised Parquet to MinIO ``mapped/``,
this module spawns the four downstream scripts **sequentially** in a
background thread so the next Spark micro-batch can start immediately.

A threading lock prevents concurrent downstream runs.  If a new micro-batch
completes while the previous downstream run is still processing, the
trigger is skipped — the NEXT successful micro-batch after the downstream
finishes will pick up all accumulated data.

Expected end-to-end latency
----------------------------
  micro-batch interval (10 s)
+ Spark mapping (~5 s)
+ downstream pipeline (~30-90 s)
= **~45 s – 2 min**  (down from ~10-20 min with Airflow cron)

The ``streaming_downstream`` Airflow DAG remains as a reduced-interval
fallback (every 2 minutes) to catch any data missed by inline processing.
"""

import logging
import os
import subprocess
import threading
import time
from typing import Optional

logger = logging.getLogger("pulse.downstream")

# ---------------------------------------------------------------------------
# Module-level state: a single lock and a running flag prevent concurrent
# downstream runs from overlapping within the same streaming job.
# ---------------------------------------------------------------------------
_downstream_lock = threading.Lock()
_downstream_thread: Optional[threading.Thread] = None


# Scripts to execute, in order.  Paths are relative to /app/ inside the
# Python container (where the streaming job runs).
_DOWNSTREAM_STEPS = [
    {
        "name": "cleaning",
        "script": "cleaning/cleaning.py",
        "args_template": "--bucket-name {bucket} --incremental",
    },
    {
        "name": "transformation",
        "script": "transformation/transformation.py",
        "args_template": "--bucket-name {bucket}",
    },
    {
        "name": "analysis",
        "script": "analysis/analysis.py",
        "args_template": "--bucket-name {bucket}",
    },
    {
        "name": "ml_inference",
        "script": "machine-learning/infer_all.py",
        "args_template": "--bucket-name {bucket}",
    },
]


def _run_downstream_sync(bucket: str, batch_id: int) -> None:
    """
    Execute the four downstream scripts sequentially.
    Runs inside a background thread — never call directly from foreachBatch.
    """
    overall_start = time.time()
    logger.info("DOWNSTREAM PIPELINE — triggered by batch %d, bucket=%s", batch_id, bucket)

    for step in _DOWNSTREAM_STEPS:
        step_name = step["name"]
        script = step["script"]
        args_str = step["args_template"].format(bucket=bucket)
        cmd = ["python", f"/app/{script}"] + args_str.split()

        step_start = time.time()
        logger.info("  ▶ %s: %s", step_name, " ".join(cmd))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=900,        # 15-minute hard cap per step
                cwd="/app",
            )

            elapsed = time.time() - step_start

            if result.returncode == 0:
                logger.info("  ✅ %s completed in %.1fs", step_name, elapsed)
            else:
                # Log the error but continue to the next step — partial
                # downstream results are better than none.
                stderr_tail = (result.stderr or "")[-500:]
                logger.warning(
                    "  %s exited with code %d (%.1fs)%s",
                    step_name, result.returncode, elapsed,
                    f"\n     stderr: {stderr_tail}" if stderr_tail else "",
                )

        except subprocess.TimeoutExpired:
            logger.error("  %s timed out after 900s — skipping", step_name)
        except Exception as exc:
            logger.error("  %s failed: %s", step_name, exc, exc_info=True)

    total = time.time() - overall_start
    logger.info("DOWNSTREAM PIPELINE completed in %.1fs (batch %d)", total, batch_id)


def trigger_downstream(bucket: str, batch_id: int) -> bool:
    """
    Attempt to start a downstream pipeline run in a background thread.

    Returns True if a new run was started, False if a previous run is still
    in progress (the trigger is silently skipped).

    This function is safe to call from Spark's foreachBatch callback — it
    returns immediately and never blocks the streaming query.
    """
    global _downstream_thread

    if not _downstream_lock.acquire(blocking=False):
        logger.debug("Downstream already running — skipping for batch %d", batch_id)
        return False

    # If there's an old thread reference, check if it's still alive
    if _downstream_thread is not None and _downstream_thread.is_alive():
        _downstream_lock.release()
        logger.debug("Downstream thread still alive — skipping for batch %d", batch_id)
        return False

    def _wrapped():
        try:
            _run_downstream_sync(bucket, batch_id)
        finally:
            _downstream_lock.release()

    _downstream_thread = threading.Thread(
        target=_wrapped,
        name=f"downstream-batch-{batch_id}",
        daemon=True,
    )
    _downstream_thread.start()
    logger.info("Downstream pipeline started in background (batch %d)", batch_id)
    return True
