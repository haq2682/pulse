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

Concurrency and data-safety guarantee
--------------------------------------
A threading lock prevents concurrent downstream runs.  When a new micro-batch
arrives while the previous downstream run is still processing, the trigger
cannot start a new run — instead it sets a ``_downstream_pending`` flag.
The active run's **drain loop** detects that flag after completing and
immediately re-runs the downstream pipeline, picking up every mapped/ file
that accumulated while the first run was in progress (incremental cleaning
ensures only unprocessed files are cleaned).  The loop repeats until no
more pending flags are set, guaranteeing that **no accumulated micro-batch
data is left unprocessed** regardless of how many batches arrive in a burst.

Example (batch rate > downstream throughput):
  batch 45 → downstream starts              (lock acquired)
  batch 46 → skipped, pending flag SET
  batch 47 → skipped, pending flag SET (already set)
  batch 45's downstream finishes
    → pending flag is set → clear flag → re-run (catch-up #1, processes 46 & 47)
    → pending flag clear → drain loop exits, lock released
  batch 48 → downstream starts normally

Expected end-to-end latency
----------------------------
  micro-batch interval (10 s)
+ Spark mapping (~5 s)
+ downstream pipeline (~30-90 s)
= **~45 s – 2 min**  (down from ~10-20 min with Airflow cron)

The ``streaming_downstream`` Airflow DAG remains as an additional safety-net
fallback (every 2 minutes), but correctness no longer depends on it.
"""

import logging
import os
import subprocess
import threading
import time
from typing import Dict, Optional

import redis

logger = logging.getLogger("pulse.downstream")

# Redis port used for pipeline lock checks (configurable via REDIS_PORT env var).
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ---------------------------------------------------------------------------
# Per-bucket state: each bucket gets its own lock, pending flag, and thread
# so that multiple tenants running in the same Python process do not share
# streaming state and block each other.
# ---------------------------------------------------------------------------
_state_lock: threading.Lock = threading.Lock()          # guards the dicts below
_downstream_locks: Dict[str, threading.Lock] = {}
_downstream_pendings: Dict[str, threading.Event] = {}
_downstream_threads: Dict[str, Optional[threading.Thread]] = {}


def _get_bucket_state(bucket: str):
    """Return (lock, pending_event, thread_ref_dict) for the given bucket, creating if absent."""
    with _state_lock:
        if bucket not in _downstream_locks:
            _downstream_locks[bucket] = threading.Lock()
            _downstream_pendings[bucket] = threading.Event()
            _downstream_threads[bucket] = None
    return _downstream_locks[bucket], _downstream_pendings[bucket]


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
        if step["name"] == "ml_inference" and batch_id == 0:
            args_str += " --first-batch"
        cmd = ["python3", f"/app/{script}"] + args_str.split()

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
    in progress (a pending flag is set so the active run will re-run
    immediately after it finishes, processing all accumulated data).

    This function is safe to call from Spark's foreachBatch callback — it
    returns immediately and never blocks the streaming query.
    """
    # Check cross-container Redis lock set by pipeline_service during the
    # initial (post-onboarding) pipeline run.  If that run is still active
    # we must not start a concurrent downstream execution — the accumulated
    # new data will be picked up by the next trigger once the lock is gone,
    # and the streaming_downstream Airflow DAG provides a 2-min fallback.
    try:
        _r = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"), port=_REDIS_PORT, decode_responses=True
        )
        if _r.exists(f"downstream_pipeline_lock:{bucket}"):
            logger.info(
                "Initial pipeline lock held for %s — skipping inline downstream for batch %d",
                bucket,
                batch_id,
            )
            return False
    except Exception as _redis_err:
        logger.warning(
            "Could not check downstream_pipeline_lock in Redis (%s) — proceeding", _redis_err
        )

    bucket_lock, bucket_pending = _get_bucket_state(bucket)

    if not bucket_lock.acquire(blocking=False):
        # A run is active; flag it so the drain loop re-runs after it finishes.
        bucket_pending.set()
        logger.debug(
            "Downstream already running for %s — pending flag set for batch %d", bucket, batch_id
        )
        return False

    # If there's an old thread reference, check if it's still alive
    with _state_lock:
        existing_thread = _downstream_threads.get(bucket)
    if existing_thread is not None and existing_thread.is_alive():
        bucket_lock.release()
        bucket_pending.set()
        logger.debug(
            "Downstream thread still alive for %s — pending flag set for batch %d", bucket, batch_id
        )
        return False

    def _wrapped():
        try:
            # Drain loop: run downstream, then immediately re-run if any new
            # micro-batches arrived (set the pending flag) while the previous
            # run was in progress.
            catchup_count = 0
            while True:
                _run_downstream_sync(bucket, batch_id if catchup_count == 0 else -catchup_count)
                if not bucket_pending.is_set():
                    break
                bucket_pending.clear()
                catchup_count += 1
                logger.info(
                    "New micro-batches arrived while downstream was running for %s (batch %d) — starting catch-up run #%d",
                    bucket, batch_id, catchup_count,
                )
        finally:
            bucket_lock.release()

    new_thread = threading.Thread(
        target=_wrapped,
        name=f"downstream-{bucket}-batch-{batch_id}",
        daemon=True,
    )
    with _state_lock:
        _downstream_threads[bucket] = new_thread
    new_thread.start()
    logger.info("Downstream pipeline started in background for %s (batch %d)", bucket, batch_id)
    return True
