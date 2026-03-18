"""
Pipeline service – orchestrates data processing via the Airflow batch_downstream DAG.

Execution model
---------------
POST /pipeline/start  (or /pipeline/retry, /pipeline/trigger-streaming)
  │  ├─ Insert pipeline_status row  (status = 'running')
  ├─ Trigger Airflow REST API → batch_downstream dagRun
  │    conf = {"bucket": business_id, "pipeline_id": pipeline_id}
  ├─ Persist dag_run_id in the process_ids JSON column of pipeline_status
  └─ Launch asyncio background task: _poll_airflow_status(…)
       polls every _POLL_INTERVAL seconds
       maps Airflow task states → current_step / progress %
       broadcasts via WebSocket to InlinePipelineProgress component
       exits when DAG state is terminal (success / failed)

Crash recovery
--------------
recover_stuck_pipelines(db) is called from api/main.py on startup.
It scans pipeline_status for status='running' rows and for each:
  • dag_run_id present + DAG still active in Airflow  → restart polling task
  • DAG already terminal                              → sync pipeline_status
  • no dag_run_id (pre-Airflow asyncio row)           → mark failed instantly
"""

import os
import asyncio
import uuid
import json
import base64
import urllib.request
import urllib.error
import urllib.parse
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text

logger = logging.getLogger("pulse.pipeline")

# ── Airflow REST config ────────────────────────────────────────────────────────
_AIRFLOW_BASE  = os.getenv("AIRFLOW_BASE_URL",  "http://airflow-webserver:8080")
_AIRFLOW_USER  = os.getenv("AIRFLOW_USERNAME",  "admin")
_AIRFLOW_PASS  = os.getenv("AIRFLOW_PASSWORD",  "admin")
_DAG_ID        = "batch_downstream"
_POLL_INTERVAL = int(os.getenv("PIPELINE_POLL_INTERVAL", "15"))  # seconds

# ── Airflow task-id → (description, progress_start %, progress_end %) ─────────
_TASK_PROGRESS: Dict[str, tuple] = {
    "clean":               ("Cleaning Data",               0,   25),
    "transform":           ("Transforming & Aggregating", 25,   55),
    "analyze":             ("Analyzing Data",             55,   85),
    "ml_train":            ("Training ML Models",         85,   90),
    "ml_infer":            ("Running ML Predictions",     90,  100),
    "trigger_drift_check": ("Running ML Predictions",    100,  100),
}

_RUNNING_STATES = {"running", "queued", "up_for_retry", "restarting", "scheduled", "deferred"}
_SUCCESS_STATES = {"success", "skipped"}
_FAILURE_STATES = {"failed", "upstream_failed"}


# ── Module-level helpers ───────────────────────────────────────────────────────

def _airflow_headers() -> dict:
    creds = base64.b64encode(
        f"{_AIRFLOW_USER}:{_AIRFLOW_PASS}".encode()
    ).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def _airflow_request(method: str, path: str, body: Optional[dict] = None):
    """Synchronous Airflow REST API call.  Returns parsed JSON or raises."""
    url = f"{_AIRFLOW_BASE}/api/v1/{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, headers=_airflow_headers(), method=method
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _airflow_cancel_dag_run(dag_id: str, dag_run_id: str) -> bool:
    """
    Best-effort hard cancel:
      1) mark running/queued task instances as failed,
      2) patch dagRun state to failed,
      3) verify dagRun reaches terminal state.
    Returns True only when Airflow confirms terminal state.
    """
    encoded_run_id = urllib.parse.quote(dag_run_id, safe="")
    encoded_dag_id = urllib.parse.quote(dag_id, safe="")

    try:
        task_resp = _airflow_request(
            "GET",
            f"dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/taskInstances",
        )
        for task in task_resp.get("task_instances", []):
            task_state = (task.get("state") or "").lower()
            if task_state not in _RUNNING_STATES:
                continue

            task_id = task.get("task_id")
            if not task_id:
                continue

            encoded_task_id = urllib.parse.quote(task_id, safe="")
            patch_path = (
                f"dags/{encoded_dag_id}/dagRuns/{encoded_run_id}/"
                f"taskInstances/{encoded_task_id}"
            )

            try:
                _airflow_request("PATCH", patch_path, {"new_state": "failed"})
            except Exception:
                _airflow_request("PATCH", patch_path, {"state": "failed"})
    except Exception as exc:
        logger.warning("Could not patch task instances for dag_run %s: %s", dag_run_id, exc)

    _airflow_request(
        "PATCH",
        f"dags/{encoded_dag_id}/dagRuns/{encoded_run_id}",
        {"state": "failed"},
    )

    # Verify cancellation reached terminal state.
    for _ in range(5):
        dag_info = _airflow_request(
            "GET",
            f"dags/{encoded_dag_id}/dagRuns/{encoded_run_id}",
        )
        dag_state = (dag_info.get("state") or "").lower()
        if dag_state not in _RUNNING_STATES:
            return True
        time.sleep(1)

    return False


def _interpret_state(dag_state: str, tasks: dict) -> tuple:
    """
    Map Airflow DAG + task states to (current_step: str, progress: int, failed_phase: str|None).
    Walk tasks in pipeline order; the first non-success state determines the
    description and progress to report.
    """
    last_step = "Cleaning Data"
    last_progress = 0
    for task_id, (desc, prog_start, prog_end) in _TASK_PROGRESS.items():
        state = tasks.get(task_id)
        if state in _FAILURE_STATES:
            return desc, prog_start, task_id
        if state in _SUCCESS_STATES:
            last_step = desc
            last_progress = prog_end
        elif state in _RUNNING_STATES:
            return desc, prog_start, None
        # state is None (not yet scheduled) → continue walking
    return last_step, last_progress, None


def _mark_failed_sync(
    db,
    pipeline_id: str,
    reason: str = "Pipeline interrupted (API restart). Please retry.",
) -> None:
    """Mark a pipeline_status row as failed synchronously (for startup recovery)."""
    db.execute(
        text("""
            UPDATE pipeline_status
            SET status        = 'failed',
                current_step  = 'Pipeline interrupted',
                error_message = :reason,
                completed_at  = :now
            WHERE pipeline_id = :pid
        """),
        {"reason": reason, "now": datetime.now(), "pid": pipeline_id},
    )


async def recover_stuck_pipelines(db) -> None:
    """
    Called on API startup.  For every pipeline_status row still in 'running':
      1. dag_run_id + DAG still active in Airflow → restart polling background task
      2. DAG already finished → sync pipeline_status to match (completed / failed)
      3. No dag_run_id (pre-Airflow asyncio run) → mark as failed immediately
    """
    try:
        rows = db.execute(
            text("""
                SELECT pipeline_id, business_id, process_ids
                FROM pipeline_status
                WHERE status = 'running'
            """)
        ).fetchall()
    except Exception as exc:
        logger.error("recover_stuck_pipelines: DB error: %s", exc)
        return

    if not rows:
        return

    logger.info("recover_stuck_pipelines: %d stuck pipeline(s) found", len(rows))

    for pipeline_id, business_id, process_ids_json in rows:
        dag_run_id = None
        dag_id_hint = None
        if process_ids_json:
            try:
                pids = json.loads(process_ids_json)
                dag_run_id = pids.get("dag_run_id")
                dag_id_hint = pids.get("dag_id")
            except Exception:
                pass

        # Rows owned by the scheduled_batch DAG are managed entirely by the
        # Airflow DAG itself (via _update_pipeline_status_for_bucket).  The
        # API must not overwrite them on startup; the scheduled_batch will set
        # the terminal status when it finishes.
        if dag_id_hint == "scheduled_batch":
            logger.info(
                "  %s: owned by scheduled_batch DAG — leaving status unchanged",
                pipeline_id,
            )
            continue

        if not dag_run_id:
            # Pre-Airflow asyncio pipeline — subprocess is definitely dead.
            logger.info("  %s: no dag_run_id (old asyncio run) → marking failed", pipeline_id)
            _mark_failed_sync(db, pipeline_id)
            continue

        # Ask Airflow for the current DAG run state.
        try:
            dag_info  = _airflow_request("GET", f"dags/{_DAG_ID}/dagRuns/{dag_run_id}")
            dag_state = dag_info.get("state", "")
        except Exception as exc:
            logger.warning("  %s: cannot reach Airflow (%s) → marking failed", pipeline_id, exc)
            _mark_failed_sync(db, pipeline_id)
            continue

        if dag_state == "success":
            logger.info("  %s: DAG already succeeded → marking completed", pipeline_id)
            db.execute(
                text("""
                    UPDATE pipeline_status
                    SET status = 'completed', progress_percentage = 100,
                        current_step = 'Pipeline completed successfully',
                        completed_at = :now
                    WHERE pipeline_id = :pid
                """),
                {"now": datetime.now(), "pid": pipeline_id},
            )
        elif dag_state == "failed":
            logger.info("  %s: DAG already failed → marking failed", pipeline_id)
            _mark_failed_sync(db, pipeline_id, "Pipeline failed. Please retry.")
        elif dag_state in ("running", "queued"):
            logger.info(
                "  %s: DAG %s still '%s' → restarting poller",
                pipeline_id, dag_run_id, dag_state,
            )
            from services.websocket_manager import WebSocketManager
            svc = PipelineService(db, WebSocketManager())
            asyncio.create_task(
                svc._poll_airflow_status(pipeline_id, business_id, dag_run_id, db)
            )
        else:
            logger.warning("  %s: DAG state '%s' → marking failed", pipeline_id, dag_state)
            _mark_failed_sync(
                db, pipeline_id,
                f"Pipeline entered unexpected state '{dag_state}'. Please retry.",
            )

    db.commit()
    logger.info("recover_stuck_pipelines: done")

class PipelineService:
    """Service for managing data processing pipeline execution via Airflow."""

    def __init__(self, db, websocket_manager=None):
        """
        Args:
            db: SQLAlchemy session (request-scoped; only used for the initial
                INSERT.  Background tasks open their own connection via
                get_db_connection().)
            websocket_manager: WebSocketManager for real-time progress broadcast.
        """
        self.db = db
        self.websocket_manager = websocket_manager
    
    # ──────────────────────────────────────────────────────────────────────────
    # Public interface
    # ──────────────────────────────────────────────────────────────────────────

    async def start_pipeline(
        self, business_id: str, user_id: str,
        start_from_phase: Optional[str] = None,
    ) -> str:
        """
        Trigger the Airflow batch_downstream DAG for this business and start a
        background polling task that pushes WebSocket progress updates.

        start_from_phase is accepted for API compatibility but ignored — Airflow
        runs each task from the beginning and retries each task up to 3× internally.
        """
        pipeline_id = str(uuid.uuid4())

        # Insert initial status row.
        self.db.execute(
            text("""
                INSERT INTO pipeline_status
                (pipeline_id, business_id, user_id, status, current_step,
                 progress_percentage, started_at)
                VALUES (:pipeline_id, :business_id, :user_id, :status,
                        :current_step, :progress_percentage, :started_at)
            """),
            {
                "pipeline_id":         pipeline_id,
                "business_id":         business_id,
                "user_id":             user_id,
                "status":              "running",
                "current_step":        "Triggering Pipeline",
                "progress_percentage": 0,
                "started_at":          datetime.now(),
            },
        )
        self.db.commit()

        # Broadcast immediately so the frontend shows the progress overlay
        # before the first Airflow poll completes.
        await self._broadcast_progress(business_id, {
            "pipeline_id": pipeline_id,
            "status":       "running",
            "current_step": "Triggering Pipeline",
            "progress":     0,
        })

        # Fire-and-forget: trigger DAG + poll until terminal state.
        # Opens its own DB connection so it survives the request lifecycle.
        asyncio.create_task(
            self._launch_and_poll(pipeline_id, business_id, user_id)
        )

        return pipeline_id
    
    # ──────────────────────────────────────────────────────────────────────────
    # Internal: DAG trigger + polling loop
    # ──────────────────────────────────────────────────────────────────────────

    async def _launch_and_poll(
        self, pipeline_id: str, business_id: str, user_id: str
    ) -> None:
        """
        Opens a fresh DB connection, triggers the Airflow DAG, persists the
        dag_run_id, then drives the polling loop until the DAG terminates.
        """
        from database import get_db_connection
        db = get_db_connection()
        loop = asyncio.get_event_loop()
        try:
            # ── 1. Trigger Airflow DAG ──────────────────────────────────────────────
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda: _airflow_request(
                        "POST",
                        f"dags/{_DAG_ID}/dagRuns",
                        {
                            "conf": {
                                "bucket":      business_id,
                                "pipeline_id": pipeline_id,
                            },
                            "note": (
                                f"Triggered by Pulse API for business {business_id}"
                            ),
                        },
                    ),
                )
                dag_run_id = resp["dag_run_id"]
                logger.info(
                    "Airflow DAG run %s created for pipeline %s / business %s",
                    dag_run_id, pipeline_id, business_id,
                )
            except Exception as exc:
                logger.error(
                    "Failed to trigger Airflow DAG for %s: %s", business_id, exc
                )
                await self._update_progress(
                    pipeline_id, business_id,
                    status="failed",
                    current_step="Failed to trigger pipeline",
                    progress=0,
                    error_message=f"Could not trigger the Airflow pipeline: {exc}",
                    failed_phase="trigger",
                    db_connection=db,
                )
                return

            # ── 2. Persist dag_run_id so cancel / recovery can find it ───────────
            try:
                db.execute(
                    text("""
                        UPDATE pipeline_status
                        SET process_ids = :pids
                        WHERE pipeline_id = :pid
                    """),
                    {
                        "pids": json.dumps({"dag_run_id": dag_run_id, "dag_id": _DAG_ID}),
                        "pid":  pipeline_id,
                    },
                )
                db.commit()
            except Exception as exc:
                logger.warning(
                    "Could not persist dag_run_id for %s: %s", pipeline_id, exc
                )

            # ── 3. Poll until terminal ──────────────────────────────────────────────
            await self._poll_airflow_status(pipeline_id, business_id, dag_run_id, db)

        except Exception as exc:
            logger.error(
                "_launch_and_poll unhandled error for %s: %s",
                business_id, exc, exc_info=True,
            )
        finally:
            try:
                db.close()
            except Exception:
                pass
    
    async def _poll_airflow_status(
        self,
        pipeline_id:  str,
        business_id:  str,
        dag_run_id:   str,
        db_connection=None,
    ) -> None:
        """
        Poll Airflow every _POLL_INTERVAL seconds until the DAG reaches a
        terminal state, then update pipeline_status and broadcast WebSocket
        events to the InlinePipelineProgress component.
        """
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(_POLL_INTERVAL)

            # Bail out if the user cancelled from the frontend.
            current = self._get_pipeline_status(pipeline_id, db_connection)
            if current == "cancelled":
                return

            # ── Fetch DAG run + task states ───────────────────────────────────────
            try:
                dag_info = await loop.run_in_executor(
                    None,
                    lambda: _airflow_request(
                        "GET", f"dags/{_DAG_ID}/dagRuns/{dag_run_id}"
                    ),
                )
                task_resp = await loop.run_in_executor(
                    None,
                    lambda: _airflow_request(
                        "GET",
                        f"dags/{_DAG_ID}/dagRuns/{dag_run_id}/taskInstances",
                    ),
                )
            except Exception as exc:
                logger.warning("Airflow poll error for %s: %s", dag_run_id, exc)
                continue  # transient network error — retry next cycle

            dag_state = dag_info.get("state", "")
            tasks = {
                ti["task_id"]: ti["state"]
                for ti in task_resp.get("task_instances", [])
            }
            step, progress, failed_phase = _interpret_state(dag_state, tasks)

            # ── Terminal: success ─────────────────────────────────────────────────
            if dag_state == "success":
                await self._update_progress(
                    pipeline_id, business_id,
                    status="completed",
                    current_step="Pipeline completed successfully",
                    progress=100,
                    completed=True,
                    db_connection=db_connection,
                )
                return

            # ── Terminal: failed ─────────────────────────────────────────────────
            if dag_state == "failed":
                await self._update_progress(
                    pipeline_id, business_id,
                    status="failed",
                    current_step=step,
                    progress=progress,
                    error_message=(
                        f"Pipeline failed during {failed_phase} phase"
                        if failed_phase else "Pipeline failed"
                    ),
                    failed_phase=failed_phase,
                    db_connection=db_connection,
                )
                return

            # ── Still running — push incremental progress update ────────────────
            await self._update_progress(
                pipeline_id, business_id,
                status="running",
                current_step=step,
                progress=progress,
                db_connection=db_connection,
            )
    
    async def _update_progress(
        self,
        pipeline_id: str,
        business_id: str,
        status: str,
        current_step: str,
        progress: int,
        error_message: Optional[str] = None,
        failed_phase: Optional[str] = None,
        completed: bool = False,
        process_ids: Optional[Dict] = None,
        db_connection=None
    ):
        """
        Update pipeline progress in database and broadcast via WebSocket.
        
        Args:
            pipeline_id: Pipeline execution ID
            business_id: Business ID
            status: Pipeline status
            current_step: Current step description
            progress: Progress percentage (0-100)
            error_message: Error message if failed
            failed_phase: Name of phase where pipeline failed
            completed: Whether pipeline is completed
            process_ids: Dict of process IDs for each phase
            db_connection: Database connection to use (if None, uses self.db)
        """
        # Use the provided connection or fall back to self.db
        db = db_connection if db_connection is not None else self.db
        
        # Log which connection is being used
        if db_connection is not None:
            logger.debug("_update_progress using provided db_connection for pipeline %s", pipeline_id)
        else:
            logger.warning("_update_progress using self.db (request-scoped) for pipeline %s", pipeline_id)
        
        try:
            # Prepare update data
            update_data = {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress_percentage": min(progress, 100),
                "error_message": error_message,
                "failed_phase": failed_phase
            }
            
            if completed:
                update_data["completed_at"] = datetime.now()
            
            if process_ids:
                update_data["process_ids"] = json.dumps(process_ids)
            
            # Build dynamic UPDATE query
            set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys() if k != "pipeline_id"])
            query = f"UPDATE pipeline_status SET {set_clause} WHERE pipeline_id = :pipeline_id"
            
            db.execute(text(query), update_data)
            db.commit()
            
            # Broadcast via WebSocket
            await self._broadcast_progress(business_id, {
                "pipeline_id": pipeline_id,
                "status": status,
                "current_step": current_step,
                "progress": progress,
                "error_message": error_message,
                "failed_phase": failed_phase
            })
            
        except Exception as e:
            logger.error("Error updating progress: %s", e, exc_info=True)
    
    async def _broadcast_progress(self, business_id: str, data: Dict[str, Any]):
        """
        Broadcast progress update via WebSocket.
        
        Args:
            business_id: Business ID (used as channel/room)
            data: Progress data to broadcast
        """
        if self.websocket_manager:
            try:
                await self.websocket_manager.broadcast(
                    message=data,
                    business_id=business_id
                )
            except Exception as e:
                logger.error("Error broadcasting progress: %s", e, exc_info=True)
    
    def _get_pipeline_status(self, pipeline_id: str, db_connection=None) -> Optional[str]:
        """
        Get current pipeline status from database.
        
        Args:
            pipeline_id: Pipeline execution ID
            db_connection: Database connection to use (if None, uses self.db)
            
        Returns:
            Status string or None
        """
        # Use the provided connection or fall back to self.db
        db = db_connection if db_connection is not None else self.db
        
        try:
            result = db.execute(
                text("SELECT status FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
                {"pipeline_id": pipeline_id}
            ).fetchone()
            
            return result[0] if result else None
        except Exception as e:
            logger.error("Error getting pipeline status: %s", e, exc_info=True)
            return None
    
    async def cancel_pipeline(self, pipeline_id: str, business_id: str) -> bool:
        """
        Cancel a running pipeline by marking the Airflow DAG run as failed,
        then updating pipeline_status to 'cancelled'.
        """
        try:
            # Look up the Airflow dag_run_id stored during start_pipeline.
            result = self.db.execute(
                text("SELECT process_ids FROM pipeline_status WHERE pipeline_id = :pipeline_id"),
                {"pipeline_id": pipeline_id},
            ).fetchone()
            dag_run_id = None
            dag_id = _DAG_ID
            if result and result[0]:
                try:
                    process_ids = json.loads(result[0])
                    dag_run_id = process_ids.get("dag_run_id")
                    dag_id = process_ids.get("dag_id") or _DAG_ID
                except Exception:
                    pass

            # Ask Airflow to abort the DAG run and require confirmation.
            airflow_cancelled = True
            if dag_run_id:
                try:
                    loop = asyncio.get_event_loop()
                    airflow_cancelled = await loop.run_in_executor(
                        None,
                        lambda: _airflow_cancel_dag_run(dag_id, dag_run_id),
                    )
                    if airflow_cancelled:
                        logger.info(
                            "Cancelled Airflow DAG run %s for pipeline %s",
                            dag_run_id, pipeline_id,
                        )
                    else:
                        logger.error(
                            "Airflow DAG run %s is still active after cancel attempt",
                            dag_run_id,
                        )
                except Exception as exc:
                    airflow_cancelled = False
                    logger.warning(
                        "Could not cancel Airflow DAG run %s: %s", dag_run_id, exc
                    )

            if not airflow_cancelled:
                # Do NOT flip local status to cancelled unless Airflow actually stopped.
                return False

            # Update local status table.  The polling task will see 'cancelled'
            # on its next wake-up and exit cleanly.
            self.db.execute(
                text("""
                    UPDATE pipeline_status
                    SET status       = 'cancelled',
                        current_step = 'Pipeline cancelled by user',
                        completed_at = :completed_at
                    WHERE pipeline_id = :pipeline_id
                """),
                {"pipeline_id": pipeline_id, "completed_at": datetime.now()},
            )
            self.db.commit()

            await self._broadcast_progress(business_id, {
                "pipeline_id": pipeline_id,
                "status":       "cancelled",
                "current_step": "Pipeline cancelled by user",
                "progress":     0,
            })

            return True

        except Exception as exc:
            logger.error("Error cancelling pipeline: %s", exc, exc_info=True)
            return False

    async def cleanup_pipeline_data(self, business_id: str):
        """
        Clean up pipeline data from MinIO for a cancelled/failed pipeline.
        
        Args:
            business_id: Business ID (bucket name)
        """
        try:
            import boto3
            from botocore.client import Config
            
            # Initialize S3 client for MinIO
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                config=Config(signature_version="s3v4"),
                region_name="us-east-1"
            )
            
            # Folders to clean up
            folders = ["cleaned", "transformed", "analytics", "ml-predictions"]
            
            for folder in folders:
                prefix = f"{folder}/"
                
                # List and delete objects
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=business_id, Prefix=prefix):
                    if "Contents" in page:
                        objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                        if objects:
                            s3_client.delete_objects(
                                Bucket=business_id,
                                Delete={"Objects": objects}
                            )
                            logger.info("Deleted %d objects from %s/%s/", len(objects), business_id, folder)
            
            logger.info("Cleaned up pipeline data for business %s", business_id)
            
        except Exception as e:
            logger.error("Error cleaning up pipeline data: %s", e, exc_info=True)
    
    def has_pipeline_ever_completed(self, business_id: str) -> bool:
        """
        Return True if at least one pipeline has ever completed successfully for this business.
        Used by the frontend to decide whether to suppress the full-screen Knob for
        subsequent streaming microbatches (analytics are already displayed).

        Args:
            business_id: Business ID

        Returns:
            True if a completed pipeline exists, False otherwise
        """
        try:
            result = self.db.execute(
                text("""
                    SELECT EXISTS(
                        SELECT 1 FROM pipeline_status
                        WHERE business_id = :business_id AND status = 'completed'
                    )
                """),
                {"business_id": business_id}
            ).scalar()
            return bool(result)
        except Exception as e:
            logger.error("Error checking pipeline_ever_completed for business %s: %s", business_id, e, exc_info=True)
            return False

    def get_pipeline_status_info(self, business_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current pipeline status for a business.
        
        Args:
            business_id: Business ID
            
        Returns:
            Pipeline status info dict or None
        """
        try:
            result = self.db.execute(
                text("""
                    SELECT pipeline_id, status, current_step, progress_percentage, 
                           started_at, completed_at, error_message, failed_phase
                    FROM pipeline_status
                    WHERE business_id = :business_id
                    ORDER BY started_at DESC
                    LIMIT 1
                """),
                {"business_id": business_id}
            ).fetchone()
            
            if result:
                return {
                    "pipeline_id": result[0],
                    "status": result[1],
                    "current_step": result[2],
                    "progress": result[3],
                    "started_at": result[4].isoformat() if result[4] else None,
                    "completed_at": result[5].isoformat() if result[5] else None,
                    "error_message": result[6],
                    "failed_phase": result[7]
                }
            
            return None
            
        except Exception as e:
            logger.error("Error getting pipeline status info: %s", e, exc_info=True)
            return None
    
    async def cleanup_streaming_resources(self, business_id: str) -> None:
        """
        Remove ALL streaming-layer resources created for a tenant:
          1. Airflow  – mark any running db_streaming / api_streaming DAG runs as failed
          2. Debezium – delete the Kafka Connect connector
          3. Kafka    – delete all topics whose name starts with ``{business_id}.``
          4. MinIO    – delete the Spark Structured Streaming checkpoint directory
                        (s3a://pulse-checkpoints/normalize-stream-{business_id}/)
          5. Redis    – delete all per-business keys

        Every step is best-effort: a failure in one step is logged but does NOT
        prevent the remaining steps from running.

        Called by:
          - ``delete_business``  (analytics router) – full teardown
          - ``cancel_mapping``   (onboarding router) – mid-onboarding teardown
        """
        logger.info("Starting streaming resource cleanup for business %s", business_id)

        # ── 1. Cancel Airflow streaming DAG runs ─────────────────────────────
        try:
            import base64
            import urllib.request
            import json as _json

            airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
            airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
            airflow_password = os.getenv("AIRFLOW_PASSWORD", "admin")
            encoded_creds = base64.b64encode(
                f"{airflow_user}:{airflow_password}".encode()
            ).decode()
            headers = {"Authorization": f"Basic {encoded_creds}", "Content-Type": "application/json"}

            for dag_id in ("db_streaming", "api_streaming"):
                list_url = f"{airflow_base}/api/v1/dags/{dag_id}/dagRuns?state=running&limit=100"
                try:
                    req = urllib.request.Request(list_url, headers=headers)
                    with urllib.request.urlopen(req, timeout=10) as r:
                        runs = _json.loads(r.read()).get("dag_runs", [])
                except Exception:
                    runs = []

                for run in runs:
                    conf = run.get("conf") or {}
                    if conf.get("bucket") != business_id:
                        continue
                    run_id = run["dag_run_id"]
                    patch_url = f"{airflow_base}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                    patch_payload = _json.dumps({"state": "failed"}).encode()
                    req = urllib.request.Request(
                        patch_url, data=patch_payload, headers=headers, method="PATCH"
                    )
                    try:
                        urllib.request.urlopen(req, timeout=10)
                        logger.info("Cancelled Airflow DAG run %s/%s", dag_id, run_id)
                    except Exception as e:
                        logger.warning("Could not cancel DAG run %s/%s: %s", dag_id, run_id, e)
        except Exception as e:
            logger.error("Error cancelling Airflow DAG runs for %s: %s", business_id, e, exc_info=True)

        # ── 2. Delete Debezium connector ──────────────────────────────────────
        try:
            import requests as _req
            connect_url = os.getenv("KAFKA_CONNECT_URL", "http://10.5.0.10:8083")
            connector_name = f"pulse-{business_id}-connector"
            resp = _req.delete(f"{connect_url}/connectors/{connector_name}", timeout=10)
            if resp.status_code in (200, 204):
                logger.info("Deleted Debezium connector: %s", connector_name)
            elif resp.status_code == 404:
                logger.info("Debezium connector %s not found (already deleted)", connector_name)
            else:
                logger.warning("Unexpected status deleting connector %s: %s", connector_name, resp.status_code)
        except Exception as e:
            logger.error("Error deleting Debezium connector for %s: %s", business_id, e, exc_info=True)

        # ── 3. Delete Kafka topics ────────────────────────────────────────────
        try:
            from kafka.admin import KafkaAdminClient
            from kafka.errors import UnknownTopicOrPartitionError

            bootstrap = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id=f"pulse-cleanup-{business_id}",
                request_timeout_ms=10000,
            )
            all_topics = admin.list_topics()
            prefix = f"{business_id}."
            tenant_topics = [t for t in all_topics if t.startswith(prefix)]
            if tenant_topics:
                admin.delete_topics(tenant_topics, timeout_ms=15000)
                logger.info("Deleted %d Kafka topics for %s: %s", len(tenant_topics), business_id, tenant_topics)
            else:
                logger.info("No Kafka topics found for prefix %s", prefix)
            admin.close()
        except Exception as e:
            logger.error("Error deleting Kafka topics for %s: %s", business_id, e, exc_info=True)

        # ── 4. Delete Spark checkpoint from MinIO ─────────────────────────────
        try:
            import boto3
            from botocore.client import Config

            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                config=Config(signature_version="s3v4"),
                region_name="us-east-1",
            )
            checkpoint_bucket = "pulse-checkpoints"
            checkpoint_prefix = f"normalize-stream-{business_id}/"
            paginator = s3_client.get_paginator("list_objects_v2")
            deleted_count = 0
            for page in paginator.paginate(Bucket=checkpoint_bucket, Prefix=checkpoint_prefix):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    if objects:
                        s3_client.delete_objects(
                            Bucket=checkpoint_bucket, Delete={"Objects": objects}
                        )
                        deleted_count += len(objects)
            logger.info(
                "Deleted %d checkpoint objects for %s (prefix: %s)",
                deleted_count, business_id, checkpoint_prefix,
            )
        except Exception as e:
            logger.error("Error deleting Spark checkpoint for %s: %s", business_id, e, exc_info=True)

        # ── 5. Clean up Redis keys ────────────────────────────────────────────
        try:
            import redis as _redis

            redis_client = _redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
            )
            keys_to_delete = [
                f"manual_mappings:{business_id}",
                f"mapping_results:{business_id}",
                f"streaming_first_batch_done:{business_id}",
                f"streaming_use_temp:{business_id}",
                f"mapping_process:{business_id}",
                f"mapping_log:{business_id}",
            ]
            deleted = redis_client.delete(*keys_to_delete)
            logger.info("Deleted %d Redis keys for %s", deleted, business_id)
        except Exception as e:
            logger.error("Error cleaning up Redis keys for %s: %s", business_id, e, exc_info=True)

        logger.info("Streaming resource cleanup complete for business %s", business_id)

    async def delete_business_bucket(self, business_id: str):
        """
        Delete the entire business bucket from MinIO.
        
        Args:
            business_id: Business ID (bucket name)
        """
        try:
            import boto3
            from botocore.client import Config
            
            # Initialize S3 client for MinIO
            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                config=Config(signature_version="s3v4"),
                region_name="us-east-1"
            )
            
            # List and delete all objects in the bucket
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=business_id):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    if objects:
                        s3_client.delete_objects(
                            Bucket=business_id,
                            Delete={"Objects": objects}
                        )
                        logger.info("Deleted %d objects from bucket %s", len(objects), business_id)
            
            # Delete the bucket itself
            s3_client.delete_bucket(Bucket=business_id)
            logger.info("Deleted bucket: %s", business_id)
            
        except Exception as e:
            logger.error("Error deleting business bucket: %s", e, exc_info=True)
            raise
