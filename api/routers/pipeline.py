"""
Pipeline API router for managing data processing pipeline execution.
"""

import logging
import asyncio
import os
import json
import base64
import urllib.request
import urllib.parse
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from database import get_db, get_db_connection
from sqlalchemy import text
from services.pipeline_service import PipelineService
from services.websocket_manager import WebSocketManager
from utils.connectivity_validator import validate_database_connection, validate_api_endpoint

logger = logging.getLogger("pulse.pipeline")


router = APIRouter(
    prefix="/pipeline",
    tags=["pipeline"],
)

# Global WebSocket manager instance
websocket_manager = WebSocketManager()


def _airflow_auth_header() -> str:
    user = os.getenv("AIRFLOW_USERNAME", "admin")
    pwd = os.getenv("AIRFLOW_PASSWORD", "admin")
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _find_active_batch_downstream_run(business_id: str) -> Optional[Dict[str, Any]]:
    """
    Return active batch_downstream dagRun metadata for this business, if any.
    """
    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    url = (
        f"{airflow_base}/api/v1/dags/batch_downstream/dagRuns"
        "?state=running&state=queued&limit=200&order_by=-execution_date"
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": _airflow_auth_header(),
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read())
    for run in data.get("dag_runs", []):
        conf = run.get("conf") or {}
        if conf.get("bucket") == business_id:
            return run
    return None


def _find_latest_batch_downstream_run(business_id: str) -> Optional[Dict[str, Any]]:
    """
    Return most recent batch_downstream dagRun metadata for this business,
    regardless of state.
    """
    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    url = (
        f"{airflow_base}/api/v1/dags/batch_downstream/dagRuns"
        "?limit=200&order_by=-execution_date"
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": _airflow_auth_header(),
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read())

    for run in data.get("dag_runs", []):
        conf = run.get("conf") or {}
        if conf.get("bucket") == business_id:
            return run
    return None


def _reconcile_terminal_airflow_run(db, business_id: str, status_info: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    If no active run exists but Airflow's latest run is terminal (success/failed),
    persist that outcome into pipeline_status.

    Handles two critical cases:
      1) stale DB row stuck in 'running' after Airflow already ended,
      2) first-ever batch succeeded but no pipeline_status row exists.
    """
    latest = _find_latest_batch_downstream_run(business_id)
    if not latest:
        return status_info

    dag_state = (latest.get("state") or "").lower()
    if dag_state not in {"success", "failed"}:
        return status_info

    mapped_status = "completed" if dag_state == "success" else "failed"
    current_step = (
        "Pipeline completed successfully"
        if mapped_status == "completed"
        else "Pipeline failed"
    )
    progress = 100 if mapped_status == "completed" else 0
    error_message = None if mapped_status == "completed" else "Pipeline failed"

    run_conf = latest.get("conf") or {}
    dag_run_id = latest.get("dag_run_id")
    airflow_pipeline_id = run_conf.get("pipeline_id")
    terminal_pipeline_id = (airflow_pipeline_id or dag_run_id or "")[:50] or None

    try:
        # 1) Heal stale status row when DB and latest Airflow terminal state differ.
        # This covers stale 'running' and stale 'failed' rows alike.
        if (
            status_info
            and status_info.get("pipeline_id")
            and status_info.get("status") != mapped_status
        ):
            db.execute(
                text("""
                    UPDATE pipeline_status
                    SET status = :status,
                        current_step = :current_step,
                        progress_percentage = :progress,
                        completed_at = NOW(),
                        error_message = :error_message,
                        failed_phase = NULL
                    WHERE pipeline_id = :pipeline_id
                """),
                {
                    "status": mapped_status,
                    "current_step": current_step,
                    "progress": progress,
                    "error_message": error_message,
                    "pipeline_id": status_info["pipeline_id"],
                },
            )
            db.commit()
            refreshed = db.execute(
                text("""
                    SELECT pipeline_id, status, current_step, progress_percentage,
                           started_at, completed_at, error_message, failed_phase
                    FROM pipeline_status
                    WHERE pipeline_id = :pipeline_id
                    LIMIT 1
                """),
                {"pipeline_id": status_info["pipeline_id"]},
            ).fetchone()
            if refreshed:
                return {
                    "pipeline_id": refreshed[0],
                    "status": refreshed[1],
                    "current_step": refreshed[2],
                    "progress": refreshed[3],
                    "started_at": refreshed[4].isoformat() if refreshed[4] else None,
                    "completed_at": refreshed[5].isoformat() if refreshed[5] else None,
                    "error_message": refreshed[6],
                    "failed_phase": refreshed[7],
                }

        # 2) First-ever run with missing row: materialize terminal status row.
        if status_info is None and terminal_pipeline_id:
            owner = db.execute(
                text("""
                    SELECT user_id
                    FROM businesses
                    WHERE business_id = :business_id
                    LIMIT 1
                """),
                {"business_id": business_id},
            ).fetchone()

            if owner and owner[0]:
                db.execute(
                    text("""
                        INSERT INTO pipeline_status
                            (pipeline_id, business_id, user_id, status, current_step,
                             progress_percentage, started_at, completed_at,
                             error_message, process_ids)
                        VALUES
                            (:pipeline_id, :business_id, :user_id, :status, :current_step,
                             :progress, NOW(), NOW(), :error_message,
                             CAST(:process_ids AS JSONB))
                        ON CONFLICT (pipeline_id) DO UPDATE
                        SET status = EXCLUDED.status,
                            current_step = EXCLUDED.current_step,
                            progress_percentage = EXCLUDED.progress_percentage,
                            completed_at = NOW(),
                            error_message = EXCLUDED.error_message
                    """),
                    {
                        "pipeline_id": terminal_pipeline_id,
                        "business_id": business_id,
                        "user_id": owner[0],
                        "status": mapped_status,
                        "current_step": current_step,
                        "progress": progress,
                        "error_message": error_message,
                        "process_ids": json.dumps(
                            {
                                "dag_id": "batch_downstream",
                                "dag_run_id": dag_run_id,
                            }
                        ),
                    },
                )
                db.commit()

                refreshed = db.execute(
                    text("""
                        SELECT pipeline_id, status, current_step, progress_percentage,
                               started_at, completed_at, error_message, failed_phase
                        FROM pipeline_status
                        WHERE pipeline_id = :pipeline_id
                        LIMIT 1
                    """),
                    {"pipeline_id": terminal_pipeline_id},
                ).fetchone()
                if refreshed:
                    return {
                        "pipeline_id": refreshed[0],
                        "status": refreshed[1],
                        "current_step": refreshed[2],
                        "progress": refreshed[3],
                        "started_at": refreshed[4].isoformat() if refreshed[4] else None,
                        "completed_at": refreshed[5].isoformat() if refreshed[5] else None,
                        "error_message": refreshed[6],
                        "failed_phase": refreshed[7],
                    }
    except Exception as reconcile_err:
        logger.warning("Could not reconcile terminal Airflow run for %s: %s", business_id, reconcile_err)

    return status_info


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


def _interpret_airflow_state(dag_state: str, tasks: Dict[str, str]) -> tuple:
    """
    Map Airflow DAG + task states to (current_step, progress, failed_phase).
    """
    if dag_state in ("queued", "scheduled"):
        return "Queued in Airflow", 0, None

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

    return last_step, last_progress, None


def _get_live_batch_downstream_status(business_id: str) -> Optional[Dict[str, Any]]:
    """
    Resolve live status/progress from Airflow for active batch_downstream dagRun.
    Returns None when no active dagRun is found.
    """
    run = _find_active_batch_downstream_run(business_id)
    if not run:
        return None

    dag_run_id = run.get("dag_run_id")
    dag_state = (run.get("state") or "").lower()
    if not dag_run_id:
        return {
            "dag_run_id": None,
            "status": "running",
            "current_step": "Pipeline running",
            "progress": 0,
            "failed_phase": None,
        }

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    encoded_run_id = urllib.parse.quote(dag_run_id, safe="")
    task_url = (
        f"{airflow_base}/api/v1/dags/batch_downstream/"
        f"dagRuns/{encoded_run_id}/taskInstances"
    )
    req = urllib.request.Request(
        task_url,
        headers={
            "Authorization": _airflow_auth_header(),
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        task_payload = json.loads(resp.read())

    tasks = {
        ti.get("task_id"): (ti.get("state") or "").lower()
        for ti in task_payload.get("task_instances", [])
        if ti.get("task_id")
    }
    step, progress, failed_phase = _interpret_airflow_state(dag_state, tasks)

    return {
        "dag_run_id": dag_run_id,
        "status": "running",
        "current_step": step,
        "progress": progress,
        "failed_phase": failed_phase,
    }


def _build_pipeline_status_payload(db, business_id: str) -> Dict[str, Any]:
    """
    Build status payload with DB state reconciled against live Airflow state.
    """
    pipeline_service = PipelineService(db, websocket_manager)
    status_info = pipeline_service.get_pipeline_status_info(business_id)
    ever_completed = pipeline_service.has_pipeline_ever_completed(business_id)

    live_status = _get_live_batch_downstream_status(business_id)
    if live_status:
        if status_info and status_info.get("pipeline_id"):
            try:
                db.execute(
                    text("""
                        UPDATE pipeline_status
                        SET status = 'running',
                            current_step = :current_step,
                            progress_percentage = :progress,
                            error_message = NULL,
                            failed_phase = NULL,
                            completed_at = NULL
                        WHERE pipeline_id = :pipeline_id
                    """),
                    {
                        "pipeline_id": status_info["pipeline_id"],
                        "current_step": live_status.get("current_step") or "Pipeline running",
                        "progress": int(live_status.get("progress") or 0),
                    },
                )
                db.commit()
            except Exception as heal_err:
                logger.warning("Could not heal stale pipeline_status row: %s", heal_err)

        status_info = {
            "pipeline_id": (status_info or {}).get("pipeline_id")
                or live_status.get("dag_run_id"),
            "status": "running",
            "current_step": live_status.get("current_step") or "Pipeline running",
            "progress": int(live_status.get("progress") or 0),
            "started_at": (status_info or {}).get("started_at"),
            "completed_at": None,
            "error_message": None,
            "failed_phase": live_status.get("failed_phase"),
        }

    # If there is no active run, reconcile stale/missing DB state from latest
    # terminal Airflow run so first-ever and subsequent batches flip to terminal
    # status immediately after DAG completion.
    if not live_status:
        status_info = _reconcile_terminal_airflow_run(db, business_id, status_info)

    if not status_info:
        return {
            "status": 200,
            "pipeline_status": "not_started",
            "pipeline_ever_completed": ever_completed,
            "message": "No pipeline execution found for this business",
        }

    # Recompute once after reconciliation so first-ever completion is visible
    # on the same response that healed/inserted the terminal status row.
    if status_info.get("status") == "completed" and not ever_completed:
        ever_completed = pipeline_service.has_pipeline_ever_completed(business_id)

    return {
        "status": 200,
        "pipeline_status": status_info["status"],
        "pipeline_ever_completed": ever_completed,
        "data": status_info,
    }


def _build_pipeline_status_payload_for_business(business_id: str) -> Dict[str, Any]:
    """
    Thread-safe wrapper that owns DB connection lifecycle.
    """
    db = get_db_connection()
    try:
        return _build_pipeline_status_payload(db, business_id)
    finally:
        try:
            db.close()
        except Exception:
            pass


@router.post("/start")
async def start_pipeline(request: Request, db=Depends(get_db)):
    """
    Start the batch downstream pipeline for a business.

    Triggered by the frontend after the user confirms column mapping.
    For db/api ingestion modes the streaming supervisor DAGs (db_streaming /
    api_streaming) run continuously in Airflow; this endpoint only handles
    the batch post-mapping stages (clean → transform → analyze → ml_infer).

    Request body:
        - userId: User ID
        - businessId: Business ID (used as bucket name)
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        business_id = body.get("businessId")

        if not user_id or not business_id:
            raise HTTPException(status_code=400, detail="userId and businessId are required")

        # Verify business belongs to user
        result = db.execute(
            text("SELECT business_id FROM businesses WHERE business_id = :business_id AND user_id = :user_id"),
            {"business_id": business_id, "user_id": user_id}
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Business not found or access denied")

        # Check if pipeline is already running
        existing = db.execute(
            text("""
                SELECT pipeline_id, status FROM pipeline_status
                WHERE business_id = :business_id AND status = 'running'
                ORDER BY started_at DESC LIMIT 1
            """),
            {"business_id": business_id}
        ).fetchone()

        if existing:
            raise HTTPException(
                status_code=409,
                detail="Pipeline is already running for this business"
            )

        pipeline_service = PipelineService(db, websocket_manager)
        pipeline_id = await pipeline_service.start_pipeline(business_id, user_id)

        return {
            "status": 200,
            "message": "Batch pipeline started successfully",
            "pipeline_id": pipeline_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error starting pipeline: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_pipeline_status(business_id: str, db=Depends(get_db)):
    """
    Get current pipeline status for a business.
    
    Query params:
        - business_id: Business ID
    """
    try:
        payload = await run_in_threadpool(_build_pipeline_status_payload_for_business, business_id)
        return payload
        
    except Exception as e:
        logger.error("Error getting pipeline status: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cancel")
async def cancel_pipeline(request: Request, db=Depends(get_db)):
    """
    Cancel a running pipeline.
    
    Request body:
        - pipelineId: Pipeline execution ID
        - businessId: Business ID
        - cleanupData: Whether to cleanup pipeline data from MinIO (optional, default: true)
    """
    try:
        body = await request.json()
        pipeline_id = body.get("pipelineId")
        business_id = body.get("businessId")
        cleanup_data = body.get("cleanupData", True)
        
        if not pipeline_id or not business_id:
            raise HTTPException(status_code=400, detail="pipelineId and businessId are required")
        
        # Verify pipeline exists and is running
        result = db.execute(
            text("""
                SELECT status FROM pipeline_status 
                WHERE pipeline_id = :pipeline_id AND business_id = :business_id
            """),
            {"pipeline_id": pipeline_id, "business_id": business_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        if result[0] != "running":
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot cancel pipeline with status '{result[0]}'"
            )
        
        # Cancel pipeline
        pipeline_service = PipelineService(db, websocket_manager)
        success = await pipeline_service.cancel_pipeline(pipeline_id, business_id)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to cancel pipeline")
        
        # Cleanup data if requested
        if cleanup_data:
            await pipeline_service.cleanup_pipeline_data(business_id)
        
        return {
            "status": 200,
            "message": "Pipeline cancelled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error cancelling pipeline: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to cancel pipeline")


@router.post("/retry")
async def retry_pipeline(request: Request, db=Depends(get_db)):
    """
    Retry a failed pipeline, resuming from the failed phase.
    
    Request body:
        - userId: User ID
        - businessId: Business ID
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        business_id = body.get("businessId")
        
        if not user_id or not business_id:
            raise HTTPException(status_code=400, detail="userId and businessId are required")
        
        # Verify business belongs to user
        result = db.execute(
            text("SELECT business_id FROM businesses WHERE business_id = :business_id AND user_id = :user_id"),
            {"business_id": business_id, "user_id": user_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Business not found or access denied")
        
        # Check current pipeline status and get failed phase
        current = db.execute(
            text("""
                SELECT pipeline_id, status, failed_phase FROM pipeline_status 
                WHERE business_id = :business_id
                ORDER BY started_at DESC LIMIT 1
            """),
            {"business_id": business_id}
        ).fetchone()
        
        if current and current[1] == "running":
            raise HTTPException(
                status_code=409, 
                detail="Pipeline is already running for this business"
            )
        
        # Get the failed phase to resume from
        failed_phase = current[2] if current and current[2] else None
        
        # Start new pipeline execution, resuming from failed phase if available
        pipeline_service = PipelineService(db, websocket_manager)
        pipeline_id = await pipeline_service.start_pipeline(business_id, user_id, start_from_phase=failed_phase)
        
        return {
            "status": 200,
            "message": f"Pipeline retry started successfully{' from ' + failed_phase + ' phase' if failed_phase else ''}",
            "pipeline_id": pipeline_id,
            "resumed_from_phase": failed_phase
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrying pipeline: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retry pipeline")


@router.post("/trigger-streaming")
async def trigger_streaming_pipeline(request: Request, db=Depends(get_db)):
    """
    Manually trigger a downstream pipeline run for a db/api-mode streaming business.

    Called from the dashboard's streaming-indicator refresh button so the user can
    force-process any data that has accumulated in mapped/ since the last run.

    Request body:
        - businessId: Business ID
        - userId: Authenticated user ID (used to verify ownership)
    """
    try:
        body = await request.json()
        business_id = body.get("businessId")
        user_id = body.get("userId")

        if not business_id or not user_id:
            raise HTTPException(status_code=400, detail="businessId and userId are required")

        # Verify business belongs to user and is in a streaming ingestion mode
        result = db.execute(
            text("""
                SELECT b.business_id, o.ingestion_type
                FROM businesses b
                JOIN onboarding o ON b.business_id = o.business_id
                WHERE b.business_id = :business_id AND b.user_id = :user_id
                LIMIT 1
            """),
            {"business_id": business_id, "user_id": user_id},
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Business not found or access denied")

        ingestion_type = result[1]
        if ingestion_type not in ("db", "api"):
            raise HTTPException(
                status_code=400,
                detail="Streaming pipeline trigger is only available for db and api ingestion modes",
            )

        # Reject if a pipeline run is already active
        existing = db.execute(
            text("""
                SELECT pipeline_id FROM pipeline_status
                WHERE business_id = :business_id AND status = 'running'
                ORDER BY started_at DESC LIMIT 1
            """),
            {"business_id": business_id},
        ).fetchone()

        if existing:
            raise HTTPException(
                status_code=409,
                detail="A pipeline is already running for this business",
            )

        pipeline_service = PipelineService(db, websocket_manager)
        pipeline_id = await pipeline_service.start_pipeline(business_id, user_id)

        return {
            "success": True,
            "message": "Streaming pipeline triggered successfully",
            "pipeline_id": pipeline_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error triggering streaming pipeline: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to trigger streaming pipeline")


@router.get("/streaming-health")
async def get_streaming_health(business_id: str, db=Depends(get_db)):
    """
    Return the live connectivity status of the streaming ingestion layer for a business.

    For **db** mode: queries the Debezium Kafka Connect REST API.
      - connector state RUNNING + all tasks RUNNING → "running"
      - anything else                                → "stopped"
    For **api** mode: checks whether the api_streaming Airflow DAG has an active
    run whose conf.bucket matches this business.
      - active run found   → "running"
      - no active run      → "stopped"
    Non-streaming businesses → "unknown"
    Network / config errors  → "unknown"  (frontend treats this as no-change)
    """
    import os
    import json
    import base64
    import urllib.request
    import urllib.error

    result = db.execute(
        text(
            """
            SELECT ingestion_type, db_uri, api_url
            FROM onboarding
            WHERE business_id = :bid AND is_completed = true
            LIMIT 1
            """
        ),
        {"bid": business_id},
    ).fetchone()

    if not result:
        result = db.execute(
            text("SELECT ingestion_type, db_uri, api_url FROM onboarding WHERE business_id = :bid LIMIT 1"),
            {"bid": business_id},
        ).fetchone()

    if not result or result[0] not in ("db", "api"):
        return {"connector_status": "unknown"}

    ingestion_type = result[0]
    stored_db_uri = (result[1] or "").strip() if len(result) > 1 else ""
    stored_api_url = (result[2] or "").strip() if len(result) > 2 else ""

    # ── db mode: Debezium connector health ────────────────────────────────
    if ingestion_type == "db":
        # Primary signal for DB mode indicator: source DB reachability.
        # This keeps the dashboard green when the database is reachable,
        # even if Debezium is transiently restarting.
        if stored_db_uri:
            try:
                is_connected, message = await run_in_threadpool(
                    validate_database_connection,
                    stored_db_uri,
                    5,
                )
                if is_connected:
                    return {
                        "connector_status": "running",
                        "health_source": "database_connectivity",
                    }
                return {
                    "connector_status": "stopped",
                    "reason": "database_unreachable",
                    "detail": message,
                    "health_source": "database_connectivity",
                }
            except Exception as exc:
                logger.warning("Database connectivity probe failed for %s: %s", business_id, exc)

        debezium_url = os.getenv("KAFKA_CONNECT_URL", "http://10.5.0.10:8083")
        connector_name = f"pulse-{business_id}-connector"
        try:
            req = urllib.request.Request(
                f"{debezium_url}/connectors/{connector_name}/status"
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read())
            connector_state = data.get("connector", {}).get("state", "")
            tasks = data.get("tasks", [])
            # All tasks must be RUNNING; an empty task list is treated as healthy
            # (connector just started and hasn't assigned any tasks yet).
            all_tasks_running = all(t.get("state") == "RUNNING" for t in tasks) if tasks else True
            if connector_state == "RUNNING" and all_tasks_running:
                return {"connector_status": "running"}
            return {"connector_status": "stopped", "connector_state": connector_state}
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {"connector_status": "stopped", "reason": "connector_not_found"}
            return {"connector_status": "unknown", "reason": str(exc)}
        except Exception as exc:
            return {"connector_status": "unknown", "reason": str(exc)}

    # ── api mode: Airflow api_streaming DAG health ────────────────────────
    # Primary signal for API mode indicator: source API reachability.
    # This keeps the dashboard green when the API endpoint is reachable,
    # even if Airflow streaming supervision is transiently restarting.
    if stored_api_url:
        try:
            is_connected, message = await run_in_threadpool(
                validate_api_endpoint,
                stored_api_url,
                5,
            )
            if is_connected:
                return {
                    "connector_status": "running",
                    "health_source": "api_connectivity",
                }
            return {
                "connector_status": "stopped",
                "reason": "api_unreachable",
                "detail": message,
                "health_source": "api_connectivity",
            }
        except Exception as exc:
            logger.warning("API connectivity probe failed for %s: %s", business_id, exc)

    airflow_base = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
    airflow_user = os.getenv("AIRFLOW_USERNAME", "admin")
    airflow_pass = os.getenv("AIRFLOW_PASSWORD", "admin")
    headers = {
        "Authorization": "Basic "
        + base64.b64encode(f"{airflow_user}:{airflow_pass}".encode()).decode(),
        "Content-Type": "application/json",
    }
    try:
        url = f"{airflow_base}/api/v1/dags/api_streaming/dagRuns?state=running&limit=100"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as r:
            runs = json.loads(r.read()).get("dag_runs", [])
        for run in runs:
            if (run.get("conf") or {}).get("bucket") == business_id:
                return {"connector_status": "running"}
        return {"connector_status": "stopped", "reason": "no_active_dag_run"}
    except Exception as exc:
        return {"connector_status": "unknown", "reason": str(exc)}


@router.websocket("/ws/{business_id}")
async def websocket_endpoint(websocket: WebSocket, business_id: str):
    """
    WebSocket endpoint for real-time pipeline progress updates.
    
    Path params:
        - business_id: Business ID to receive updates for
    """
    try:
        await websocket_manager.connect(websocket, business_id)
    except RuntimeError as exc:
        logger.info("WebSocket connect skipped for %s: %s", business_id, exc)
        return
    
    try:
        # Keep connection alive and handle incoming messages if needed
        while True:
            # Wait for messages (client can send ping to keep alive)
            data = await websocket.receive_text()
            
            # Handle ping/pong for keep-alive
            if data == "ping":
                await websocket.send_text("pong")
                continue

            # Client-requested status sync (live Airflow + DB reconciled).
            if data == "sync":
                try:
                    payload = await run_in_threadpool(
                        _build_pipeline_status_payload_for_business,
                        business_id,
                    )

                    status_data = payload.get("data")
                    if status_data:
                        await websocket.send_json(status_data)
                except Exception as sync_err:
                    logger.warning("WebSocket sync failed for %s: %s", business_id, sync_err)
                continue
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, business_id)
    except RuntimeError as exc:
        logger.info("WebSocket closed for %s: %s", business_id, exc)
        websocket_manager.disconnect(websocket, business_id)
    except Exception as e:
        logger.error("WebSocket error: %s", e, exc_info=True)
        websocket_manager.disconnect(websocket, business_id)
