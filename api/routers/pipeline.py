"""
Pipeline management API endpoints.
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from database import get_db, redis
from sqlalchemy import text
import asyncio
import json
from services.pipeline_service import (
    execute_pipeline,
    cancel_pipeline
)

router = APIRouter(
    prefix="/pipeline",
    tags=["pipeline"],
)


@router.post("/start")
async def start_pipeline(request: Request, db=Depends(get_db)):
    """
    Start the data processing pipeline for a business.
    This should be called after mapping is confirmed.
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        
        # Get business_id from onboarding
        result = db.execute(
            text("""
                SELECT business_id, is_completed, mapping_status 
                FROM onboarding 
                WHERE user_id = :user_id
            """),
            {"user_id": user_id}
        )
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Onboarding record not found")
        
        business_id, is_completed, mapping_status = row
        
        if not business_id:
            raise HTTPException(status_code=400, detail="Business ID not found")
        
        # Verify mapping is completed
        if mapping_status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot start pipeline - mapping status is '{mapping_status}', must be 'completed'"
            )
        
        # Check if pipeline is already running
        existing = db.execute(
            text("""
                SELECT pipeline_id, status 
                FROM pipeline_executions 
                WHERE business_id = :business_id 
                AND status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        existing_row = existing.fetchone()
        
        if existing_row:
            return {
                "status": 200,
                "pipeline_id": existing_row[0],
                "message": "Pipeline is already running"
            }
        
        # Start pipeline execution in background
        asyncio.create_task(execute_pipeline(business_id, user_id))
        
        # Give it a moment to create the record
        await asyncio.sleep(0.5)
        
        # Get the pipeline_id from the latest record
        result = db.execute(
            text("""
                SELECT pipeline_id 
                FROM pipeline_executions 
                WHERE business_id = :business_id 
                ORDER BY started_at DESC 
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        pipeline_row = result.fetchone()
        pipeline_id = pipeline_row[0] if pipeline_row else None
        
        return {
            "status": 200,
            "pipeline_id": pipeline_id,
            "message": "Pipeline started successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error starting pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_pipeline_status(userId: str, db=Depends(get_db)):
    """
    Get the current status of the pipeline for a user.
    """
    try:
        if not userId:
            raise HTTPException(status_code=400, detail="userId is required")
        
        # Get business_id from onboarding
        result = db.execute(
            text("SELECT business_id FROM onboarding WHERE user_id = :user_id"),
            {"user_id": userId}
        )
        row = result.fetchone()
        
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Business not found")
        
        business_id = row[0]
        
        # Get latest pipeline execution
        pipeline_result = db.execute(
            text("""
                SELECT 
                    pipeline_id,
                    status,
                    current_phase,
                    progress_percentage,
                    step_description,
                    error_message,
                    started_at,
                    completed_at
                FROM pipeline_executions
                WHERE business_id = :business_id
                ORDER BY started_at DESC
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        pipeline_row = pipeline_result.fetchone()
        
        if not pipeline_row:
            return {
                "status": 200,
                "pipeline_status": None,
                "message": "No pipeline execution found"
            }
        
        pipeline_id, status, current_phase, progress_percentage, step_description, error_message, started_at, completed_at = pipeline_row
        
        return {
            "status": 200,
            "pipeline_id": pipeline_id,
            "pipeline_status": status,
            "current_phase": current_phase,
            "progress_percentage": progress_percentage,
            "step_description": step_description,
            "error_message": error_message,
            "started_at": started_at.isoformat() if started_at else None,
            "completed_at": completed_at.isoformat() if completed_at else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cancel")
async def cancel_pipeline_execution(request: Request, db=Depends(get_db)):
    """
    Cancel a running pipeline execution.
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        
        # Get business_id and pipeline_id
        result = db.execute(
            text("SELECT business_id FROM onboarding WHERE user_id = :user_id"),
            {"user_id": user_id}
        )
        row = result.fetchone()
        
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Business not found")
        
        business_id = row[0]
        
        # Get running pipeline
        pipeline_result = db.execute(
            text("""
                SELECT pipeline_id 
                FROM pipeline_executions 
                WHERE business_id = :business_id 
                AND status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        pipeline_row = pipeline_result.fetchone()
        
        if not pipeline_row:
            raise HTTPException(status_code=404, detail="No running pipeline found")
        
        pipeline_id = pipeline_row[0]
        
        # Cancel the pipeline
        await cancel_pipeline(pipeline_id, business_id, db)
        
        return {
            "status": 200,
            "message": "Pipeline cancelled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error cancelling pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/retry")
async def retry_pipeline(request: Request, db=Depends(get_db)):
    """
    Retry a failed pipeline execution.
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        
        # Get business_id
        result = db.execute(
            text("SELECT business_id FROM onboarding WHERE user_id = :user_id"),
            {"user_id": user_id}
        )
        row = result.fetchone()
        
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Business not found")
        
        business_id = row[0]
        
        # Check latest pipeline status
        pipeline_result = db.execute(
            text("""
                SELECT status 
                FROM pipeline_executions 
                WHERE business_id = :business_id 
                ORDER BY started_at DESC 
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        pipeline_row = pipeline_result.fetchone()
        
        if pipeline_row and pipeline_row[0] == "running":
            raise HTTPException(status_code=400, detail="Pipeline is already running")
        
        # Start new pipeline execution
        asyncio.create_task(execute_pipeline(business_id, user_id))
        
        # Give it a moment to create the record
        await asyncio.sleep(0.5)
        
        # Get the new pipeline_id
        result = db.execute(
            text("""
                SELECT pipeline_id 
                FROM pipeline_executions 
                WHERE business_id = :business_id 
                ORDER BY started_at DESC 
                LIMIT 1
            """),
            {"business_id": business_id}
        )
        new_pipeline_row = result.fetchone()
        pipeline_id = new_pipeline_row[0] if new_pipeline_row else None
        
        return {
            "status": 200,
            "pipeline_id": pipeline_id,
            "message": "Pipeline restarted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error retrying pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status-stream")
async def stream_pipeline_status(userId: str, db=Depends(get_db)):
    """
    Stream pipeline status updates using Server-Sent Events (SSE).
    """
    if not userId:
        raise HTTPException(status_code=400, detail="userId is required")
    
    # Get business_id
    result = db.execute(
        text("SELECT business_id FROM onboarding WHERE user_id = :user_id"),
        {"user_id": userId}
    )
    row = result.fetchone()
    
    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="Business not found")
    
    business_id = row[0]
    
    async def event_stream():
        """Generator function to stream pipeline status updates."""
        try:
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Connected to pipeline status stream'})}\n\n"
            
            last_status = None
            no_change_count = 0
            max_no_change = 120  # 60 seconds with 0.5s interval
            
            while no_change_count < max_no_change:
                # Get latest pipeline status from database
                pipeline_result = db.execute(
                    text("""
                        SELECT 
                            pipeline_id,
                            status,
                            current_phase,
                            progress_percentage,
                            step_description,
                            error_message
                        FROM pipeline_executions
                        WHERE business_id = :business_id
                        ORDER BY started_at DESC
                        LIMIT 1
                    """),
                    {"business_id": business_id}
                )
                pipeline_row = pipeline_result.fetchone()
                
                if pipeline_row:
                    pipeline_id, status, current_phase, progress, step_desc, error_msg = pipeline_row
                    
                    current_status = f"{status}|{current_phase}|{progress}|{step_desc}"
                    
                    # Only send update if status changed
                    if current_status != last_status:
                        no_change_count = 0
                        last_status = current_status
                        
                        data = {
                            "type": "status_update",
                            "pipeline_id": pipeline_id,
                            "status": status,
                            "current_phase": current_phase,
                            "progress_percentage": progress,
                            "step_description": step_desc,
                            "error_message": error_msg
                        }
                        
                        yield f"data: {json.dumps(data)}\n\n"
                        
                        # If pipeline completed, failed, or cancelled, close stream
                        if status in ["completed", "failed", "cancelled"]:
                            break
                    else:
                        no_change_count += 1
                else:
                    no_change_count += 1
                
                # Wait before next check
                await asyncio.sleep(0.5)
            
            # Send completion message
            yield f"data: {json.dumps({'type': 'stream_end', 'message': 'Stream ended'})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
