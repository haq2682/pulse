"""
Pipeline API router for managing data processing pipeline execution.
"""

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from database import get_db
from sqlalchemy import text
from services.pipeline_service import PipelineService
from services.streaming_pipeline_service import StreamingPipelineService
from services.websocket_manager import WebSocketManager


router = APIRouter(
    prefix="/pipeline",
    tags=["pipeline"],
)

# Global WebSocket manager instance
websocket_manager = WebSocketManager()


@router.post("/start")
async def start_pipeline(request: Request, db=Depends(get_db)):
    """
    Start the data processing pipeline for a business.
    
    Request body:
        - userId: User ID
        - businessId: Business ID (used as bucket name)
        - mode: Pipeline mode - "batch" (default), "streaming" 
        - ingestionMode: Data ingestion mode - "batch", "db" (CDC), or "api" (for streaming)
    """
    try:
        body = await request.json()
        user_id = body.get("userId")
        business_id = body.get("businessId")
        pipeline_mode = body.get("mode", "batch")  # batch or streaming
        ingestion_mode = body.get("ingestionMode", "batch")  # batch, db, or api
        
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
        
        # Start appropriate pipeline based on mode
        if pipeline_mode == "streaming":
            # Start streaming pipeline
            streaming_service = StreamingPipelineService(db, websocket_manager)
            pipeline_id = await streaming_service.start_streaming_pipeline(
                business_id, user_id, mode=ingestion_mode
            )
            message = f"Streaming pipeline started successfully (mode: {ingestion_mode})"
        else:
            # Start traditional batch pipeline
            pipeline_service = PipelineService(db, websocket_manager)
            pipeline_id = await pipeline_service.start_pipeline(business_id, user_id)
            message = "Batch pipeline started successfully"
        
        return {
            "status": 200,
            "message": message,
            "pipeline_id": pipeline_id,
            "pipeline_mode": pipeline_mode
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error starting pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_pipeline_status(business_id: str, db=Depends(get_db)):
    """
    Get current pipeline status for a business.
    
    Query params:
        - business_id: Business ID
    """
    try:
        pipeline_service = PipelineService(db, websocket_manager)
        status_info = pipeline_service.get_pipeline_status_info(business_id)
        
        if not status_info:
            return {
                "status": 200,
                "pipeline_status": "not_started",
                "message": "No pipeline execution found for this business"
            }
        
        return {
            "status": 200,
            "pipeline_status": status_info["status"],
            "data": status_info
        }
        
    except Exception as e:
        print(f"Error getting pipeline status: {e}")
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
        print(f"Error cancelling pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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
        print(f"Error retrying pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.websocket("/ws/{business_id}")
async def websocket_endpoint(websocket: WebSocket, business_id: str):
    """
    WebSocket endpoint for real-time pipeline progress updates.
    
    Path params:
        - business_id: Business ID to receive updates for
    """
    await websocket_manager.connect(websocket, business_id)
    
    try:
        # Keep connection alive and handle incoming messages if needed
        while True:
            # Wait for messages (client can send ping to keep alive)
            data = await websocket.receive_text()
            
            # Handle ping/pong for keep-alive
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, business_id)
    except Exception as e:
        print(f"WebSocket error: {e}")
        websocket_manager.disconnect(websocket, business_id)
