from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from database import get_db
from sqlalchemy import text
import aioredis
from services.pipeline_service import PipelineService
from services.websocket_manager import WebSocketManager

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

router = APIRouter(
    prefix="/analytics",
    tags=["analytics"],
)

# WebSocket manager for pipeline operations
websocket_manager = WebSocketManager()

@router.get("/get-businesses")
async def get_businesses(userId: str, db=Depends(get_db)):
    result = db.execute(text("SELECT DISTINCT b.business_id, b.business_name, o.ingestion_type FROM businesses b JOIN onboarding o ON b.business_id = o.business_id WHERE o.user_id = :user_id AND o.is_completed = true"), {"user_id": userId})
    businesses = [{"business_id": row[0], "business_name": row[1], "ingestion_type": row[2]} for row in result.fetchall()]
    return {"businesses": businesses}


@router.delete("/delete-business")
async def delete_business(request: Request, db=Depends(get_db)):
    """
    Delete a business and all associated data.
    
    This will:
    - Stop any running pipeline and Spark sessions
    - Delete pipeline status records
    - Remove the business bucket from MinIO
    - Delete the business and onboarding records from database
    
    Request body:
        - userId: User ID (for authorization)
        - businessId: Business ID to delete
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
        
        # Initialize pipeline service for cleanup operations
        pipeline_service = PipelineService(db, websocket_manager)
        
        # 1. Check if there's a running pipeline and cancel it
        pipeline_status = db.execute(
            text("""
                SELECT pipeline_id, status FROM pipeline_status 
                WHERE business_id = :business_id AND status = 'running'
                ORDER BY started_at DESC LIMIT 1
            """),
            {"business_id": business_id}
        ).fetchone()
        
        if pipeline_status:
            print(f"Cancelling running pipeline {pipeline_status[0]} before deleting business")
            await pipeline_service.cancel_pipeline(pipeline_status[0], business_id)
        
        # 2. Delete all pipeline status records for this business
        db.execute(
            text("DELETE FROM pipeline_status WHERE business_id = :business_id"),
            {"business_id": business_id}
        )
        db.commit()
        print(f"Deleted pipeline status records for business {business_id}")
        
        # 3. Delete the business bucket from MinIO
        try:
            await pipeline_service.delete_business_bucket(business_id)
        except Exception as e:
            print(f"Warning: Error deleting MinIO bucket (may not exist): {e}")
            # Continue even if bucket deletion fails - it might not exist
        
        # 4. Delete onboarding records
        db.execute(
            text("DELETE FROM onboarding WHERE business_id = :business_id"),
            {"business_id": business_id}
        )
        db.commit()
        print(f"Deleted onboarding records for business {business_id}")
        
        # 5. Delete business record
        db.execute(
            text("DELETE FROM businesses WHERE business_id = :business_id"),
            {"business_id": business_id}
        )
        db.commit()
        print(f"Deleted business record for business {business_id}")
        
        return {
            "status": 200,
            "message": "Business deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting business: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))