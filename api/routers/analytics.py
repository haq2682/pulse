from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from database import get_db
from sqlalchemy import text
import aioredis
from services.pipeline_service import PipelineService
from services.websocket_manager import WebSocketManager
from services.analytics_service import AnalyticsService

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

router = APIRouter(
    prefix="/analytics",
    tags=["analytics"],
)

# WebSocket manager for pipeline operations
websocket_manager = WebSocketManager()

# Analytics service
analytics_service = AnalyticsService()

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


@router.get("/data/{business_id}")
async def get_all_analytics(
    business_id: str,
    categories: str = Query(None, description="Comma-separated list of categories to fetch")
):
    """
    Fetch all analytics data for a business from MinIO.
    
    Args:
        business_id: Business ID (MinIO bucket name)
        categories: Optional comma-separated list of categories (default: all)
        
    Returns:
        JSON with all analytics data organized by category
        
    Example:
        GET /analytics/data/business_123
        GET /analytics/data/business_123?categories=customer_acquisition,revenue_analysis
    """
    try:
        category_list = None
        if categories:
            category_list = [c.strip() for c in categories.split(",")]
        
        result = await analytics_service.fetch_all_analytics(business_id, category_list)
        return result
        
    except Exception as e:
        print(f"Error fetching analytics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{business_id}/category/{category}")
async def get_category_analytics(business_id: str, category: str):
    """
    Fetch analytics for a specific category.
    
    Args:
        business_id: Business ID (MinIO bucket name)
        category: Category name (e.g., "customer_acquisition")
        
    Returns:
        JSON with category analytics data
        
    Example:
        GET /analytics/data/business_123/category/customer_acquisition
    """
    try:
        result = await analytics_service.fetch_category_analytics(business_id, category)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error fetching category analytics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/{business_id}/file/{file_name}")
async def get_analytics_file(business_id: str, file_name: str):
    """
    Fetch a single analytics file.
    
    Args:
        business_id: Business ID (MinIO bucket name)
        file_name: Analytics file name (without .parquet extension)
        
    Returns:
        JSON with analytics data
        
    Example:
        GET /analytics/data/business_123/file/customer_acquisition_daily
    """
    try:
        df = await analytics_service.fetch_analytics_file(business_id, file_name)
        
        if df is None:
            raise HTTPException(status_code=404, detail=f"Analytics file not found: {file_name}")
        
        return {
            "file_name": file_name,
            "data": df.to_dict(orient="records"),
            "columns": list(df.columns),
            "row_count": len(df)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching analytics file: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list/{business_id}")
async def list_available_analytics(business_id: str):
    """
    List all available analytics files for a business.
    
    Args:
        business_id: Business ID (MinIO bucket name)
        
    Returns:
        List of available analytics file names
        
    Example:
        GET /analytics/list/business_123
    """
    try:
        available = await analytics_service.list_available_analytics(business_id)
        return {
            "business_id": business_id,
            "available_analytics": available,
            "count": len(available)
        }
        
    except Exception as e:
        print(f"Error listing analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories")
async def get_analytics_categories():
    """
    Get list of all analytics categories and their file counts.
    
    Returns:
        Dict of categories with file lists
    """
    return {
        "categories": analytics_service.ANALYTICS_CATEGORIES,
        "category_names": list(analytics_service.ANALYTICS_CATEGORIES.keys()),
        "total_categories": len(analytics_service.ANALYTICS_CATEGORIES),
        "total_analytics": sum(len(files) for files in analytics_service.ANALYTICS_CATEGORIES.values())
    }


@router.post("/clear-cache/{business_id}")
async def clear_analytics_cache(business_id: str):
    """
    Clear analytics cache for a specific business.
    
    Args:
        business_id: Business ID
        
    Returns:
        Success message
    """
    try:
        analytics_service.clear_cache(business_id)
        return {"message": f"Cache cleared for business {business_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))