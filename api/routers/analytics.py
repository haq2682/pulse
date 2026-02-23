from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from database import get_db
from sqlalchemy import text
import aioredis
from datetime import datetime
from services.pipeline_service import PipelineService
from services.websocket_manager import WebSocketManager
from services.analytics_service import AnalyticsService
from services.analytics_watcher_service import get_analytics_watcher
from services.forecasting_service import ForecastingService
from fastapi.responses import Response
import numpy as np
import json
import pandas as pd
from datetime import datetime, date

class AnalyticsJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            f = float(obj)
            if np.isnan(f) or np.isinf(f):
                return None
            return f
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return None
        try:
            if pd.isna(obj):
                return None
        except (TypeError, ValueError):
            pass
        return super().default(obj)

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

router = APIRouter(
    prefix="/analytics",
    tags=["analytics"],
)

# WebSocket manager for analytics updates
websocket_manager = WebSocketManager()

# Analytics service
analytics_service = AnalyticsService()

# Forecasting service
forecasting_service = ForecastingService()

@router.get("/get-businesses")
async def get_businesses(userId: str, db=Depends(get_db)):
    result = db.execute(text("SELECT DISTINCT b.business_id, b.business_name, o.ingestion_type FROM businesses b JOIN onboarding o ON b.business_id = o.business_id WHERE o.user_id = :user_id AND o.is_completed = true"), {"user_id": userId})
    businesses = [{"business_id": row[0], "business_name": row[1], "ingestion_type": row[2]} for row in result.fetchall()]
    return {"businesses": businesses}


@router.get("/get-business-currency/{business_id}")
async def get_business_currency(business_id: str, db=Depends(get_db)):
    """
    Fetch the currency configured for a specific business.

    Args:
        business_id: Business ID

    Returns:
        JSON with business currency information
    """
    try:
        result = db.execute(
            text("SELECT business_currency FROM businesses WHERE business_id = :business_id"),
            {"business_id": business_id}
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Business not found")

        currency = result[0] if result[0] else "USD"

        return {
            "business_id": business_id,
            "currency": currency
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching business currency: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
        
        # Use allow_nan=False to catch any remaining bad floats, replace them first
        json_str = json.dumps(result, cls=AnalyticsJSONEncoder, allow_nan=False)
        return Response(content=json_str, media_type="application/json")
        
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


@router.websocket("/ws/{business_id}")
async def analytics_websocket(websocket: WebSocket, business_id: str):
    """
    WebSocket endpoint for real-time analytics updates.
    
    Clients connect to this endpoint to receive real-time notifications
    when analytics parquet files are updated in MinIO.
    
    Args:
        websocket: WebSocket connection
        business_id: Business ID to monitor
        
    Message format sent to clients:
        {
            "event": "analytics_updated",
            "business_id": "business_123",
            "files": ["customer_acquisition_daily", "new_customers_weekly"],
            "categories": ["customer", "acquisition"],
            "changed_count": 1,
            "new_count": 1,
            "timestamp": "2026-02-17T18:58:00Z",
            "total_files": 2
        }
    """
    await websocket_manager.connect(websocket, business_id)
    
    # Start monitoring this business's analytics if not already monitoring
    watcher = get_analytics_watcher()
    if watcher and not watcher.is_monitoring(business_id):
        watcher.start_monitoring(business_id)
        print(f"Started analytics monitoring for business {business_id}")
    
    try:
        # Send initial connection confirmation
        await websocket.send_json({
            "event": "connected",
            "business_id": business_id,
            "message": "Connected to analytics updates",
            "timestamp": str(datetime.now())
        })
        
        # Keep connection alive and wait for disconnect
        while True:
            # Receive any messages from client (ping/pong, etc.)
            data = await websocket.receive_text()
            
            # Handle client commands
            if data == "ping":
                await websocket.send_json({"event": "pong"})
            elif data == "refresh":
                # Manually trigger update check
                if watcher:
                    await watcher.manual_trigger_update(business_id)
                    
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, business_id)
        print(f"Analytics WebSocket disconnected for business {business_id}")
        
        # Stop monitoring if no more connections for this business
        if websocket_manager.get_connection_count(business_id) == 0 and watcher:
            watcher.stop_monitoring(business_id)
            print(f"Stopped analytics monitoring for business {business_id} (no more connections)")


@router.post("/trigger-update/{business_id}")
async def trigger_analytics_update(business_id: str):
    """
    Manually trigger an analytics update check (for testing).
    
    Args:
        business_id: Business ID
        
    Returns:
        Success message
    """
    watcher = get_analytics_watcher()
    if not watcher:
        raise HTTPException(status_code=503, detail="Analytics watcher not available")
    
    try:
        await watcher.manual_trigger_update(business_id)
        return {
            "message": f"Update check triggered for business {business_id}",
            "is_monitoring": watcher.is_monitoring(business_id)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/exports")
async def record_export(request: Request, db=Depends(get_db)):
    """
    Record metadata for a generated analytics PDF export.

    Request body:
        - business_id: Business ID
        - user_id: User ID
        - file_name: Name of the exported file
        - sections_exported: List of section labels that were exported
        - total_sections: Total number of sections exported

    Returns:
        Created export record metadata
    """
    try:
        body = await request.json()
        business_id = body.get("business_id")
        user_id = body.get("user_id")
        file_name = body.get("file_name")
        sections_exported = body.get("sections_exported", [])
        total_sections = body.get("total_sections", 0)

        if not business_id or not user_id or not file_name:
            raise HTTPException(status_code=400, detail="business_id, user_id, and file_name are required")

        # Verify business belongs to user
        result = db.execute(
            text("SELECT business_id FROM businesses WHERE business_id = :business_id AND user_id = :user_id"),
            {"business_id": business_id, "user_id": user_id},
        ).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Business not found or access denied")

        import uuid
        export_id = str(uuid.uuid4())
        db.execute(
            text(
                """
                INSERT INTO analytics_exports
                    (export_id, business_id, user_id, file_name, sections_exported, total_sections)
                VALUES
                    (:export_id, :business_id, :user_id, :file_name, CAST(:sections_exported AS jsonb), :total_sections)
                """
            ),
            {
                "export_id": export_id,
                "business_id": business_id,
                "user_id": user_id,
                "file_name": file_name,
                "sections_exported": json.dumps(sections_exported),
                "total_sections": total_sections,
            },
        )
        db.commit()

        return {
            "export_id": export_id,
            "business_id": business_id,
            "file_name": file_name,
            "total_sections": total_sections,
            "created_at": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error recording export: {e}")
        raise HTTPException(status_code=500, detail="Failed to record export")


@router.get("/exports/{business_id}")
async def list_exports(business_id: str, user_id: str = Query(...), db=Depends(get_db)):
    """
    List all export records for a business.

    Args:
        business_id: Business ID
        user_id: User ID (query param for ownership verification)

    Returns:
        List of export records
    """
    try:
        # Verify ownership
        biz = db.execute(
            text("SELECT business_id FROM businesses WHERE business_id = :business_id AND user_id = :user_id"),
            {"business_id": business_id, "user_id": user_id},
        ).fetchone()
        if not biz:
            raise HTTPException(status_code=404, detail="Business not found or access denied")

        rows = db.execute(
            text(
                """
                SELECT export_id, file_name, sections_exported, total_sections, created_at
                FROM analytics_exports
                WHERE business_id = :business_id
                ORDER BY created_at DESC
                LIMIT 50
                """
            ),
            {"business_id": business_id},
        ).fetchall()

        return {
            "exports": [
                {
                    "export_id": r[0],
                    "file_name": r[1],
                    "sections_exported": r[2],
                    "total_sections": r[3],
                    "created_at": r[4].isoformat() if r[4] else None,
                }
                for r in rows
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error listing exports: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve export history")


@router.get("/forecasts/{business_id}")
async def get_all_forecasts(
    business_id: str,
    groups: str = Query(None, description="Comma-separated list of inference groups to fetch"),
    row_limit: int = Query(500, ge=1, le=10000, description="Maximum rows per inference (default 500)"),
):
    """
    Fetch all available ML inference results for a business.

    Inference results are read from the business's own MinIO bucket at
    machine-learning/{type}/predictions/{output_name}[/|.parquet].
    Missing inferences are silently skipped so the response only contains
    what has actually been run for this business.

    Args:
        business_id: Business ID (also the MinIO bucket name)
        groups: Optional comma-separated filter of inference groups
                (general_classification, general_clustering, general_regression,
                 specific_classification, specific_clustering, specific_regression)
        row_limit: Cap on rows returned per inference — prevents OOM on large datasets

    Returns:
        JSON with available inference results grouped by type
    """
    try:
        group_list = None
        if groups:
            group_list = [g.strip() for g in groups.split(",")]

        result = await forecasting_service.fetch_all_inferences(business_id, group_list, row_limit=row_limit)
        json_str = json.dumps(result, cls=AnalyticsJSONEncoder, allow_nan=False)
        return Response(content=json_str, media_type="application/json")

    except Exception as e:
        print(f"Error fetching forecasts for {business_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load forecast data")


@router.get("/forecasts/{business_id}/inference/{inference_name}")
async def get_single_forecast(
    business_id: str,
    inference_name: str,
    row_limit: int = Query(500, ge=1, le=10000, description="Maximum rows to return (default 500)"),
):
    """
    Fetch a single ML inference result for a business.

    Args:
        business_id: Business ID (MinIO bucket name)
        inference_name: Inference identifier (key from INFERENCE_CATALOG)
        row_limit: Cap on rows returned — prevents OOM on large datasets

    Returns:
        JSON with inference result data, or 404 if not available
    """
    try:
        if inference_name not in forecasting_service.INFERENCE_CATALOG:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown inference: {inference_name}",
            )

        result = await forecasting_service.fetch_inference(business_id, inference_name, row_limit=row_limit)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail="Inference results not available for this business",
            )

        json_str = json.dumps(result, cls=AnalyticsJSONEncoder, allow_nan=False)
        return Response(content=json_str, media_type="application/json")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching inference {inference_name} for {business_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load inference data")


@router.post("/forecasts/clear-cache/{business_id}")
async def clear_forecasts_cache(business_id: str):
    """Clear the forecasting cache for a specific business."""
    try:
        forecasting_service.clear_cache(business_id)
        return {"message": f"Forecast cache cleared for business {business_id}"}
    except Exception as e:
        print(f"Error clearing forecast cache: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear cache")



@router.get("/monitoring-status")
async def get_monitoring_status():
    """
    Get status of analytics monitoring service.

    Returns:
        List of monitored businesses and their status
    """
    watcher = get_analytics_watcher()
    if not watcher:
        return {
            "status": "not_initialized",
            "monitored_businesses": []
        }
    
    return {
        "status": "active",
        "monitored_businesses": watcher.get_monitored_businesses(),
        "count": len(watcher.get_monitored_businesses())
    }