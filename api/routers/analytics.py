from fastapi import APIRouter, Depends, HTTPException, Response, Request, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from database import get_db
from sqlalchemy import text
import aioredis

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

router = APIRouter(
    prefix="/analytics",
    tags=["analytics"],
)

@router.get("/get-businesses")
async def get_businesses(userId: str, db=Depends(get_db)):
    result = db.execute(text("SELECT DISTINCT b.business_id, b.business_name, o.ingestion_type FROM businesses b JOIN onboarding o ON b.business_id = o.business_id WHERE o.user_id = :user_id AND o.is_completed = true"), {"user_id": userId})
    businesses = [{"business_id": row[0], "business_name": row[1], "ingestion_type": row[2]} for row in result.fetchall()]
    return {"businesses": businesses}