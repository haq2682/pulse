from fastapi import APIRouter, Depends, HTTPException, Response, Request
from database import get_db
from sqlalchemy import text
import uuid

router = APIRouter(
    prefix="/onboarding",
    tags=["onboarding"],
)

@router.post("/create")
async def create_onboarding(request: Request, db=Depends(get_db)):
    body = await request.json()
    user_id = body.get("userId")
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="userId is required")
        result = db.execute(text("SELECT onboarding_id, current_step FROM onboarding WHERE user_id = :user_id"), {"user_id": user_id})
        existing = result.fetchone()
        if existing:
            onboarding_id, current_step = existing
            return {
                "status": 200,
                "onboarding_id": onboarding_id,
                "current_step": current_step,
                "message": "Onboarding already exists."
            }
        onboarding_id = str(uuid.uuid4())
        db.execute(text("INSERT INTO onboarding (onboarding_id, user_id, current_step) VALUES (:onboarding_id, :user_id, :current_step)"), {"onboarding_id": onboarding_id, "user_id": user_id, "current_step": "business"})
        db.commit()
        return {"status": 200, "onboarding_id": onboarding_id, "current_step": "business", "message": "Onboarding created."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/create-business")
async def create_business(userId: str, businessName: str, businessRegion: str, businessCurrency: str, db=Depends(get_db)):
    try:
        if not userId or not businessName:
            raise HTTPException(status_code=400, detail="userId and businessName are required")
        business_id = str(uuid.uuid4())
        db.execute(text("INSERT INTO businesses (business_id, user_id, business_name, business_region, business_currency) VALUES (:business_id, :user_id, :business_name, :business_region, :business_currency)"), {"business_id": business_id, "user_id": userId, "business_name": businessName, "business_region": businessRegion, "business_currency": businessCurrency})
        db.commit()
        db.execute(text("UPDATE onboarding SET current_step = :next_step WHERE user_id = :user_id AND business_id = :business_id"), {"next_step": "integration", "user_id": userId, "business_id": business_id})
        db.commit()
        return {"status": 200}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))