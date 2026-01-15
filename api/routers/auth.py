"""
Authentication API endpoints (RAW SQL, UUID, Session, Redis, Pydantic)
"""
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from sqlalchemy import text
from utils.password import hash_password, verify_password
from services.session_service import session_service
from schemas.auth import UserRegister, UserLogin
import uuid

from database import get_db

router = APIRouter(prefix="/auth", tags=["Authentication"])

# Cookie settings
COOKIE_NAME = "session_id"
COOKIE_MAX_AGE = 60 * 60 * 24  # 1 day (seconds)

# ============ Helper ============

def get_session_user(request: Request):
    session_id = request.cookies.get(COOKIE_NAME)
    if not session_id:
        return None
    session = session_service.get_session(session_id)
    return session

def require_auth(request: Request):
    user = get_session_user(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    return user

# ========== ENDPOINTS ===========

@router.post("/register")
def register(user: UserRegister, response: Response, db=Depends(get_db)):
    """
    Registers user ONLY if email is valid and password long enough.
    """
    user_id = str(uuid.uuid4())
    pw_hash = hash_password(user.password)
    # Check if email exists
    existing = db.execute(
        text("SELECT 1 FROM users WHERE email = :email"),
        {"email": user.email},
    ).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    # Insert user
    db.execute(
        text("INSERT INTO users (user_id, username, email, password_hash) VALUES (:user_id, :username, :email, :pw_hash)"),
        {"user_id": user_id, "username": user.username, "email": user.email, "pw_hash": pw_hash}
    )
    db.commit()
    # Create session
    session_id = session_service.create_session(user_id, user.email, user.username)
    response.set_cookie(
        key=COOKIE_NAME,
        value=session_id,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=False,
        samesite="lax"
    )
    return {
        "user_id": user_id,
        "username": user.username,
        "email": user.email,
        "message": "Registered & logged in."
    }

@router.post("/login")
def login(credentials: UserLogin, response: Response, db=Depends(get_db)):
    """
    Validates email format and password using Pydantic.
    """
    row = db.execute(
        text("SELECT user_id, username, password_hash FROM users WHERE email = :email"),
        {"email": credentials.email}
    ).fetchone()
    if not row or not verify_password(credentials.password, row.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    session_id = session_service.create_session(row.user_id, credentials.email, row.username)
    response.set_cookie(
        key=COOKIE_NAME,
        value=session_id,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=False,
        samesite="lax"
    )
    return {
        "user_id": row.user_id,
        "username": row.username,
        "email": credentials.email,
        "message": "Login successful"
    }

@router.post("/logout")
def logout(request: Request, response: Response):
    session_id = request.cookies.get(COOKIE_NAME)
    if session_id:
        session_service.delete_session(session_id)
    response.delete_cookie(COOKIE_NAME)
    return {"message": "Logged out successfully"}

@router.get("/me")
def get_me(request: Request):
    session = get_session_user(request)
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {
        "user_id": session['user_id'],
        "username": session['name'],
        "email": session['email'],
    }

@router.get("/status")
def auth_status(request: Request):
    session = get_session_user(request)
    if session:
        return {
            "authenticated": True,
            "user": {
                "user_id": session['user_id'],
                "username": session['name'],
                "email": session['email']
            }
        }
    return {"authenticated": False, "user": None}

@router.post("/business")
def create_business(user_id: str, business_name: str, region: str, currency: str, db=Depends(get_db)):
    result = db.execute(
        text(
            "INSERT INTO businesses (user_id, business_name, business_region, business_currency) "
            "VALUES (:user_id, :business_name, :region, :currency) RETURNING business_id"
        ),
        {
            "user_id": user_id,
            "business_name": business_name,
            "region": region,
            "currency": currency
        }
    )
    db.commit()
    business_id = result.scalar()
    return {"business_id": business_id, "message": "Business created"}