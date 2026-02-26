"""
Authentication API Endpoints (RAW SQL, matches your schema with password reset support)
"""
from fastapi import APIRouter, Depends, HTTPException, Response, Request, status
from sqlalchemy import text
from datetime import datetime, timedelta
import uuid
import secrets
from fastapi.responses import RedirectResponse
from services.google_oauth_service import google_oauth_service
from database import get_db
from schemas.auth import (
    UserRegister, UserLogin, ForgotPasswordRequest,
    ResetPasswordRequest
)
from services.session_service import session_service
from services.email_service import email_service
from utils.password import hash_password, verify_password
from utils.token import create_password_reset_token, verify_password_reset_token
from config import get_settings

settings = get_settings()
router = APIRouter(prefix="/auth", tags=["Authentication"])

COOKIE_NAME = "session_id"
COOKIE_SETTINGS = {
    "httponly": True,
    "secure": settings.is_production,
    "samesite": "lax",
    "max_age": settings.session_expire_minutes * 60
}

# ==== SESSION HELPERS ====
def get_session_user(request: Request):
    session_id = request.cookies.get(COOKIE_NAME)
    if not session_id:
        return None
    return session_service.get_session(session_id)

def require_auth(request: Request):
    user = get_session_user(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    return user

# ==== ENDPOINTS ====

@router.post("/register")
def register(user: UserRegister, response: Response, db=Depends(get_db)):
    existing = db.execute(
        text("SELECT 1 FROM users WHERE email = :email"),
        {"email": user.email}
    ).fetchone()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_id = str(uuid.uuid4())
    pw_hash = hash_password(user.password)
    db.execute(
        text("INSERT INTO users (user_id, username, email, password_hash) VALUES (:user_id, :username, :email, :pw_hash)"),
        {
            "user_id": user_id,
            "username": user.username,
            "email": user.email,
            "pw_hash": pw_hash
        }
    )
    db.commit()
    session_id = session_service.create_session(user_id, user.email, user.username)
    response.set_cookie(key=COOKIE_NAME, value=session_id, **COOKIE_SETTINGS)
    return {
        "user_id": user_id,
        "username": user.username,
        "email": user.email,
        "message": "Registered & logged in."
    }

@router.post("/login")
def login(credentials: UserLogin, response: Response, db=Depends(get_db)):
    row = db.execute(
        text("SELECT user_id, username, password_hash FROM users WHERE email = :email"),
        {"email": credentials.email}
    ).fetchone()
    if not row or not verify_password(credentials.password, row.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    session_id = session_service.create_session(row.user_id, credentials.email, row.username)
    response.set_cookie(key=COOKIE_NAME, value=session_id, **COOKIE_SETTINGS)
    business = db.execute(
        text("""
            SELECT business_id 
            FROM businesses 
            WHERE user_id = :user_id 
            ORDER BY business_id ASC 
            LIMIT 1
        """),
        {"user_id": row.user_id}
    ).fetchone()

    return {
        "user_id": row.user_id,
        "username": row.username,
        "email": credentials.email,
        "message": "Login successful",
        "business_id": business[0] if business else None
    }

@router.post("/logout")
def logout(request: Request, response: Response):
    session_id = request.cookies.get(COOKIE_NAME)
    if session_id:
        session_service.delete_session(session_id)
    response.delete_cookie(COOKIE_NAME)
    return {"message": "Logged out successfully"}

@router.get("/me")
def get_me(request: Request, db=Depends(get_db)):
    session = get_session_user(request)
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    row = db.execute(
        text("SELECT user_id, username, email FROM users WHERE user_id = :user_id"),
        {"user_id": session["user_id"]}
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "user_id": row.user_id,
        "username": row.username,
        "email": row.email
    }

@router.post("/forgot-password")
async def forgot_password(data: ForgotPasswordRequest, db=Depends(get_db)):
    row = db.execute(
        text("SELECT user_id, email, username FROM users WHERE email = :email"),
        {"email": data.email}
    ).fetchone()
    # Always "success" for anti-enumeration
    if not row:
        return {"message": "If an account exists, a reset link has been sent"}

    reset_token = create_password_reset_token(row.email)
    expiry = datetime.utcnow() + timedelta(minutes=settings.password_reset_expire_minutes)
    db.execute(
        text("UPDATE users SET reset_token = :reset_token, reset_token_expires = :expiry WHERE user_id = :user_id"),
        {
            "reset_token": reset_token,
            "expiry": expiry,
            "user_id": row.user_id
        }
    )
    db.commit()
    await email_service.send_password_reset_email(row.email, reset_token, row.username)
    return {"message": "If an account exists, a reset link has been sent"}

@router.post("/reset-password")
def reset_password(data: ResetPasswordRequest, db=Depends(get_db)):
    email = verify_password_reset_token(data.token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    row = db.execute(
        text("SELECT user_id FROM users WHERE email = :email AND reset_token = :token AND reset_token_expires > :now"),
        {"email": email, "token": data.token, "now": datetime.utcnow()}
    ).fetchone()
    if not row:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    hashed = hash_password(data.new_password)
    db.execute(
        text("UPDATE users SET password_hash = :pw, reset_token = NULL, reset_token_expires = NULL WHERE user_id = :user_id"),
        {"pw": hashed, "user_id": row.user_id}
    )
    db.commit()
    # Optionally: use session_service.delete_all_user_sessions(row.user_id) if you want to force logout everywhere.
    return {"message": "Password reset successfully"}

@router.get("/status")
def auth_status(request: Request):
    session = get_session_user(request)
    if session:
        return {
            "authenticated": True,
            "user": {
                "user_id": session['user_id'],
                "username": session['name'],
                "email": session['email'],
            }
        }
    return {"authenticated": False, "user": None}
@router.get("/session/validate")
def validate_session(request: Request, db=Depends(get_db)):
    token = request.cookies.get(COOKIE_NAME) # Ensure this is COOKIE_NAME constant
    if not token:
        return {"authenticated": False, "user": None}

    session_data = session_service.get_session(token)
    if not session_data:
        return {"authenticated": False, "user": None}

    user_id = session_data.get("user_id")

    # FIX: Removed 'auth_provider' and 'is_active' because they don't exist in your DB yet
    row = db.execute(
        text("SELECT user_id, username, email FROM users WHERE user_id = :uid"),
        {"uid": user_id}
    ).mappings().fetchone()

    if row:
        return {
            "authenticated": True,
            "user": {
                "user_id": row["user_id"],
                "username": row["username"],
                "email": row["email"]
                # Removed the extra fields here too
            }
        }
    
    return {"authenticated": False, "user": None}

@router.get("/google")
async def login_google():
    """
    Step 1: Redirect the user's browser to the Google Login Page.
    """
    # Generate a random state string for security
    state = secrets.token_urlsafe(16)
    
    # Get the Google URL from our service
    auth_url = google_oauth_service.get_authorization_url(state=state)
    
    # Redirect the user there
    return RedirectResponse(url=auth_url)
@router.get("/google/callback")
async def google_callback(code: str, state: str, db=Depends(get_db)):
    """
    Handles Google Login matching YOUR specific schema.
    Schema: user_id, username, email, password_hash (NULL), reset_token, etc.
    """
    try:
        # 1. Exchange the code for an Access Token
        tokens = await google_oauth_service.get_tokens(code)
        access_token = tokens.get("access_token")
        
        if not access_token:
            raise HTTPException(status_code=400, detail="Failed to get access token from Google")

        # 2. Get User Info (Email, Name)
        user_info = await google_oauth_service.get_user_info(access_token)
        if not user_info:
            raise HTTPException(status_code=400, detail="Failed to get user info from Google")
            
        google_email = user_info.get("email")
        google_name = user_info.get("name")
        
        # 3. Check if user exists in DB
        row = db.execute(
            text("SELECT user_id, username, email FROM users WHERE email = :email"),
            {"email": google_email}
        ).fetchone()

        user_id = None
        username = google_name

        if row:
            # Case A: User exists -> Log them in
            user_id = row.user_id
            username = row.username
            print(f"Logging in existing Google user: {google_email}")
        else:
            # Case B: New User -> Register them
            user_id = str(uuid.uuid4())
            print(f"Registering new Google user: {google_email}")
            
            # STRICT INSERT: Only columns that exist in your provided schema
            # We explicitly pass None for password_hash since your schema allows NULL
            db.execute(
                text("""
                    INSERT INTO users (user_id, username, email, password_hash) 
                    VALUES (:user_id, :username, :email, :password_hash)
                """),
                {
                    "user_id": user_id,
                    "username": google_name,
                    "email": google_email,
                    "password_hash": None  # <--- Allowed because your schema says VARCHAR(255) NULL
                }
            )
            db.commit()

        # 4. Create Session
        session_id = session_service.create_session(user_id, google_email, username)
        
        business = db.execute(
            text("""
                SELECT business_id 
                FROM businesses 
                WHERE user_id = :user_id 
                ORDER BY business_id ASC 
                LIMIT 1
            """),
            {"user_id": row.user_id}
        ).fetchone()

        # 5. Redirect to Frontend Dashboard
        redirect_url = f"{settings.frontend_url}/analytics{'/' + business[0] if business else ''}"
        
        response = RedirectResponse(url=redirect_url)
        
        # Set the secure session cookie
        response.set_cookie(
            key=COOKIE_NAME, 
            value=session_id, 
            **COOKIE_SETTINGS
        )
        
        return response

    except Exception as e:
        print(f"Google Login Error: {e}")
        # On failure, redirect to login page with error
        return RedirectResponse(url=f"{settings.frontend_url}/login?error=google_auth_failed")
    
@router.post("/business")
def create_business(user_id: str, business_name: str, region: str, currency: str, db=Depends(get_db)):
    # 1. Generate a unique Business ID
    biz_id = str(uuid.uuid4()) 

    # 2. Insert with the ID
    db.execute(
        text("""
            INSERT INTO businesses (business_id, user_id, business_name, business_region, business_currency) 
            VALUES (:biz_id, :user_id, :business_name, :region, :currency)
        """),
        {
            "biz_id": biz_id,  # <--- Pass the generated ID here
            "user_id": user_id,
            "business_name": business_name,
            "region": region,
            "currency": currency
        }
    )
    db.commit()
    
    # 3. Return the generated ID
    return {"business_id": biz_id, "message": "Business created"}