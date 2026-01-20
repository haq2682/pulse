from fastapi import APIRouter, Depends, HTTPException, Response, Request
from sqlalchemy import text
import uuid
from database import get_db
from schemas.auth import UserLogin, UserRegister
from datetime import datetime, timedelta
from schemas.auth import ForgotPasswordRequest, ResetPasswordRequest
from services.email_service import email_service
from utils.token import create_password_reset_token, verify_password_reset_token
from services.session_service import session_service
from utils.password import hash_password, verify_password
from config import get_settings  # <--- 1. Import Settings

settings = get_settings()        # <--- 2. Initialize Settings

# Prefix all routes with /admin
router = APIRouter(prefix="/admin", tags=["Admin Panel"])

ADMIN_COOKIE_NAME = "admin_session_id" 

# 3. Define Cookie Settings (Matches auth.py)
COOKIE_SETTINGS = {
    "httponly": True,
    "secure": False,  # Set to True in production (HTTPS)
    "samesite": "lax",
    "max_age": settings.session_expire_minutes * 60 
}

@router.post("/register")
def admin_register(admin: UserRegister, response: Response, db=Depends(get_db)):
    # 1. Check if admin email exists
    existing = db.execute(
        text("SELECT 1 FROM admins WHERE email = :email"),
        {"email": admin.email}
    ).fetchone()
    
    if existing:
        raise HTTPException(status_code=400, detail="Admin email already registered")
        
    # 2. Create Admin
    admin_id = str(uuid.uuid4())
    pw_hash = hash_password(admin.password)
    
    db.execute(
        text("INSERT INTO admins (admin_id, username, email, password_hash) VALUES (:id, :name, :email, :pw)"),
        {"id": admin_id, "name": admin.username, "email": admin.email, "pw": pw_hash}
    )
    db.commit()
    

    session_id = session_service.create_session(admin_id, admin.email, admin.username, role="admin")
    
    # 4. Set the Cookie immediately
    response.set_cookie(
        key=ADMIN_COOKIE_NAME, 
        value=session_id, 
        **COOKIE_SETTINGS
    )
    # -----------------------------

    return {"message": "Admin registered and logged in successfully"}

@router.post("/login")
def admin_login(credentials: UserLogin, response: Response, db=Depends(get_db)):
    # 1. Verify Admin Credentials
    row = db.execute(
        text("SELECT admin_id, username, password_hash FROM admins WHERE email = :email"),
        {"email": credentials.email}
    ).fetchone()
    
    if not row or not verify_password(credentials.password, row.password_hash):
        raise HTTPException(status_code=401, detail="Invalid admin credentials")
    
    
    session_id = session_service.create_session(row.admin_id, credentials.email, row.username, role="admin")
    
    # 3. Set a specific ADMIN cookie
    response.set_cookie(
        key=ADMIN_COOKIE_NAME, 
        value=session_id, 
        **COOKIE_SETTINGS
    )
    
    return {"message": "Admin login successful", "admin_name": row.username}

@router.post("/logout")
def admin_logout(response: Response):
    response.delete_cookie(ADMIN_COOKIE_NAME)
    return {"message": "Logged out"}

@router.post("/forgot-password")
async def admin_forgot_password(data: ForgotPasswordRequest, db=Depends(get_db)):
    # 1. Check if admin exists
    row = db.execute(
        text("SELECT admin_id, email, username FROM admins WHERE email = :email"),
        {"email": data.email}
    ).fetchone()

    # Always return success to prevent email enumeration scanning
    if not row:
        return {"message": "If an account exists, a reset link has been sent"}

    # 2. Generate Token & Expiry
    reset_token = create_password_reset_token(row.email)
    expiry = datetime.utcnow() + timedelta(minutes=settings.password_reset_expire_minutes)

    # 3. Save Token to Admins Table
    db.execute(
        text("UPDATE admins SET reset_token = :reset_token, reset_token_expires = :expiry WHERE admin_id = :admin_id"),
        {
            "reset_token": reset_token,
            "expiry": expiry,
            "admin_id": row.admin_id
        }
    )
    db.commit()

    # 4. Send Email
    # Note: Ensure your email_service handles the admin URL generation or pass the specific link
    # For now, we reuse the existing service.
    await email_service.send_admin_password_reset_email(row.email, reset_token, row.username)
    
    return {"message": "If an account exists, a reset link has been sent"}


@router.post("/reset-password")
def admin_reset_password(data: ResetPasswordRequest, db=Depends(get_db)):
    # 1. Verify the Token Signature
    email = verify_password_reset_token(data.token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    # 2. Check Database for matching Admin & Token Expiry
    row = db.execute(
        text("SELECT admin_id FROM admins WHERE email = :email AND reset_token = :token AND reset_token_expires > :now"),
        {"email": email, "token": data.token, "now": datetime.utcnow()}
    ).fetchone()

    if not row:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    # 3. Hash New Password
    hashed = hash_password(data.new_password)

    # 4. Update Password & Clear Token
    db.execute(
        text("UPDATE admins SET password_hash = :pw, reset_token = NULL, reset_token_expires = NULL WHERE admin_id = :admin_id"),
        {"pw": hashed, "admin_id": row.admin_id}
    )
    db.commit()

    return {"message": "Password reset successfully"}

@router.get("/dashboard-stats")
def get_dashboard_stats(request: Request, db=Depends(get_db)):
    # 1. Security Check: Ensure requester has an ADMIN cookie
    session_id = request.cookies.get(ADMIN_COOKIE_NAME)
    if not session_id or not session_service.get_session(session_id):
        raise HTTPException(status_code=401, detail="Not authenticated as Admin")

    # 2. Fetch Total Counts
    total_users = db.execute(text("SELECT COUNT(*) FROM users")).scalar()
    total_businesses = db.execute(text("SELECT COUNT(*) FROM businesses")).scalar()

    query = text("""
        SELECT u.username, u.email, b.business_name, b.business_region 
        FROM users u 
        LEFT JOIN businesses b ON u.user_id = b.user_id
        ORDER BY u.username ASC
    """)
    rows = db.execute(query).mappings().all()

    return {
        "stats": {
            "total_users": total_users,
            "total_businesses": total_businesses
        },
        "table_data": rows
    }