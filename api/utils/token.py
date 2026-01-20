import secrets
from datetime import datetime, timedelta
from jose import jwt, JWTError
from config import get_settings

settings = get_settings()

def generate_session_id() -> str:
    """Generate a secure random session ID."""
    return secrets.token_urlsafe(32)

def generate_reset_token() -> str:
    """Generate a secure random reset token."""
    return secrets.token_urlsafe(32)

def create_password_reset_token(email: str) -> str:
    """Create a JWT token for password reset."""
    expire = datetime.utcnow() + timedelta(minutes=settings.password_reset_expire_minutes)
    to_encode = {"sub": email, "exp": expire, "type": "password_reset"}
    return jwt.encode(to_encode, settings.secret_key, algorithm="HS256")

def verify_password_reset_token(token: str) -> str | None:
    """Verify password reset token and return email if valid."""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=["HS256"])
        if payload. get("type") != "password_reset":
            return None
        return payload.get("sub")
    except JWTError:
        return None