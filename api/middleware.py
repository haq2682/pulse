from fastapi import Request, HTTPException, status, Response
import logging
from services.session_service import session_service

logger = logging.getLogger(__name__)

# Define paths that should be excluded from authentication
public_paths = [
    "/openapi.json",
    "/health",
    "/favicon.ico",
    "/auth/register",
    "/auth/login",
    "/auth/reset-password",
    "/auth/forgot-password",
    "/auth/google",
    "/auth/google/callback",
    "/",
    # Ingest stream is polled by api_ingest_service.py without browser auth
    "/ingest",
]

# Only expose docs/redoc in non-production environments
try:
    from config import get_settings
    _settings = get_settings()
    if not _settings.is_production:
        public_paths.extend(["/docs", "/redoc"])
except Exception:
    # Fallback: hide docs if config unavailable (fail secure)
    logger.error(
        "Failed to load settings; API documentation routes (/docs, /redoc) will remain protected.",
        exc_info=True,
    )

async def auth_middleware(request: Request, call_next):
    """
    Middleware to check authentication for protected routes.
    Raises HTTPException if user is not authenticated.
    """
    path = request.url.path
    
    # Skip auth check for public paths
    if any(path.startswith(public_path) for public_path in public_paths):
        return await call_next(request)
    
    # Extract session_id from cookies
    session_id = request.cookies.get("session_id")
    
    # Get session data
    session_data = session_service.get_session(session_id) if session_id else None
    
    # Check if session is valid
    if not session_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not allowed to perform this action"
        )
    
    # Attach session data to request state for downstream use
    request.state.session = session_data
    request.state.user_id = session_data.get("user_id")
    
    return await call_next(request)