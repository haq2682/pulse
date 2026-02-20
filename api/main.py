from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from middleware import auth_middleware
from minio import Minio
import os
from urllib.parse import urlparse

# Import your configuration (if you use .env and frontend URL there)
try:
    from config import get_settings
    settings = get_settings()
    frontend_url = settings.frontend_url
except ImportError:
    frontend_url = "http://localhost:5173"

# Import your auth router (assuming in routers/auth.py)
from routers.auth import router as auth_router
from routers.admin import router as admin_router
from routers.onboarding import router as onboarding_router
from routers.analytics import router as analytics_router
from routers.pipeline import router as pipeline_router
from routers.analytics import router as analytics_router

# Import services for initialization
from services.websocket_manager import WebSocketManager
from services.analytics_watcher_service import AnalyticsWatcherService, set_analytics_watcher

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    frontend_url
]

app = FastAPI(
    title="Pulse Analytics API",
    description="E-Commerce Analytics Platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # So cookies work between React & FastAPI
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def analytics_middleware(request, call_next):
    return await auth_middleware(request, call_next)


# Register your authentication router
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(onboarding_router)
app.include_router(analytics_router)
app.include_router(pipeline_router)

@app.get("/")
def read_root():
    return {"client": "FastAPI", "message": "Hello from FastAPI!"}

@app.get("/health")
def health():
    return {"status": "healthy"}


@app.on_event("startup")
async def startup_event():
    """
    Initialize services on application startup.
    """
    print("Initializing Analytics Watcher Service...")
    
    # Get MinIO credentials from environment
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    
    parsed = urlparse(minio_endpoint)
    host_port = parsed.netloc if parsed.scheme else minio_endpoint
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    # Initialize MinIO client
    minio_client = Minio(
        host_port,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to True if using HTTPS
    )
    
    # Initialize WebSocket manager for analytics
    analytics_ws_manager = WebSocketManager()
    
    # Initialize and set global analytics watcher
    watcher = AnalyticsWatcherService(minio_client, analytics_ws_manager)
    set_analytics_watcher(watcher)
    
    print("Analytics Watcher Service initialized successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanup on application shutdown.
    """
    print("Shutting down Analytics Watcher Service...")
    from services.analytics_watcher_service import get_analytics_watcher
    
    watcher = get_analytics_watcher()
    if watcher:
        # Stop all monitoring tasks
        for business_id in watcher.get_monitored_businesses():
            watcher.stop_monitoring(business_id)
    
    print("Analytics Watcher Service shut down")