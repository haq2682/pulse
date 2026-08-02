from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from middleware import auth_middleware
from minio import Minio
from prometheus_fastapi_instrumentator import Instrumentator
import os
import logging
from urllib.parse import urlparse

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pulse")


def _minio_host_port(endpoint: str) -> str:
    """Strip a scheme from MINIO_ENDPOINT if present, for the `minio` SDK's
    Minio(endpoint, ...) constructor which expects a bare host:port.

    urlparse("host:port") without "//" misparses "host" as the URL *scheme*
    (parsed.scheme is truthy) and "port" as the path, leaving netloc empty -
    `if parsed.scheme` alone doesn't distinguish that from a real
    "http://host:port" endpoint. Check for "://" explicitly instead.
    """
    if "://" in endpoint:
        return urlparse(endpoint).netloc
    return endpoint

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
from routers.analytics import router as analytics_router, analytics_service
from routers.pipeline import router as pipeline_router
from routers.xai import router as xai_router
from routers.ingest import router as ingest_router

# Import services for initialization
from services.websocket_manager import WebSocketManager
from services.analytics_watcher_service import AnalyticsWatcherService, set_analytics_watcher

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    frontend_url
]

# Hide interactive API docs in production
docs_kwargs = {}
if settings.is_production:
    docs_kwargs = {"docs_url": None, "redoc_url": None, "openapi_url": None}

app = FastAPI(
    title="Pulse Analytics API",
    description="E-Commerce Analytics Platform",
    version="1.0.0",
    **docs_kwargs,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # So cookies work between React & FastAPI
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With",
                   "X-File-Id", "X-User-Id", "X-Business-Id",
                   "X-File-Name", "X-File-Size", "X-File-Type"],
)

@app.middleware("http")
async def analytics_middleware(request, call_next):
    return await auth_middleware(request, call_next)

# Exposes per-route request count/latency/size at GET /metrics for
# monitoring.yaml's pulse-api ServiceMonitor to scrape.
Instrumentator().instrument(app).expose(app, include_in_schema=False)


# Register your authentication router
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(onboarding_router)
app.include_router(analytics_router)
app.include_router(pipeline_router)
app.include_router(xai_router)
app.include_router(ingest_router)

@app.get("/")
def read_root():
    return {"client": "FastAPI", "message": "Hello from FastAPI!"}

@app.get("/health")
def health():
    """
    Health check endpoint for container orchestration.
    Verifies connectivity to critical dependencies.
    """
    checks = {"api": "healthy"}
    healthy = True

    # Check database
    try:
        from database import get_db_connection
        db = get_db_connection()
        db.execute(text("SELECT 1"))
        checks["database"] = "healthy"
        db.close()
    except Exception as e:
        checks["database"] = "unhealthy"
        logger.warning("Database health check failed: %s", e, exc_info=True)
        healthy = False

    # Check Redis
    try:
        import redis as redis_lib
        r = redis_lib.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            socket_timeout=2,
        )
        r.ping()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = "unhealthy"
        logger.warning("Redis health check failed: %s", e, exc_info=True)
        healthy = False

    # Check MinIO
    try:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        host_port = _minio_host_port(minio_endpoint)
        mc = Minio(
            host_port,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )
        mc.list_buckets()
        checks["minio"] = "healthy"
    except Exception as e:
        checks["minio"] = "unhealthy"
        logger.warning("MinIO health check failed: %s", e, exc_info=True)
        healthy = False

    status_code = 200 if healthy else 503
    from fastapi.responses import JSONResponse
    return JSONResponse(
        content={"status": "healthy" if healthy else "unhealthy", "checks": checks},
        status_code=status_code,
    )


@app.on_event("startup")
async def startup_event():
    """
    Initialize services on application startup.
    """
    logger.info("Initializing Analytics Watcher Service...")
    
    # Get MinIO credentials from environment
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")

    host_port = _minio_host_port(minio_endpoint)
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
    watcher.set_cache_invalidator(lambda business_id: analytics_service.clear_cache(business_id))
    set_analytics_watcher(watcher)
    
    logger.info("Analytics Watcher Service initialized successfully")

    # Recover any pipeline_status rows that were left in 'running' state by a
    # previous API process that crashed or was restarted.
    # For Airflow-backed pipelines: restarts the polling task if the DAG is
    # still active; syncs the DB status otherwise.
    # For legacy asyncio-backed rows (no dag_run_id): marks failed immediately.
    logger.info("Running pipeline crash recovery...")
    try:
        from database import get_db_connection
        from services.pipeline_service import recover_stuck_pipelines
        _recovery_db = get_db_connection()
        await recover_stuck_pipelines(_recovery_db)
        _recovery_db.close()
    except Exception as _exc:
        logger.error("Pipeline crash recovery failed: %s", _exc, exc_info=True)


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanup on application shutdown.
    """
    logger.info("Shutting down Analytics Watcher Service...")
    from services.analytics_watcher_service import get_analytics_watcher
    
    watcher = get_analytics_watcher()
    if watcher:
        # Stop all monitoring tasks
        for business_id in watcher.get_monitored_businesses():
            watcher.stop_monitoring(business_id)
    
    logger.info("Analytics Watcher Service shut down")