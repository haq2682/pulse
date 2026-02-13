from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from middleware import auth_middleware

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