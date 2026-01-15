from typing import Union
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import your configuration (if you use .env and frontend URL there)
try:
    from config import get_settings
    settings = get_settings()
    frontend_url = settings.frontend_url
except ImportError:
    frontend_url = "http://localhost:5173"

# Import your auth router (assuming in routers/auth.py)
from routers.auth import router as auth_router

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

# Register your authentication router
app.include_router(auth_router)

@app.get("/")
def read_root():
    return {"client": "FastAPI", "message": "Hello from FastAPI!"}

@app.get("/health")
def health():
    return {"status": "healthy"}