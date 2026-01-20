from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Database (FIXED: matches your .env)
    postgres_user: str
    postgres_password:  str
    postgres_database_name: str  # ← ONLY THIS CHANGED
    postgres_server: str
    
    # Redis (UNCHANGED:  your IP works fine)
    redis_host: str = "10.5.0.10"  # ← KEEP THIS
    redis_port: int = 6379
    
    # Security
    secret_key:  str
    session_expire_minutes: int = 1440
    password_reset_expire_minutes:  int = 30
    
    # Email (OPTIONAL: won't crash if not set)
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    from_email: str = "noreply@pulseanalytics.com"
    
    # Google OAuth (OPTIONAL:  won't crash if not set)
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/auth/google/callback"
    
    # Frontend
    frontend_url: str = "http://localhost:5173"
    
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_server}/{self.postgres_database_name}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False  # ← ADDED
        extra = "ignore"        # ← ADDED

@lru_cache()
def get_settings() -> Settings:
    return Settings()