"""
Centralized database and cache connection management.

This module provides:
- SQLAlchemy engine and session factory
- Redis connection for caching and task management
- Database dependency injection for FastAPI
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import aioredis
from config import get_settings

settings = get_settings()

# ============================================================================
# PostgreSQL Database Configuration
# ============================================================================

# 1. Create the Connection Pool
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,  # Verify connections before using them
    pool_size=10,        # Number of connections to maintain in the pool
    max_overflow=20      # Maximum number of connections that can be created beyond pool_size
)

# 2. Create SessionLocal for ORM sessions
# Used for manual session management in background tasks and streaming contexts
SessionLocal = sessionmaker(
    autocommit=False,  # Require explicit commit() calls
    autoflush=False,   # Prevent automatic flushes
    bind=engine        # Bind to the connection pool
)

# ============================================================================
# Redis Cache Configuration
# ============================================================================

# 3. Create Redis connection for async operations
# Used for: caching, session management, task coordination, temporary data storage
redis = aioredis.from_url(
    f"redis://{settings.redis_host}:{settings.redis_port}",
    decode_responses=True  # Automatically decode responses to strings
)

# ============================================================================
# FastAPI Dependency Functions
# ============================================================================

# 4. Database dependency for FastAPI route handlers
def get_db():
    """
    Provides a database connection for FastAPI dependency injection.
    
    Usage:
        @router.get("/example")
        def example_route(db = Depends(get_db)):
            result = db.execute(text("SELECT * FROM table"))
            ...
    
    The connection is automatically closed after the request completes.
    """
    with engine.connect() as connection:
        yield connection
        # The connection automatically closes here because of the 'with' block