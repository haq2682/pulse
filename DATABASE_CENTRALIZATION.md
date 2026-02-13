# Database.py Centralization - Complete Guide

## Overview
This document explains the centralization of database and Redis connections in the `api/database.py` file.

## Problem Statement
Previously, the application had multiple issues:
1. **Duplicate Redis Connections**: Redis instances were created in multiple files
   - `services/pipeline_service.py` (Line 21)
   - `routers/onboarding.py` (Line 22)
2. **Incorrect Imports**: `routers/pipeline.py` was importing `redis` from `services/pipeline_service` instead of a centralized location
3. **Inconsistent Configuration**: Each Redis instance used hardcoded connection strings
4. **No Single Source of Truth**: Database connections scattered across multiple files

## Solution: Centralized database.py

### New Structure

```python
"""
Centralized database and cache connection management.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import aioredis
from config import get_settings

settings = get_settings()

# ============================================================================
# PostgreSQL Database Configuration
# ============================================================================

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# ============================================================================
# Redis Cache Configuration
# ============================================================================

redis = aioredis.from_url(
    f"redis://{settings.redis_host}:{settings.redis_port}",
    decode_responses=True
)

# ============================================================================
# FastAPI Dependency Functions
# ============================================================================

def get_db():
    """Database dependency for FastAPI route handlers."""
    with engine.connect() as connection:
        yield connection
```

### Key Components

#### 1. PostgreSQL Engine
- **Purpose**: Connection pool for database operations
- **Configuration**:
  - `pool_pre_ping=True`: Verifies connections before use
  - `pool_size=10`: Maintains 10 active connections
  - `max_overflow=20`: Allows up to 20 additional connections

#### 2. SessionLocal Factory
- **Purpose**: Creates ORM sessions for manual management
- **Use Cases**:
  - Background tasks
  - Streaming contexts
  - Long-running operations
- **Configuration**:
  - `autocommit=False`: Requires explicit `.commit()` calls
  - `autoflush=False`: Prevents automatic flushes
  - `bind=engine`: Binds to the connection pool

#### 3. Redis Connection
- **Purpose**: Async operations for caching and coordination
- **Use Cases**:
  - Session management
  - Task coordination
  - Temporary data storage
  - Process ID tracking
- **Configuration**:
  - Uses `settings.redis_host` and `settings.redis_port` from config
  - `decode_responses=True`: Automatically decodes responses to strings

#### 4. get_db() Dependency
- **Purpose**: FastAPI dependency injection
- **Usage Pattern**:
  ```python
  @router.get("/example")
  def example_route(db = Depends(get_db)):
      result = db.execute(text("SELECT * FROM table"))
  ```
- **Behavior**: Automatically closes connection after request

## Changes Made to Other Files

### 1. services/pipeline_service.py
**Before:**
```python
from database import SessionLocal
import aioredis

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)
```

**After:**
```python
from database import SessionLocal, redis
```

### 2. routers/onboarding.py
**Before:**
```python
from database import get_db
import aioredis

redis = aioredis.from_url("redis://redis:6379", decode_responses=True)
```

**After:**
```python
from database import get_db, redis
```

### 3. routers/pipeline.py
**Before:**
```python
from database import get_db
from services.pipeline_service import (
    execute_pipeline,
    cancel_pipeline,
    redis  # ❌ Wrong location
)
```

**After:**
```python
from database import get_db, redis  # ✅ Correct location
from services.pipeline_service import (
    execute_pipeline,
    cancel_pipeline
)
```

## Benefits

### 1. Single Source of Truth
- All database and cache connections defined in one place
- Easy to locate and modify connection configurations
- Reduces confusion about where connections are created

### 2. Consistent Configuration
- Redis connections use `settings.redis_host` and `settings.redis_port`
- Can be configured via environment variables
- No hardcoded connection strings

### 3. Better Maintainability
- Changes to connection settings only need to be made once
- Easier to add new connection types (e.g., MongoDB, Elasticsearch)
- Clear separation of concerns

### 4. Improved Testability
- Can easily mock the entire database module
- Centralized location for connection mocking
- Easier to create test fixtures

### 5. Clear Import Structure
```
database.py (defines connections)
    ↓
services/pipeline_service.py (imports and uses)
    ↓
routers/pipeline.py (imports from database, not service)
```

## Usage Examples

### Using get_db() in Route Handlers
```python
from fastapi import Depends
from database import get_db
from sqlalchemy import text

@router.get("/users")
def get_users(db = Depends(get_db)):
    result = db.execute(text("SELECT * FROM users"))
    return result.fetchall()
```

### Using SessionLocal in Background Tasks
```python
from database import SessionLocal
from sqlalchemy import text

def background_task():
    db = SessionLocal()
    try:
        db.execute(text("INSERT INTO tasks VALUES (...)"))
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()
```

### Using Redis for Caching
```python
from database import redis
import json

# Store data
await redis.setex("key", 3600, json.dumps(data))

# Retrieve data
cached = await redis.get("key")
if cached:
    data = json.loads(cached)
```

## Migration Checklist

If you need to add more services in the future:

- [ ] Define connection in `database.py`
- [ ] Add appropriate configuration from `settings`
- [ ] Export the connection for other modules to import
- [ ] Update imports in files that need the connection
- [ ] Remove duplicate connection definitions
- [ ] Update documentation

## Testing

To verify the changes work correctly:

1. **Start the application:**
   ```bash
   uvicorn main:app --reload
   ```

2. **Check for import errors:**
   - Application should start without ModuleNotFoundError
   - No errors about missing Redis or database connections

3. **Test pipeline operations:**
   - Start a pipeline execution
   - Verify Redis operations work
   - Verify database operations work

4. **Test streaming endpoints:**
   - Test mapping log streaming
   - Verify SessionLocal usage in streaming contexts

## Troubleshooting

### Import Error: cannot import name 'redis'
- **Cause**: Old cached Python files
- **Solution**: Clear `__pycache__` directories:
  ```bash
  find . -type d -name __pycache__ -exec rm -rf {} +
  ```

### Redis Connection Error
- **Cause**: Redis settings not configured
- **Solution**: Ensure `REDIS_HOST` and `REDIS_PORT` are set in `.env`

### Database Connection Error
- **Cause**: PostgreSQL settings not configured
- **Solution**: Verify database settings in `.env`:
  ```
  POSTGRES_USER=...
  POSTGRES_PASSWORD=...
  POSTGRES_DATABASE_NAME=...
  POSTGRES_SERVER=...
  ```

## References

- SQLAlchemy Engine: https://docs.sqlalchemy.org/en/14/core/engines.html
- aioredis: https://aioredis.readthedocs.io/
- FastAPI Dependencies: https://fastapi.tiangolo.com/tutorial/dependencies/
