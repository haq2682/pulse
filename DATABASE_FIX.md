# Database.py Fix - SessionLocal Addition

## Problem
The application was failing to start with an `ImportError` because `SessionLocal` was being imported from `database.py` but was not defined in that file.

### Error Location
```
File "/app/services/pipeline_service.py", line 11
    from database import SessionLocal
ImportError: cannot import name 'SessionLocal' from 'database'
```

### Files Importing SessionLocal
1. `api/services/pipeline_service.py` - Line 11
2. `api/routers/onboarding.py` - Line 1048

## Solution
Added the `SessionLocal` sessionmaker factory to `database.py`:

```python
from sqlalchemy.orm import sessionmaker

# 2. Create SessionLocal for ORM sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
```

## Why This Works

### Two Database Connection Patterns
The codebase uses two different patterns for database access:

1. **FastAPI Dependency Injection** (via `get_db()`)
   - Used in route handlers
   - Provides a raw connection via `engine.connect()`
   - Auto-managed by FastAPI's dependency system
   
2. **Manual Session Management** (via `SessionLocal`)
   - Used in background tasks and streaming contexts
   - Creates ORM Session instances
   - Requires manual `.close()` call

### SessionLocal Usage Pattern
```python
# Create a session
db = SessionLocal()

try:
    # Execute queries
    db.execute(text("..."), params)
    db.commit()
except Exception as e:
    db.rollback()
    raise
finally:
    db.close()
```

### Parameters Explained
- `autocommit=False` - Requires explicit `.commit()` calls
- `autoflush=False` - Prevents automatic flushes before queries
- `bind=engine` - Binds sessions to the connection pool

## Impact
This fix resolves the import error and allows:
- Background pipeline execution to work
- Streaming responses with separate database sessions
- Proper isolation between concurrent operations

## Testing
The fix can be verified by:
1. Starting the FastAPI application: `uvicorn main:app --reload`
2. Confirming no import errors on startup
3. Testing pipeline execution after mapping confirmation
4. Testing streaming log endpoints
