# Pipeline Service Bug Fixes - Summary

## Issues Resolved

### Issue 1: Database Column Name Mismatch
**Error:**
```
psycopg2.errors.UndefinedColumn: column "progress" of relation "pipeline_status" does not exist
LINE 1: ...atus = 'running', current_step = 'Cleaning Data', progress =...
```

**Root Cause:**
The code was using `progress` as the column name, but the database schema defines it as `progress_percentage`.

**Files Modified:**
- `api/services/pipeline_service.py`

**Changes Made:**
1. **INSERT statement** (line 101): Changed `:progress` to `:progress_percentage`
2. **Parameter in INSERT** (line 109): Changed `"progress": 0` to `"progress_percentage": 0`
3. **UPDATE data dict** (line 338): Changed `"progress"` to `"progress_percentage"`

**Important:** WebSocket broadcast messages and API responses correctly continue to use `"progress"` (not `"progress_percentage"`) for frontend compatibility.

### Issue 2: Script Path Construction Error
**Error:**
```
ERROR: Script not found: /cleaning/cleaning.py
```

**Root Cause:**
The `project_root` calculation in `__init__` was too simplistic. It assumed a fixed directory structure that didn't work in the container environment where the script might be at `/app/services/pipeline_service.py` instead of `/path/to/pulse/api/services/pipeline_service.py`.

**Files Modified:**
- `api/services/pipeline_service.py`

**Changes Made:**
Enhanced the `__init__` method (lines 54-81) with intelligent path detection:
1. **Development structure**: Detects `api/services/` and goes up 3 levels
2. **Container structure**: Detects just `services/` and goes up 2 levels  
3. **Fallback**: Uses parent directory if neither pattern matches
4. **Logging**: Prints the detected `project_root` for debugging

**Example Paths:**
- `/home/user/pulse/api/services/pipeline_service.py` → `/home/user/pulse`
- `/app/services/pipeline_service.py` → `/app`
- `/app/api/services/pipeline_service.py` → `/app`

## Testing

Created verification script: `api/services/verify_pipeline_fixes.py`

**Verification Results:**
```
✅ PASS: Column Names
✅ PASS: Project Root Detection  
✅ PASS: Path Simulation
✅ ALL VERIFICATIONS PASSED
```

**What was verified:**
1. INSERT statement uses `:progress_percentage`
2. UPDATE data uses `"progress_percentage"` key
3. WebSocket messages still use `"progress"` (correct)
4. No incorrect `"progress"` usage in database operations
5. Path detection logic handles all scenarios correctly

## Files Changed

1. **api/services/pipeline_service.py** - Fixed both issues
2. **api/services/verify_pipeline_fixes.py** - New verification script

## Next Steps

1. **Deploy**: The fixes are ready for deployment
2. **Monitor**: Watch for the log message: `PipelineService initialized with project_root: {path}`
3. **Verify**: Ensure scripts are found at the correct paths
4. **Test**: Run a complete pipeline to confirm both fixes work together

## How to Test in Container

```bash
# Check the log output when the service starts
# Should see: "PipelineService initialized with project_root: /app"

# Trigger a pipeline and check for:
# 1. No more "column progress does not exist" errors
# 2. No more "Script not found" errors
# 3. Pipeline executes all phases successfully
```

## Database Schema Reference

For reference, the correct `pipeline_status` table structure:
```sql
CREATE TABLE pipeline_status (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    current_step VARCHAR(100),
    progress_percentage INTEGER DEFAULT 0,  -- Note: progress_percentage, not progress
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    process_ids JSONB NULL,
    ...
);
```
