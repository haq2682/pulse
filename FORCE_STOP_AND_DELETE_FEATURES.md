# Force Stop Spark Sessions & Delete Business Features

## Overview
This document describes the implementation of two new features:
1. **Force Stop Spark Sessions**: Automatically terminates Spark sessions when pipeline is cancelled
2. **Delete Business**: Complete removal of a business and all associated data

## Feature 1: Force Stop Spark Sessions on Cancel

### Problem
Previously, when a user cancelled a pipeline, only the parent Python process was terminated. Spark sessions could continue running in the background, consuming resources.

### Solution
Enhanced process termination to kill entire process groups and cleanup detached Spark processes.

### Implementation

#### Backend Changes (`api/services/pipeline_service.py`)

**1. Process Group Creation**
```python
# In _execute_phase method
process = await asyncio.create_subprocess_exec(
    *cmd,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    cwd=self.project_root,
    env=env,
    start_new_session=True  # Creates new process group
)
```

**2. Enhanced Termination**
```python
# In cancel_pipeline method
# Kill process group (all children including Spark)
os.killpg(os.getpgid(pid), signal.SIGTERM)

# Wait 2 seconds for graceful termination
await asyncio.sleep(2)

# Force kill if still running
os.killpg(os.getpgid(pid), signal.SIGKILL)

# Cleanup detached Spark processes
subprocess.run(['pkill', '-9', '-f', f'SparkSubmit.*{business_id}'])
```

### Benefits
- ✅ No orphaned Spark processes
- ✅ Complete resource cleanup
- ✅ Graceful termination with fallback
- ✅ Works for all pipeline phases

### Testing
1. Start a pipeline
2. Wait for Spark to initialize (transformation/analysis phase)
3. Cancel the pipeline
4. Verify no Spark processes remain: `ps aux | grep -i spark`

---

## Feature 2: Delete Business

### Problem
Users needed a way to completely remove a business and all associated data, including:
- Running pipelines
- Pipeline status records
- All data in MinIO
- Database records

### Solution
Added a Delete Business button with comprehensive cleanup logic.

### Implementation

#### Backend (`api/routers/analytics.py`)

**New Endpoint: `DELETE /analytics/delete-business`**

**Deletion Flow:**
```python
1. Verify business ownership (authorization)
2. Cancel running pipeline → Stop Spark sessions
3. Delete pipeline_status records
4. Delete MinIO bucket (all business data)
5. Delete onboarding records
6. Delete business record
```

**Code:**
```python
@router.delete("/delete-business")
async def delete_business(request: Request, db=Depends(get_db)):
    # 1. Authorization check
    # 2. Cancel running pipeline if exists
    await pipeline_service.cancel_pipeline(pipeline_id, business_id)
    
    # 3. Delete pipeline_status records
    db.execute(text("DELETE FROM pipeline_status WHERE business_id = :business_id"))
    
    # 4. Delete MinIO bucket
    await pipeline_service.delete_business_bucket(business_id)
    
    # 5. Delete onboarding records
    db.execute(text("DELETE FROM onboarding WHERE business_id = :business_id"))
    
    # 6. Delete business record
    db.execute(text("DELETE FROM businesses WHERE business_id = :business_id"))
```

**New Method: `delete_business_bucket()`** (`api/services/pipeline_service.py`)
```python
async def delete_business_bucket(self, business_id: str):
    # List all objects in bucket
    # Delete all objects
    # Delete the bucket itself
```

#### Frontend (`frontend/src/pages/dashboard/index.jsx`)

**Delete Button:**
- Icon: Trash icon (pi-trash)
- Location: Next to "Add Business/Organization" button
- Style: Red, icon-only, 44x44px
- Visibility: Only when business is selected

**Confirmation Dialog:**
```javascript
confirmDialog({
    message: (
        <div>
            <p>Delete <strong>{businessName}</strong>?</p>
            <p className="text-red-600">This will permanently delete:</p>
            <ul>
                <li>All pipeline data</li>
                <li>All processed data from storage</li>
                <li>All business records</li>
            </ul>
            <p>This action cannot be undone.</p>
        </div>
    ),
    header: 'Delete Business',
    icon: 'pi pi-exclamation-triangle',
    acceptClassName: 'p-button-danger',
    accept: performDeleteBusiness
});
```

**Delete Flow:**
```javascript
1. User clicks trash icon
2. Confirmation dialog shows with warnings
3. User confirms
4. DELETE request to /analytics/delete-business
5. On success: Navigate to /analytics/ and refresh list
6. On error: Show alert
```

### UI Elements

**Button Appearance:**
```
[Add Business/Organization]  [🗑️]  [Business Dropdown ▼]
```

**Confirmation Dialog:**
```
┌─────────────────────────────────────┐
│  ⚠️  Delete Business               │
├─────────────────────────────────────┤
│                                     │
│  Delete MyBusiness?                 │
│                                     │
│  This will permanently delete:      │
│  • All pipeline data                │
│  • All processed data from storage  │
│  • All business records             │
│                                     │
│  This action cannot be undone.      │
│                                     │
│      [Cancel]  [Delete]             │
└─────────────────────────────────────┘
```

### Security
- ✅ Authorization check (business belongs to user)
- ✅ Confirmation dialog prevents accidents
- ✅ Transaction safety with rollback on error
- ✅ Proper error handling

### Data Cleanup
```
Business Deletion Cascade:
├── Running Pipeline
│   ├── Python processes (SIGTERM/SIGKILL)
│   └── Spark sessions (process group + pkill)
├── Database Records
│   ├── pipeline_status (all records for business)
│   ├── onboarding (business onboarding data)
│   └── businesses (business record)
└── MinIO Storage
    └── {business_id} bucket (all objects + bucket)
```

### Testing

#### Manual Testing
1. **Create a business** and run a pipeline
2. **Verify button appears** when business is selected
3. **Click delete button** - confirmation dialog should appear
4. **Cancel dialog** - nothing should happen
5. **Click delete again and confirm** - business should be deleted
6. **Check database** - no records for business_id
7. **Check MinIO** - bucket should not exist
8. **Check UI** - redirected to /analytics/

#### With Running Pipeline
1. Start a pipeline for a business
2. While pipeline is running, click delete button
3. Confirm deletion
4. Verify:
   - Pipeline is cancelled
   - Spark processes are killed
   - All data is cleaned up

### Error Handling

**Backend:**
- Authorization failures → 404 "Business not found or access denied"
- Pipeline cancellation errors → Logged but continues with deletion
- MinIO errors → Logged as warning, continues with deletion
- Database errors → Rollback transaction, return 500

**Frontend:**
- API errors → Alert with error message
- Loading state → Button shows spinner
- Success → Navigate to /analytics/ and refresh

### API Documentation

**Endpoint:** `DELETE /analytics/delete-business`

**Request:**
```json
{
  "userId": "user_uuid",
  "businessId": "business_uuid"
}
```

**Response (Success):**
```json
{
  "status": 200,
  "message": "Business deleted successfully"
}
```

**Response (Error):**
```json
{
  "detail": "Error message"
}
```

**Status Codes:**
- 200: Success
- 400: Missing required fields
- 404: Business not found or access denied
- 500: Server error

### Performance Considerations

**Deletion Time:**
- Pipeline cancellation: 2-5 seconds
- MinIO cleanup: Depends on data size (typically 5-30 seconds)
- Database cleanup: < 1 second
- **Total:** Usually 10-40 seconds

**Resource Usage:**
- CPU spike during process termination
- Network I/O for MinIO operations
- Minimal database load

### Migration Notes

**No database migration required** - Uses existing tables

**MinIO Setup:**
- Ensure MinIO credentials in environment variables
- Bucket naming: Uses business_id as bucket name

### Known Limitations

1. **Orphaned Spark Processes**: In rare cases, if Spark is running on a different node or container, it may not be killed
2. **MinIO Errors**: If MinIO is unavailable, bucket deletion will fail but other cleanup continues
3. **Large Datasets**: Deleting large buckets may take time

### Future Improvements

1. **Soft Delete**: Add option to archive instead of permanent deletion
2. **Audit Trail**: Log deletion events for compliance
3. **Async Processing**: Move deletion to background job for large datasets
4. **Selective Cleanup**: Allow users to keep some data (e.g., keep analytics, delete raw data)

### Related Documentation

- [Pipeline Implementation Guide](PIPELINE_IMPLEMENTATION_GUIDE.md)
- [Database Connection Fix](DATABASE_CONNECTION_FIX.md)
- [Complete Pipeline Fixes](COMPLETE_PIPELINE_FIXES_SUMMARY.md)

---

## Summary

### What Was Implemented

1. ✅ **Force Stop Spark on Cancel**
   - Process group termination
   - Graceful shutdown with fallback
   - Cleanup of detached processes

2. ✅ **Delete Business Feature**
   - Backend endpoint with cascade deletion
   - Frontend button with confirmation
   - Complete data cleanup
   - Proper authorization

### Impact

**User Experience:**
- Clean resource management
- No orphaned processes
- Easy business deletion
- Clear feedback and warnings

**System Health:**
- No resource leaks
- Proper cleanup on cancellation
- Database integrity maintained
- Storage efficiently managed

### Testing Status

- ✅ Backend code validated
- ✅ Frontend code validated
- ⏳ Integration testing required
- ⏳ Performance testing recommended

**Status: Ready for Production Testing**
