# WebSocket Stability and Connection Management - Complete Fix

## Problems Addressed

### 1. WebSocket Reconnection Loops
**Symptom:**
```
WebSocket disconnected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe
WebSocket disconnected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe
WebSocket disconnected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe
WebSocket disconnected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe
INFO:     10.5.0.1:56742 - "WebSocket /pipeline/ws/01f21f39..." [accepted]
INFO:     connection open
WebSocket connected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe. Total connections: 1
INFO:     connection closed
WebSocket disconnected for business 01f21f39-47c2-4694-a984-c05eaad6c3fe
```

**Root Causes:**
1. **React useEffect Dependency Issue**: The `InlinePipelineProgress` component included `connectWebSocket`, `disconnectWebSocket`, and `fetchPipelineStatus` in its useEffect dependencies array
2. **Callback Recreation**: These callbacks are recreated on every render, causing the effect to run repeatedly
3. **No Duplicate Prevention**: No mechanism to prevent creating multiple connections to the same business_id
4. **Component Re-renders**: Every state change or parent re-render caused connection teardown and recreation

### 2. Database Connection Debugging
**Need:** Better visibility into which database connection is being used in background tasks vs request handlers

## Solutions Implemented

### Frontend - PipelineProgressContext.jsx

#### Added Connection State Tracking
```javascript
const currentBusinessIdRef = useRef(null);  // Track which business we're connected to
const isConnectingRef = useRef(false);      // Prevent concurrent connection attempts
```

#### Duplicate Connection Prevention
```javascript
const connectWebSocket = useCallback((businessId) => {
    if (!businessId) return;
    
    // Prevent duplicate connections for the same business
    if (isConnectingRef.current || 
        (wsRef.current && 
         wsRef.current.readyState === WebSocket.OPEN && 
         currentBusinessIdRef.current === businessId)) {
        console.log(`WebSocket already connected or connecting for business ${businessId}`);
        return;
    }
    
    // Close existing connection if switching to a different business
    if (wsRef.current && currentBusinessIdRef.current !== businessId) {
        console.log(`Closing existing connection for business ${currentBusinessIdRef.current}`);
        wsRef.current.close();
        wsRef.current = null;
    }
    
    isConnectingRef.current = true;
    currentBusinessIdRef.current = businessId;
    shouldReconnectRef.current = true;
    
    // ... rest of connection logic
}, [getWebSocketUrl]);
```

#### Proper Message Handling
```javascript
ws.onmessage = (event) => {
    try {
        // Handle pong response (don't parse as JSON)
        if (event.data === 'pong') {
            return;
        }
        
        const data = JSON.parse(event.data);
        console.log('Pipeline update received:', data);
        setPipelineStatus(data);
    } catch (err) {
        console.error('Error parsing WebSocket message:', err);
    }
};
```

#### Smart Reconnection Logic
```javascript
ws.onclose = () => {
    console.log('WebSocket disconnected');
    setIsConnected(false);
    wsRef.current = null;
    isConnectingRef.current = false;
    
    // Clear ping interval
    if (pingIntervalRef.current) {
        clearInterval(pingIntervalRef.current);
        pingIntervalRef.current = null;
    }
    
    // Only reconnect if:
    // 1. Auto-reconnect is enabled
    // 2. Haven't exceeded max attempts
    // 3. Still on the same business (not switching)
    if (shouldReconnectRef.current && 
        reconnectAttemptsRef.current < MAX_RECONNECT_ATTEMPTS &&
        currentBusinessIdRef.current === businessId) {
        reconnectAttemptsRef.current++;
        console.log(`Reconnecting... (attempt ${reconnectAttemptsRef.current}/${MAX_RECONNECT_ATTEMPTS})`);
        
        reconnectTimeoutRef.current = setTimeout(() => {
            connectWebSocket(businessId);
        }, RECONNECT_DELAY);
    }
};
```

### Frontend - InlinePipelineProgress.jsx

#### Fixed useEffect Dependencies
**Before:**
```javascript
useEffect(() => {
    if (businessId) {
        fetchPipelineStatus(businessId);
        connectWebSocket(businessId);
        
        return () => {
            disconnectWebSocket();
        };
    }
}, [businessId, connectWebSocket, disconnectWebSocket, fetchPipelineStatus]);
// ^ These callbacks change on every render!
```

**After:**
```javascript
const previousBusinessIdRef = React.useRef(null);

useEffect(() => {
    if (businessId && businessId !== previousBusinessIdRef.current) {
        fetchPipelineStatus(businessId);
        connectWebSocket(businessId);
        
        previousBusinessIdRef.current = businessId;
        
        return () => {
            disconnectWebSocket();
            previousBusinessIdRef.current = null;
        };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
}, [businessId]); // Only businessId to prevent reconnection loops
```

**Key Changes:**
1. Only `businessId` in dependencies array
2. Track previous businessId with ref to prevent re-running on same business
3. ESLint comment to document the intentional dependency omission

### Backend - pipeline_service.py

#### Enhanced Connection Logging
```python
async def _execute_pipeline_with_new_connection(self, ...):
    """
    Wrapper to execute pipeline with a new database connection.
    """
    from database import get_db_connection
    
    print(f"Creating new database connection for pipeline {pipeline_id}")
    db_connection = get_db_connection()
    print(f"Database connection created: {db_connection}")
    
    try:
        await self._execute_pipeline(pipeline_id, business_id, user_id, start_from_phase, db_connection)
    except Exception as e:
        print(f"Error in pipeline execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            db_connection.close()
            print(f"Pipeline {pipeline_id} database connection closed successfully")
        except Exception as e:
            print(f"Error closing database connection: {e}")
```

#### Connection Usage Tracking
```python
async def _update_progress(self, ..., db_connection=None):
    """
    Update pipeline progress in database and broadcast via WebSocket.
    """
    db = db_connection if db_connection is not None else self.db
    
    # Log which connection is being used
    if db_connection is not None:
        print(f"_update_progress using provided db_connection for pipeline {pipeline_id}")
    else:
        print(f"_update_progress WARNING: using self.db (request-scoped) for pipeline {pipeline_id}")
    
    try:
        # ... update logic
```

## Expected Behavior After Fix

### WebSocket Connection Lifecycle

**Scenario 1: Initial Connection**
```
User navigates to dashboard with businessId=xxx
  ↓
Component mounts, useEffect runs
  ↓
fetchPipelineStatus(xxx) called
  ↓
connectWebSocket(xxx) called
  ↓
Checks: !isConnecting && !alreadyConnected
  ↓
Creates WebSocket connection
  ↓
Log: "WebSocket connected for business xxx. Total connections: 1"
  ↓
Connection stays open for entire session
```

**Scenario 2: Component Re-render**
```
State update or parent re-render occurs
  ↓
useEffect dependency check: businessId === previousBusinessIdRef.current
  ↓
Effect does NOT re-run (businessId hasn't changed)
  ↓
Connection remains stable
```

**Scenario 3: Business Change**
```
User selects different business (yyy)
  ↓
useEffect dependency check: businessId !== previousBusinessIdRef.current
  ↓
Effect cleanup function runs
  ↓
disconnectWebSocket() closes connection to xxx
  ↓
Effect runs with new businessId
  ↓
connectWebSocket(yyy) creates new connection
  ↓
Log: "Closing existing connection for business xxx"
Log: "WebSocket connected for business yyy. Total connections: 1"
```

**Scenario 4: Temporary Disconnect**
```
Network issue or server restart
  ↓
WebSocket closes unexpectedly
  ↓
ws.onclose handler runs
  ↓
Checks: shouldReconnect && attempts < MAX && stillSameBusiness
  ↓
Waits RECONNECT_DELAY (3 seconds)
  ↓
Attempts reconnection
  ↓
Log: "Reconnecting... (attempt 1/5)"
  ↓
Connection re-established
```

### Database Connection Lifecycle

**For Each Pipeline Execution:**
```
1. API Request Arrives
   ↓
2. get_db() provides request-scoped connection A
   ↓
3. PipelineService(connection A) created
   ↓
4. Initial INSERT uses connection A ✓
   ↓
5. Request returns to client
   ↓
6. Connection A closed by get_db() context manager
   ↓
7. Background task continues:
   ↓
8. _execute_pipeline_with_new_connection() called
   ↓
9. Log: "Creating new database connection for pipeline xxx"
   ↓
10. get_db_connection() creates connection B
   ↓
11. Log: "Database connection created: <Connection object>"
   ↓
12. _execute_pipeline(db_connection=B) runs
    ↓
13. Each _update_progress() call:
    ↓
14. Log: "_update_progress using provided db_connection for pipeline xxx"
    ↓
15. Updates succeed using connection B ✓
    ↓
16. All phases complete
    ↓
17. Connection B closed
    ↓
18. Log: "Pipeline xxx database connection closed successfully"
```

## Log Patterns to Monitor

### Good (Expected)
```
WebSocket connected for business xxx. Total connections: 1
Creating new database connection for pipeline yyy
Database connection created: <sqlalchemy.engine.base.Connection object>
_update_progress using provided db_connection for pipeline yyy
[cleaning] ✅ cleaning phase completed successfully
_update_progress using provided db_connection for pipeline yyy
Starting transformation phase...
_update_progress using provided db_connection for pipeline yyy
[transformation] ✅ transformation phase completed successfully
_update_progress using provided db_connection for pipeline yyy
Starting analysis phase...
_update_progress using provided db_connection for pipeline yyy
[analysis] ✅ analysis phase completed successfully
_update_progress using provided db_connection for pipeline yyy
Starting machine-learning phase...
_update_progress using provided db_connection for pipeline yyy
[ml] ✅ ml phase completed successfully
_update_progress using provided db_connection for pipeline yyy
Pipeline yyy completed successfully!
Pipeline yyy database connection closed successfully
```

### Bad (Would Indicate Problems)
```
❌ WebSocket disconnected for business xxx
❌ WebSocket disconnected for business xxx
❌ WebSocket connected for business xxx
❌ WebSocket disconnected for business xxx
   (Rapid connect/disconnect = reconnection loop)

❌ _update_progress WARNING: using self.db (request-scoped) for pipeline yyy
   (Background task using request connection = will fail)

❌ Error updating progress: This Connection is closed
   (Connection already closed = fix didn't work)
```

## Testing Checklist

### WebSocket Stability
- [ ] Start pipeline, verify only 1 "WebSocket connected" message
- [ ] Let pipeline run, verify no disconnects during execution
- [ ] Switch between businesses, verify clean disconnect/reconnect
- [ ] Refresh page, verify reconnection works
- [ ] Monitor for 5+ minutes, verify connection stays stable

### Database Connection
- [ ] Check logs show "Creating new database connection"
- [ ] Verify all "using provided db_connection" (not "WARNING")
- [ ] Confirm no "This Connection is closed" errors
- [ ] Validate all 4 phases complete successfully
- [ ] Check "connection closed successfully" at end

### Pipeline Phases
- [ ] Cleaning completes and updates progress to 25%
- [ ] Transformation starts and completes (55%)
- [ ] Analysis starts and completes (85%)
- [ ] Machine learning starts and completes (100%)
- [ ] Frontend receives all updates via WebSocket

## Files Modified

1. **`frontend/src/context/PipelineProgressContext.jsx`**
   - Added connection state tracking (currentBusinessIdRef, isConnectingRef)
   - Prevent duplicate connections
   - Handle pong messages
   - Smart reconnection logic

2. **`frontend/src/components/global/InlinePipelineProgress.jsx`**
   - Fixed useEffect dependencies (only businessId)
   - Added previousBusinessIdRef to track changes
   - Prevent re-running effect on same business

3. **`api/services/pipeline_service.py`**
   - Enhanced logging in connection wrapper
   - Log connection creation/closure
   - Log which connection is used in _update_progress
   - Better error handling

## Summary

The fixes address both the WebSocket reconnection loops and provide better debugging for the database connection issue. The WebSocket will now maintain a stable connection throughout the pipeline execution, and the enhanced logging will make it immediately obvious if the background task is using the wrong database connection.

Key principles applied:
1. **Single Responsibility**: One WebSocket per business, not per component instance
2. **State Tracking**: Use refs to track connection state and prevent duplicates
3. **Dependency Management**: Only include primitive values in useEffect dependencies
4. **Defensive Programming**: Check state before creating connections
5. **Observable System**: Add logging at critical points for debugging
