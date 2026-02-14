# Pipeline UI and Error Handling Improvements - Complete Implementation

## Overview

This implementation addresses all requirements for improving the pipeline UI and error handling:
1. Inline display of pipeline progress (not in popup dialog)
2. Smart button display based on pipeline state
3. Phase-based error recovery with automatic resume

## Changes Summary

### Database Schema
**File:** `sql/schema.sql`
- Added `failed_phase VARCHAR(50) NULL` column to `pipeline_status` table
- Tracks which phase failed for smart recovery

### Backend - Pipeline Service
**File:** `api/services/pipeline_service.py`

1. **Enhanced `start_pipeline()` method**
   - Added `start_from_phase` parameter to support resuming from specific phase
   - Allows skipping already-completed phases

2. **Updated `_execute_pipeline()` method**
   - Calculates starting index based on `start_from_phase`
   - Skips completed phases when resuming
   - Correctly calculates cumulative progress for resumed pipelines
   - Improved exception handling to capture failed phase

3. **Modified `_update_progress()` method**
   - Added `failed_phase` parameter
   - Stores failed phase in database and broadcasts via WebSocket

4. **Updated `get_pipeline_status_info()` method**
   - Returns `failed_phase` in status response
   - Frontend can display which phase failed

### Backend - API Router
**File:** `api/routers/pipeline.py`

1. **Enhanced `/pipeline/retry` endpoint**
   - Fetches `failed_phase` from database
   - Passes it to `start_pipeline()` for smart resume
   - Returns resumed phase info in response

### Frontend - New Component
**File:** `frontend/src/components/global/InlinePipelineProgress.jsx`

Complete inline pipeline progress component with 4 states:

1. **No Pipeline State** (`hasNoPipeline`)
   - Shows "Start Analysis" button
   - Icon: Chart line
   - Message: "Ready to Analyze Your Data"
   - Action: Calls `handleStartAnalysis()`

2. **Running State** (`isRunning`)
   - Shows progress knob (0-100%)
   - Displays current phase
   - Shows phase checklist:
     - ✓/○ Cleaning Data (0-25%)
     - ✓/○ Transforming & Aggregating (25-55%)
     - ✓/○ Analyzing Data (55-85%)
     - ✓/○ Running ML Predictions (85-100%)
   - "Cancel Pipeline" button
   - Connection status indicator

3. **Completed State** (`isCompleted`)
   - Shows 100% knob in green
   - Success icon (check-circle)
   - Message: "Analysis Complete!"

4. **Failed State** (`isFailed`)
   - Shows progress knob in red at failure point
   - Error icon (exclamation-triangle)
   - Displays error message
   - Shows failed phase name
   - "Retry from {phase}" button

### Frontend - Dashboard Update
**File:** `frontend/src/pages/dashboard/index.jsx`

1. **Removed modal dialog**
   - Removed `PipelineProgressLoader` import and usage
   - No more popup dialogs

2. **Added inline display**
   - Imported `InlinePipelineProgress` component
   - Added conditional rendering:
     ```jsx
     {businessId ? (
         <InlinePipelineProgress 
             businessId={businessId}
             onStartAnalysis={handleStartAnalysis}
         />
     ) : (
         // Show "Add business" message
     )}
     ```

3. **Added manual start function**
   - `handleStartAnalysis()` calls `startPipeline(businessId)`
   - Integrated with `usePipelineProgress` hook

## User Flow Examples

### Scenario 1: New Analysis
1. User selects business from dropdown
2. Dashboard shows "Start Analysis" button (inline)
3. User clicks "Start Analysis"
4. Knob appears showing progress (0-100%)
5. Phases are checked off as they complete
6. Upon completion, success message shows

### Scenario 2: Failed Analysis
1. Pipeline fails at "analysis" phase (55-85%)
2. Dashboard shows knob at ~55% in red
3. Error message displays: "Pipeline failed during analysis phase"
4. Shows: "Failed at: **analysis** phase"
5. Button shows: "Retry from analysis"
6. User clicks retry
7. Pipeline resumes from analysis phase (not from start)

### Scenario 3: Cancellation
1. User cancels running pipeline
2. Status changes to "cancelled"
3. Dashboard shows "Start Analysis" button again
4. User can start fresh analysis

## Technical Details

### Phase Weight Distribution
- Cleaning: 25% (0-25%)
- Transformation: 30% (25-55%)
- Analysis: 30% (55-85%)
- Machine Learning: 15% (85-100%)

### Resume Logic
When pipeline fails at a phase:
1. `failed_phase` is stored in database
2. On retry, backend:
   - Finds the phase index in `PIPELINE_PHASES`
   - Calculates cumulative progress up to that phase
   - Starts execution from that phase onwards
   - Skips completed phases

Example:
```python
# If failed at "analysis" (index 2):
start_index = 2
cumulative_progress = 25 + 30  # Skip cleaning + transformation weights
# Execution starts from analysis phase
```

### WebSocket Updates
Real-time updates include:
- `status`: running/completed/failed/cancelled
- `current_step`: Phase description
- `progress`: Percentage (0-100)
- `error_message`: Error details
- `failed_phase`: Phase name where failure occurred

## Testing

### Manual Testing Steps

1. **Test Start Analysis**
   - Select a business with no pipeline
   - Verify "Start Analysis" button appears inline
   - Click button, verify pipeline starts
   - Check knob appears and updates

2. **Test Running Pipeline**
   - Wait for pipeline to run
   - Verify knob updates in real-time
   - Check phase checklist updates
   - Verify cancel button works

3. **Test Failed Pipeline**
   - Simulate failure (or wait for natural failure)
   - Verify error message displays
   - Check failed phase is shown
   - Verify "Retry from {phase}" button appears
   - Click retry, verify it resumes from correct phase

4. **Test Completion**
   - Let pipeline complete successfully
   - Verify 100% knob in green
   - Check success message displays

### Database Verification

Check `pipeline_status` table:
```sql
SELECT pipeline_id, status, failed_phase, progress_percentage, error_message
FROM pipeline_status
WHERE business_id = '{your_business_id}'
ORDER BY started_at DESC;
```

Expected columns:
- `failed_phase`: Should contain phase name when status = 'failed'
- `progress_percentage`: Should reflect progress at failure point
- `error_message`: Should contain error details

## Benefits

1. **Better UX**
   - No intrusive popups
   - Clear visual feedback inline
   - Obvious action buttons

2. **Efficient Recovery**
   - Doesn't re-run completed phases
   - Saves processing time
   - Preserves already-processed data

3. **Clear Error Information**
   - Users know exactly what failed
   - Retry button shows what will happen
   - Error messages are actionable

4. **Proper State Management**
   - Pipeline state persisted in database
   - WebSocket provides real-time updates
   - Frontend stays in sync with backend

## Files Modified

### Backend (3 files)
1. `sql/schema.sql` - Added failed_phase column
2. `api/services/pipeline_service.py` - Resume logic
3. `api/routers/pipeline.py` - Enhanced retry endpoint

### Frontend (2 files)
1. `frontend/src/components/global/InlinePipelineProgress.jsx` - New inline component
2. `frontend/src/pages/dashboard/index.jsx` - Dashboard integration

## Migration Notes

If database already exists, run this migration:
```sql
ALTER TABLE pipeline_status 
ADD COLUMN failed_phase VARCHAR(50) NULL;
```

## Future Enhancements

Potential improvements:
1. Show estimated time remaining for each phase
2. Add detailed phase logs in expandable section
3. Allow manual phase selection for retry
4. Add pause/resume functionality
5. Email notifications on completion/failure
