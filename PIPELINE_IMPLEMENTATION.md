# Automated Data Processing Pipeline Implementation

## Overview

This implementation creates an automated data processing pipeline that integrates React frontend, FastAPI backend, PostgreSQL, and Python scripts to process data through four phases: Cleaning, Transformation, Analysis, and Machine Learning.

## Architecture

### Backend Components

1. **Pipeline Service** (`api/services/pipeline_service.py`)
   - Manages pipeline execution lifecycle
   - Executes scripts using subprocess with proper environment variables
   - Tracks progress in PostgreSQL
   - Manages process IDs in Redis
   - Handles MinIO cleanup on cancellation

2. **Pipeline Router** (`api/routers/pipeline.py`)
   - REST API endpoints for pipeline management
   - SSE streaming for real-time status updates
   - Endpoints:
     - `POST /pipeline/start` - Start pipeline
     - `GET /pipeline/status` - Get current status
     - `POST /pipeline/cancel` - Cancel running pipeline
     - `POST /pipeline/retry` - Retry failed pipeline
     - `GET /pipeline/status-stream` - SSE stream for real-time updates

3. **Database Schema** (`sql/schema.sql`)
   - New table: `pipeline_executions`
   - Tracks: pipeline_id, business_id, status, current_phase, progress_percentage, step_description, error_message, timestamps

### Frontend Components

1. **Pipeline Context** (`frontend/src/context/PipelineContext.jsx`)
   - Global state management for pipeline status
   - SSE connection handling
   - Auto-reconnect on connection loss
   - Functions: startPipeline, cancelPipeline, retryPipeline

2. **Pipeline Progress Component** (`frontend/src/components/PipelineProgress.jsx`)
   - PrimeReact Knob displaying progress (0-100%)
   - Shows current phase and step description
   - Cancel button for running pipelines
   - Error dialog with retry option
   - Success completion notification

### Integration Points

1. **Mapping Confirmation Trigger** (`api/routers/onboarding.py`)
   - Modified `confirm-mapping` endpoint
   - Automatically starts pipeline after user confirms mapping
   - Pipeline runs in background

2. **Script Modifications**
   - `cleaning/cleaning_config.py` - Added BUCKET_NAME env support
   - `transformation/config/minio_config.py` - Added BUCKET_NAME env support
   - `analysis/analysis_utils.py` - Added BUCKET_NAME env support
   - `analysis/analysis_export_config.py` - Added BUCKET_NAME env support
   - `machine-learning/infer_all.py` - Already supports --bucket-name arg

## Pipeline Flow

1. User completes mapping phase in onboarding
2. User confirms mapping results
3. Backend automatically starts pipeline execution:
   - Creates pipeline_executions record
   - Executes cleaning script with business_id as bucket name
   - Updates progress to 25% on completion
   - Executes transformation script
   - Updates progress to 50% on completion
   - Executes analysis script
   - Updates progress to 75% on completion
   - Executes ML inference script
   - Updates progress to 100% on completion
4. Frontend receives real-time updates via SSE
5. Progress knob updates automatically
6. On completion: Shows success message
7. On failure: Shows error dialog with retry option

## Error Handling

1. **Subprocess Failures**
   - Captures stdout/stderr
   - Updates database with error message
   - Sends failure status via SSE

2. **Pipeline Cancellation**
   - Terminates running processes (SIGTERM then SIGKILL)
   - Cleans up MinIO folders: cleaned, transformed, analytics, machine-learning
   - Updates database status to 'cancelled'

3. **Network Issues**
   - Frontend auto-reconnects SSE after 5 seconds
   - Polls database for status if SSE unavailable

## Progress Tracking

Each pipeline phase contributes to overall progress:
- Cleaning: 0-25%
- Transformation: 25-50%
- Analysis: 50-75%
- Machine Learning: 75-100%

Progress updates happen:
- At phase start
- At phase completion
- On errors
- On cancellation

## UI/UX Features

1. **Global Progress Indicator**
   - Fixed position (top-right)
   - Visible on all dashboard pages
   - Shows current phase and description
   - Cancel button always available

2. **Completion State**
   - 100% knob with green color
   - Success message
   - Dismiss button

3. **Failure State**
   - Error dialog with details
   - Retry button
   - Close button

4. **Real-time Updates**
   - SSE connection for instant updates
   - Smooth progress transitions
   - Phase descriptions update live

## Configuration

### Environment Variables Required

For all pipeline scripts:
- `BUCKET_NAME` - Business ID used as MinIO bucket name
- `MINIO_ENDPOINT` - MinIO server endpoint
- `MINIO_ACCESS_KEY` - MinIO access key
- `MINIO_SECRET_KEY` - MinIO secret key

### Database Migration

Run the updated schema.sql to create the pipeline_executions table:
```sql
CREATE TABLE IF NOT EXISTS pipeline_executions (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'running',
    current_phase VARCHAR(50),
    progress_percentage INTEGER DEFAULT 0,
    step_description TEXT,
    error_message TEXT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

## Testing Checklist

- [ ] Test pipeline starts after mapping confirmation
- [ ] Test progress updates in real-time
- [ ] Test each phase completes successfully
- [ ] Test error handling when script fails
- [ ] Test cancellation cleans up resources
- [ ] Test retry after failure
- [ ] Test SSE reconnection
- [ ] Test UI displays on all dashboard pages
- [ ] Test completion message
- [ ] Test error dialog

## Dependencies

Already included in packages.txt:
- aioredis - For Redis async operations
- fastapi - Web framework
- boto3 - MinIO/S3 operations
- sqlalchemy - Database operations
- asyncio - Async subprocess management

## File Changes Summary

### New Files
- `api/services/pipeline_service.py` - Pipeline orchestration
- `api/routers/pipeline.py` - Pipeline API endpoints
- `frontend/src/context/PipelineContext.jsx` - Pipeline state management
- `frontend/src/components/PipelineProgress.jsx` - Progress UI component

### Modified Files
- `sql/schema.sql` - Added pipeline_executions table
- `api/main.py` - Register pipeline router
- `api/routers/onboarding.py` - Trigger pipeline on mapping confirmation
- `frontend/src/main.jsx` - Add PipelineProvider
- `frontend/src/pages/dashboard/index.jsx` - Add PipelineProgress component
- `cleaning/cleaning_config.py` - Support BUCKET_NAME env
- `transformation/config/minio_config.py` - Support BUCKET_NAME env
- `analysis/analysis_utils.py` - Support BUCKET_NAME env
- `analysis/analysis_export_config.py` - Support BUCKET_NAME env

## Notes

- Pipeline runs in background using asyncio.create_task
- Database sessions are managed properly for background tasks
- All subprocess output is captured but not stored (only errors)
- Pipeline is resilient to restarts (can resume from failure)
- MinIO cleanup is thorough on cancellation
- SSE is used instead of WebSockets for simplicity
