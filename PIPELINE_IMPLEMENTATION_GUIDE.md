# Data Processing Pipeline - Implementation Guide

## Overview

This document describes the automated data processing pipeline that integrates the React frontend, FastAPI backend, PostgreSQL database, and Python data processing scripts. The pipeline automatically executes after the onboarding mapping phase completes.

## Architecture

### Components

1. **Backend Services**
   - `api/services/pipeline_service.py` - Orchestrates pipeline execution
   - `api/services/websocket_manager.py` - Manages WebSocket connections
   - `api/routers/pipeline.py` - Pipeline API endpoints

2. **Database**
   - `sql/schema.sql` - Pipeline status tracking table

3. **Frontend**
   - `frontend/src/context/PipelineProgressContext.jsx` - Global state management
   - `frontend/src/components/global/PipelineProgressLoader.jsx` - Progress UI component

4. **Data Processing Scripts**
   - `cleaning/cleaning.py` - Data cleaning phase
   - `transformation/transformation.py` - Data transformation phase
   - `analysis/analysis.py` - Data analysis phase
   - `machine-learning/infer_all.py` - ML inference phase

## Pipeline Flow

```
User Completes Mapping
       ↓
/onboarding/confirm-mapping (triggered)
       ↓
PipelineService.start_pipeline()
       ↓
Execute Phases Sequentially:
1. Cleaning (0-25%)
2. Transformation (25-55%)
3. Analysis (55-85%)
4. ML Inference (85-100%)
       ↓
Update PostgreSQL + WebSocket Broadcast
       ↓
Dashboard displays progress via Knob
       ↓
Pipeline Complete / Failed
```

## Database Schema

### pipeline_status Table

```sql
CREATE TABLE pipeline_status (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    current_step VARCHAR(100),
    progress_percentage INTEGER DEFAULT 0 CHECK (progress_percentage >= 0 AND progress_percentage <= 100),
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    process_ids JSONB NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
);
```

## API Endpoints

### POST /pipeline/start
Start a new pipeline execution.

**Request:**
```json
{
  "userId": "user_id_here",
  "businessId": "business_id_here"
}
```

**Response:**
```json
{
  "status": 200,
  "message": "Pipeline started successfully",
  "pipeline_id": "uuid-here"
}
```

### GET /pipeline/status
Get current pipeline status for a business.

**Query Parameters:**
- `business_id` - Business ID

**Response:**
```json
{
  "status": 200,
  "pipeline_status": "running",
  "data": {
    "pipeline_id": "uuid-here",
    "status": "running",
    "current_step": "Cleaning Data",
    "progress": 15,
    "started_at": "2024-01-01T00:00:00",
    "completed_at": null,
    "error_message": null
  }
}
```

### POST /pipeline/cancel
Cancel a running pipeline.

**Request:**
```json
{
  "pipelineId": "uuid-here",
  "businessId": "business_id_here",
  "cleanupData": true
}
```

**Response:**
```json
{
  "status": 200,
  "message": "Pipeline cancelled successfully"
}
```

### POST /pipeline/retry
Retry a failed pipeline.

**Request:**
```json
{
  "userId": "user_id_here",
  "businessId": "business_id_here"
}
```

**Response:**
```json
{
  "status": 200,
  "message": "Pipeline retry started successfully",
  "pipeline_id": "new-uuid-here"
}
```

### WebSocket /pipeline/ws/{business_id}
Real-time pipeline progress updates.

**Connection:**
```javascript
const ws = new WebSocket('ws://localhost:8000/pipeline/ws/{business_id}');
```

**Message Format:**
```json
{
  "pipeline_id": "uuid-here",
  "status": "running",
  "current_step": "Transforming & Aggregating Data",
  "progress": 45,
  "error_message": null
}
```

## Script Modifications

All data processing scripts now accept a `--bucket-name` argument:

### Cleaning Script
```bash
python cleaning/cleaning.py --bucket-name {business_id}
```

### Transformation Script
```bash
python transformation/transformation.py --bucket-name {business_id}
```

### Analysis Script
```bash
python analysis/analysis.py --bucket-name {business_id}
```

### ML Inference Script
```bash
python machine-learning/infer_all.py --bucket-name {business_id}
```

## Frontend Usage

### Using PipelineProgressContext

```jsx
import { usePipelineProgress } from '@/context/PipelineProgressContext';

function MyComponent() {
  const {
    pipelineStatus,
    isConnected,
    connectWebSocket,
    fetchPipelineStatus,
    startPipeline,
    cancelPipeline,
    retryPipeline
  } = usePipelineProgress();
  
  useEffect(() => {
    if (businessId) {
      fetchPipelineStatus(businessId);
      connectWebSocket(businessId);
    }
  }, [businessId]);
  
  return (
    <div>
      {pipelineStatus && (
        <div>
          Status: {pipelineStatus.status}
          Progress: {pipelineStatus.progress}%
        </div>
      )}
    </div>
  );
}
```

### PipelineProgressLoader Component

The `PipelineProgressLoader` component automatically displays a modal with:
- **Knob Progress Indicator** - Shows 0-100% progress
- **Current Step Description** - Shows which phase is running
- **Phase Breakdown** - Shows all 4 phases with checkmarks
- **Cancel Button** - Allows user to cancel pipeline
- **Retry Button** - Shows on failure with retry option
- **Success Message** - Shows on completion

```jsx
import PipelineProgressLoader from '@/components/global/PipelineProgressLoader';

function Dashboard() {
  return (
    <div>
      <PipelineProgressLoader 
        businessId={businessId} 
        visible={!!businessId}
      />
      {/* Rest of dashboard */}
    </div>
  );
}
```

## Progress Tracking

### Phase Weights

Each phase has a progress weight that determines its percentage contribution:

| Phase | Weight | Progress Range |
|-------|--------|----------------|
| Cleaning | 25% | 0-25% |
| Transformation | 30% | 25-55% |
| Analysis | 30% | 55-85% |
| ML Inference | 15% | 85-100% |

### Status Flow

```
pending → running → completed
                 → failed
                 → cancelled
```

## Error Handling

### Subprocess Failures

When a subprocess fails:
1. Pipeline status updates to `failed`
2. Error message captured from stderr
3. WebSocket broadcasts failure
4. Frontend displays error with retry button

### Cancellation

When user cancels:
1. Pipeline status updates to `cancelled`
2. Running processes terminated (if possible)
3. MinIO data cleaned up:
   - `{business_id}/cleaned/`
   - `{business_id}/transformed/`
   - `{business_id}/analytics/`
   - `{business_id}/ml-predictions/`
4. WebSocket broadcasts cancellation

## MinIO Data Structure

```
{business_id}/               # UUID bucket (one per business)
├── ingested/                # Raw uploaded files
├── mapped/                  # Schema-mapped data (15 tables)
├── cleaned/                 # Cleaned data (15 tables)
├── transformed/             # Aggregations (29 tables)
├── analytics/               # Analytics results
└── ml-predictions/          # ML model outputs
```

## Logging

### Backend Logs

All pipeline operations log to console:
- Pipeline start/stop events
- Phase execution start/end
- Real-time subprocess output (prefixed with phase name)
- Error messages and stack traces
- WebSocket connection events

### Frontend Logs

Console logs include:
- WebSocket connection status
- Pipeline status updates received
- API call results
- Error messages

## Environment Variables

### Backend
```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

### Frontend
```env
VITE_API_URL=http://localhost:8000
```

## Deployment Considerations

### Database Migration

Run the SQL schema updates:
```bash
psql -U postgres -d pulse < sql/schema.sql
```

### Dependencies

No new Python dependencies required. WebSocket support is built into FastAPI.

### WebSocket Configuration

Ensure your deployment supports WebSocket connections:
- NGINX: Configure WebSocket proxy
- Load Balancer: Enable WebSocket support
- Firewall: Allow WebSocket traffic

## Monitoring

### Health Checks

Monitor pipeline health:
```sql
-- Active pipelines
SELECT COUNT(*) FROM pipeline_status WHERE status = 'running';

-- Recent failures
SELECT * FROM pipeline_status 
WHERE status = 'failed' 
ORDER BY started_at DESC 
LIMIT 10;

-- Average pipeline duration
SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) / 60 as avg_minutes
FROM pipeline_status 
WHERE status = 'completed';
```

### WebSocket Monitoring

Check WebSocket connections:
```python
# In pipeline service
print(f"Active WebSocket connections: {websocket_manager.get_connection_count(business_id)}")
```

## Troubleshooting

### Pipeline Won't Start

1. Check if another pipeline is running for the same business
2. Verify mapping status is 'completed'
3. Check logs for errors in PipelineService

### WebSocket Not Connecting

1. Verify VITE_API_URL is correct
2. Check browser console for WebSocket errors
3. Ensure WebSocket endpoint is accessible
4. Check for CORS issues

### Progress Not Updating

1. Check if WebSocket is connected
2. Verify database updates are happening
3. Check pipeline service logs
4. Ensure business_id matches

### Scripts Failing

1. Check if bucket exists in MinIO
2. Verify script can be executed with --bucket-name
3. Check for missing data in previous phases
4. Review subprocess stderr output

## Testing

### Manual Testing Flow

1. **Complete Onboarding**
   - Create business
   - Upload/connect data
   - Complete mapping

2. **Confirm Mapping**
   - Click "Continue to Dashboard"
   - Pipeline should auto-start

3. **Monitor Progress**
   - Dashboard should show progress modal
   - Knob should update in real-time
   - Phase descriptions should change

4. **Test Cancellation**
   - Click "Cancel Pipeline"
   - Confirm cancellation
   - Verify data cleanup

5. **Test Retry**
   - Cancel or wait for failure
   - Click "Retry"
   - Pipeline should restart

### API Testing

```bash
# Start pipeline
curl -X POST http://localhost:8000/pipeline/start \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-id", "businessId": "business-id"}'

# Check status
curl http://localhost:8000/pipeline/status?business_id=business-id

# Cancel pipeline
curl -X POST http://localhost:8000/pipeline/cancel \
  -H "Content-Type: application/json" \
  -d '{"pipelineId": "pipeline-id", "businessId": "business-id"}'
```

## Future Enhancements

1. **Email Notifications** - Notify users when pipeline completes
2. **Pause/Resume** - Allow pausing and resuming pipeline
3. **Parallel Execution** - Run multiple phases in parallel where possible
4. **Detailed Logs** - Store subprocess logs in database
5. **Pipeline Templates** - Allow custom pipeline configurations
6. **Cost Estimation** - Show estimated time and resources
7. **Historical Analytics** - Track pipeline performance over time
8. **Smart Retry** - Only retry failed phases, not entire pipeline

## Support

For issues or questions:
1. Check backend logs for detailed error messages
2. Verify database schema is up to date
3. Ensure all scripts accept --bucket-name argument
4. Check WebSocket connectivity
5. Review MinIO bucket permissions

---

**Last Updated:** February 2026
**Version:** 1.0.0
