# Real-Time Analytics WebSocket - Quick Reference

## TL;DR

✅ **Backend Complete:** WebSocket server monitors MinIO for analytics file changes and broadcasts updates
🔄 **Frontend Ready:** Complete hook and components documented, ready to implement (2-3 hours)

## Backend API

### WebSocket Connection
```bash
ws://localhost:8000/analytics/ws/{business_id}
```

### Test Commands
```bash
# Test connection
wscat -c ws://localhost:8000/analytics/ws/business_123

# Manual trigger
curl -X POST http://localhost:8000/analytics/trigger-update/business_123

# Check status
curl http://localhost:8000/analytics/monitoring-status
```

## Frontend Implementation (Copy These)

### 1. Create Hook (30 min)
**File:** `frontend/src/hooks/useAnalyticsWebSocket.js`

See complete code in `REALTIME_ANALYTICS_WEBSOCKET_GUIDE.md` (100 lines)

### 2. Use in Dashboard (1 hour)
```javascript
import { useAnalyticsWebSocket } from '../hooks/useAnalyticsWebSocket';

const AnalyticsDashboard = () => {
  const { businessId } = useParams();
  const { updates, isConnected, lastUpdate } = useAnalyticsWebSocket(businessId);

  useEffect(() => {
    if (lastUpdate?.files) {
      toast.success(`${lastUpdate.total_files} charts updated`);
      refreshCharts(lastUpdate.files);
    }
  }, [lastUpdate]);

  return (
    <>
      <Badge value={isConnected ? 'Live' : 'Offline'} />
      {/* Your charts here - they'll auto-refresh */}
    </>
  );
};
```

### 3. Update Message Format
```json
{
  "event": "analytics_updated",
  "business_id": "business_123",
  "files": ["customer_acquisition_daily"],
  "categories": ["customer"],
  "changed_count": 1,
  "new_count": 0,
  "timestamp": "2026-02-17T19:00:00Z",
  "total_files": 1
}
```

## How It Works

```
Streaming writes parquet → MinIO analytics/
    ↓ (poll every 15s)
Watcher detects change
    ↓ (WebSocket broadcast)
Frontend receives update
    ↓ (fetch new data)
Charts auto-refresh ✨
```

## Configuration

**Backend polling interval:**
```python
# api/services/analytics_watcher_service.py
self.poll_interval = 15  # seconds
```

**Frontend WebSocket URL:**
```javascript
// Derived from: import.meta.env.VITE_API_URL
const WS_URL = 'ws://localhost:8000';
```

## Files

**Backend (Complete):**
- ✅ `api/services/analytics_watcher_service.py` (270 lines)
- ✅ `api/routers/analytics.py` (+150 lines)
- ✅ `api/main.py` (+50 lines)

**Frontend (To Create):**
- 🔄 `hooks/useAnalyticsWebSocket.js` (100 lines) - Code in guide
- 🔄 `pages/dashboard/analytics/AnalyticsDashboard.jsx` (modify)
- 🔄 CSS animations (40 lines) - Code in guide

## Quick Start

1. **Backend is running** (already deployed)

2. **Create frontend hook:**
   ```bash
   # Copy from REALTIME_ANALYTICS_WEBSOCKET_GUIDE.md
   # Section: "Step 1: Create WebSocket Hook"
   ```

3. **Add to dashboard:**
   ```bash
   # Copy from REALTIME_ANALYTICS_WEBSOCKET_GUIDE.md
   # Section: "Step 2: Update Analytics Dashboard"
   ```

4. **Test:**
   ```bash
   # Start streaming pipeline
   # Watch charts auto-update
   ```

## Troubleshooting

**WebSocket won't connect:**
- Check API URL is correct
- Verify backend is running
- Check CORS settings

**Charts don't refresh:**
- Check browser console for errors
- Verify hook is used in component
- Test with manual trigger endpoint

**Too frequent updates:**
- Increase `poll_interval` in watcher service
- Add debouncing in frontend

## Performance

- **Latency:** 15-30 seconds (polling interval)
- **Resource:** ~10KB memory per business
- **Scalability:** Auto cleanup when no connections

## Documentation

**Complete Guide:**
`REALTIME_ANALYTICS_WEBSOCKET_GUIDE.md` (20KB)
- Full code examples
- Step-by-step instructions
- Testing procedures
- Troubleshooting

**This File:**
Quick reference for fast implementation

## Status

✅ Backend: 100% Complete
✅ Documentation: 100% Complete
🔄 Frontend: 0% (2-3 hours remaining)

## Next Action

1. Open `REALTIME_ANALYTICS_WEBSOCKET_GUIDE.md`
2. Go to "Step 1: Create WebSocket Hook"
3. Copy code to `frontend/src/hooks/useAnalyticsWebSocket.js`
4. Continue with Steps 2-4
5. Test with streaming pipeline

**Estimated Time:** 2-3 hours

**Result:** Real-time charts that auto-update during streaming! 🚀
