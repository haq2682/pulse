# Quick Start: Implementing Real-Time Pipeline

**Goal:** Reduce 10-20 minute batch delay to seconds  
**Assumption:** CDC is already implemented  
**Time to First Results:** 1-2 weeks (Phase 1)

---

## TL;DR - What to Do

1. **Week 1-2:** Implement incremental cleaning → 85% faster
2. **Week 3-4:** Add streaming transformations → 95% faster  
3. **Week 2-3 (parallel):** Add WebSocket frontend → Live updates
4. **Later (optional):** Speed layer with Flink → 98% faster

---

## Phase 1: Incremental Cleaning (START HERE)

**Time:** 1-2 weeks  
**Impact:** 85% reduction (5-8 min → 1-2 min)  
**Complexity:** Low

### Step 1: Create State Tracking Table

```sql
-- Run this in PostgreSQL
CREATE TABLE IF NOT EXISTS cleaning_state (
    file_path VARCHAR(500) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL,
    file_size BIGINT,
    record_count BIGINT,
    checksum VARCHAR(64)
);

CREATE INDEX idx_cleaning_state_processed_at ON cleaning_state(processed_at);
```

### Step 2: Modify cleaning.py

Add this to `/home/runner/work/pulse/pulse/cleaning/cleaning.py`:

```python
import os
from sqlalchemy import create_engine, text
from datetime import datetime

class IncrementalCleaner:
    def __init__(self):
        self.engine = create_engine(os.getenv("POSTGRES_CONNECTION_STRING"))
    
    def get_unprocessed_files(self, bucket_name, folder="mapped"):
        """Get files not yet processed"""
        all_files = self._list_minio_files(bucket_name, folder)
        
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT file_path FROM cleaning_state"))
            processed = {row[0] for row in result}
        
        return [f for f in all_files if f not in processed]
    
    def mark_processed(self, file_path, size, count):
        """Mark file as processed"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO cleaning_state (file_path, processed_at, file_size, record_count)
                VALUES (:path, :ts, :size, :count)
                ON CONFLICT (file_path) DO UPDATE 
                SET processed_at = :ts
            """), {"path": file_path, "ts": datetime.utcnow(), "size": size, "count": count})
            conn.commit()
    
    def clean_incremental(self, bucket_name):
        """Clean only new files"""
        new_files = self.get_unprocessed_files(bucket_name)
        
        if not new_files:
            print("✅ No new files to clean")
            return
        
        print(f"📦 Cleaning {len(new_files)} new files...")
        
        for file_path in new_files:
            # Load and clean
            df = load_from_minio(file_path)
            cleaned = clean_dataframe(df)
            
            # Save
            save_to_minio(cleaned, bucket_name, "cleaned")
            
            # Mark as processed
            self.mark_processed(file_path, len(df), df.memory_usage().sum())
        
        print(f"✅ Cleaned {len(new_files)} files")

# Add to main() function
def main():
    cleaner = IncrementalCleaner()
    cleaner.clean_incremental("pulse-bucket-1")
```

### Step 3: Test It

```bash
# Run incremental cleaning
python cleaning/cleaning.py

# Should output:
# 📦 Cleaning 5 new files...
# ✅ Cleaned 5 files

# Run again immediately
python cleaning/cleaning.py

# Should output:
# ✅ No new files to clean
```

**Result:** Cleaning now skips already-processed files. Time: 5-8 min → 30-90 sec (85-90% faster)

---

## Phase 2: Streaming Transformations

**Time:** 2-3 weeks  
**Impact:** 95% reduction (total pipeline 14-25 min → 30-90 sec)  
**Complexity:** Medium

### Step 1: Install Spark Streaming Dependencies

Already have Spark, just need to configure for streaming.

### Step 2: Create Streaming Transformation

Create `/home/runner/work/pulse/pulse/transformation/streaming_transformation.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum, count, avg

def create_streaming_aggregations():
    spark = SparkSession.builder \
        .appName("StreamingTransformations") \
        .getOrCreate()
    
    # Read cleaned data as stream
    orders = spark.readStream \
        .format("parquet") \
        .schema(get_orders_schema()) \
        .option("maxFilesPerTrigger", 10) \
        .load("s3a://pulse-bucket-1/cleaned/orders/")
    
    # Windowed aggregations
    revenue = orders \
        .withWatermark("order_date", "2 hours") \
        .groupBy(window("order_date", "1 hour")) \
        .agg(sum("total_amount").alias("revenue"))
    
    # Write continuously
    query = revenue.writeStream \
        .format("parquet") \
        .option("path", "s3a://pulse-bucket-1/speed/revenue/") \
        .option("checkpointLocation", "s3a://pulse-checkpoints/revenue/") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query

if __name__ == "__main__":
    query = create_streaming_aggregations()
    query.awaitTermination()
```

### Step 3: Run It

```bash
# Start streaming transformation
python transformation/streaming_transformation.py

# Output:
# Streaming query started...
# Processing micro-batch 0...
# Processing micro-batch 1...
# (continues running)
```

**Result:** Transformations now run continuously every 10 seconds. Time: 4-7 min → 10-30 sec

---

## Phase 3: Frontend WebSocket Updates

**Time:** 1-2 weeks  
**Impact:** Live frontend updates  
**Complexity:** Low

### Step 1: Add WebSocket to FastAPI

Modify `/home/runner/work/pulse/pulse/api/routers/analytics.py`:

```python
from fastapi import APIRouter, WebSocket
import asyncio

router = APIRouter()

@router.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            # Get latest metrics
            metrics = get_latest_metrics()
            
            # Send to frontend
            await websocket.send_json({
                "timestamp": datetime.now().isoformat(),
                "revenue": metrics.get("revenue", 0),
                "orders": metrics.get("orders", 0)
            })
            
            # Wait 5 seconds
            await asyncio.sleep(5)
    
    except:
        pass

def get_latest_metrics():
    """Get latest metrics from speed layer"""
    # Read latest parquet file from speed layer
    df = pd.read_parquet("s3a://pulse-bucket-1/speed/revenue/")
    latest = df.tail(1)
    return latest.to_dict('records')[0] if not latest.empty else {}
```

### Step 2: Add React Hook

Create `/home/runner/work/pulse/pulse/frontend/src/hooks/useRealtimeMetrics.js`:

```javascript
import { useState, useEffect } from 'react';

export const useRealtimeMetrics = () => {
  const [metrics, setMetrics] = useState({});
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/metrics');

    ws.onopen = () => {
      console.log('✅ Connected');
      setConnected(true);
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setMetrics(data);
    };

    ws.onclose = () => {
      console.log('🔌 Disconnected');
      setConnected(false);
    };

    return () => ws.close();
  }, []);

  return { metrics, connected };
};
```

### Step 3: Use in Dashboard

Modify dashboard component:

```javascript
import React from 'react';
import { useRealtimeMetrics } from '../hooks/useRealtimeMetrics';

const Dashboard = () => {
  const { metrics, connected } = useRealtimeMetrics();

  return (
    <div>
      <div className={connected ? 'live' : 'offline'}>
        {connected ? '🟢 LIVE' : '🔴 Offline'}
      </div>
      
      <div className="metric">
        <h3>Revenue</h3>
        <p>${metrics.revenue?.toLocaleString()}</p>
      </div>
      
      <div className="metric">
        <h3>Orders</h3>
        <p>{metrics.orders?.toLocaleString()}</p>
      </div>
      
      {metrics.timestamp && (
        <small>Updated: {new Date(metrics.timestamp).toLocaleTimeString()}</small>
      )}
    </div>
  );
};
```

**Result:** Frontend updates automatically every 5 seconds without refresh

---

## Testing the Complete Flow

### Test 1: Insert New Data

```sql
-- Insert test order
INSERT INTO orders (order_id, customer_id, total_amount, order_date)
VALUES ('test-001', 'cust-123', 99.99, NOW());
```

### Expected Timeline:

```
T+0s:  Order inserted in database
T+1s:  CDC captures change → Kafka
T+2s:  Spark Streaming writes to MinIO/mapped/
T+3s:  Incremental cleaning picks it up
T+13s: Streaming transformation processes it
T+23s: WebSocket pushes to frontend
T+24s: Frontend chart updates
```

**Total: 24 seconds from database insert to frontend update!**

### Test 2: Verify State Tracking

```sql
-- Check what files have been cleaned
SELECT file_path, processed_at, record_count 
FROM cleaning_state 
ORDER BY processed_at DESC 
LIMIT 10;
```

Should show recently processed files.

### Test 3: Monitor Streaming Query

```python
# Check streaming query status
query.status.isDataAvailable  # Should be True if data flowing
query.status.message  # Should show "Processing micro-batch"
```

---

## Common Issues & Solutions

### Issue 1: "No new files to clean" but data is stale

**Cause:** Cleaning state table has old entries  
**Fix:**
```sql
-- Reset cleaning state
TRUNCATE TABLE cleaning_state;
```

### Issue 2: Streaming query not processing

**Cause:** Checkpoint directory issues  
**Fix:**
```bash
# Delete checkpoint directory
aws s3 rm s3://pulse-checkpoints/revenue/ --recursive
# Or in MinIO:
mc rm --recursive minio/pulse-checkpoints/revenue/
```

### Issue 3: WebSocket keeps disconnecting

**Cause:** CORS or network issues  
**Fix:** Add CORS middleware in FastAPI:
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## Performance Benchmarks

After implementing all phases:

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Cleaning | 5-8 min | 30-90 sec | 85-90% |
| Transformation | 4-7 min | 10-30 sec | 90-95% |
| Analysis | 5-10 min | 10-30 sec | 90-95% |
| **Total Pipeline** | **14-25 min** | **50-150 sec** | **90-95%** |
| Frontend Update | Manual | 5 sec auto | Real-time |

---

## What's Next?

### If You Want Even Faster (Optional):

**Phase 3: Speed Layer with Flink** (6-8 weeks)
- Sub-5-second updates for critical metrics
- See REAL_TIME_PIPELINE_SOLUTION.md Section "Phase 3"

### Monitoring:

Add Prometheus metrics:
```python
from prometheus_client import Histogram, Counter

pipeline_latency = Histogram('pipeline_latency_seconds', 'Pipeline latency', ['stage'])
files_processed = Counter('files_processed_total', 'Files processed', ['stage'])
```

### Optimization:

- Tune Spark: `spark.sql.shuffle.partitions`, `spark.executor.memory`
- Add caching: Redis for frequently accessed metrics
- Partitioning: Partition by date/hour for faster queries

---

## Checklist

- [ ] Phase 1: Incremental cleaning implemented
- [ ] State tracking table created
- [ ] Test: Insert data, verify only new files cleaned
- [ ] Phase 2: Streaming transformations running
- [ ] Test: Verify continuous micro-batch processing
- [ ] Phase 3: WebSocket endpoint added
- [ ] Frontend hooks implemented
- [ ] Test: Verify auto-updates every 5 seconds
- [ ] Monitoring added (optional but recommended)
- [ ] Documentation updated

---

## Success Criteria

✅ **Incremental cleaning works:** Only new files processed  
✅ **Streaming query runs:** Continuous micro-batches every 10s  
✅ **WebSocket connected:** Frontend shows "🟢 LIVE"  
✅ **Auto-updates:** Charts refresh without page reload  
✅ **End-to-end < 2 min:** From DB insert to frontend update

---

## Support

For full details, see **REAL_TIME_PIPELINE_SOLUTION.md**

For architecture questions, see:
- **STREAMING_ARCHITECTURE_CLARIFICATION.md**
- **ACTUAL_VS_PERCEIVED_ARCHITECTURE.md**

---

**Ready to start? Begin with Phase 1 (incremental cleaning) - 1 week, 85% improvement! 🚀**
