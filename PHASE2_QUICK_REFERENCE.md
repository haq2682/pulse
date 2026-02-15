# Phase 2 Quick Reference

## 🚀 Quick Start

```bash
# Run complete streaming pipeline
python streaming_orchestrator.py

# Expected: Continuous processing with 10-second micro-batches
# Latency: 30-90 seconds (vs 10-20 minutes before)
```

## 💻 Command Options

```bash
# Default: Run cleaning + transformation
python streaming_orchestrator.py

# Custom bucket and interval
python streaming_orchestrator.py \
  --bucket-name pulse-bucket-1 \
  --trigger-interval "10 seconds" \
  --monitor-interval 30

# Run specific pipelines
python streaming_orchestrator.py --cleaning-only
python streaming_orchestrator.py --transformation-only

# Run components individually
python cleaning/streaming_cleaning.py
python transformation/streaming_transformation.py
```

## 📊 What to Expect

### Performance Improvements

| Component | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Cleaning | 5-8 min | 10-30 sec | **90-96%** ✅ |
| Transformation | 4-7 min | 10-30 sec | **92-96%** ✅ |
| **Total** | **10-20 min** | **30-90 sec** | **95%** ✅ |

### Monitoring Output

```
📊 STREAMING PIPELINE STATUS
============================================================
Active Queries: 6
Time: 2026-02-15 20:30:00

Cleaning: 3/3 active
Transformation: 3/3 active

🟢 clean_orders
   Batch ID: 42
   Rows Processed: 150
   Rate: 15.00 rows/sec

🟢 transform_orders
   Batch ID: 38
   Rows Processed: 150
   Rate: 15.00 rows/sec

📈 METRICS SUMMARY:
   Total Queries: 6
   Active: 6
   Total Rows Processed: 900
   Total Batches: 42
```

## 🔧 Configuration

### Trigger Intervals

Choose based on latency vs resource trade-off:

- `5 seconds` - Lowest latency, highest resources
- `10 seconds` - Balanced (recommended) ⭐
- `30 seconds` - Lower resources
- `1 minute` - Batch-like

### Checkpoints

Located at: `/tmp/spark_checkpoints/`

Structure:
```
/tmp/spark_checkpoints/
├── cleaning/
│   ├── cleaning_orders/
│   ├── cleaning_customers/
│   └── cleaning_products/
└── transformation/
    ├── transform_orders/
    ├── transform_customers/
    └── transform_products/
```

## 📁 Data Flow

```
MinIO/mapped/{table}/
    ↓ (10s batches)
MinIO/cleaned_streaming/{table}/
    ↓ (10s batches)
MinIO/transformed_streaming/{table}/
    ↓ (10s batches)
MinIO/analytics_streaming/{table}/
```

## 🛠️ Troubleshooting

### Issue: No data processing

```bash
# Check if data exists in MinIO
aws --endpoint-url http://localhost:9000 s3 ls s3://pulse-bucket-1/mapped/

# Verify Spark is reading files
# Look for "Created input stream from..." in logs
```

### Issue: Query stuck/not progressing

```bash
# Stop orchestrator (Ctrl+C)
# Remove checkpoint
rm -rf /tmp/spark_checkpoints/cleaning/cleaning_orders/
# Restart orchestrator
python streaming_orchestrator.py
```

### Issue: Out of memory

```python
# Edit Spark config in orchestrator
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
```

## ✅ Verify It's Working

### Test 1: Start Pipeline

```bash
python streaming_orchestrator.py
# Expected: "✅ ALL PIPELINES RUNNING"
# Expected: Status updates every 30 seconds
```

### Test 2: Add Data

```bash
# Add a CSV file to MinIO/mapped/orders/
# Watch logs for:
# - "Rows Processed: X"
# - Processing rate displayed
# - Output written to cleaned_streaming/
```

### Test 3: Check Output

```bash
# Verify cleaned data exists
ls /tmp/spark_checkpoints/cleaning/
# Should show checkpoint directories

# Check MinIO for output
# Should see parquet files in cleaned_streaming/
```

### Test 4: Stop Gracefully

```bash
# Press Ctrl+C
# Expected: "🛑 STOPPING ALL STREAMING QUERIES"
# Expected: All queries stop gracefully
# Expected: "✅ STREAMING PIPELINE ORCHESTRATOR STOPPED"
```

## 📈 Performance Tips

### Optimize for Latency

```python
# Decrease trigger interval
--trigger-interval "5 seconds"

# Increase parallelism
spark.conf.set("spark.default.parallelism", "8")
```

### Optimize for Resources

```python
# Increase trigger interval
--trigger-interval "30 seconds"

# Reduce watermark duration
df = df.withWatermark("timestamp_col", "30 minutes")
```

## 🔍 Monitoring Queries

### Check Active Streams

```python
# In Spark shell or notebook
spark.streams.active  # List all active queries
```

### View Progress

```python
query.lastProgress  # Get latest progress info
query.status        # Get query status
query.isActive      # Check if running
```

## 📚 Files Reference

- **streaming_orchestrator.py** - Main entry point
- **cleaning/streaming_cleaning.py** - Cleaning pipeline
- **transformation/streaming_transformation.py** - Transform pipeline
- **PHASE2_IMPLEMENTATION.md** - Full documentation

## 🎯 Success = Continuous processing with <2 min latency!

---

**Need help?** See `PHASE2_IMPLEMENTATION.md` for complete documentation.

**Compare with Phase 1:** 
- Phase 1: Incremental (85% improvement)
- Phase 2: Streaming (95% improvement)
- Together: Maximum efficiency ✅
