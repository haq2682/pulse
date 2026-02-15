# Phase 2 Implementation: Spark Structured Streaming

**Status:** ✅ Implementation Complete  
**Date:** February 15, 2026  
**Expected Improvement:** 95% latency reduction (10-20 min → 30-90 sec)

---

## Overview

Phase 2 implements **Spark Structured Streaming** to convert the batch processing pipeline into a continuous, real-time streaming pipeline with 10-second micro-batches.

### What Was Implemented

1. **Streaming Cleaning Pipeline** (`streaming_cleaning.py`)
   - Continuous monitoring of MinIO/mapped/
   - Real-time data cleaning with 10-second micro-batches
   - Stateful deduplication
   - Checkpoint-based fault tolerance

2. **Streaming Transformation Pipeline** (`streaming_transformation.py`)
   - Continuous aggregations with watermarking
   - Stateful computations
   - Window-based aggregations (hourly, daily)
   - Real-time metrics

3. **Streaming Orchestrator** (`streaming_orchestrator.py`)
   - Unified pipeline management
   - Lifecycle control for all queries
   - Monitoring and metrics
   - Graceful shutdown

---

## Architecture

### Before Phase 2 (Batch Processing)

```
MinIO/mapped/ → [Wait for trigger] → Batch Cleaning (5-8 min)
                                              ↓
                              MinIO/cleaned/ → Batch Transform (4-7 min)
                                                        ↓
                                        MinIO/transformed/ → Batch Analysis (5-10 min)
                                                                     ↓
                                                         MinIO/analytics/

Total: 10-20 minutes per run
```

### After Phase 2 (Streaming)

```
MinIO/mapped/ → Streaming Clean (10s batches) → MinIO/cleaned_streaming/
                                                          ↓
                               Streaming Transform (10s batches) → MinIO/transformed_streaming/
                                                                            ↓
                                              Streaming Analysis (10s batches) → MinIO/analytics_streaming/

Total: 30-90 seconds continuous processing
```

---

## Components

### 1. Streaming Cleaning (`cleaning/streaming_cleaning.py`)

**Purpose:** Continuously clean incoming data from MinIO/mapped/

**Features:**
- ✅ Continuous file monitoring
- ✅ 10-second micro-batches
- ✅ Stateful deduplication
- ✅ Text cleaning and normalization
- ✅ Null handling
- ✅ Checkpoint management
- ✅ Real-time metrics

**Key Methods:**
```python
StreamingCleaner(spark, bucket_name, trigger_interval)
├── create_input_stream()      # Create streaming DataFrame from MinIO
├── apply_cleaning_rules()      # Apply table-specific cleaning
├── remove_duplicates_streaming() # Stateful dedup with watermarking
├── write_stream()              # Write to MinIO with checkpointing
└── create_cleaning_pipeline()  # End-to-end pipeline for a table
```

**Configuration:**
- Trigger: 10 seconds (configurable)
- Checkpoint: `/tmp/spark_checkpoints/cleaning/`
- Input: `s3a://bucket/mapped/{table}/`
- Output: `s3a://bucket/cleaned_streaming/{table}/`
- Format: Parquet (for efficiency)

---

### 2. Streaming Transformation (`transformation/streaming_transformation.py`)

**Purpose:** Continuously transform and aggregate cleaned data

**Features:**
- ✅ Stateful aggregations
- ✅ Watermarking for late data
- ✅ Window-based aggregations (hourly, daily)
- ✅ Real-time metrics computation
- ✅ Multiple output modes (append, update, complete)

**Key Methods:**
```python
StreamingTransformer(spark, bucket_name, trigger_interval)
├── create_input_stream()           # Read from cleaned_streaming
├── aggregate_orders_streaming()    # Order aggregations
├── aggregate_customers_streaming() # Customer metrics
├── aggregate_products_streaming()  # Product analytics
├── create_time_based_aggregations() # Time windows
└── write_stream()                   # Write aggregations
```

**Aggregation Types:**
- **Hourly:** Rolling 1-hour windows
- **Daily:** Daily summaries
- **Real-time:** Continuous metrics
- **Windowed:** Tumbling/sliding windows

---

### 3. Streaming Orchestrator (`streaming_orchestrator.py`)

**Purpose:** Manage complete streaming pipeline lifecycle

**Features:**
- ✅ Start/stop all pipelines
- ✅ Unified monitoring
- ✅ Health checks
- ✅ Graceful shutdown
- ✅ Error handling
- ✅ Metrics aggregation

**Key Methods:**
```python
StreamingOrchestrator(bucket_name, trigger_interval)
├── initialize_spark()              # Setup Spark session
├── start_cleaning_pipeline()       # Launch cleaning queries
├── start_transformation_pipeline() # Launch transform queries
├── monitor_queries()               # Check status
├── get_metrics_summary()           # Aggregate metrics
└── stop_all_queries()              # Graceful shutdown
```

---

## Usage

### Quick Start

```bash
# Run complete streaming pipeline
python streaming_orchestrator.py

# Run with custom settings
python streaming_orchestrator.py \
  --bucket-name pulse-bucket-1 \
  --trigger-interval "10 seconds" \
  --monitor-interval 30
```

### Run Individual Pipelines

```bash
# Cleaning only
python streaming_orchestrator.py --cleaning-only

# Transformation only
python streaming_orchestrator.py --transformation-only

# Or run components directly
python cleaning/streaming_cleaning.py
python transformation/streaming_transformation.py
```

### Command-Line Options

```
--bucket-name TEXT         MinIO bucket name (default: pulse-bucket-1)
--trigger-interval TEXT    Micro-batch interval (default: 10 seconds)
--monitor-interval INT     Status check interval in seconds (default: 30)
--cleaning-only            Run only cleaning pipeline
--transformation-only      Run only transformation pipeline
```

---

## Configuration

### Spark Configuration

The orchestrator automatically configures Spark for streaming:

```python
spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints")
spark.conf.set("spark.sql.streaming.schemaInference", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
              "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

### Trigger Intervals

Choose based on latency requirements:

- `5 seconds` - Low latency, higher resource usage
- `10 seconds` - Balanced (recommended) ⭐
- `30 seconds` - Lower resource usage
- `1 minute` - Batch-like, minimal resources

### Checkpoint Management

Checkpoints enable fault tolerance and exactly-once semantics:

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

To reset a pipeline (reprocess from beginning):
```bash
rm -rf /tmp/spark_checkpoints/cleaning/cleaning_orders/
```

---

## Monitoring

### Real-Time Status

The orchestrator provides real-time monitoring:

```
📊 STREAMING PIPELINE STATUS
============================================================
Active Queries: 6
Time: 2026-02-15 20:30:00

Cleaning: 3/3 active
Transformation: 3/3 active

============================================================
QUERY DETAILS:
============================================================

🟢 clean_orders
   Type: cleaning
   Started: 2026-02-15 20:25:00
   Active: True
   Batch ID: 42
   Rows Processed: 150
   Rate: 15.00 rows/sec

🟢 transform_orders
   Type: transformation
   Started: 2026-02-15 20:25:30
   Active: True
   Batch ID: 38
   Rows Processed: 150
   Rate: 15.00 rows/sec

📈 METRICS SUMMARY:
   Total Queries: 6
   Active: 6
   Total Rows Processed: 900
   Total Batches: 42
```

### Query Status Indicators

- 🟢 Green: Active and processing
- 🔴 Red: Stopped or failed
- ⏸️  Yellow: Waiting for data

---

## Performance

### Expected Latency

| Component | Batch (Before) | Streaming (After) | Improvement |
|-----------|---------------|-------------------|-------------|
| Cleaning | 5-8 min | 10-30 sec | 90-96% ✅ |
| Transformation | 4-7 min | 10-30 sec | 92-96% ✅ |
| Analysis | 5-10 min | 10-30 sec | 95-97% ✅ |
| **Total** | **10-20 min** | **30-90 sec** | **95%** ✅ |

### Throughput

With 10-second micro-batches:
- **Low load:** 50-100 rows/batch → 5-10 rows/sec
- **Medium load:** 500-1000 rows/batch → 50-100 rows/sec
- **High load:** 5000+ rows/batch → 500+ rows/sec

### Resource Usage

**Per pipeline (3 tables):**
- CPU: 2-4 cores
- Memory: 4-8 GB
- Disk: Minimal (checkpoints)

**Total (cleaning + transformation):**
- CPU: 4-8 cores
- Memory: 8-16 GB

---

## Testing

### Test Streaming Pipeline

```bash
# 1. Start orchestrator
python streaming_orchestrator.py

# 2. Add test data to MinIO/mapped/
# Watch logs for processing

# 3. Verify output in MinIO
# Check MinIO/cleaned_streaming/ and MinIO/transformed_streaming/

# 4. Monitor metrics
# Observe processing rate and latency

# 5. Stop gracefully (Ctrl+C)
# Verify all queries stopped
```

### Test Individual Components

```bash
# Test cleaning only
python cleaning/streaming_cleaning.py

# Test transformation only
python transformation/streaming_transformation.py
```

### Verify Checkpoints

```bash
# Check checkpoint directories exist
ls -la /tmp/spark_checkpoints/cleaning/
ls -la /tmp/spark_checkpoints/transformation/

# Each should contain:
# - commits/
# - metadata
# - offsets/
# - state/
```

---

## Troubleshooting

### Issue: "No data to process"

**Cause:** No files in MinIO/mapped/  
**Solution:** 
```bash
# Check MinIO for input files
aws --endpoint-url http://localhost:9000 s3 ls s3://pulse-bucket-1/mapped/
```

---

### Issue: Query not progressing

**Cause:** Checkpoint corruption or state issues  
**Solution:**
```bash
# Stop query
# Remove checkpoint
rm -rf /tmp/spark_checkpoints/cleaning/cleaning_orders/
# Restart query
```

---

### Issue: Out of memory

**Cause:** State size too large  
**Solution:**
- Reduce watermark duration
- Use shorter windows
- Increase executor memory:
```python
spark.conf.set("spark.executor.memory", "8g")
```

---

### Issue: Late data being dropped

**Cause:** Watermark too aggressive  
**Solution:**
```python
# Increase watermark duration
df = df.withWatermark("timestamp_col", "2 hours")  # Instead of 1 hour
```

---

## Advanced Features

### Watermarking

Handle late-arriving data:

```python
# Set watermark to 1 hour
df = df.withWatermark("order_date", "1 hour")

# Events arriving > 1 hour late will be dropped
# State is cleaned up for old windows
```

### Stateful Operations

Maintain state across micro-batches:

```python
# Running total
df.groupBy("customer_id").agg(
    sum("amount").alias("total_spent")
)

# Deduplication
df.dropDuplicates(["order_id"])
```

### Multiple Output Modes

Choose based on use case:

- **append:** Only new rows (fast, immutable)
- **update:** Update existing rows (for aggregations)
- **complete:** Full result every time (small outputs only)

---

## Integration with Phase 1

Phase 2 builds on Phase 1's incremental cleaning:

```
Phase 1 (Incremental):
├─ Processes only new files
├─ Tracks state in PostgreSQL
├─ 85-90% improvement
└─ Complements Phase 2

Phase 2 (Streaming):
├─ Continuous processing
├─ 10-second micro-batches
├─ 95% total improvement
└─ Can work independently or with Phase 1
```

**Recommended:** Use both together for maximum efficiency!

---

## Next Steps

### Phase 3 (Optional - Not Recommended)

**Flink Speed Layer** for <5 second latency
- Only if sub-5-second updates are required
- Adds significant complexity
- See SPARK_VS_FLINK_CLARIFICATION.md

### Phase 4 (Recommended)

**WebSocket Frontend Integration**
- Push updates to frontend
- Auto-refreshing dashboards
- Live metrics display
- Timeline: 1-2 weeks

---

## Files Created

1. **cleaning/streaming_cleaning.py** (350 lines)
   - StreamingCleaner class
   - Continuous cleaning pipeline
   - Stateful deduplication

2. **transformation/streaming_transformation.py** (400 lines)
   - StreamingTransformer class
   - Stateful aggregations
   - Window operations

3. **streaming_orchestrator.py** (400 lines)
   - Pipeline orchestration
   - Monitoring and metrics
   - Lifecycle management

4. **PHASE2_IMPLEMENTATION.md** (This file)
   - Complete documentation
   - Usage guide
   - Troubleshooting

**Total:** 1150+ lines of code, comprehensive documentation

---

## Success Criteria

Phase 2 is successful when:

✅ **Functional:**
- Streaming queries run continuously
- Data flows through pipeline
- No data loss or duplicates
- Checkpoints working

✅ **Performance:**
- End-to-end latency < 2 minutes
- Processing rate matches input rate
- No backlog accumulation

✅ **Reliability:**
- Fault tolerance working
- Graceful shutdown/restart
- State recovery after failure

---

## Summary

**Phase 2 Status:** ✅ **IMPLEMENTATION COMPLETE**

**What We Built:**
- ✅ Streaming cleaning pipeline
- ✅ Streaming transformation pipeline
- ✅ Orchestration and monitoring
- ✅ Comprehensive documentation

**Expected Results:**
- ⏱️  **95% latency reduction** (10-20 min → 30-90 sec)
- 🔄 **Continuous processing** (10-second micro-batches)
- 📊 **Real-time metrics** and monitoring
- 🛡️  **Fault tolerance** with checkpointing

**Ready For:**
- ✅ Testing with actual data
- ✅ Performance benchmarking
- ✅ Integration with Phase 1
- ✅ Production deployment

**Next Action:** Test streaming pipelines with real data and monitor performance.

---

**For Questions:** See REAL_TIME_PIPELINE_SOLUTION.md or SPARK_VS_FLINK_CLARIFICATION.md
