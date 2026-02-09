# Real-Time Pipeline Solution: Eliminating 10-20 Minute Delay

**Date:** February 9, 2026  
**Context:** CDC is now implemented, ingestion is fast (<3 seconds)  
**Problem:** Batch processing (cleaning → transformation → analysis → inference) takes 10-20 minutes  
**Goal:** Reduce to seconds for incremental updates, only initial bulk load can be slow

---

## Executive Summary

With CDC implemented, data arrives in MinIO/mapped/ within seconds. However, the batch processing pipeline still takes 10-20 minutes because it:
1. **Reprocesses ALL data** on every run (not incremental)
2. **Runs as batch jobs** (not streaming)
3. **Has no speed layer** for real-time aggregations

**Solution:** Implement a **Lambda Architecture** with:
- **Batch Layer (Cold Path):** Accurate, complete processing for historical data (initial load)
- **Speed Layer (Hot Path):** Fast, incremental processing for recent data (<5 seconds)
- **Serving Layer:** Hybrid queries combining both layers

**Expected Results:**
- Initial bulk load: 10-20 minutes (acceptable, one-time)
- Incremental updates: 3-30 seconds (depending on phase)
- Frontend updates: Continuous (WebSocket push)
- 95-98% latency reduction for normal operations

---

## Table of Contents

1. [Current Architecture Analysis](#current-architecture-analysis)
2. [Solution Overview: Lambda Architecture](#solution-overview-lambda-architecture)
3. [Phase 1: Incremental Processing (Quick Win)](#phase-1-incremental-processing-quick-win)
4. [Phase 2: Streaming Pipeline](#phase-2-streaming-pipeline)
5. [Phase 3: Speed Layer Implementation](#phase-3-speed-layer-implementation)
6. [Phase 4: Frontend Real-Time Updates](#phase-4-frontend-real-time-updates)
7. [Phase 5: Optimization & Monitoring](#phase-5-optimization--monitoring)
8. [Implementation Roadmap](#implementation-roadmap)
9. [Architecture Diagrams](#architecture-diagrams)
10. [Code Examples](#code-examples)

---

## Current Architecture Analysis

### Current Flow (After CDC Implementation)

```
CDC → Kafka → Spark Streaming → MinIO/mapped/    [Fast: 3 seconds]
                                      ↓
                           [Data sits here waiting]
                                      ↓
       Manual/Scheduled Trigger → Batch Pipeline   [Slow: 10-20 minutes]
                ↓                       ↓
         cleaning.py               transformation.py          analysis.py
         (5-8 min)                    (4-7 min)              (5-10 min)
       - Loads ALL data           - Loads ALL aggregates   - Loads ALL transforms
       - Reprocesses everything   - Recalculates all       - Computes ALL metrics
                                      ↓
                          MinIO/analytics/ → Frontend
```

### Bottleneck Analysis

| Phase | Current Time | Why Slow | Impact |
|-------|-------------|----------|---------|
| Cleaning | 5-8 min | Loads ALL files from MinIO/mapped/, reprocesses duplicates/nulls | 35% |
| Transformation | 4-7 min | Loads ALL data, recalculates ALL aggregations from scratch | 30% |
| Analysis | 5-10 min | Loads ALL transforms, computes ALL 100+ analytics | 35% |

**Root Causes:**
1. **No state tracking** - Pipeline doesn't know what's already processed
2. **Full reprocessing** - Every run processes entire history
3. **Batch-oriented** - Not designed for incremental updates
4. **No caching** - No intermediate results stored
5. **Synchronous** - Each phase waits for previous to complete

---

## Solution Overview: Lambda Architecture

### The Three-Layer Approach

```
                    ┌─────────────────────────────────────┐
                    │     CDC + Kafka (Implemented)       │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────┴──────────────────────┐
                    │                                      │
         ┌──────────▼──────────┐            ┌────────────▼─────────┐
         │   BATCH LAYER       │            │    SPEED LAYER       │
         │   (Cold Path)       │            │    (Hot Path)        │
         │                     │            │                      │
         │ • Complete history  │            │ • Last 24-48 hours   │
         │ • Perfect accuracy  │            │ • Near real-time     │
         │ • Runs: Daily       │            │ • Runs: Continuous   │
         │ • Time: 10-20 min   │            │ • Time: <5 seconds   │
         │                     │            │                      │
         │ MinIO/batch/        │            │ MinIO/speed/         │
         └──────────┬──────────┘            └────────────┬─────────┘
                    │                                     │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │       SERVING LAYER                  │
                    │  (Hybrid Query Engine)               │
                    │                                      │
                    │  • Queries batch for historical      │
                    │  • Queries speed for recent          │
                    │  • Merges results                    │
                    │  • Caches frequently accessed        │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │       Frontend (React)               │
                    │  • WebSocket for real-time push      │
                    │  • Charts update every 5 seconds     │
                    └──────────────────────────────────────┘
```

### Key Principles

1. **Batch Layer (Cold Path):**
   - Processes complete historical data
   - Runs daily or when needed (e.g., schema changes)
   - Produces perfect, accurate results
   - Takes 10-20 minutes (acceptable for full reprocessing)

2. **Speed Layer (Hot Path):**
   - Processes only recent data (last 24-48 hours)
   - Runs continuously in real-time
   - Produces approximate results quickly
   - Updates in <5 seconds

3. **Serving Layer:**
   - Merges batch and speed results
   - Handles query routing
   - Provides unified API to frontend
   - Manages cache invalidation

---

## Phase 1: Incremental Processing (Quick Win)

**Goal:** Reduce batch processing from 10-20 min to 3-5 min for incremental updates  
**Effort:** 2-3 weeks  
**Improvement:** 75-80%

### Solution: State Tracking + Selective Processing

#### 1.1 Incremental Cleaning

**Current Problem:** Loads ALL files from MinIO/mapped/, even already cleaned ones

**Solution:** Track processed files in state store (PostgreSQL)

**Implementation:**

```python
# cleaning/incremental_cleaning.py

import os
from sqlalchemy import create_engine, text
from datetime import datetime

# State tracking table schema
STATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS cleaning_state (
    file_path VARCHAR(500) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL,
    file_size BIGINT,
    record_count BIGINT,
    checksum VARCHAR(64)
);

CREATE INDEX idx_cleaning_state_processed_at ON cleaning_state(processed_at);
"""

class IncrementalCleaning:
    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string)
        self._init_state_table()
    
    def _init_state_table(self):
        """Initialize state tracking table"""
        with self.engine.connect() as conn:
            conn.execute(text(STATE_TABLE_DDL))
            conn.commit()
    
    def get_unprocessed_files(self, bucket_name, folder):
        """Get list of files not yet processed"""
        # Get all files in MinIO
        all_files = list_minio_files(bucket_name, folder)
        
        # Get already processed files from state
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT file_path FROM cleaning_state"
            ))
            processed_files = {row[0] for row in result}
        
        # Return only unprocessed files
        unprocessed = [f for f in all_files if f not in processed_files]
        return unprocessed
    
    def mark_as_processed(self, file_path, file_size, record_count):
        """Mark file as processed in state store"""
        with self.engine.connect() as conn:
            conn.execute(text(
                """
                INSERT INTO cleaning_state (file_path, processed_at, file_size, record_count)
                VALUES (:path, :ts, :size, :count)
                ON CONFLICT (file_path) DO UPDATE 
                SET processed_at = :ts, file_size = :size, record_count = :count
                """
            ), {
                "path": file_path,
                "ts": datetime.utcnow(),
                "size": file_size,
                "count": record_count
            })
            conn.commit()
    
    def clean_incremental(self, bucket_name):
        """Clean only new files since last run"""
        print("🔍 Checking for new files to clean...")
        
        unprocessed_files = self.get_unprocessed_files(bucket_name, "mapped")
        
        if not unprocessed_files:
            print("✅ No new files to clean")
            return {}
        
        print(f"📦 Found {len(unprocessed_files)} new files to clean")
        
        # Load and clean only new files
        results = {}
        for file_path in unprocessed_files:
            df = load_parquet_from_minio(file_path)
            cleaned_df = clean_dataframe(df)
            
            # Save cleaned data
            output_path = save_to_minio(cleaned_df, bucket_name, "cleaned")
            
            # Mark as processed
            self.mark_as_processed(file_path, df.memory_usage(), len(df))
            
            results[file_path] = output_path
        
        print(f"✅ Cleaned {len(results)} files incrementally")
        return results


# Usage in cleaning.py
def run_cleaning(mode="incremental"):
    """
    Run cleaning in incremental or full mode.
    
    Args:
        mode: "incremental" (default) or "full"
    """
    if mode == "incremental":
        cleaner = IncrementalCleaning(os.getenv("POSTGRES_CONNECTION_STRING"))
        return cleaner.clean_incremental("pulse-bucket-1")
    else:
        # Full reprocessing (for initial load or schema changes)
        return clean_all_files("pulse-bucket-1")
```

**Time Reduction:**
- Current: 5-8 minutes (processes ALL files)
- With incremental: 30-90 seconds (processes only NEW files)
- **Improvement: 85-90%**

#### 1.2 Incremental Transformations

**Current Problem:** Recalculates ALL aggregations from scratch

**Solution:** Use Spark Structured Streaming with state management

**Implementation:**

```python
# transformation/streaming_transformations.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum, count, avg, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_streaming_aggregations():
    """
    Create streaming aggregations that maintain state.
    Updates only when new data arrives.
    """
    spark = SparkSession.builder \
        .appName("StreamingTransformations") \
        .getOrCreate()
    
    # Read from cleaned data as stream
    orders_stream = spark.readStream \
        .format("parquet") \
        .schema(get_orders_schema()) \
        .option("maxFilesPerTrigger", 10) \
        .load("s3a://pulse-bucket-1/cleaned/orders/")
    
    # Windowed aggregations with watermarking
    # This maintains state and updates incrementally
    aggregations = orders_stream \
        .withWatermark("order_date", "2 hours") \
        .groupBy(
            window("order_date", "1 hour"),
            "customer_id"
        ) \
        .agg(
            sum("total_amount").alias("total_spent"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value")
        )
    
    # Write to speed layer location
    query = aggregations.writeStream \
        .format("parquet") \
        .option("path", "s3a://pulse-bucket-1/speed/aggregations/") \
        .option("checkpointLocation", "s3a://pulse-checkpoints/transformations/") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query


def create_materialized_views():
    """
    Pre-compute frequently accessed aggregations.
    Updates incrementally as new data arrives.
    """
    
    # Customer lifetime value (updated incrementally)
    clv_query = """
    SELECT 
        customer_id,
        SUM(total_amount) as lifetime_value,
        COUNT(order_id) as total_orders,
        MAX(order_date) as last_order_date,
        CURRENT_TIMESTAMP as computed_at
    FROM cleaned_orders
    WHERE order_date >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
    GROUP BY customer_id
    """
    
    # Product performance (updated incrementally)
    product_query = """
    SELECT 
        product_id,
        COUNT(DISTINCT order_id) as times_ordered,
        SUM(quantity) as total_quantity,
        AVG(price) as avg_price,
        CURRENT_TIMESTAMP as computed_at
    FROM cleaned_order_items
    WHERE created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
    GROUP BY product_id
    """
    
    return [clv_query, product_query]


# Usage
if __name__ == "__main__":
    # Start streaming transformations
    query = create_streaming_aggregations()
    
    # Create materialized views
    views = create_materialized_views()
    
    query.awaitTermination()
```

**Time Reduction:**
- Current: 4-7 minutes (recalculates ALL aggregations)
- With streaming: 10-30 seconds (updates only changed aggregations)
- **Improvement: 90-95%**

#### 1.3 Incremental Analysis

**Current Problem:** Computes ALL 100+ analytics every run

**Solution:** Delta computation - calculate only what changed

**Implementation:**

```python
# analysis/incremental_analysis.py

import pandas as pd
from datetime import datetime, timedelta

class IncrementalAnalysis:
    def __init__(self):
        self.last_run_timestamp = self._get_last_run_timestamp()
    
    def _get_last_run_timestamp(self):
        """Get timestamp of last successful run"""
        # Read from state store or metadata file
        return pd.read_parquet("s3a://pulse-bucket-1/metadata/last_run.parquet")['timestamp'][0]
    
    def compute_delta_analytics(self):
        """
        Compute only analytics affected by recent data changes.
        Uses smart dependency tracking.
        """
        # Get data changed since last run
        changed_data = self._get_changed_data_since(self.last_run_timestamp)
        
        # Determine which analytics are affected
        affected_analytics = self._determine_affected_analytics(changed_data)
        
        print(f"📊 Computing {len(affected_analytics)} affected analytics (out of 100+)")
        
        results = {}
        for analytic in affected_analytics:
            # Compute only this analytic incrementally
            result = self._compute_analytic_incremental(analytic, changed_data)
            results[analytic] = result
        
        # Merge with previous results (for unaffected analytics)
        final_results = self._merge_with_previous(results)
        
        return final_results
    
    def _get_changed_data_since(self, timestamp):
        """Get tables/records that changed since timestamp"""
        # Query transformation layer for recent data
        query = f"""
        SELECT table_name, COUNT(*) as record_count
        FROM transformation_metadata
        WHERE updated_at > '{timestamp}'
        GROUP BY table_name
        """
        return pd.read_sql(query, self.engine)
    
    def _determine_affected_analytics(self, changed_data):
        """
        Use dependency graph to determine which analytics need recomputation.
        
        Example dependency graph:
        - If 'orders' changed → Recompute: revenue_analysis, customer_metrics
        - If 'products' changed → Recompute: product_performance, inventory_metrics
        - If nothing changed → Recompute: None (use cached)
        """
        dependencies = {
            'orders': ['revenue_analysis', 'customer_metrics', 'order_trends'],
            'products': ['product_performance', 'inventory_metrics'],
            'customers': ['customer_segments', 'clv_analysis'],
            # ... etc
        }
        
        affected = set()
        for table in changed_data['table_name']:
            if table in dependencies:
                affected.update(dependencies[table])
        
        return list(affected)
    
    def _compute_analytic_incremental(self, analytic_name, changed_data):
        """
        Compute single analytic incrementally.
        Uses cached previous results + delta computation.
        """
        # Load previous result
        previous = self._load_cached_result(analytic_name)
        
        # Compute delta
        delta = self._compute_delta(analytic_name, changed_data)
        
        # Merge previous + delta
        updated = self._merge_results(previous, delta)
        
        return updated
    
    def _merge_with_previous(self, new_results):
        """Merge newly computed analytics with cached ones"""
        # Load all previous analytics
        previous_all = self._load_all_previous_analytics()
        
        # Update with new results
        previous_all.update(new_results)
        
        return previous_all


# Usage
if __name__ == "__main__":
    analyzer = IncrementalAnalysis()
    
    # Compute only what changed
    results = analyzer.compute_delta_analytics()
    
    # Save to speed layer
    save_to_speed_layer(results)
```

**Time Reduction:**
- Current: 5-10 minutes (computes ALL 100+ analytics)
- With delta: 30-90 seconds (computes only affected analytics)
- **Improvement: 85-90%**

### Phase 1 Summary

| Component | Current | With Incremental | Improvement |
|-----------|---------|------------------|-------------|
| Cleaning | 5-8 min | 30-90 sec | 85-90% |
| Transformation | 4-7 min | 10-30 sec | 90-95% |
| Analysis | 5-10 min | 30-90 sec | 85-90% |
| **Total** | **14-25 min** | **70-210 sec (1.2-3.5 min)** | **85-88%** |

**Benefits:**
- ✅ Quick win (2-3 weeks implementation)
- ✅ No major architecture change
- ✅ Significant improvement
- ✅ Foundation for speed layer

---

## Phase 2: Streaming Pipeline

**Goal:** Convert batch processing to continuous streaming  
**Effort:** 3-4 weeks  
**Improvement:** 95% (updates in 10-30 seconds)

### Solution: End-to-End Streaming with Spark Structured Streaming

#### 2.1 Streaming Cleaning Pipeline

```python
# cleaning/streaming_cleaning.py

def create_streaming_cleaning_pipeline():
    """
    Continuous cleaning pipeline that processes data as it arrives.
    """
    spark = SparkSession.builder \
        .appName("StreamingCleaning") \
        .getOrCreate()
    
    # Read from mapped folder as stream
    raw_stream = spark.readStream \
        .format("parquet") \
        .schema(get_schema()) \
        .option("maxFilesPerTrigger", 5) \
        .load("s3a://pulse-bucket-1/mapped/")
    
    # Apply cleaning transformations
    cleaned_stream = raw_stream \
        .dropDuplicates(["id"]) \
        .filter(col("id").isNotNull()) \
        .withColumn("cleaned_at", current_timestamp())
    
    # Write to cleaned folder (continuous)
    query = cleaned_stream.writeStream \
        .format("parquet") \
        .option("path", "s3a://pulse-bucket-1/cleaned/") \
        .option("checkpointLocation", "s3a://pulse-checkpoints/cleaning/") \
        .partitionBy("date") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query
```

#### 2.2 Streaming Transformation Pipeline

```python
# transformation/streaming_transformation.py

def create_streaming_transformation_pipeline():
    """
    Continuous transformation pipeline with stateful aggregations.
    """
    spark = SparkSession.builder \
        .appName("StreamingTransformation") \
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()
    
    # Read cleaned data as stream
    orders = spark.readStream \
        .format("parquet") \
        .schema(orders_schema) \
        .load("s3a://pulse-bucket-1/cleaned/orders/")
    
    # Stateful aggregations (maintains state across micro-batches)
    daily_revenue = orders \
        .withWatermark("order_date", "2 hours") \
        .groupBy(
            window("order_date", "1 day"),
            "store_id"
        ) \
        .agg(
            sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count")
        )
    
    # Write aggregations
    query = daily_revenue.writeStream \
        .format("parquet") \
        .option("path", "s3a://pulse-bucket-1/speed/transformations/") \
        .option("checkpointLocation", "s3a://pulse-checkpoints/transformations/") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query
```

#### 2.3 Streaming Analysis Pipeline

```python
# analysis/streaming_analysis.py

def create_streaming_analysis_pipeline():
    """
    Continuous analysis pipeline for key metrics.
    """
    spark = SparkSession.builder \
        .appName("StreamingAnalysis") \
        .getOrCreate()
    
    # Read transformations as stream
    aggregations = spark.readStream \
        .format("parquet") \
        .schema(aggregation_schema) \
        .load("s3a://pulse-bucket-1/speed/transformations/")
    
    # Compute key metrics
    metrics = aggregations \
        .selectExpr(
            "window.start as time_window",
            "store_id",
            "revenue",
            "order_count",
            "revenue / order_count as avg_order_value"
        )
    
    # Write to speed layer analytics
    query = metrics.writeStream \
        .format("parquet") \
        .option("path", "s3a://pulse-bucket-1/speed/analytics/") \
        .option("checkpointLocation", "s3a://pulse-checkpoints/analysis/") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query
```

### Phase 2 Summary

**Architecture:**
```
CDC → Kafka → Spark Streaming → MinIO/mapped/
                      ↓
           Streaming Cleaning (10s trigger)
                      ↓
              MinIO/cleaned/
                      ↓
        Streaming Transformation (10s trigger)
                      ↓
              MinIO/speed/
                      ↓
          Streaming Analysis (10s trigger)
                      ↓
         MinIO/speed/analytics/ → Frontend
```

**Latency:** 30-40 seconds end-to-end (3x 10-second micro-batches)

---

## Phase 3: Speed Layer Implementation

**Goal:** Sub-5-second updates for critical metrics  
**Effort:** 6-8 weeks  
**Improvement:** 98% (updates in 2-5 seconds)

### Solution: Apache Flink for Ultra-Low Latency

#### 3.1 Flink Real-Time Aggregations

```python
# speed_layer/flink_realtime.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def create_flink_speed_layer():
    """
    Ultra-low latency speed layer using Apache Flink.
    Updates in <5 seconds.
    """
    # Setup Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Define Kafka source (from CDC)
    table_env.execute_sql("""
        CREATE TABLE orders_stream (
            order_id STRING,
            customer_id STRING,
            total_amount DOUBLE,
            order_date TIMESTAMP(3),
            WATERMARK FOR order_date AS order_date - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ecom.orders',
            'properties.bootstrap.servers' = '10.5.0.7:9092',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)
    
    # Real-time revenue (tumbling window)
    table_env.execute_sql("""
        CREATE TABLE realtime_revenue (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_revenue DOUBLE,
            order_count BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://pulse-bucket-1/speed/realtime/',
            'format' = 'parquet'
        )
    """)
    
    # Insert real-time aggregations
    table_env.execute_sql("""
        INSERT INTO realtime_revenue
        SELECT
            TUMBLE_START(order_date, INTERVAL '5' SECOND) as window_start,
            TUMBLE_END(order_date, INTERVAL '5' SECOND) as window_end,
            SUM(total_amount) as total_revenue,
            COUNT(*) as order_count
        FROM orders_stream
        GROUP BY TUMBLE(order_date, INTERVAL '5' SECOND)
    """)
```

#### 3.2 Hybrid Serving Layer

```python
# serving_layer/hybrid_query.py

class HybridQueryEngine:
    """
    Merges results from batch layer (accurate, historical) 
    and speed layer (recent, fast).
    """
    
    def __init__(self):
        self.batch_path = "s3a://pulse-bucket-1/batch/analytics/"
        self.speed_path = "s3a://pulse-bucket-1/speed/analytics/"
        self.speed_cutoff = timedelta(hours=24)  # Last 24 hours from speed
    
    def query_revenue(self, start_date, end_date):
        """
        Query revenue with hybrid approach.
        - Historical data (>24h ago) from batch layer
        - Recent data (<24h ago) from speed layer
        """
        cutoff_time = datetime.now() - self.speed_cutoff
        
        # Query batch layer for historical data
        historical = self._query_batch(
            "revenue",
            start_date,
            min(end_date, cutoff_time)
        )
        
        # Query speed layer for recent data
        recent = self._query_speed(
            "revenue",
            max(start_date, cutoff_time),
            end_date
        )
        
        # Merge results
        merged = pd.concat([historical, recent]) \
            .sort_values('date') \
            .drop_duplicates(subset=['date'], keep='last')
        
        return merged
    
    def _query_batch(self, metric, start, end):
        """Query batch layer (accurate, slower)"""
        path = f"{self.batch_path}/{metric}/"
        df = pd.read_parquet(path)
        return df[(df['date'] >= start) & (df['date'] <= end)]
    
    def _query_speed(self, metric, start, end):
        """Query speed layer (approximate, faster)"""
        path = f"{self.speed_path}/{metric}/"
        df = pd.read_parquet(path)
        return df[(df['date'] >= start) & (df['date'] <= end)]
    
    def invalidate_cache(self, metric):
        """Invalidate cache when batch layer updates"""
        # Batch layer runs daily, invalidate speed layer for overlap
        cutoff = datetime.now() - self.speed_cutoff
        # Delete speed layer data older than 24h (now in batch)
        self._cleanup_speed_layer(metric, cutoff)
```

#### 3.3 Real-Time Metrics Dashboard

```python
# api/routers/realtime_analytics.py

from fastapi import APIRouter, WebSocket
from fastapi.responses import StreamingResponse
import asyncio

router = APIRouter()

@router.websocket("/ws/realtime")
async def websocket_realtime(websocket: WebSocket):
    """
    WebSocket endpoint for real-time metric updates.
    Pushes updates to frontend every 5 seconds.
    """
    await websocket.accept()
    
    try:
        while True:
            # Query speed layer for latest metrics
            metrics = await get_latest_metrics()
            
            # Send to frontend
            await websocket.send_json({
                "timestamp": datetime.now().isoformat(),
                "metrics": metrics
            })
            
            # Wait 5 seconds before next update
            await asyncio.sleep(5)
    
    except WebSocketDisconnect:
        print("Client disconnected")


@router.get("/analytics/realtime/{metric}")
async def get_realtime_metric(metric: str):
    """
    REST endpoint for latest value of a metric.
    Queries speed layer only (sub-second response).
    """
    query_engine = HybridQueryEngine()
    
    # Get last 5 minutes from speed layer
    start = datetime.now() - timedelta(minutes=5)
    end = datetime.now()
    
    data = query_engine._query_speed(metric, start, end)
    
    return {
        "metric": metric,
        "value": data.iloc[-1]['value'] if not data.empty else None,
        "timestamp": data.iloc[-1]['timestamp'] if not data.empty else None,
        "latency_ms": "<5000"
    }
```

### Phase 3 Summary

**Architecture:**
```
                    Flink Speed Layer
                    (2-5 second latency)
                           ↓
                   MinIO/speed/realtime/
                           │
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                   │
 Batch Layer         Speed Layer        Serving Layer
(Daily, accurate)  (24h, fast)      (Hybrid queries)
        │                  │                   │
        └──────────────────┴───────────────────┘
                           ↓
                 REST API + WebSocket
                           ↓
               Frontend (React Charts)
              Updates every 5 seconds
```

**Latency Breakdown:**
- Flink processing: 2-5 seconds
- Query speed layer: <100ms
- Frontend update: 5 seconds (WebSocket push)
- **Total: 5-10 seconds end-to-end**

---

## Phase 4: Frontend Real-Time Updates

**Goal:** Push updates to frontend without polling  
**Effort:** 1-2 weeks  
**Improvement:** Instant updates when data available

### Solution: WebSocket + React Hooks

#### 4.1 Backend WebSocket Server

```python
# api/websocket_server.py

from fastapi import WebSocket, WebSocketDisconnect
from typing import List
import asyncio
import json

class ConnectionManager:
    """Manages WebSocket connections"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                # Handle disconnected clients
                self.disconnect(connection)


manager = ConnectionManager()


@router.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """
    WebSocket endpoint that pushes metric updates to frontend.
    """
    await manager.connect(websocket)
    
    try:
        while True:
            # Check for new data in speed layer
            updates = await check_for_updates()
            
            if updates:
                # Broadcast to all connected clients
                await manager.broadcast({
                    "type": "metrics_update",
                    "data": updates,
                    "timestamp": datetime.now().isoformat()
                })
            
            # Check every 5 seconds
            await asyncio.sleep(5)
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def check_for_updates():
    """
    Check speed layer for new data.
    Returns updates if available.
    """
    # Query speed layer for data newer than last sent
    query_engine = HybridQueryEngine()
    
    latest = query_engine._query_speed(
        "all_metrics",
        datetime.now() - timedelta(seconds=10),
        datetime.now()
    )
    
    if not latest.empty:
        return latest.to_dict('records')
    
    return None
```

#### 4.2 React Frontend WebSocket Hook

```javascript
// frontend/src/hooks/useRealtimeMetrics.js

import { useState, useEffect, useCallback } from 'react';

export const useRealtimeMetrics = () => {
  const [metrics, setMetrics] = useState({});
  const [isConnected, setIsConnected] = useState(false);
  const [ws, setWs] = useState(null);

  useEffect(() => {
    // Create WebSocket connection
    const websocket = new WebSocket('ws://localhost:8000/ws/metrics');

    websocket.onopen = () => {
      console.log('✅ WebSocket connected');
      setIsConnected(true);
    };

    websocket.onmessage = (event) => {
      const update = JSON.parse(event.data);
      
      if (update.type === 'metrics_update') {
        console.log('📊 Received metric update:', update.timestamp);
        
        // Update metrics state
        setMetrics(prevMetrics => ({
          ...prevMetrics,
          ...update.data,
          lastUpdate: update.timestamp
        }));
      }
    };

    websocket.onerror = (error) => {
      console.error('❌ WebSocket error:', error);
      setIsConnected(false);
    };

    websocket.onclose = () => {
      console.log('🔌 WebSocket disconnected');
      setIsConnected(false);
      
      // Reconnect after 5 seconds
      setTimeout(() => {
        console.log('🔄 Reconnecting...');
        setWs(new WebSocket('ws://localhost:8000/ws/metrics'));
      }, 5000);
    };

    setWs(websocket);

    // Cleanup on unmount
    return () => {
      websocket.close();
    };
  }, []);

  return { metrics, isConnected };
};
```

#### 4.3 React Dashboard Component

```javascript
// frontend/src/pages/Dashboard.jsx

import React from 'react';
import { useRealtimeMetrics } from '../hooks/useRealtimeMetrics';
import { Line, Bar } from 'react-chartjs-2';

const Dashboard = () => {
  const { metrics, isConnected } = useRealtimeMetrics();

  return (
    <div className="dashboard">
      {/* Connection status indicator */}
      <div className={`status ${isConnected ? 'connected' : 'disconnected'}`}>
        {isConnected ? '🟢 Live' : '🔴 Disconnected'}
        {metrics.lastUpdate && (
          <span> • Updated {new Date(metrics.lastUpdate).toLocaleTimeString()}</span>
        )}
      </div>

      {/* Real-time revenue chart */}
      <div className="chart-container">
        <h3>Real-Time Revenue</h3>
        <Line
          data={{
            labels: metrics.revenueTimestamps || [],
            datasets: [{
              label: 'Revenue',
              data: metrics.revenueValues || [],
              borderColor: 'rgb(75, 192, 192)',
              tension: 0.1
            }]
          }}
          options={{
            animation: {
              duration: 500  // Smooth animation on update
            },
            scales: {
              x: { display: true },
              y: { display: true }
            }
          }}
        />
      </div>

      {/* Real-time KPIs */}
      <div className="kpi-grid">
        <div className="kpi-card">
          <h4>Total Orders</h4>
          <p className="kpi-value">{metrics.totalOrders?.toLocaleString() || 0}</p>
          <span className="update-badge">Live</span>
        </div>

        <div className="kpi-card">
          <h4>Revenue (Today)</h4>
          <p className="kpi-value">${metrics.todayRevenue?.toLocaleString() || 0}</p>
          <span className="update-badge">Live</span>
        </div>

        <div className="kpi-card">
          <h4>Active Customers</h4>
          <p className="kpi-value">{metrics.activeCustomers?.toLocaleString() || 0}</p>
          <span className="update-badge">Live</span>
        </div>
      </div>

      {/* Update log */}
      <div className="update-log">
        <h4>Recent Updates</h4>
        <ul>
          {metrics.updateLog?.slice(0, 5).map((log, idx) => (
            <li key={idx}>
              {new Date(log.timestamp).toLocaleTimeString()}: {log.message}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};

export default Dashboard;
```

### Phase 4 Summary

**User Experience:**
- ✅ Charts update automatically every 5 seconds
- ✅ No page refresh needed
- ✅ "Live" indicator shows connection status
- ✅ Smooth animations on updates
- ✅ Sub-second response when clicking around

---

## Phase 5: Optimization & Monitoring

**Goal:** Fine-tune performance and ensure reliability  
**Effort:** Ongoing  

### 5.1 Caching Strategy

```python
# serving_layer/cache.py

import redis
import json
from datetime import timedelta

class MetricsCache:
    """
    Multi-tier caching for frequently accessed metrics.
    """
    
    def __init__(self):
        self.redis_client = redis.Redis(host='10.5.0.8', port=6379)
        self.ttl_seconds = 300  # 5 minutes
    
    def get_metric(self, metric_name, params):
        """
        Get metric from cache or compute if not cached.
        """
        cache_key = f"metric:{metric_name}:{json.dumps(params)}"
        
        # Try cache first
        cached = self.redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
        
        # Cache miss - compute metric
        result = self._compute_metric(metric_name, params)
        
        # Store in cache
        self.redis_client.setex(
            cache_key,
            self.ttl_seconds,
            json.dumps(result)
        )
        
        return result
    
    def invalidate_metric(self, metric_name):
        """Invalidate all cached versions of a metric"""
        pattern = f"metric:{metric_name}:*"
        keys = self.redis_client.keys(pattern)
        if keys:
            self.redis_client.delete(*keys)
```

### 5.2 Monitoring & Alerting

```python
# monitoring/pipeline_monitor.py

import prometheus_client as prom
from datetime import datetime

# Define metrics
pipeline_latency = prom.Histogram(
    'pipeline_latency_seconds',
    'Time taken for data to flow through pipeline',
    ['stage']
)

data_freshness = prom.Gauge(
    'data_freshness_seconds',
    'Age of most recent data in analytics',
    ['metric']
)

processing_errors = prom.Counter(
    'processing_errors_total',
    'Total number of processing errors',
    ['stage', 'error_type']
)


class PipelineMonitor:
    """
    Monitor pipeline performance and data freshness.
    """
    
    def measure_latency(self, stage):
        """Context manager to measure stage latency"""
        return pipeline_latency.labels(stage=stage).time()
    
    def check_data_freshness(self):
        """
        Check how fresh the data is in analytics layer.
        Alert if data is too stale.
        """
        latest_timestamp = self._get_latest_analytics_timestamp()
        age_seconds = (datetime.now() - latest_timestamp).total_seconds()
        
        data_freshness.labels(metric='analytics').set(age_seconds)
        
        # Alert if data is more than 2 minutes old
        if age_seconds > 120:
            self._send_alert(f"Data is {age_seconds}s old - pipeline may be stalled")
    
    def record_error(self, stage, error_type):
        """Record processing error"""
        processing_errors.labels(stage=stage, error_type=error_type).inc()
    
    def get_dashboard_metrics(self):
        """
        Get metrics for monitoring dashboard.
        """
        return {
            'average_latency': self._get_average_latency(),
            'data_freshness': self._get_data_freshness(),
            'error_rate': self._get_error_rate(),
            'throughput': self._get_throughput()
        }
```

### 5.3 Performance Tuning

```python
# optimization/performance_tuning.py

def optimize_spark_config():
    """
    Optimized Spark configuration for streaming workloads.
    """
    return {
        # Memory optimization
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        
        # Shuffle optimization
        "spark.sql.shuffle.partitions": "200",
        "spark.shuffle.service.enabled": "true",
        
        # Streaming optimization
        "spark.sql.streaming.stateStore.providerClass": 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        "spark.sql.streaming.stateStore.rocksdb.compactOnCommit": "true",
        
        # Checkpoint optimization
        "spark.sql.streaming.checkpointLocation.cleanup": "true",
        "spark.sql.streaming.minBatchesToRetain": "10",
        
        # Resource optimization
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "10",
    }


def optimize_parquet_writes():
    """
    Optimize Parquet writing for speed layer.
    """
    return {
        # Compression (balance between speed and size)
        "compression": "snappy",  # Fast compression
        
        # Partitioning (for faster queries)
        "partition_cols": ["date", "hour"],
        
        # Row group size (smaller for streaming)
        "row_group_size": 32 * 1024 * 1024,  # 32MB
        
        # Write mode
        "mode": "append",  # For streaming
    }
```

---

## Implementation Roadmap

### Timeline & Prioritization

| Phase | Duration | Effort | Impact | Priority | Dependencies |
|-------|----------|--------|--------|----------|-------------|
| **Phase 1: Incremental** | 2-3 weeks | Medium | 85% | 🔴 High | None |
| **Phase 2: Streaming** | 3-4 weeks | Medium | 95% | 🟡 Medium | Phase 1 |
| **Phase 3: Speed Layer** | 6-8 weeks | High | 98% | 🟢 Low | Phase 2 |
| **Phase 4: Frontend** | 1-2 weeks | Low | UX++ | 🔴 High | Phase 1 |
| **Phase 5: Optimization** | Ongoing | Low | 10-20% | 🟢 Low | Any phase |

### Recommended Implementation Order

**Sprint 1-2 (Weeks 1-4): Quick Wins**
- ✅ Implement incremental cleaning
- ✅ Add state tracking to PostgreSQL
- ✅ Convert cleaning.py to use incremental mode
- ✅ Test with sample data
- **Outcome: 85% reduction in cleaning time**

**Sprint 3-4 (Weeks 5-8): Streaming Foundation**
- ✅ Implement streaming transformations
- ✅ Add Spark Structured Streaming pipelines
- ✅ Configure watermarking and state stores
- ✅ Test end-to-end streaming
- **Outcome: 30-40 second end-to-end latency**

**Sprint 5 (Weeks 9-10): Frontend Real-Time**
- ✅ Add WebSocket support to FastAPI
- ✅ Implement React WebSocket hooks
- ✅ Update dashboard components
- ✅ Add "Live" indicators
- **Outcome: Auto-updating frontend every 5 seconds**

**Sprint 6 (Weeks 11-12): Incremental Analysis**
- ✅ Implement delta computation for analysis
- ✅ Add dependency tracking
- ✅ Integrate with speed layer
- **Outcome: Sub-2-minute full pipeline**

**Sprint 7+ (Weeks 13+): Speed Layer (Optional)**
- ✅ Deploy Apache Flink cluster
- ✅ Implement ultra-low latency aggregations
- ✅ Add hybrid serving layer
- **Outcome: Sub-5-second updates for critical metrics**

### Milestones

**Milestone 1: Incremental Processing (Week 4)**
- ✅ Cleaning time: 5-8 min → 1-2 min
- ✅ Transformation time: 4-7 min → 1-2 min
- ✅ Total pipeline: 14-25 min → 3-5 min

**Milestone 2: Streaming Pipeline (Week 8)**
- ✅ End-to-end latency: 10-20 min → 30-40 sec
- ✅ Continuous processing (no manual trigger)
- ✅ State management working

**Milestone 3: Real-Time Frontend (Week 10)**
- ✅ WebSocket push working
- ✅ Charts update every 5 seconds
- ✅ No page refresh needed

**Milestone 4: Production Ready (Week 12)**
- ✅ Monitoring in place
- ✅ Error handling robust
- ✅ Performance acceptable
- ✅ Documentation complete

---

## Architecture Diagrams

### Current Architecture (After CDC)

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA INGESTION (FAST)                    │
└──────────────────────────────────────────────────────────────┘

Database → CDC (<1s) → Kafka → Spark Streaming (500ms) → MinIO/mapped/

                            ↓ [Data waits here]

┌──────────────────────────────────────────────────────────────┐
│                   BATCH PROCESSING (SLOW)                    │
└──────────────────────────────────────────────────────────────┘

Manual Trigger → cleaning.py (5-8 min) → MinIO/cleaned/
                        ↓
                 transformation.py (4-7 min) → MinIO/transformed/
                        ↓
                 analysis.py (5-10 min) → MinIO/analytics/
                        ↓
                    Frontend (Stale data)

Total Latency: 14-25 minutes + wait time for trigger
```

### Target Architecture (Lambda + Streaming)

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA INGESTION (FAST)                    │
└──────────────────────────────────────────────────────────────┘

Database → CDC (<1s) → Kafka
                        ↓
                        ├─────────────────┬─────────────────┐
                        ↓                 ↓                 ↓
                 Spark Streaming   Flink Speed Layer  Batch Layer
                   (10-30s)           (2-5s)          (Daily)
                        ↓                 ↓                 ↓
                  MinIO/speed/      MinIO/realtime/   MinIO/batch/


┌──────────────────────────────────────────────────────────────┐
│                    SERVING LAYER (HYBRID)                    │
└──────────────────────────────────────────────────────────────┘

                    Hybrid Query Engine
                            ↓
            ┌───────────────┼───────────────┐
            ↓               ↓               ↓
     Batch Results   Speed Results   Realtime Results
    (Historical)     (Last 24h)      (Last 5 min)
            │               │               │
            └───────────────┴───────────────┘
                            ↓
                    Merged Results
                            ↓
                    REST API + WebSocket
                            ↓
                 Frontend (Live Updates)

Total Latency: 2-30 seconds (depending on metric criticality)
```

### Data Flow Timeline

```
Time: 0s
Event: New order placed in database

Time: 1s
CDC: Change captured and sent to Kafka

Time: 2s
Flink: Real-time aggregation computed
Output: Revenue counter updated in MinIO/realtime/

Time: 5s
WebSocket: Push update to all connected clients
Frontend: Charts refresh automatically

Time: 15s
Spark Streaming: Cleaning micro-batch completes
Output: Cleaned data in MinIO/cleaned/

Time: 30s
Spark Streaming: Transformation micro-batch completes
Output: Aggregations in MinIO/speed/

Time: 40s
Spark Streaming: Analysis micro-batch completes
Output: All metrics updated in MinIO/speed/analytics/

Time: 24h (Daily)
Batch Layer: Full reprocessing for accuracy
Output: Perfect historical data in MinIO/batch/
```

---

## Code Examples

### Example 1: Incremental Cleaning Script

```bash
#!/bin/bash
# scripts/run_incremental_cleaning.sh

echo "🔍 Starting incremental cleaning..."

# Run cleaning in incremental mode
python cleaning/cleaning.py --mode incremental

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ Incremental cleaning completed successfully"
else
    echo "❌ Incremental cleaning failed"
    exit 1
fi

# Trigger next phase
python transformation/transformation.py --mode incremental
```

### Example 2: Streaming Pipeline Manager

```python
# pipeline/streaming_manager.py

class StreamingPipelineManager:
    """
    Manages multiple streaming queries.
    Ensures all stages are running.
    """
    
    def __init__(self):
        self.queries = {}
    
    def start_all(self):
        """Start all streaming pipelines"""
        print("🚀 Starting streaming pipelines...")
        
        # Start cleaning stream
        self.queries['cleaning'] = create_streaming_cleaning_pipeline()
        print("✅ Cleaning stream started")
        
        # Start transformation stream
        self.queries['transformation'] = create_streaming_transformation_pipeline()
        print("✅ Transformation stream started")
        
        # Start analysis stream
        self.queries['analysis'] = create_streaming_analysis_pipeline()
        print("✅ Analysis stream started")
        
        # Start speed layer (Flink)
        self.queries['speed'] = create_flink_speed_layer()
        print("✅ Speed layer started")
        
        print(f"✅ All {len(self.queries)} streams running")
    
    def monitor(self):
        """Monitor streaming query health"""
        for name, query in self.queries.items():
            status = query.status
            print(f"{name}: {status}")
            
            if not status.isDataAvailable:
                print(f"⚠️ {name} stream has no data")
    
    def await_termination(self):
        """Wait for all queries to complete"""
        for query in self.queries.values():
            query.awaitTermination()


if __name__ == "__main__":
    manager = StreamingPipelineManager()
    manager.start_all()
    
    # Monitor every 30 seconds
    while True:
        time.sleep(30)
        manager.monitor()
```

### Example 3: Frontend Chart Component

```javascript
// frontend/src/components/RevenueChart.jsx

import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import { useRealtimeMetrics } from '../hooks/useRealtimeMetrics';

const RevenueChart = () => {
  const { metrics, isConnected } = useRealtimeMetrics();
  const [chartData, setChartData] = useState({
    labels: [],
    datasets: [{
      label: 'Revenue',
      data: [],
      borderColor: 'rgb(75, 192, 192)',
      backgroundColor: 'rgba(75, 192, 192, 0.2)',
    }]
  });

  useEffect(() => {
    if (metrics.revenue) {
      // Update chart data when new metrics arrive
      setChartData(prevData => {
        const newLabels = [...prevData.labels, new Date().toLocaleTimeString()];
        const newData = [...prevData.datasets[0].data, metrics.revenue];
        
        // Keep only last 20 data points
        if (newLabels.length > 20) {
          newLabels.shift();
          newData.shift();
        }
        
        return {
          labels: newLabels,
          datasets: [{
            ...prevData.datasets[0],
            data: newData
          }]
        };
      });
    }
  }, [metrics.revenue]);

  return (
    <div className="chart-card">
      <div className="chart-header">
        <h3>Revenue (Last 100 minutes)</h3>
        <div className={`status-indicator ${isConnected ? 'live' : 'offline'}`}>
          {isConnected ? '🔴 LIVE' : '⚫ Offline'}
        </div>
      </div>
      
      <Line 
        data={chartData}
        options={{
          responsive: true,
          animation: {
            duration: 300  // Smooth update animation
          },
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: (value) => `$${value.toLocaleString()}`
              }
            }
          },
          plugins: {
            legend: {
              display: false
            },
            tooltip: {
              callbacks: {
                label: (context) => `Revenue: $${context.parsed.y.toLocaleString()}`
              }
            }
          }
        }}
      />
      
      {metrics.lastUpdate && (
        <div className="last-update">
          Last updated: {new Date(metrics.lastUpdate).toLocaleString()}
        </div>
      )}
    </div>
  );
};

export default RevenueChart;
```

---

## Success Metrics

### Performance Metrics

| Metric | Before | After Phase 1 | After Phase 2 | After Phase 3 | Target |
|--------|--------|---------------|---------------|---------------|--------|
| **Cleaning time** | 5-8 min | 1-2 min | 10-30 sec | 10-30 sec | <1 min |
| **Transformation time** | 4-7 min | 1-2 min | 10-30 sec | 10-30 sec | <1 min |
| **Analysis time** | 5-10 min | 1-2 min | 10-30 sec | 2-5 sec | <1 min |
| **Total pipeline** | 14-25 min | 3-6 min | 30-90 sec | 5-30 sec | <1 min |
| **Frontend latency** | Hours | 5-10 min | 1-2 min | 5-30 sec | <1 min |
| **Data freshness** | Hours | 5-10 min | 1-2 min | 5-30 sec | <1 min |

### User Experience Metrics

- **Dashboard load time:** <2 seconds
- **Chart update frequency:** Every 5 seconds
- **WebSocket connection uptime:** >99.9%
- **Query response time:** <500ms
- **Cache hit rate:** >80%

### Business Metrics

- **Time to insight:** Hours → Seconds (99% improvement)
- **Decision latency:** Hours → Minutes (95% improvement)
- **User satisfaction:** Stale data → Real-time data
- **Operational cost:** Minimal increase (<10%)

---

## Risk Mitigation

### Risk 1: Complexity Increase

**Risk:** Lambda architecture adds complexity  
**Mitigation:**
- Start with Phase 1 (incremental) - simple change
- Phase 2-3 optional based on needs
- Good documentation and monitoring
- Gradual rollout

### Risk 2: Data Consistency

**Risk:** Speed layer may have approximate results  
**Mitigation:**
- Batch layer runs daily for perfect accuracy
- Speed layer marked as "near real-time" in UI
- Serving layer merges both for best of both worlds
- Define SLAs for accuracy vs latency tradeoff

### Risk 3: Resource Usage

**Risk:** Streaming uses more resources  
**Mitigation:**
- Use dynamic allocation in Spark
- Monitor resource usage
- Set resource limits
- Auto-scaling for Flink

### Risk 4: State Management

**Risk:** Streaming state can grow large  
**Mitigation:**
- Use RocksDB state store (compact storage)
- Configure state TTL (time-to-live)
- Regular checkpointing
- State cleanup policies

---

## Conclusion

By implementing a Lambda Architecture with incremental processing and streaming pipelines:

1. **Phase 1 (Weeks 1-4):** Incremental processing reduces latency by 85% (14-25 min → 3-5 min)
2. **Phase 2 (Weeks 5-8):** Streaming pipeline reduces latency by 95% (14-25 min → 30-90 sec)
3. **Phase 3 (Weeks 9-16):** Speed layer reduces latency by 98% (14-25 min → 5-30 sec)
4. **Phase 4 (Weeks 9-10):** Real-time frontend provides instant updates

**Expected Final State:**
- CDC captures changes in <1 second
- Speed layer processes in 2-5 seconds
- Frontend updates every 5 seconds via WebSocket
- Users see near real-time data (5-30 second latency)
- Initial bulk load still takes 10-20 minutes (acceptable, one-time)

**Total improvement: 95-98% latency reduction for incremental updates**

---

## Next Steps

1. **Review this document** with team
2. **Prioritize phases** based on business needs
3. **Start Phase 1** (incremental cleaning) - quick win
4. **Set up monitoring** for current pipeline
5. **Plan Phase 2** (streaming) if needed
6. **Iterate and optimize** based on metrics

---

**Document Version:** 1.0  
**Last Updated:** February 9, 2026  
**Author:** Engineering Team  
**Status:** Ready for Implementation
