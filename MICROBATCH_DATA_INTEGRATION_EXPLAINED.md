# Real-Time Micro-Batch Data Integration - Complete Answer

**Date:** February 16, 2026  
**Question:** If new microbatch data comes in real-time via database URI or API endpoint, during aggregation and analysis, will the new data completely integrate with existing data? For example, if we're summing a column and new data comes, will the sum automatically be updated to include both old and new data?

---

## TL;DR (30-second answer)

**❌ NO** - In the current PR #54 implementation, new micro-batch data **DOES NOT automatically integrate** with existing aggregated data.

**Why?** The aggregation functions are designed for **batch processing**, not **incremental updates**. Each micro-batch is processed independently, but the aggregations **recalculate from ALL data** in the source, not just the new batch.

**Example:** If you have a sum of $10,000 and new data adds $500:
- ✅ The system **will** include both old and new data
- ❌ But it does so by **re-reading and re-summing ALL data** (not incremental)
- 🔴 Performance degrades as data grows (full scan every 10 seconds)

---

## Understanding the Current Architecture

### Phase 1: Streaming Ingestion (✅ Real-Time)

```
Database → Polling (10s) → Kafka → Spark Streaming → MinIO/mapped/
                                     (500ms batches)     (CSV files)
```

**What happens:**
1. Database polled every 10 seconds for new records
2. New records sent to Kafka topics
3. Spark Streaming consumes Kafka continuously (~500ms micro-batches)
4. Each micro-batch written as CSV file to MinIO/mapped/

**Result:** New data lands in MinIO within **10-15 seconds** ✅

---

### Phase 2: Streaming Cleaning (🟡 Micro-Batch Processing)

**File:** `cleaning/streaming_cleaning.py`

```python
def create_cleaning_stream(spark, source_path, table_name, 
                          trigger_interval="10 seconds"):
    # Read stream from MinIO/mapped/
    df = spark.readStream.format("csv").load(source_path)
    
    # Process each micro-batch
    def apply_batch_cleaning(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Wrap in dict format
        dataframes = {table_name: batch_df}
        
        # REUSE existing cleaning functions
        dataframes = drop_duplicates(dataframes)
        dataframes = drop_null_rows(dataframes)
        dataframes = clean_text_columns(dataframes)
        dataframes = fill_null_values(dataframes)
        dataframes = remove_outliers(dataframes)
        
        # Get cleaned data
        cleaned_df = dataframes[table_name]
        
        # Write to output (THIS IS KEY)
        cleaned_df.write.mode("append").save(output_path)
    
    # Apply to each micro-batch every 10 seconds
    return df.writeStream.foreachBatch(apply_batch_cleaning).start()
```

**How it works:**
1. Every 10 seconds, check for **new files** in MinIO/mapped/
2. Process **only the new files** (incremental file processing)
3. Apply cleaning functions to the micro-batch
4. **Append** cleaned data to MinIO/cleaned/

**Result:** Cleaned data available within **10-20 seconds** ✅

---

### Phase 3: Streaming Transformation/Aggregation (🔴 **ISSUE HERE**)

**File:** `transformation/streaming_transformation.py`

```python
def apply_batch_transformation(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    # Wrap batch_df in dict format
    dataframes = {table_name: batch_df}
    
    # PROBLEM: Call aggregate functions on ONLY the micro-batch
    if table_name == "orders":
        aggregate_orders(dataframes)  # ← This expects ALL data!
```

**What `aggregate_orders` does:**

**File:** `transformation/aggregations/customers.py` (example)

```python
def aggregate_customers(dataframes):
    # Expects dataframes["orders"] to contain ALL orders, not just new ones
    orders_with_items = (
        dataframes["orders"]
        .join(dataframes["order_items"], "order_id", "inner")
        .select("order_id", "customer_id", "total_amount", ...)
    )
    
    # GROUP BY customer_id and SUM
    customer_order_agg = orders_with_items.groupBy("customer_id").agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum(col("total_amount")).alias("total_revenue"),  # ← SUM HERE
        spark_avg(col("total_amount")).alias("avg_order_value"),
        ...
    )
```

**The Problem:**

1. **Micro-batch contains:** Only NEW orders (e.g., 10 orders from last 10 seconds)
2. **Aggregation function expects:** ALL orders (e.g., 100,000 orders total)
3. **What happens:** Aggregation calculates metrics for ONLY the 10 new orders
4. **Result:** Each micro-batch produces **partial aggregates**, not **cumulative aggregates**

---

## Answering Your Specific Question

### Q: "Will the sum automatically be updated to include old + new data?"

**Answer depends on WHICH approach is used:**

### Approach 1: Current Implementation (🔴 INCORRECT for cumulative metrics)

```python
# Time 0: First batch - 3 orders
batch_1 = [
    {"customer_id": "C1", "total_amount": 100},
    {"customer_id": "C1", "total_amount": 200},
    {"customer_id": "C2", "total_amount": 150}
]
aggregate_customers({"orders": batch_1})
# Output:
#   C1: total_revenue = 300 (100 + 200)
#   C2: total_revenue = 150

# Time 10s: Second batch - 2 new orders
batch_2 = [
    {"customer_id": "C1", "total_amount": 50},  # C1's 3rd order
    {"customer_id": "C3", "total_amount": 200}  # New customer
]
aggregate_customers({"orders": batch_2})
# Output:
#   C1: total_revenue = 50  ← WRONG! Should be 350 (300 + 50)
#   C3: total_revenue = 200 ← Correct (first order)
```

**Result:** ❌ Each micro-batch produces **independent aggregates**, not cumulative

---

### Approach 2: Full Re-Aggregation (🟡 CORRECT but SLOW)

To get correct cumulative aggregates, you need to **re-read ALL data**:

```python
def apply_batch_transformation(batch_df, batch_id):
    # Don't just process the micro-batch!
    # Re-read ALL cleaned data from MinIO
    all_orders = spark.read.parquet("s3a://bucket/cleaned/orders/")
    all_order_items = spark.read.parquet("s3a://bucket/cleaned/order_items/")
    
    dataframes = {
        "orders": all_orders,         # ← ALL data
        "order_items": all_order_items  # ← ALL data
    }
    
    # Now aggregate ALL data
    aggregate_customers(dataframes)
    
    # Output replaces previous aggregates
    dataframes["customer_aggregates"].write.mode("overwrite").save(output_path)
```

**Result:** ✅ Correct cumulative aggregates, but:
- Re-reads ALL data every 10 seconds
- Re-calculates ALL aggregates every 10 seconds
- Performance degrades as data grows: O(n) where n = total records
- **NOT truly streaming** - just "fast batch"

---

### Approach 3: Stateful Streaming (✅ CORRECT and SCALABLE)

To achieve true incremental updates, you need **stateful streaming**:

```python
from pyspark.sql.functions import sum, count

def create_stateful_aggregation_stream(spark, source_path):
    # Read stream of orders
    orders_stream = spark.readStream.parquet(source_path)
    
    # Stateful aggregation using groupBy
    customer_aggregates = (
        orders_stream
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue")
        )
    )
    
    # Write with UPDATE mode (not APPEND)
    query = (
        customer_aggregates.writeStream
        .outputMode("update")  # ← KEY: Only output CHANGED aggregates
        .format("parquet")
        .option("checkpointLocation", "/checkpoints/customer_agg")
        .start(output_path)
    )
    
    return query
```

**How it works:**

```python
# Spark maintains STATE in checkpoint directory
# Time 0: First batch
# Input: [C1: 100, C1: 200, C2: 150]
# State: {C1: {count: 2, sum: 300}, C2: {count: 1, sum: 150}}
# Output: {C1: total_revenue=300, C2: total_revenue=150}

# Time 10s: Second batch
# Input: [C1: 50, C3: 200]
# State UPDATE:
#   C1: {count: 2+1=3, sum: 300+50=350}  ← Incremental update!
#   C3: {count: 1, sum: 200}
# Output: {C1: total_revenue=350, C3: total_revenue=200}  ← Only changed rows
```

**Result:** ✅ True incremental updates with O(1) processing per micro-batch

---

## Current State of PR #54

### What's Implemented ✅

1. **Streaming ingestion:** Database → Kafka → MinIO/mapped/ (working correctly)
2. **Streaming cleaning:** Incremental file processing (working correctly)
3. **Streaming transformation:** Infrastructure in place

### What's NOT Working 🔴

1. **Aggregations are batch-style:** Functions expect ALL data, not incremental batches
2. **No stateful aggregations:** Each micro-batch produces independent results
3. **No state management:** No checkpoint-based aggregation state

### What This Means for Your Question

**"If new data comes, will the sum automatically include old + new data?"**

**Current Answer:**
- ❌ **NO** - Each micro-batch produces separate aggregates
- 🟡 **UNLESS** you re-read all data every time (slow, not scalable)
- ✅ **YES** - If you implement stateful streaming aggregations (not done yet)

---

## Real-World Example

### Scenario: E-commerce Revenue Dashboard

**Goal:** Show real-time customer total revenue

**Current Implementation (PR #54):**

```
10:00:00 - Customer C1 orders $100
10:00:10 - Micro-batch processes:
           → C1: total_revenue = $100 ✅

10:00:15 - Customer C1 orders $200
10:00:20 - Micro-batch processes:
           → C1: total_revenue = $200 ❌ (should be $300)

10:00:25 - Customer C1 orders $50
10:00:30 - Micro-batch processes:
           → C1: total_revenue = $50 ❌ (should be $350)

Dashboard shows: C1: $50 (latest batch only)
Should show: C1: $350 (cumulative)
```

**With Full Re-Aggregation (Workaround):**

```
Every 10 seconds:
1. Read ALL 100,000 orders from MinIO/cleaned/
2. Re-calculate ALL customer aggregates
3. Overwrite previous results

Dashboard shows: C1: $350 ✅ (correct)
But: Re-processes 100,000 orders every 10 seconds (slow)
```

**With Stateful Streaming (Proper Solution):**

```
Spark maintains state:
- C1: {order_count: 2, total_revenue: $300}

New order: C1 orders $50
→ Spark updates state:
  C1: {order_count: 3, total_revenue: $350}
→ Outputs ONLY: C1: total_revenue = $350

Dashboard shows: C1: $350 ✅ (correct)
And: Processes only 1 new order (fast)
```

---

## Recommendations

### Immediate (This Sprint)

1. **Document the limitation:** Current implementation doesn't provide cumulative aggregates
2. **Choose approach based on use case:**
   - If data volume is small (<10K records): Use full re-aggregation
   - If data volume is large (>100K records): Implement stateful streaming

### Short-Term (Next 2-4 Weeks)

1. **Refactor aggregation functions** to support stateful streaming:
   ```python
   # Instead of:
   def aggregate_customers(dataframes):
       # Batch-style aggregation
   
   # Create:
   def create_customer_aggregation_stream(spark, source_path):
       # Stateful streaming aggregation
   ```

2. **Implement state management:**
   - Use Spark's built-in state management
   - Configure checkpoint directories
   - Use `outputMode("update")` or `outputMode("complete")`

3. **Handle late-arriving data:**
   - Configure watermarking for event-time processing
   - Define allowed lateness (e.g., 1 hour)

### Long-Term (1-3 Months)

1. **Migrate complex aggregations** to stateful streaming:
   - RFM segmentation with running metrics
   - Product affinity with incremental similarity
   - Time-based aggregations with windowing

2. **Optimize state storage:**
   - Use RocksDB for large state
   - Configure state retention policies
   - Implement state cleanup for old data

---

## Code Examples

### Example 1: Simple Stateful Aggregation

```python
def create_customer_revenue_stream(spark, orders_path, output_path):
    """
    Calculate cumulative customer revenue using stateful streaming.
    New orders automatically update existing customer totals.
    """
    from pyspark.sql.functions import sum, count, avg
    
    # Read streaming orders
    orders = spark.readStream.parquet(orders_path)
    
    # Stateful aggregation
    customer_agg = (
        orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        )
    )
    
    # Write with update mode (only changed rows)
    query = (
        customer_agg.writeStream
        .outputMode("update")  # Key: incremental updates
        .format("parquet")
        .option("checkpointLocation", "/checkpoints/customer_revenue")
        .trigger(processingTime="10 seconds")
        .start(output_path)
    )
    
    return query
```

**How this solves your question:**
- ✅ New orders automatically add to existing customer totals
- ✅ Sum includes old + new data (maintained in Spark state)
- ✅ Only processes new records (O(1) per micro-batch)
- ✅ Outputs only changed customer aggregates

### Example 2: Time-Windowed Aggregation

```python
def create_hourly_sales_stream(spark, orders_path, output_path):
    """
    Calculate hourly sales using tumbling windows.
    Automatically closes windows and outputs final results.
    """
    from pyspark.sql.functions import window, sum
    
    orders = spark.readStream.parquet(orders_path)
    
    # Tumbling 1-hour windows
    hourly_sales = (
        orders
        .withWatermark("order_placed_at", "1 hour")  # Allow 1hr late data
        .groupBy(
            window("order_placed_at", "1 hour"),
            "product_category"
        )
        .agg(sum("total_amount").alias("hourly_revenue"))
    )
    
    query = (
        hourly_sales.writeStream
        .outputMode("append")  # Output complete windows only
        .format("parquet")
        .option("checkpointLocation", "/checkpoints/hourly_sales")
        .trigger(processingTime="10 seconds")
        .start(output_path)
    )
    
    return query
```

### Example 3: Merging with Existing Aggregates

```python
def update_customer_aggregates_incremental(batch_df, batch_id):
    """
    Merge new batch aggregates with existing cumulative aggregates.
    This is a workaround if you can't use stateful streaming.
    """
    from pyspark.sql.functions import coalesce, sum, col
    
    # Calculate aggregates for this batch
    new_agg = (
        batch_df
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("batch_orders"),
            sum("total_amount").alias("batch_revenue")
        )
    )
    
    # Read existing aggregates
    try:
        existing_agg = spark.read.parquet(output_path)
    except:
        # First batch - no existing aggregates
        new_agg.write.mode("overwrite").parquet(output_path)
        return
    
    # Merge new with existing
    merged_agg = (
        existing_agg
        .join(new_agg, "customer_id", "full_outer")
        .select(
            coalesce(existing_agg.customer_id, new_agg.customer_id).alias("customer_id"),
            (coalesce(existing_agg.total_orders, lit(0)) + 
             coalesce(new_agg.batch_orders, lit(0))).alias("total_orders"),
            (coalesce(existing_agg.total_revenue, lit(0)) + 
             coalesce(new_agg.batch_revenue, lit(0))).alias("total_revenue")
        )
    )
    
    # Overwrite with merged results
    merged_agg.write.mode("overwrite").parquet(output_path)
```

**⚠️ Warning:** This approach has race conditions if multiple batches process concurrently!

---

## Summary: Direct Answer to Your Question

### Question Breakdown

**"If a new microbatch comes in real time via database URI or API endpoint..."**
- ✅ **Current state:** Yes, data arrives in real-time (10-second polling)

**"...then during aggregation and analysis will the new data completely integrate with existing data?"**
- ❌ **Current implementation:** No, each micro-batch produces independent aggregates
- 🟡 **With workaround:** Yes, but requires re-processing ALL data (slow)
- ✅ **With stateful streaming:** Yes, automatic incremental integration

**"For example, suppose in aggregation we are summing a column and new data comes, then will the sum automatically be updated for the new data?"**
- ❌ **Current answer:** No, the sum will only reflect the NEW data, not old + new

**"Meaning old data plus new data will be included in the sum or not?"**
- ❌ **Current state:** Not automatically
- ✅ **Required:** Implement stateful streaming aggregations (see examples above)

---

## Migration Path

### Phase 1: Document & Assess
- [x] Document current behavior
- [ ] Identify which aggregations need cumulative state
- [ ] Measure current data volumes and growth rate

### Phase 2: Implement Stateful Streaming
- [ ] Refactor simple aggregations (sums, counts, averages)
- [ ] Test with small data volumes
- [ ] Configure checkpointing and state management

### Phase 3: Migrate Complex Aggregations
- [ ] Refactor RFM segmentation for streaming
- [ ] Implement incremental product affinity
- [ ] Add time-windowed aggregations

### Phase 4: Production Deployment
- [ ] Performance testing with production data volumes
- [ ] Monitor state size and memory usage
- [ ] Implement state cleanup policies

---

## Conclusion

**The short answer:** In the **current PR #54 implementation**, new micro-batch data does **NOT automatically integrate** with existing aggregated data for cumulative metrics like sums.

**The solution:** Implement **stateful streaming aggregations** using Spark Structured Streaming's built-in state management to achieve true incremental updates where new data automatically adds to existing totals.

**The recommendation:** 
1. For **Phase 2.5 (current):** Document the limitation and use full re-aggregation if data volumes are manageable
2. For **Phase 3 (next sprint):** Implement stateful streaming aggregations for critical metrics
3. For **Phase 4 (production):** Migrate all aggregations to stateful streaming for true real-time analytics

---

**References:**
- Current implementation: `cleaning/streaming_cleaning.py`, `transformation/streaming_transformation.py`
- Aggregation functions: `transformation/aggregations/*.py`
- Spark Structured Streaming docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- State management: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#arbitrary-stateful-operations
