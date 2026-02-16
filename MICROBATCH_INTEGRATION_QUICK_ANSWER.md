# Micro-Batch Data Integration - Quick Answer

**Date:** February 16, 2026

---

## Your Question

> "If a new microbatch comes in real time via database URI or API endpoint, then during aggregation and analysis will the new data completely integrate with existing data? For example, suppose in aggregation we are summing a column and new data comes, then will the sum automatically be updated for the new data, meaning old data plus new data will be included in the sum or not?"

---

## Short Answer

**❌ NO** - In the current PR #54 implementation, **new data does NOT automatically integrate with existing aggregated data**.

**Why?** The aggregation functions calculate metrics on **each micro-batch independently**, not cumulatively.

**Example:**
```
Initial state: Customer C1 has total_revenue = $300
New order arrives: Customer C1 orders $50

❌ Current behavior: New micro-batch shows C1 total_revenue = $50
✅ Expected behavior: Should show C1 total_revenue = $350 ($300 + $50)
```

---

## What's Actually Happening

### 1. Data Flow (✅ Works Correctly)

```
Database → Polling (10s) → Kafka → Spark Streaming → MinIO/mapped/
                                    (500ms batches)     (CSV files)
```

**Result:** New data arrives in MinIO within 10-15 seconds ✅

### 2. Cleaning (✅ Works Correctly)

```python
# Every 10 seconds, process NEW files only
new_files = ["orders_batch_001.csv"]
cleaned_data = clean(new_files)
# Append to MinIO/cleaned/
```

**Result:** Incremental file processing works correctly ✅

### 3. Aggregation (🔴 **PROBLEM HERE**)

```python
# Current implementation in streaming_transformation.py
def apply_batch_transformation(batch_df, batch_id):
    # batch_df contains ONLY new orders from last 10 seconds
    dataframes = {"orders": batch_df}
    
    # Aggregation function expects ALL data, but gets only new batch!
    aggregate_customers(dataframes)  # ← Calculates metrics for NEW data only
```

**What `aggregate_customers` does:**

```python
def aggregate_customers(dataframes):
    customer_agg = (
        dataframes["orders"]  # ← Contains ONLY new orders!
        .groupBy("customer_id")
        .agg(
            sum("total_amount").alias("total_revenue")  # ← Sums ONLY new orders
        )
    )
```

**Result:** Each micro-batch produces **independent aggregates**, not cumulative ❌

---

## Concrete Example

### Scenario: Customer Total Revenue

**Timeline:**

```
Time 10:00:00 - Customer C1 places order for $100
Time 10:00:10 - Micro-batch 1 processes:
    Input: [{customer_id: "C1", total_amount: 100}]
    Output: C1: total_revenue = $100 ✅

Time 10:00:15 - Customer C1 places order for $200  
Time 10:00:20 - Micro-batch 2 processes:
    Input: [{customer_id: "C1", total_amount: 200}]
    Output: C1: total_revenue = $200 ❌ (should be $300)

Time 10:00:25 - Customer C1 places order for $50
Time 10:00:30 - Micro-batch 3 processes:
    Input: [{customer_id: "C1", total_amount: 50}]
    Output: C1: total_revenue = $50 ❌ (should be $350)
```

**Dashboard shows:** C1 total_revenue = $50 (latest batch only)  
**Should show:** C1 total_revenue = $350 (cumulative total)

---

## Why This Happens

The aggregation functions in `transformation/aggregations/*.py` were designed for **batch processing**:

**Batch Processing (Original Design):**
```python
# Load ALL orders from MinIO
all_orders = spark.read.parquet("s3a://bucket/cleaned/orders/")

# Calculate aggregates on ALL data
aggregate_customers({"orders": all_orders})

# Output shows cumulative metrics for all time
```

**Streaming Processing (Current PR #54):**
```python
# Receive ONLY new orders from micro-batch
new_orders = micro_batch_dataframe  # Only last 10 seconds of data

# Calculate aggregates on NEW data only
aggregate_customers({"orders": new_orders})

# Output shows metrics for this batch only (not cumulative)
```

---

## Solutions

### Solution 1: Full Re-Aggregation (🟡 Simple but Slow)

Re-read ALL data every time:

```python
def apply_batch_transformation(batch_df, batch_id):
    # Re-read ALL data from MinIO (not just the batch)
    all_orders = spark.read.parquet("s3a://bucket/cleaned/orders/")
    all_order_items = spark.read.parquet("s3a://bucket/cleaned/order_items/")
    
    dataframes = {
        "orders": all_orders,
        "order_items": all_order_items
    }
    
    # Calculate aggregates on ALL data
    aggregate_customers(dataframes)
    
    # Overwrite previous results
    output.write.mode("overwrite").save()
```

**Pros:**
- ✅ Simple to implement (just change data loading)
- ✅ Produces correct cumulative aggregates
- ✅ No code changes to aggregation functions

**Cons:**
- ❌ Re-processes ALL data every 10 seconds (O(n) where n = total records)
- ❌ Performance degrades as data grows (100K orders → 10 min processing)
- ❌ Not scalable for large datasets
- ❌ Not truly "streaming" - just fast batch

**When to use:** Small datasets (<10K records) or infrequent processing

---

### Solution 2: Stateful Streaming (✅ Correct and Scalable)

Use Spark's built-in state management:

```python
def create_customer_revenue_stream(spark, orders_path, output_path):
    # Read streaming orders
    orders = spark.readStream.parquet(orders_path)
    
    # Stateful aggregation - Spark maintains cumulative state
    customer_agg = (
        orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue")  # ← Cumulative sum!
        )
    )
    
    # Write with UPDATE mode - only changed rows
    query = (
        customer_agg.writeStream
        .outputMode("update")  # ← Key: Spark maintains state
        .format("parquet")
        .option("checkpointLocation", "/checkpoints/customer_revenue")
        .trigger(processingTime="10 seconds")
        .start(output_path)
    )
    
    return query
```

**How it works:**

```
Time 10:00:00 - First batch processes:
    Input: [C1: $100]
    Spark State: {C1: {count: 1, sum: 100}}
    Output: {C1: total_revenue: $100}

Time 10:00:10 - Second batch processes:
    Input: [C1: $200]
    Spark State UPDATE: {C1: {count: 2, sum: 300}}  ← Automatic merge!
    Output: {C1: total_revenue: $300}  ← Correct cumulative

Time 10:00:20 - Third batch processes:
    Input: [C1: $50]
    Spark State UPDATE: {C1: {count: 3, sum: 350}}  ← Automatic merge!
    Output: {C1: total_revenue: $350}  ← Correct cumulative
```

**Pros:**
- ✅ Produces correct cumulative aggregates
- ✅ Only processes new records (O(1) per micro-batch)
- ✅ Scalable for large datasets
- ✅ True streaming - real-time updates
- ✅ Automatic state management by Spark

**Cons:**
- ❌ Requires refactoring aggregation functions
- ❌ More complex to implement (state management, checkpointing)
- ❌ Needs careful handling of late-arriving data (watermarking)

**When to use:** Production systems, large datasets (>100K records), real-time dashboards

---

## What You Need to Do

### Option A: Keep Current Implementation (Not Recommended)

**Use case:** Testing, demo, very small datasets

**Limitation:** Aggregates only show latest batch, not cumulative totals

**Action:** Document that this is a limitation and not production-ready

---

### Option B: Implement Full Re-Aggregation (Temporary Solution)

**Use case:** Small to medium datasets (<50K records), can tolerate 1-2 minute latency

**Implementation:**
1. Modify `streaming_transformation.py` to load ALL data instead of just batch
2. Accept that processing time increases as data grows
3. Plan to migrate to stateful streaming later

**Time:** 2-4 hours

---

### Option C: Implement Stateful Streaming (Recommended)

**Use case:** Production systems, large datasets, real-time requirements

**Implementation:**
1. Refactor aggregation functions to use Spark's streaming aggregations
2. Configure state management and checkpointing
3. Test with production-scale data

**Time:** 1-2 weeks (includes testing)

**Example commit plan:**
- Phase 1: Simple aggregations (sums, counts, averages)
- Phase 2: Complex aggregations (RFM, product affinity)
- Phase 3: Time-windowed aggregations (hourly, daily)

---

## Recommendation Summary

| Aspect | Current State | Recommendation |
|--------|--------------|----------------|
| **Data ingestion** | ✅ Works (10s latency) | Keep as-is |
| **Cleaning** | ✅ Works (incremental) | Keep as-is |
| **Aggregation** | 🔴 Batch-only logic | **Refactor to stateful streaming** |
| **Timeline** | Immediate | Within 2-4 weeks |
| **Priority** | Blocking for production | **High priority** |

---

## Key Takeaway

**Your Question:** "Will new data integrate with existing data in aggregations?"

**Current Answer:** ❌ **NO** - Each micro-batch produces independent aggregates

**Required Fix:** Implement stateful streaming aggregations so Spark automatically maintains cumulative state

**Impact:** Without this fix, dashboards will show incorrect metrics (only latest batch instead of all-time totals)

---

## Next Steps

1. **Immediate:** Read full documentation in `MICROBATCH_DATA_INTEGRATION_EXPLAINED.md`
2. **This week:** Decide between Option B (temporary) or Option C (production)
3. **Next sprint:** Implement chosen solution
4. **Testing:** Validate that sums include old + new data correctly

---

**For detailed examples, code samples, and migration guide, see:**
- `MICROBATCH_DATA_INTEGRATION_EXPLAINED.md` (comprehensive 500+ line document)

**For questions, contact:** Your streaming architecture team or Spark Structured Streaming experts
