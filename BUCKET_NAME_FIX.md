# Bucket Name Fix - Transformation Phase

## Issue Summary
The transformation phase was successfully processing data but storing output in the default 'pulse-bucket-1' bucket instead of the business-specific bucket (business_id). This prevented proper data isolation between different businesses.

## Problem Details

### What Was Happening
```
User Business ID: 01f21f39-47c2-4694-a984-c05eaad6c3fe

Expected Output Location:
✓ 01f21f39-47c2-4694-a984-c05eaad6c3fe/transformed/agg_customers.parquet
✓ 01f21f39-47c2-4694-a984-c05eaad6c3fe/transformed/agg_orders.parquet
...

Actual Output Location:
✗ pulse-bucket-1/transformed/agg_customers.parquet
✗ pulse-bucket-1/transformed/agg_orders.parquet
...
```

### Root Cause

**File:** `transformation/transformation.py`

The function signature was correct:
```python
def main(bucket_name=None):
    """
    Args:
        bucket_name: MinIO bucket name (business_id). If None, uses default from config.
    """
```

And the bucket was used for loading data:
```python
dataframes = load_data_from_minio(spark, minio_client, bucket_name)
```

But the `export_to_minio()` call was missing the bucket_name parameter:
```python
# BEFORE (Wrong - line 91-98)
export_to_minio(
    dataframes,                    # ✓ Has dataframes
    # bucket_name missing here!    # ✗ Missing bucket_name!
    sql_schema_path=sql_schema_path,
    enforce_schemas=True,
    preserve_types=True,
    compression='snappy'
)
```

This caused the export function to fall back to its default:
```python
# From transformation/exporters/minio_exporter.py line 97
bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "pulse-bucket-1")
```

## Solution

Added the `bucket_name` parameter to the `export_to_minio()` call:

```python
# AFTER (Correct - line 91-98)
export_to_minio(
    dataframes,
    bucket_name=bucket_name,        # ✓ Now passes business_id bucket
    sql_schema_path=sql_schema_path,
    enforce_schemas=True,
    preserve_types=True,
    compression='snappy'
)
```

## Data Flow - Before and After

### Before Fix (Broken Multi-Tenancy)
```
Pipeline Service
    ↓
Execute: python3 transformation.py --bucket-name 01f21f39-47c2-4694-a984-c05eaad6c3fe
    ↓
Load from: 01f21f39-47c2-4694-a984-c05eaad6c3fe/cleaned/  ✓ Correct
    ↓
Process transformations...
    ↓
Export to: pulse-bucket-1/transformed/  ✗ Wrong! (all businesses mixed)
```

### After Fix (Proper Multi-Tenancy)
```
Pipeline Service
    ↓
Execute: python3 transformation.py --bucket-name 01f21f39-47c2-4694-a984-c05eaad6c3fe
    ↓
Load from: 01f21f39-47c2-4694-a984-c05eaad6c3fe/cleaned/  ✓ Correct
    ↓
Process transformations...
    ↓
Export to: 01f21f39-47c2-4694-a984-c05eaad6c3fe/transformed/  ✓ Correct!
```

## Complete Pipeline Data Isolation

After this fix, all phases properly use business_id:

```
Business Bucket: 01f21f39-47c2-4694-a984-c05eaad6c3fe/
├── cleaned/           ✓ Phase 1: Cleaning
│   ├── customers.parquet
│   ├── orders.parquet
│   └── ...
├── transformed/       ✓ Phase 2: Transformation (NOW FIXED!)
│   ├── agg_customers.parquet
│   ├── agg_orders.parquet
│   └── ...
├── analytics/         ✓ Phase 3: Analysis
│   ├── kpis/
│   ├── customer_analytics/
│   └── ...
└── machine-learning/  ✓ Phase 4: ML Inference
    ├── general/
    └── specific/
```

## Verification of All Phases

### Phase 1: Cleaning ✓
**File:** `cleaning/cleaning.py`
```python
def main(bucket_name):  # Requires bucket_name
    save_to_minio(df, f"{bucket_name}", "cleaned", table_name)
```
Status: Already correct

### Phase 2: Transformation ✓ (FIXED)
**File:** `transformation/transformation.py`
```python
export_to_minio(
    dataframes,
    bucket_name=bucket_name,  # ✓ Now passes bucket_name
    ...
)
```
Status: Fixed in this commit

### Phase 3: Analysis ✓
**File:** `analysis/analysis.py`
```python
dataframes = get_agg_tables(spark, bucket_name=bucket_name)
export_analytics_to_minio(..., business_id=bucket_name, ...)
```
Status: Already correct

### Phase 4: Machine Learning ✓
**File:** `machine-learning/infer_all.py`
```python
general_infer(args.bucket_name)
specific_infer(args.bucket_name)
```
Status: Already correct

## Impact

### Multi-Tenancy Benefits
1. **Data Isolation**: Each business's data stays in their own bucket
2. **Security**: No cross-contamination between businesses
3. **Compliance**: Proper data segregation for privacy regulations
4. **Cleanup**: Easy to delete all data for a single business
5. **Scalability**: No single bucket bottleneck

### Expected Log Output

**After Fix:**
```
🚀 Starting transformation pipeline - Using bucket: 01f21f39-47c2-4694-a984-c05eaad6c3fe

📤 EXPORTING TRANSFORMED DATA TO MINIO (PARQUET)
Bucket: 01f21f39-47c2-4694-a984-c05eaad6c3fe  ✓ Correct!
Directory: transformed/
Format: Parquet
...
✅ agg_customers: 10,000 rows saved (1.25 MB)
✅ agg_orders: 13,889 rows saved (1.85 MB)
...
```

## Testing Checklist

- [x] Python syntax validated
- [x] Parameter correctly passed to export function
- [ ] Integration test: Run transformation phase
- [ ] Verify output is in business bucket, not pulse-bucket-1
- [ ] Check MinIO browser: `{business_id}/transformed/` exists
- [ ] Verify subsequent phases can read from business bucket
- [ ] Test with multiple businesses to confirm isolation

## Migration Notes

If you already have data in `pulse-bucket-1/transformed/`:

1. **Identify which business** the data belongs to
2. **Copy data** to correct business bucket:
   ```bash
   mc cp --recursive pulse-bucket-1/transformed/ {business_id}/transformed/
   ```
3. **Verify** analysis phase can read from new location
4. **Clean up** old location:
   ```bash
   mc rm --recursive --force pulse-bucket-1/transformed/
   ```

## Related Issues

This fix is part of ensuring consistent bucket usage across all pipeline phases:
- ✓ Database column name fix (progress_percentage)
- ✓ Database connection management (background tasks)
- ✓ WebSocket stability improvements
- ✓ Spark local mode configuration
- ✓ **Bucket name consistency (this fix)**

## Files Changed
- `transformation/transformation.py` - Added bucket_name parameter to export_to_minio() call

## Conclusion

This simple one-line fix ensures proper multi-tenancy by storing each business's transformed data in their own isolated bucket. All four pipeline phases now correctly use the business_id as the bucket name.
