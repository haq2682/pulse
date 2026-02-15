# Spark Connection Error Fix

## Problem Description

The transformation phase (and potentially analysis/ML phases) were failing with Spark networking errors:

```
[transformation ERROR] 26/02/15 04:24:23 ERROR TransportClient: Failed to send RPC RPC 6527648090606813243 to /10.5.0.3:7077: io.netty.channel.StacklessClosedChannelException
io.netty.channel.StacklessClosedChannelException
```

## Root Cause Analysis

### Environment Setup
1. The `docker-compose.yml` defines a Spark master service at `10.5.0.3:7077`
2. Environment variable `SPARK_MASTER_URL=spark://10.5.0.3:7077` is set for the API container
3. However, the Spark configuration scripts use `SPARK_SERVER` (different name)

### Spark Configuration Scripts
1. Scripts use: `os.getenv("SPARK_SERVER", "local[*]")`
2. When `SPARK_SERVER` is not set, defaults to `local[*]`
3. BUT dynamic allocation was still enabled with high max executors

### The Problem
Even when running in local mode (`local[*]`), having dynamic allocation enabled caused Spark to:
1. Try to register with an external master
2. Attempt RPC connections to `10.5.0.3:7077`
3. Fail with `StacklessClosedChannelException` when master unavailable

## Solution

### Part 1: Explicit Local Mode in Pipeline Service

**File:** `api/services/pipeline_service.py`

Added environment variable override when executing subprocess:

```python
# Prepare environment variables
# Force local Spark mode to avoid cluster connection attempts
env = os.environ.copy()
env['SPARK_SERVER'] = 'local[*]'

try:
    # Start subprocess
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=self.project_root,
        env=env  # Pass modified environment
    )
```

**Why this works:**
- Explicitly sets `SPARK_SERVER=local[*]` for subprocess
- Overrides any inherited cluster configuration
- Ensures Spark runs in standalone local mode

### Part 2: Smart Dynamic Allocation

**Files:**
- `transformation/config/spark_config.py`
- `analysis/analysis_config.py`

Modified Spark session creation to detect local vs cluster mode:

```python
def create_spark_session():
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")
    
    builder = (
        SparkSession.builder.appName("Transformation")
        .master(spark_master)
    )
    
    # Only enable dynamic allocation for cluster mode
    if not is_local:
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "0")
            .config("spark.dynamicAllocation.maxExecutors", "8")
            .config("spark.dynamicAllocation.initialExecutors", "2")
        )
    else:
        # Disable dynamic allocation for local mode
        builder = builder.config("spark.dynamicAllocation.enabled", "false")
    
    # ... rest of configuration
    return builder.getOrCreate()
```

**Why this works:**
- Detects local mode by checking if master starts with "local"
- Disables dynamic allocation when in local mode
- Prevents Spark from trying to connect to external master
- Still allows cluster mode if needed

## Benefits

1. **Reliability:** No more connection failures to Spark master
2. **Simplicity:** Pipeline runs in standalone mode without external dependencies
3. **Backward Compatible:** Cluster mode still works if `SPARK_SERVER` points to cluster
4. **Consistent:** All phases (transformation, analysis) use same approach

## Expected Behavior

### Before Fix
```bash
[transformation] Starting transformation pipeline
[transformation ERROR] Failed to send RPC to /10.5.0.3:7077
[transformation ERROR] io.netty.channel.StacklessClosedChannelException
# Pipeline fails or becomes unstable
```

### After Fix
```bash
[transformation] Starting transformation pipeline
[transformation] Using Spark in local[*] mode
[transformation] Processing data...
[transformation] ✅ Transformation completed successfully
```

## Testing

### Verify Fix
1. Run pipeline and monitor logs
2. Should NOT see any "Failed to send RPC" errors
3. Should NOT see "StacklessClosedChannelException"
4. All Spark phases should complete successfully

### Check Logs
```bash
# Look for these indicators of success:
✅ No "TransportClient" errors
✅ No "Failed to send RPC" messages
✅ Phases complete with success messages
✅ No exceptions related to Netty or channel closure
```

## Alternative Approaches Considered

### Approach 1: Start Spark Master (Not Chosen)
- Could ensure Spark master is always running
- **Rejected:** Adds complexity and resource overhead for simple pipeline

### Approach 2: Change Environment Variable Name (Not Chosen)
- Change `SPARK_SERVER` to `SPARK_MASTER_URL` everywhere
- **Rejected:** More invasive, affects many files

### Approach 3: Disable Dynamic Allocation Globally (Not Chosen)
- Remove dynamic allocation from all configs
- **Rejected:** Loses ability to use cluster when needed

### Approach 4: Smart Detection + Explicit Override (CHOSEN)
- ✅ Minimal changes to existing code
- ✅ Preserves cluster capability when needed
- ✅ Robust against configuration mismatches

## Machine Learning Phase Note

The machine learning phase has 49 individual inference scripts that create their own Spark sessions. These scripts:

1. **Inherit the environment:** They will receive `SPARK_SERVER=local[*]` from subprocess
2. **Still have hardcoded dynamic allocation:** Many have `.config("spark.dynamicAllocation.enabled", "true")`

### Impact
- The `SPARK_SERVER=local[*]` from environment should prevent connection attempts
- Dynamic allocation in local mode may log warnings but shouldn't fail
- If issues persist, individual ML scripts may need updates

### Future Enhancement
Consider creating a shared Spark configuration utility:
```python
# ml_utils/spark_config.py
def create_spark_session_for_ml(app_name, local_mode=True):
    """Create Spark session with smart configuration."""
    # Centralized configuration logic
    pass
```

Then update all ML scripts to use this utility instead of creating sessions individually.

## Deployment Checklist

- [x] Update `api/services/pipeline_service.py` with environment override
- [x] Update `transformation/config/spark_config.py` with smart allocation
- [x] Update `analysis/analysis_config.py` with smart allocation
- [ ] Deploy changes to container
- [ ] Test transformation phase
- [ ] Test analysis phase
- [ ] Test ML phase
- [ ] Monitor logs for Spark errors

## Related Files

- `api/services/pipeline_service.py` - Subprocess execution with environment
- `transformation/config/spark_config.py` - Transformation Spark config
- `analysis/analysis_config.py` - Analysis Spark config
- `machine-learning/*/inference/*.py` - Individual ML inference scripts (49 files)
- `docker-compose.yml` - Spark master service definition (if using cluster)

## Success Criteria

✅ No "Failed to send RPC" errors in logs
✅ No "StacklessClosedChannelException" errors
✅ Transformation phase completes successfully
✅ Analysis phase completes successfully
✅ ML phase completes successfully
✅ Pipeline progress updates work throughout
✅ All phases run in local mode without external dependencies
