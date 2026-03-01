"""Spark Structured Streaming - Kafka Consumer with Existing Map Integration"""

import os
import sys

# ---------------------------------------------------------------------------
# JAR paths — baked into the repo at mapping/streaming/jars/ so they are
# available inside the api container via the ./mapping volume mount at
# /app/mapping/streaming/jars/ without requiring any image rebuild.
# ---------------------------------------------------------------------------
_SPARK_JARS_DIR = "/app/jars"
_KAFKA_JARS = [
    f"{_SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"{_SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    f"{_SPARK_JARS_DIR}/kafka-clients-3.4.0.jar",
    f"{_SPARK_JARS_DIR}/commons-pool2-2.11.1.jar",
]
_S3_JARS = [
    f"{_SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
]
_ALL_JARS = ",".join(_KAFKA_JARS + _S3_JARS)   # spark.jars / --jars (executors)
_ALL_JARS_CP = ":".join(_KAFKA_JARS + _S3_JARS)  # classpath separator

# PYSPARK_SUBMIT_ARGS must be set BEFORE pyspark is imported.
# PySpark's launch_gateway() reads this env var first when spawning the
# driver JVM — it is the only reliable way to add JARs to the driver
# classpath when using pip-installed PySpark (as opposed to spark-submit).
# spark.driver.extraClassPath set in SparkSession.builder can miss because
# the gateway may have been initialised earlier in the process lifetime.
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_ALL_JARS_CP} "
        f"--jars {_ALL_JARS} "
        "pyspark-shell"
    )

# findspark is only needed when Spark is installed as a standalone binary
# distribution (SPARK_HOME set).  When pyspark is installed via pip it bundles
# py4j itself and findspark.init() trips over the missing SPARK_HOME/python/lib
# directory.  Call it only when SPARK_HOME is explicitly configured.
if os.environ.get("SPARK_HOME"):
    import findspark
    findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, map_keys
from pyspark.sql.types import StructType, StructField, StringType, MapType
from minio import Minio
from dotenv import load_dotenv, find_dotenv

# Add parent directory to path to import from mapping root
# This allows importing map.py and List.py from the mapping directory
mapping_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if mapping_dir not in sys.path:
    sys.path.insert(0, mapping_dir)

from map import process_all_dataframes, save_dataframes_to_minio, COLUMNS_INFO
import List as mapping_list
from utils.helpers import parse_minio_endpoint

load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# TTL for the per-business "first batch done" Redis flag (7 days).
_FIRST_BATCH_FLAG_TTL_SECONDS = 86400 * 7

# TTL for mapping results stored in Redis (24 hours).
_MAPPING_RESULTS_TTL_SECONDS = 86400


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka and S3 support"""
    return (
        SparkSession.builder
        .appName("StreamingNormalization")
        .master("spark://10.5.0.3:7077")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "8")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        # Use pre-downloaded local JARs instead of spark.jars.packages so that
        # no Maven/Ivy network resolution is needed at startup.  spark.jars
        # distributes the files to executors; spark.driver.extraClassPath makes
        # them available in the driver JVM (this api container) so that
        # readStream.format("kafka") can locate the data-source provider.
        .config("spark.jars", _ALL_JARS)
        .config("spark.driver.extraClassPath", _ALL_JARS_CP)
        .config("spark.executor.extraClassPath", _ALL_JARS_CP)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def get_canonical_schema() -> StructType:
    """
    Output schema for the *normalized* rows produced by normalize_message_row.
    Used when constructing the normalized DataFrame inside extract_table_dataframes.
    The Kafka-level parsing is now done entirely in Python (see read_kafka_stream /
    normalize_message_row) so this schema is no longer used for from_json.
    """
    return StructType([
        StructField("source_type", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("table", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("payload", MapType(StringType(), StringType()), True),
    ])


def normalize_message_row(json_str):
    """
    Parse a raw Kafka JSON string and normalise to canonical format.

    Handles two wire formats:

    1. **Debezium format** (op field present):
       - RDBMS connectors (Postgres, MySQL …): ``after``/``before`` are nested
         JSON objects  → ``{"field": value, …}``.
       - MongoDB connector: ``after``/``before`` are JSON-encoded *strings*
         → ``"{\\"field\\": value, …}"``  (double-encoded).
       Both cases are handled by checking ``isinstance(after, str)`` and
       JSON-decoding when necessary.

    2. **Canonical format** (no op field): already in the expected structure.

    Returns None for tombstone events (op present but both after and before
    are empty/null) so the caller can filter them out.
    """
    if not json_str:
        return None

    import json as _json

    try:
        row = _json.loads(json_str)
    except (ValueError, TypeError):
        return None

    def _to_str_map(obj):
        """Flatten a dict to {str: str}, JSON-encoding non-scalar values."""
        if not isinstance(obj, dict):
            return {}
        out = {}
        for k, v in obj.items():
            if v is None:
                out[str(k)] = None
            elif isinstance(v, (dict, list)):
                out[str(k)] = _json.dumps(v, default=str)
            else:
                out[str(k)] = str(v)
        return out

    # ------------------------------------------------------------------
    # Debezium envelope
    # ------------------------------------------------------------------
    if "op" in row:
        op_map = {"c": "c", "u": "u", "d": "d", "r": "r"}

        source = row.get("source") or {}
        if isinstance(source, str):
            try:
                source = _json.loads(source)
            except Exception:
                source = {}

        after  = row.get("after")
        before = row.get("before")

        # MongoDB: after/before arrive as a JSON-encoded *string*.
        # RDBMS:   after/before are already a nested dict.
        if isinstance(after, str):
            try:
                after = _json.loads(after)
            except Exception:
                after = None
        if isinstance(before, str):
            try:
                before = _json.loads(before)
            except Exception:
                before = None

        payload_obj = after or before
        # Tombstone: no meaningful payload after decoding.
        if not payload_obj:
            return None

        return {
            "source_type": "db",
            "vendor": "debezium",
            "table": source.get("table") or source.get("collection"),
            "schema_version": "v1",
            "operation": op_map.get(row.get("op"), "c"),
            "payload": _to_str_map(payload_obj),
        }

    # ------------------------------------------------------------------
    # Canonical format
    # ------------------------------------------------------------------
    payload = row.get("payload")
    if isinstance(payload, str):
        try:
            payload = _json.loads(payload)
        except Exception:
            payload = {}
    return {
        "source_type": row.get("source_type"),
        "vendor":      row.get("vendor"),
        "table":       row.get("table"),
        "schema_version": row.get("schema_version"),
        "operation":   row.get("operation", "c"),
        "payload":     _to_str_map(payload) if isinstance(payload, dict) else (payload or {}),
    }


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Read from Kafka ecom.* topics with CDC support for database ingestion.
    
    Topic naming convention:
    - 'ecom.*' topics: Messages from API/DB ingestion (e.g., ecom.customers, ecom.orders)
    
    Database ingestion messages include an optional 'operation' field for CDC:
    - 'c' or 'create': New record
    - 'u' or 'update': Updated record  
    - 'd' or 'delete': Deleted record
    - 'r' or 'read': Snapshot/initial load

    Returns a DataFrame with a single column ``json_str`` containing the raw
    Kafka message value as a UTF-8 string.  All JSON parsing is deferred to
    Python in ``normalize_message_row`` so that both Debezium MongoDB
    (string-encoded ``after``/``before``) and Debezium RDBMS (object-encoded
    ``after``/``before``) are handled correctly without schema mismatch.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", "ecom\\..*")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
    )


def extract_table_dataframes(batch_df: DataFrame) -> tuple:
    """
    Extract DataFrames per table from batch with CDC operation info.
    Handles both canonical and Debezium message formats (RDBMS + MongoDB).

    Returns:
        tuple: (all_dataframes dict, operation dict mapping table to CDC operation)
    """
    if batch_df.rdd.isEmpty():
        return {}, {}

    # Parse raw JSON strings entirely in Python so we can handle both:
    #   • Debezium MongoDB  – after/before are JSON-encoded strings
    #   • Debezium RDBMS    – after/before are nested JSON objects
    #   • Canonical format  – flat structure with payload map
    normalized_rdd = (
        batch_df.rdd
        .map(lambda row: normalize_message_row(row["json_str"]))
        .filter(lambda r: r is not None)
    )

    # Convert to DataFrame
    normalized_schema = StructType([
        StructField("source_type", StringType()),
        StructField("vendor", StringType()),
        StructField("table", StringType()),
        StructField("schema_version", StringType()),
        StructField("operation", StringType()),
        StructField("payload", MapType(StringType(), StringType()))
    ])

    normalized_df = batch_df.sparkSession.createDataFrame(normalized_rdd, normalized_schema)

    table_names = [
        row["table"]
        for row in normalized_df.select("table").distinct().collect()
        if row["table"] is not None
    ]
    all_dataframes = {}
    operations = {}

    for table_name in table_names:
        table_df = normalized_df.filter(col("table") == table_name)

        # Get payload keys and operation from first row
        sample = table_df.select("payload", "operation").limit(1).collect()

        if not sample:
            continue

        payload_keys = list(sample[0]["payload"].keys())

        # Union keys from all rows in the table using Spark's map_keys so that
        # schema variation (e.g. partial Debezium updates) doesn't cause columns
        # present in later rows to be silently dropped.  This collects only the
        # distinct key strings — not the full rows — to the driver.
        all_keys = [
            row[0]
            for row in table_df.select(explode(map_keys(col("payload"))).alias("k"))
            .select("k").distinct().collect()
        ]
        if all_keys:
            payload_keys = all_keys
        operation = sample[0]["operation"] if sample[0]["operation"] else "c"

        # Extract payload columns
        select_exprs = [col("payload").getItem(k).alias(k) for k in payload_keys]
        payload_df = table_df.select(*select_exprs)

        # Use naming convention expected by map.py
        df_name = f"{table_name}_df"
        all_dataframes[df_name] = payload_df
        operations[table_name] = operation

    return all_dataframes, operations


def process_microbatch(batch_df: DataFrame, batch_id: int, columns_info, minio_client, manual_mappings=None, business_id=None, enable_downstream=False, trigger_once=False, output_bucket=None):
    """Process each micro-batch with CDC operation support and optional inline downstream."""
    if output_bucket is None:
        output_bucket = os.getenv("OUTPUT_BUCKET", "pulse-bucket-stream")
    print(f"\n{'='*60}")
    print(f"Processing batch {batch_id}")
    print(f"{'='*60}")
    
    all_dataframes, operations = extract_table_dataframes(batch_df)
    
    if not all_dataframes:
        print("No data in batch")
        return
    
    print(f"Tables in batch: {list(all_dataframes.keys())}")
    print(f"Operations: {operations}")
    
    # Check Redis for updated manual mappings (especially important for subsequent batches)
    # This allows new batches to use mappings applied by user after stream started
    current_manual_mappings = manual_mappings  # Default to initial mappings
    if business_id:
        try:
            import redis
            import json
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=_REDIS_PORT, decode_responses=True)
            redis_mappings_str = redis_client.get(f"manual_mappings:{business_id}")
            if redis_mappings_str:
                current_manual_mappings = json.loads(redis_mappings_str)
                if batch_id > 0 and current_manual_mappings:
                    print(f"   ✅ Retrieved updated manual mappings from Redis for batch {batch_id}")
                    print(f"   Tables with manual mappings: {list(current_manual_mappings.keys())}")
        except Exception as redis_error:
            print(f"   ⚠️  Could not retrieve manual mappings from Redis: {redis_error}")
            # Fall back to initial manual_mappings passed as parameter
    
    # Call existing mapping function with mode="stream" and current manual_mappings
    results = process_all_dataframes(
        all_dataframes,
        columns_info,
        mapping_list,
        mode="stream",
        manual_mappings=current_manual_mappings
    )
    
    # Determine target folder based on batch_id and missing columns
    target_folder = "mapped"  # Default for subsequent batches
    has_missing_columns = False
    
    # Perform first-batch initialization only once per streaming job lifetime.
    # We use a Redis flag so that on a Spark restart (where batch_id resets to 0)
    # we do NOT re-run the DB status update or Redis mapping-results write.
    is_first_ever_batch = False
    if batch_id == 0 and business_id:
        try:
            import redis as _redis_mod
            import json
            _init_client = _redis_mod.Redis(host=os.getenv("REDIS_HOST", "redis"), port=_REDIS_PORT, decode_responses=True)
            # NX = only set if key does NOT exist; returns True only on the very first call
            is_first_ever_batch = bool(_init_client.set(
                f"streaming_first_batch_done:{business_id}", "1", nx=True, ex=_FIRST_BATCH_FLAG_TTL_SECONDS
            ))
        except Exception as _guard_err:
            print(f"   ⚠️  Could not check/set first-batch guard: {_guard_err}")
            is_first_ever_batch = True  # conservative: attempt initialization
    
    # Save mapping results to Redis on the very first batch of a new streaming job
    if is_first_ever_batch and business_id:
        try:
            import redis
            import json
            
            mapping_results = {
                "missing_cols": [],
                "extra_cols": []
            }
            
            for key, result in results.items():
                table_name = result.get("table_name", "")
                missing_cols = result.get("missing_cols", [])
                extra_cols = result.get("extra_cols", [])
                
                # Add table name to each column for frontend display
                for missing_col in missing_cols:
                    mapping_results["missing_cols"].append({
                        "column": missing_col,
                        "table": table_name
                    })
                
                for extra_col in extra_cols:
                    mapping_results["extra_cols"].append({
                        "column": extra_col,
                        "table": table_name
                    })
            
            # Check if there are missing columns
            has_missing_columns = len(mapping_results['missing_cols']) > 0
            
            # Save to Redis
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=_REDIS_PORT, decode_responses=True)
            redis_client.setex(
                f"mapping_results:{business_id}",
                _MAPPING_RESULTS_TTL_SECONDS,
                json.dumps(mapping_results)
            )
            
            # Set a flag to indicate if we're using temp folder (for subsequent batches)
            if has_missing_columns:
                redis_client.setex(
                    f"streaming_use_temp:{business_id}",
                    _MAPPING_RESULTS_TTL_SECONDS,
                    "true"
                )
                target_folder = "mapped-temp"
                print(f"\n⚠️  {len(mapping_results['missing_cols'])} required columns missing")
                print(f"   Saving first batch to temporary location for review")
            else:
                # No missing columns, clear any temp flag and use final folder
                redis_client.delete(f"streaming_use_temp:{business_id}")
                print(f"\n🎉 All required columns have been successfully mapped!")
            
            print(f"\n✅ Mapping results saved to Redis")
            print(f"   Missing columns: {len(mapping_results['missing_cols'])}")
            print(f"   Extra columns: {len(mapping_results['extra_cols'])}")
            
            # Update database status to 'completed' so user can review
            if business_id:
                try:
                    import psycopg2
                    from datetime import datetime, timezone
                    
                    db_host = os.getenv("POSTGRES_SERVER", "postgresql")
                    db_name = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
                    db_user = os.getenv("POSTGRES_USER", "postgres")
                    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
                    
                    with psycopg2.connect(
                        host=db_host,
                        database=db_name,
                        user=db_user,
                        password=db_password
                    ) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                UPDATE onboarding 
                                SET mapping_status = %s,
                                    mapping_completed_at = %s,
                                    mapping_error = NULL,
                                    current_step = 'mapping'
                                WHERE business_id = %s
                                AND is_completed = false
                            """, ("completed", datetime.now(timezone.utc), business_id))
                            conn.commit()
                    print(f"   Database status updated to 'completed'")
                except Exception as db_error:
                    print(f"⚠️  Warning: Could not update database status: {db_error}")
                    
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not save mapping results to Redis: {redis_error}")
    else:
        # For subsequent batches, check Redis flag for temp folder
        if business_id:
            try:
                import redis
                redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=_REDIS_PORT, decode_responses=True)
                use_temp = redis_client.get(f"streaming_use_temp:{business_id}")
                if use_temp == "true":
                    target_folder = "mapped-temp"
                    print(f"   Using temporary folder (awaiting manual mapping review)")
            except Exception as redis_error:
                print(f"⚠️  Warning: Could not check temp folder flag: {redis_error}")
    
    # Save results using existing function with CDC operation
    # Determine the operation for each result
    for result_key, result_data in results.items():
        table_name = result_data["table_name"]
        operation = operations.get(table_name, "c")
        save_dataframes_to_minio(
            {result_key: result_data}, 
            minio_client, 
            output_bucket,
            operation=operation,
            folder=target_folder
        )
    
    print(f"✅ Batch {batch_id} completed: {len(results)} tables processed")
    print(f"   Data saved to {output_bucket}/{target_folder}/")

    # ── Trigger inline downstream pipeline (clean → transform → analyze → ML) ──
    # Only runs when:
    #  1. enable_downstream is True (set by --enable-downstream flag)
    #  2. Data was written to the final "mapped" folder (not "mapped-temp")
    #  3. Not in trigger-once mode (Airflow handles downstream for trigger-once runs)
    if enable_downstream and target_folder == "mapped" and not trigger_once:
        try:
            from streaming.downstream_runner import trigger_downstream
            trigger_downstream(output_bucket, batch_id)
        except Exception as downstream_err:
            # Never let downstream errors crash the mapping stream
            print(f"   ⚠️  Could not trigger downstream: {downstream_err}")


def run_streaming(trigger_once: bool = False, enable_downstream: bool = False, output_bucket: str = None):
    """Main streaming pipeline.

    Args:
        trigger_once: When True, uses Spark's availableNow trigger so the job
                      processes all pending Kafka messages and then exits cleanly.
                      Use this for Airflow-managed micro-batch execution.
        enable_downstream: When True, runs the downstream pipeline
                           (clean → transform → analyze → ML inference)
                           inline after each micro-batch, reducing end-to-end
                           latency from ~10 min to ~10 s – 2 min.
        output_bucket: MinIO bucket name to write mapped files to.  Defaults to
                       the OUTPUT_BUCKET environment variable.
    """
    # Resolve output bucket: prefer explicit argument, then env var, then fallback.
    if output_bucket is None:
        output_bucket = os.getenv("OUTPUT_BUCKET", "pulse-bucket-stream")

    # Retrieve manual mappings from environment variable
    manual_mappings = None
    business_id = os.getenv("BUSINESS_ID")

    # Per-business checkpoint so that different tenants do not share Kafka
    # consumer-group state and Spark streaming state.
    if business_id:
        checkpoint_location = f"s3a://pulse-checkpoints/normalize-stream-{business_id}"
    else:
        checkpoint_location = "s3a://pulse-checkpoints/normalize-stream"

    print("Starting Spark Streaming Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Checkpoint: {checkpoint_location}")
    print(f"Output bucket: {output_bucket}")
    print(f"Trigger-once mode: {trigger_once}")
    print(f"Inline downstream: {enable_downstream}\n")
    
    manual_mappings_str = os.getenv("MANUAL_MAPPINGS")
    if manual_mappings_str:
        try:
            import json
            manual_mappings = json.loads(manual_mappings_str)
            print(f"✅ Loaded manual mappings from environment")
            print(f"   Tables with manual mappings: {list(manual_mappings.keys())}\n")
        except Exception as e:
            print(f"⚠️  Warning: Could not parse manual mappings: {e}\n")
    
    # Initialize
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Use hardcoded columns_info from canonical schema
    columns_info = COLUMNS_INFO
    print(f"Loaded {len(columns_info)} columns from canonical schema")
    
    # Parse MINIO_ENDPOINT to strip protocol prefix if present
    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    
    minio_client = Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    
    # Create bucket if not exists
    if not minio_client.bucket_exists(output_bucket):
        minio_client.make_bucket(output_bucket)
        print(f"Created bucket: {output_bucket}")
    
    # Read stream
    json_stream = read_kafka_stream(spark)
    
    # Process with foreachBatch
    writer = (
        json_stream.writeStream
        .foreachBatch(lambda df, bid: process_microbatch(
            df, bid, columns_info, minio_client, manual_mappings,
            business_id, enable_downstream, trigger_once, output_bucket
        ))
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
    )

    if trigger_once:
        # availableNow: process all pending Kafka messages then stop (Airflow-friendly)
        writer = writer.trigger(availableNow=True)
        print("Streaming query started in trigger-once mode (availableNow).\n")
    else:
        print("Streaming query started. Press Ctrl+C to stop.\n")

    query = writer.start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("\nStreaming stopped")
    except Exception as stream_err:
        # Catch StreamingQueryException (and any other unexpected error) so
        # the process exits with a non-zero code, allowing Airflow to restart.
        print(f"\n❌ Streaming query terminated with error: {stream_err}")
        try:
            query.stop()
        except Exception:
            pass
        try:
            spark.stop()
        except Exception:
            pass
        raise


if __name__ == "__main__":
    run_streaming()
