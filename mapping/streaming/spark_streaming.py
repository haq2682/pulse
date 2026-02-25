"""Spark Structured Streaming - Kafka Consumer with Existing Map Integration"""

import os
import sys
import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
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
CHECKPOINT_LOCATION = "s3a://pulse-checkpoints/normalize-stream"
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "pulse-bucket-stream")


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
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def get_canonical_schema() -> StructType:
    """
    Schema supporting both canonical and Debezium message formats.
    The operation field is used for database ingestion with CDC support.
    """
    return StructType([
        # Canonical format fields
        StructField("source_type", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("table", StringType(), True),
        StructField("schema_version", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("payload", MapType(StringType(), StringType()), True),

        # Debezium format fields
        StructField("op", StringType(), True),
        StructField("before", MapType(StringType(), StringType()), True),
        StructField("after", MapType(StringType(), StringType()), True),
        StructField("source", MapType(StringType(), StringType()), True),
    ])


def normalize_message_row(row):
    """
    Normalize row to canonical format.
    Handles both canonical and Debezium formats.
    """
    if row is None:
        return None

    # Check if Debezium format (has 'op' field)
    if row.get("op") is not None:
        # Debezium format - transform it
        op_map = {"c": "c", "u": "u", "d": "d", "r": "r"}

        source = row.get("source", {})
        if isinstance(source, str):
            import json
            source = json.loads(source)

        return {
            "source_type": "db",
            "vendor": "debezium",
            "table": source.get("table"),
            "schema_version": "v1",
            "operation": op_map.get(row.get("op"), "c"),
            "payload": row.get("after") or row.get("before") or {}
        }
    else:
        # Already canonical format
        return {
            "source_type": row.get("source_type"),
            "vendor": row.get("vendor"),
            "table": row.get("table"),
            "schema_version": row.get("schema_version"),
            "operation": row.get("operation", "c"),
            "payload": row.get("payload", {})
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
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", "ecom\\..*")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), get_canonical_schema()).alias("data"))
        .select("data.*")
    )


def extract_table_dataframes(batch_df: DataFrame) -> tuple:
    """
    Extract DataFrames per table from batch with CDC operation info.
    Handles both canonical and Debezium message formats.

    Returns:
        tuple: (all_dataframes dict, operation dict mapping table to CDC operation)
    """
    if batch_df.rdd.isEmpty():
        return {}, {}

    # Normalize messages to canonical format
    normalized_rdd = batch_df.rdd.map(lambda row: normalize_message_row(row.asDict()))

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

    table_names = [row["table"] for row in normalized_df.select("table").distinct().collect()]
    all_dataframes = {}
    operations = {}

    for table_name in table_names:
        table_df = normalized_df.filter(col("table") == table_name)

        # Get payload keys and operation from first row
        sample = table_df.select("payload", "operation").limit(1).collect()

        if not sample:
            continue

        payload_keys = list(sample[0]["payload"].keys())
        operation = sample[0]["operation"] if sample[0]["operation"] else "c"

        # Extract payload columns
        select_exprs = [col("payload").getItem(k).alias(k) for k in payload_keys]
        payload_df = table_df.select(*select_exprs)

        # Use naming convention expected by map.py
        df_name = f"{table_name}_df"
        all_dataframes[df_name] = payload_df
        operations[table_name] = operation

    return all_dataframes, operations


def process_microbatch(batch_df: DataFrame, batch_id: int, columns_info, minio_client, manual_mappings=None, business_id=None, enable_downstream=False):
    """Process each micro-batch with CDC operation support and optional inline downstream."""
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
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
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
    
    # Save mapping results to Redis on first batch (batch_id == 0)
    if batch_id == 0 and business_id:
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
                for col in missing_cols:
                    mapping_results["missing_cols"].append({
                        "column": col,
                        "table": table_name
                    })
                
                for col in extra_cols:
                    mapping_results["extra_cols"].append({
                        "column": col,
                        "table": table_name
                    })
            
            # Check if there are missing columns
            has_missing_columns = len(mapping_results['missing_cols']) > 0
            
            # Save to Redis
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
            redis_client.setex(
                f"mapping_results:{business_id}",
                86400,  # 24 hours
                json.dumps(mapping_results)
            )
            
            # Set a flag to indicate if we're using temp folder (for subsequent batches)
            if has_missing_columns:
                redis_client.setex(
                    f"streaming_use_temp:{business_id}",
                    86400,  # 24 hours
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
                            """, ("completed", datetime.now(timezone.utc), business_id))
                            conn.commit()
                    print(f"   Database status updated to 'completed'")
                except Exception as db_error:
                    print(f"⚠️  Warning: Could not update database status: {db_error}")
                    
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not save mapping results to Redis: {redis_error}")
    else:
        # For subsequent batches (batch_id > 0), check Redis flag
        if business_id:
            try:
                import redis
                redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
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
            OUTPUT_BUCKET,
            operation=operation,
            folder=target_folder
        )
    
    print(f"✅ Batch {batch_id} completed: {len(results)} tables processed")
    print(f"   Data saved to {OUTPUT_BUCKET}/{target_folder}/")

    # ── Trigger inline downstream pipeline (clean → transform → analyze → ML) ──
    # Only runs when:
    #  1. enable_downstream is True (set by --enable-downstream flag)
    #  2. Data was written to the final "mapped" folder (not "mapped-temp")
    #  3. Not in trigger-once mode (batch_id > 0 or continuous streaming)
    if enable_downstream and target_folder == "mapped" and batch_id > 0:
        try:
            from streaming.downstream_runner import trigger_downstream
            trigger_downstream(OUTPUT_BUCKET, batch_id)
        except Exception as downstream_err:
            # Never let downstream errors crash the mapping stream
            print(f"   ⚠️  Could not trigger downstream: {downstream_err}")


def run_streaming(trigger_once: bool = False, enable_downstream: bool = False):
    """Main streaming pipeline.

    Args:
        trigger_once: When True, uses Spark's availableNow trigger so the job
                      processes all pending Kafka messages and then exits cleanly.
                      Use this for Airflow-managed micro-batch execution.
        enable_downstream: When True, runs the downstream pipeline
                           (clean → transform → analyze → ML inference)
                           inline after each micro-batch, reducing end-to-end
                           latency from ~10 min to ~10 s – 2 min.
    """
    print("Starting Spark Streaming Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")
    print(f"Output bucket: {OUTPUT_BUCKET}")
    print(f"Trigger-once mode: {trigger_once}")
    print(f"Inline downstream: {enable_downstream}\n")
    
    # Retrieve manual mappings from environment variable
    manual_mappings = None
    business_id = os.getenv("BUSINESS_ID")
    
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
    if not minio_client.bucket_exists(OUTPUT_BUCKET):
        minio_client.make_bucket(OUTPUT_BUCKET)
        print(f"Created bucket: {OUTPUT_BUCKET}")
    
    # Read stream
    json_stream = read_kafka_stream(spark)
    
    # Process with foreachBatch
    writer = (
        json_stream.writeStream
        .foreachBatch(lambda df, bid: process_microbatch(df, bid, columns_info, minio_client, manual_mappings, business_id, enable_downstream))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
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


if __name__ == "__main__":
    run_streaming()
