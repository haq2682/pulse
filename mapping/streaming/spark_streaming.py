"""Spark Structured Streaming - Kafka Consumer with Existing Map Integration"""

import os
import sys
import threading

# ---------------------------------------------------------------------------
# JAR paths — baked into the repo at mapping/streaming/jars/ so they are
# available inside the api container via the ./mapping volume mount at
# /app/mapping/streaming/jars/ without requiring any image rebuild.
# ---------------------------------------------------------------------------
_SPARK_JARS_DIR = "/app/jars"
_KAFKA_JARS = [
    f"{_SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    f"{_SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    f"{_SPARK_JARS_DIR}/kafka-clients-3.4.1.jar",
    f"{_SPARK_JARS_DIR}/commons-pool2-2.11.1.jar",
]
_S3_JARS = [
    f"{_SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
]
_DELTA_JARS = [
    f"{_SPARK_JARS_DIR}/delta-spark_2.12-3.0.0.jar",
    f"{_SPARK_JARS_DIR}/delta-storage-3.0.0.jar",
]
_ALL_JARS_CP = ":".join(_KAFKA_JARS + _S3_JARS + _DELTA_JARS)  # driver classpath separator

# PYSPARK_SUBMIT_ARGS must be set BEFORE pyspark is imported.
# PySpark's launch_gateway() reads this env var first when spawning the
# driver JVM — it is the only reliable way to add JARs to the driver
# classpath when using pip-installed PySpark (as opposed to spark-submit).
# spark.driver.extraClassPath set in SparkSession.builder can miss because
# the gateway may have been initialised earlier in the process lifetime.
if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    # Only set the driver-side classpath here.  The executor (spark worker
    # container, spark-master-py310 image) already has all of these JARs
    # pre-installed at /opt/spark/external-jars/ via SPARK_EXTRA_CLASSPATH.
    # Adding --jars here would re-upload them, creating duplicate class
    # definitions across two classloaders and causing ClassCastException on
    # Scala collection types (List$SerializationProxy / Seq mismatch).
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_ALL_JARS_CP} "
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
from pyspark.sql.functions import col, explode, map_keys, udf
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

# Event used in trigger-once (onboarding) mode to stop the streaming query
# immediately after the first-ever micro-batch has been written.  This
# prevents ``availableNow`` from continuing to process batch 1, 2, …
# while the user reviews the auto-mapped results on the onboarding mapping
# page.  The continuous db_streaming / api_streaming Airflow DAG picks up
# the remaining Kafka messages after the user confirms their mappings via
# POST /onboarding/confirm-mapping.
_stop_after_first_batch_event: threading.Event = threading.Event()


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka and S3 support.

    IMPORTANT: map.py creates a SparkSession at module-import time (appName
    "NormalizeData", master from SPARK_SERVER / local[*]).  Calling
    getOrCreate() naively would return that pre-existing session and the
    streaming job would run in local mode on the driver instead of using the
    cluster.  We explicitly stop any existing session first so that the
    streaming-specific configuration (cluster master, executor count, Kafka
    JARs) is always applied.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()

    # Prefer SPARK_MASTER_URL (set in docker-compose) over the hardcoded URL
    # so the master address can be changed via an env var without code edits.
    spark_master = (
        os.getenv("SPARK_MASTER_URL", "spark://10.5.0.3:7077")
    )

    return (
        SparkSession.builder
        .appName("StreamingNormalization")
        .master(spark_master)
        # Dynamic allocation requires an external shuffle service that is not
        # configured on this standalone cluster.  Use a fixed executor count
        # matching the 2 workers × 2 cores each = 4 total cores.
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.instances", os.getenv("SPARK_EXECUTOR_INSTANCES", "4"))
        # Driver classpath: JARs are mounted at /app/jars/ in the api container.
        # Do NOT use spark.jars here — that would re-upload these JARs from the
        # driver to the executor, creating duplicate class definitions on the
        # executor alongside the copies already present in
        # /opt/spark/external-jars/ (loaded via SPARK_EXTRA_CLASSPATH in the
        # spark-master-py310 image).  Duplicate JARs across two classloaders
        # produce ClassCastException on Scala collection types at runtime.
        .config("spark.driver.extraClassPath", _ALL_JARS_CP)
        # Executor classpath: JARs are pre-installed in the worker image at
        # /opt/spark/external-jars/ (COPY ./jars/deps/ in the Spark Dockerfile).
        # This must match the path inside the worker container, NOT the driver.
        .config("spark.executor.extraClassPath", "/opt/spark/external-jars/*")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Delta Lake extensions — enables MERGE, UPDATE, DELETE SQL syntax and
        # the Python ``DeltaTable`` API used for CDC upserts.
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Tune shuffle partitions to match the cluster (2 workers × 2 cores).
        # The default of 200 creates hundreds of tiny tasks for our data volume,
        # wasting scheduler overhead.  16 is a sensible 4× multiplier of cores.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Primary-key map used by the Spark-native writer for Delta MERGE operations.
# Mirrors the table_primary_keys dict in map.py.
# ---------------------------------------------------------------------------
_TABLE_PKS: dict = {
    "addresses":           "address_id",
    "customers":           "customer_id",
    "suppliers":           "supplier_id",
    "categories":          "category_id",
    "products":            "product_id",
    "inventory":           "inventory_id",
    "wishlist":            "wishlist_id",
    "shopping_cart":       "cart_id",
    "cart_items":          "cart_item_id",
    "orders":              "order_id",
    "order_items":         "order_item_id",
    "payments":            "payment_id",
    "reviews":             "review_id",
    "marketing_campaigns": "campaign_id",
    "customer_sessions":   "session_id",
}


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

    NOTE: This function is kept for non-Spark Python callers (e.g. unit tests).
    The PySpark UDF uses ``_udf_normalize`` defined locally inside
    ``extract_table_dataframes`` — a local function is serialised by cloudpickle
    by-value (bytecode), avoiding the ModuleNotFoundError that occurs when a
    module-level function from this file is reconstructed on the executor worker
    which does not have /app/mapping on sys.path.
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


def read_kafka_stream(spark: SparkSession, topic_prefix: str = "ecom") -> DataFrame:
    """
    Read from Kafka {topic_prefix}.* topics with CDC support for database ingestion.

    topic_prefix is the per-tenant Kafka topic prefix.  Each tenant's Debezium
    connector publishes to ``{topic_prefix}.{schema}.{table}`` topics; using the
    tenant's business_id UUID as the prefix ensures complete topic-level
    isolation — one tenant's Spark job never reads another tenant's data.

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
    # Cap the number of Kafka offsets consumed per micro-batch.
    # Without this, availableNow trigger attempts to read ALL pending offsets
    # (9 M+ rows from a Debezium snapshot) as a single batch, which causes
    # the Python UDF + shuffle to run for hours.  200 K rows per batch keeps
    # each batch under ~30 s and produces predictable memory pressure.
    # Override via KAFKA_MAX_OFFSETS_PER_TRIGGER env var.
    max_offsets = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "200000"))
    # Escape literal dots in the prefix (e.g. "my.org") so the regex only
    # matches topic names that start with the exact prefix string.
    escaped_prefix = topic_prefix.replace(".", "\\.")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", f"{escaped_prefix}\\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
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
    # Avoid DataFrame -> RDD conversion for Spark DataSource V2 micro-batches.
    # Some Spark/runtime combinations can fail with ClassCastException when
    # PythonRDD is used against DataSourceRDDPartition. Keep processing in
    # DataFrame APIs and apply Python normalization via a UDF.
    if not batch_df.select("json_str").limit(1).collect():
        return {}, {}

    normalized_schema = get_canonical_schema()

    # ----------------------------------------------------------------
    # Define the normalization logic as a *local* function so that
    # cloudpickle serialises it by-value (bytecode + closure) instead
    # of by-reference (module import path).  The executor Python worker
    # does not have /app/mapping on sys.path, so any module-level
    # function inside this file causes ModuleNotFoundError when the
    # worker tries to reconstruct the UDF via `import streaming`.
    # A locally-scoped function has no __module__ anchor and is always
    # serialised inline.
    # ----------------------------------------------------------------
    def _udf_normalize(json_str):
        if not json_str:
            return None
        import json as _j
        try:
            row = _j.loads(json_str)
        except (ValueError, TypeError):
            return None

        def _to_str_map(obj):
            if not isinstance(obj, dict):
                return {}
            out = {}
            for k, v in obj.items():
                if v is None:
                    out[str(k)] = None
                elif isinstance(v, (dict, list)):
                    out[str(k)] = _j.dumps(v, default=str)
                else:
                    out[str(k)] = str(v)
            return out

        if "op" in row:
            op_map = {"c": "c", "u": "u", "d": "d", "r": "r"}
            source = row.get("source") or {}
            if isinstance(source, str):
                try:
                    source = _j.loads(source)
                except Exception:
                    source = {}
            after  = row.get("after")
            before = row.get("before")
            if isinstance(after, str):
                try:
                    after = _j.loads(after)
                except Exception:
                    after = None
            if isinstance(before, str):
                try:
                    before = _j.loads(before)
                except Exception:
                    before = None
            payload_obj = after or before
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

        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = _j.loads(payload)
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

    normalize_udf = udf(_udf_normalize, normalized_schema)

    normalized_df = (
        batch_df
        .select(normalize_udf(col("json_str")).alias("normalized"))
        .select("normalized.*")
        .filter(col("table").isNotNull())
    )

    if not normalized_df.limit(1).collect():
        return {}, {}

    # ── Single-pass key + operation discovery ───────────────────────────────
    # Previously each table triggered its own distinct().collect() call,
    # resulting in O(tables) = 15 separate Spark jobs per micro-batch.
    # This single job collects all (table, operation, payload_key) tuples
    # at once, then the driver groups them in Python — 1 Spark action total.
    meta_rows = (
        normalized_df
        .select(
            "table",
            "operation",
            explode(map_keys(col("payload"))).alias("k"),
        )
        .distinct()
        .collect()
    )

    # Build per-table key sets and first-seen operation.
    table_keys: dict = {}
    table_ops: dict = {}
    for r in meta_rows:
        tbl = r["table"]
        if tbl is None:
            continue
        table_keys.setdefault(tbl, set()).add(r["k"])
        table_ops.setdefault(tbl, r["operation"] or "c")

    all_dataframes = {}
    operations = {}

    for table_name, payload_keys in table_keys.items():
        table_df = normalized_df.filter(col("table") == table_name)

        operation = table_ops.get(table_name, "c")

        # Extract payload columns
        select_exprs = [col("payload").getItem(k).alias(k) for k in payload_keys]
        payload_df = table_df.select(*select_exprs)

        # Use naming convention expected by map.py
        df_name = f"{table_name}_df"
        all_dataframes[df_name] = payload_df
        operations[table_name] = operation

    return all_dataframes, operations


def _write_spark_native(
    results: dict,
    operations: dict,
    bucket_name: str,
    batch_id: int,
    folder: str,
    spark_session,
) -> None:
    """
    Write mapped DataFrames directly to MinIO via Spark's S3A connector.
    Replaces the ``toPandas`` + MinIO SDK path used by ``save_dataframes_to_minio``.

    **Append-only (insert / snapshot, op in c/r):**
    Each table is written as Parquet files inside
    ``s3a://{bucket}/{folder}/{table}/`` — partitioned by ``_batch_id`` so that
    incremental loads never overwrite previous data and are trivially queryable.

    **CDC upsert / delete (op in u/d):**
    When Delta Lake is available (``delta-spark`` JAR on the classpath) the
    write uses ``DeltaTable.merge()`` with WHEN MATCHED UPDATE / DELETE and
    WHEN NOT MATCHED INSERT.  If Delta is not present (e.g. first run before
    JARs are loaded), we fall back to Parquet append with a warning.
    """
    from pyspark.sql import functions as F

    for result_key, result_data in results.items():
        table_name = result_data["table_name"]
        final_df   = result_data["final_df"]
        pk_col     = _TABLE_PKS.get(table_name)
        operation  = operations.get(table_name, "c")

        # Attach batch metadata columns.
        df_out = (
            final_df
            .withColumn("_batch_id",    F.lit(batch_id))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        # Cast VOID (NullType) columns to StringType.
        # Parquet rejects columns whose entire micro-batch contained only NULL
        # values — Spark infers them as NullType / VOID and the writer raises
        # AnalysisException: [UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE].  Casting
        # to StringType is safe because those columns will remain all-null; the
        # downstream cleaning / transformation stages already handle null strings.
        from pyspark.sql.types import NullType, StringType as _StringType
        void_cols = [f.name for f in df_out.schema.fields if isinstance(f.dataType, NullType)]
        if void_cols:
            for _vc in void_cols:
                df_out = df_out.withColumn(_vc, df_out[_vc].cast(_StringType()))
            print(
                f"  ℹ️  Cast {len(void_cols)} VOID column(s) to StringType for {table_name}: {void_cols}",
                flush=True,
            )

        s3_path = f"s3a://{bucket_name}/{folder}/{table_name}"

        # ── Delta MERGE path for updates and deletes ─────────────────────
        if operation in ("u", "update", "d", "delete") and pk_col:
            try:
                from delta.tables import DeltaTable

                if DeltaTable.isDeltaTable(spark_session, s3_path):
                    dt = DeltaTable.forPath(spark_session, s3_path)
                    if operation in ("d", "delete"):
                        (
                            dt.alias("t")
                            .merge(
                                df_out.alias("s"),
                                f"t.`{pk_col}` = s.`{pk_col}`",
                            )
                            .whenMatchedDelete()
                            .execute()
                        )
                        print(
                            f"  ✅ Delta DELETE {table_name} → {s3_path}",
                            flush=True,
                        )
                    else:  # update
                        (
                            dt.alias("t")
                            .merge(
                                df_out.alias("s"),
                                f"t.`{pk_col}` = s.`{pk_col}`",
                            )
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute()
                        )
                        print(
                            f"  ✅ Delta MERGE (upsert) {table_name} → {s3_path}",
                            flush=True,
                        )
                    continue
                else:
                    # Table not yet a Delta table — write initial load as Delta.
                    (
                        df_out.write
                        .format("delta")
                        .mode("append")
                        .save(s3_path)
                    )
                    print(
                        f"  ✅ Delta initial write {table_name} → {s3_path}",
                        flush=True,
                    )
                    continue

            except ImportError:
                print(
                    f"  ⚠️  delta-spark not on classpath; "
                    f"falling back to Parquet append for {table_name}",
                    flush=True,
                )

        # ── Append-only Parquet (inserts, snapshots, Delta fallback) ────────
        (
            df_out.write
            .mode("append")
            .partitionBy("_batch_id")
            .parquet(s3_path)
        )
        print(
            f"  ✅ Parquet append {table_name} → {s3_path}/_batch_id={batch_id}/",
            flush=True,
        )


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
                                    current_step = 'mapping',
                                    mapping_results = %s::jsonb
                                WHERE business_id = %s
                                AND is_completed = false
                            """, ("completed", datetime.now(timezone.utc), json.dumps(mapping_results), business_id))
                            conn.commit()
                    print(f"   Database status updated to 'completed' (mapping_results persisted to DB)")
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
    
    # ── Spark-native write: Parquet partitioned by _batch_id (append/snapshot)
    # or Delta MERGE (update/delete).  No toPandas(), no MinIO SDK in the hot path.
    _write_spark_native(
        results=results,
        operations=operations,
        bucket_name=output_bucket,
        batch_id=batch_id,
        folder=target_folder,
        spark_session=batch_df.sparkSession,
    )

    print(f"✅ Batch {batch_id} completed: {len(results)} tables processed")
    print(f"   Data saved to {output_bucket}/{target_folder}/")

    # In trigger-once (onboarding) mode, signal the main thread to stop the
    # streaming query after the first-ever micro-batch so the user can review
    # mapping results before any further batches are processed.  Subsequent
    # batches (the remaining snapshot tail + future CDC events) are consumed
    # by the continuous db_streaming / api_streaming Airflow DAG once the
    # user confirms their mappings via POST /onboarding/confirm-mapping.
    if is_first_ever_batch and trigger_once:
        print("   🛑 First batch complete — signalling trigger-once stream to stop for user review.")
        _stop_after_first_batch_event.set()

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


def run_streaming(trigger_once: bool = False, enable_downstream: bool = False, output_bucket: str = None, topic_prefix: str = None):
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
        topic_prefix: Kafka topic prefix to subscribe to.  Should be the
                      tenant's business_id UUID so only that tenant's topics
                      (``{business_id}.{schema}.{table}``) are consumed.
                      Falls back to the KAFKA_TOPIC_PREFIX env var, then "ecom".
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

    # Resolve topic prefix: explicit arg → env var → legacy default.
    if topic_prefix is None:
        topic_prefix = os.getenv("KAFKA_TOPIC_PREFIX", "ecom")
    print(f"Topic prefix: {topic_prefix}")

    # Read stream — scoped to this tenant's topics only.
    json_stream = read_kafka_stream(spark, topic_prefix)
    
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

    # Reset the stop signal before starting a new streaming query so that a
    # process restart does not immediately stop if the event is still set.
    _stop_after_first_batch_event.clear()

    if trigger_once:
        # availableNow: process all pending Kafka messages then stop (Airflow-friendly)
        writer = writer.trigger(availableNow=True)
        print("Streaming query started in trigger-once mode (availableNow).\n")
    else:
        print("Streaming query started. Press Ctrl+C to stop.\n")

    query = writer.start()

    # In trigger-once (onboarding) mode, launch a monitor thread that waits
    # for the first-ever batch to complete and then calls query.stop().
    # This ensures the job exits after batch 0 instead of continuing to
    # consume all available Kafka messages in subsequent micro-batches.
    # On a Spark restart (streaming_first_batch_done flag already set), the
    # event will never fire so the monitor times out harmlessly — the
    # availableNow trigger will let the query exit naturally on its own.
    if trigger_once:
        def _first_batch_stopper():
            # Wait at most 2 hours for the first batch to complete.
            fired = _stop_after_first_batch_event.wait(timeout=7200)
            if fired and query.isActive:
                print("\n🛑 Stopping trigger-once streaming — first batch done, user review required.")
                try:
                    query.stop()
                except Exception as _stop_err:
                    print(f"   Warning: query.stop() raised: {_stop_err}")

        _monitor = threading.Thread(
            target=_first_batch_stopper,
            name="first-batch-stopper",
            daemon=True,
        )
        _monitor.start()

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
