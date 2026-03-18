#!/usr/bin/env python3
"""
Unified entry point for the mapping phase with 3 modes:
1. batch: Load from MinIO ingested folder -> map -> save to mapped folder
2. db: Ingest from database URI via Debezium CDC -> map -> save to mapped folder
3. api: Ingest from API endpoint -> map -> save to mapped folder

Configuration:
    Edit the CONFIG section below to set the mode and parameters.
    In production, these values will come from the React frontend.
"""

import sys
import os
import multiprocessing
from typing import Optional
import redis
import json
import psycopg2
from datetime import datetime, timezone
import traceback
import time as _time
import re

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_QUALIFIED_REF_RE = re.compile(r'^\s*"(?P<table>[^"]+)"\s*\.\s*"(?P<column>[^"]+)"\s*$')
_UNQUOTED_REF_RE = re.compile(r'^\s*(?P<table>[A-Za-z_][\w$]*)\s*\.\s*(?P<column>[A-Za-z_][\w$]*)\s*$')


def _qualify_source_ref(table_name: str, source_ref: str) -> str:
    raw = str(source_ref or "").strip()
    if not raw:
        return ""

    match = _QUALIFIED_REF_RE.match(raw) or _UNQUOTED_REF_RE.match(raw)
    if match:
        src_table = match.group("table").strip().strip('"')
        src_col = match.group("column").strip().strip('"')
    else:
        src_table = str(table_name or "").strip().strip('"')
        src_col = raw.strip().strip('"')

    if not src_table or not src_col:
        return ""
    return f'"{src_table}"."{src_col}"'


def _wait_for_debezium_snapshot(
    topic_prefix: str,
    expected_tables: list,
    kafka_bootstrap: str,
    max_wait: int = 3600,
    poll_interval: int = 10,
    connector_name: str = None,
    connect_url: str = "http://10.5.0.10:8083",
) -> bool:
    """
    Block until Debezium's initial snapshot has been fully published to Kafka.

    Two independent signals are used — whichever fires first releases the wait:

    **Signal A — All-tables complete**: every expected CDC topic has ≥ 1 message.
      This is the fastest path for relatively small databases.

    **Signal B — Rate stability**: total message count across *all* observed CDC
      topics has not grown for 3 consecutive polls (``3 × poll_interval`` seconds).
      Debezium writes snapshots in a continuous flood; a pause of this length
      reliably indicates the snapshot has finished.  Requires at least 1 topic
      to have data (guards against a false-positive during connector startup).

    Both signals also verify connector health via the Debezium Connect REST API
    (``GET {connect_url}/connectors/{name}/status``).  If the connector task is
    FAILED the wait aborts immediately so the caller can surface the error.

    Args:
        topic_prefix:    Debezium topic prefix (e.g. ``"ecom"``).
        expected_tables: List of collection / table names to wait for.
        kafka_bootstrap: Kafka broker address (e.g. ``"10.5.0.7:9092"``).
        max_wait:        Hard timeout in seconds (default 3600 — 1 hour).
        poll_interval:   Seconds between each Kafka poll (default 10).
        connector_name:  Debezium connector name; used for REST health-check.
        connect_url:     Kafka Connect base URL (default ``http://10.5.0.10:8083``).
    """
    try:
        from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
        from kafka.errors import KafkaError
    except ImportError:
        print(
            "   ⚠️  kafka-python not available; falling back to fixed 90 s wait.",
            flush=True,
        )
        _time.sleep(min(max_wait, 90))
        return False

    import requests as _req

    deadline = _time.monotonic() + max_wait
    expected_set = set(t.lower() for t in expected_tables)

    print(
        f"   Waiting for Debezium snapshot (prefix='{topic_prefix}', "
        f"tables={len(expected_set)}, timeout={max_wait}s)…",
        flush=True,
    )

    prev_total: int = -1
    stable_polls: int = 0
    _STABLE_NEEDED = 3          # three consecutive unchanged polls = done
    last_report_t = -poll_interval
    consecutive_failed: int = 0   # debezium health check: consecutive FAILED polls
    _FAILED_NEEDED = 3            # abort only after this many consecutive FAILED polls

    while _time.monotonic() < deadline:
        elapsed = int(_time.monotonic() - (deadline - max_wait))

        # ── Debezium connector health-check (non-fatal if unavailable) ────────
        if connector_name:
            try:
                r = _req.get(
                    f"{connect_url}/connectors/{connector_name}/status",
                    timeout=5,
                )
                if r.ok:
                    task_states = [t["state"] for t in r.json().get("tasks", [])]
                    if task_states and all(s == "FAILED" for s in task_states):
                        consecutive_failed += 1
                        if consecutive_failed >= _FAILED_NEEDED:
                            print(
                                f"   ❌ Debezium connector '{connector_name}' task FAILED "
                                f"for {consecutive_failed} consecutive polls — "
                                "aborting snapshot wait.",
                                flush=True,
                            )
                            return False
                        else:
                            print(
                                f"   ⚠️  Debezium connector '{connector_name}' tasks FAILED "
                                f"({consecutive_failed}/{_FAILED_NEEDED} consecutive) — "
                                "may be transient, retrying…",
                                flush=True,
                            )
                    else:
                        consecutive_failed = 0  # reset on any non-all-FAILED response
            except Exception:
                pass  # REST not reachable yet — keep waiting

        # ── Kafka topic scan ──────────────────────────────────────────────────
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=kafka_bootstrap,
                request_timeout_ms=5000,
                connections_max_idle_ms=8000,
            )
            all_topics = set(admin.list_topics())
            admin.close()

            cdc_topics = [
                t for t in all_topics
                if t.startswith(f"{topic_prefix}.")
                and t.split(".")[-1].lower() in expected_set
            ]

            populated: set = set()
            current_total: int = 0

            if cdc_topics:
                consumer = KafkaConsumer(
                    bootstrap_servers=kafka_bootstrap,
                    request_timeout_ms=5000,
                    connections_max_idle_ms=8000,
                )
                for topic in cdc_topics:
                    try:
                        partitions = consumer.partitions_for_topic(topic) or set()
                        tps = [TopicPartition(topic, p) for p in partitions]
                        if not tps:
                            continue
                        end_offsets = consumer.end_offsets(tps)
                        n = sum(end_offsets.values())
                        current_total += n
                        if n > 0:
                            populated.add(topic.split(".")[-1].lower())
                    except KafkaError:
                        pass
                consumer.close()

            # ── Signal A: all tables populated ─────────────────────────────
            missing = expected_set - populated
            if elapsed - last_report_t >= poll_interval or not missing:
                last_report_t = elapsed
                print(
                    f"   [{elapsed:>4}s/{max_wait}s] "
                    f"topics with data: {len(populated)}/{len(expected_set)}, "
                    f"total msgs: {current_total:,}"
                    + (
                        f" — waiting for: "
                        f"{sorted(missing)[:5]}{'…' if len(missing) > 5 else ''}"
                        if missing
                        else ""
                    ),
                    flush=True,
                )

            if not missing:
                print(
                    f"   ✅ All {len(expected_set)} tables in Kafka ({elapsed}s).",
                    flush=True,
                )
                return True

            # ── Signal B: rate stability (snapshot write-flood ended) ───────
            if current_total > 0:
                if current_total == prev_total:
                    stable_polls += 1
                    if stable_polls >= _STABLE_NEEDED:
                        print(
                            f"   ✅ Snapshot stable for "
                            f"{stable_polls * poll_interval}s "
                            f"(total msgs: {current_total:,}, "
                            f"populated: {len(populated)}/{len(expected_set)}). "
                            "Proceeding.",
                            flush=True,
                        )
                        return True
                else:
                    stable_polls = 0  # reset on any growth
            else:
                stable_polls = 0

            prev_total = current_total

        except Exception as poll_err:
            print(f"   ⚠️  Kafka poll error (retrying): {poll_err}", flush=True)

        _time.sleep(poll_interval)

    elapsed = int(_time.monotonic() - (deadline - max_wait))
    print(
        f"   ⚠️  Snapshot wait timed out after {elapsed}s. "
        "Proceeding with whatever Debezium has published so far.",
        flush=True,
    )
    return False

# ============================================================================
# CONFIGURATION - Edit these values to change mode and parameters
# ============================================================================
# In production, these will be provided by the React frontend
# For now, edit these values directly in the code

CONFIG = {
    # Mode: "batch", "db", or "api"
    "mode": "batch",

    # Common settings
    "bucket_name": "pulse-bucket-1",  # MinIO bucket name

    # DB mode settings (only used when mode="db")
    # Uses Debezium CDC for true real-time ingestion from database transaction log.
    # The database type is auto-detected from the URI scheme.
    # Supported: postgresql, mysql, mariadb, mongodb, mssql/sqlserver,
    #            oracle, db2, vitess, spanner, informix, cassandra
    # See mapping/CDC_SETUP_GUIDE.md for database setup instructions.
    "db_uri": "postgresql://debezium_user:debezium_pass@localhost:5432/ecommerce",
    "db_tables": ["orders", "payments", "inventory", "shopping_cart", "cart_items"],

    # API mode settings (only used when mode="api")
    "api_url": "http://localhost:5000/api/data",  # API endpoint URL
    "api_poll_interval": 10,  # Polling interval in seconds

    # Optional: Kafka bootstrap servers (defaults to env var if None)
    "kafka_bootstrap": None,  # e.g., "10.5.0.7:9092" or None to use env var
}

# ============================================================================

def update_mapping_status(business_id: str, status: str, error_message: Optional[str] = None):
    """
    Update the mapping status in the database for error reporting to frontend.
    
    Args:
        business_id: Business ID (bucket name)
        status: Status to set ('running', 'completed', 'failed')
        error_message: Optional error message (required when status='failed')
    """
    try:
        # Get database connection details from environment
        db_host = os.getenv("POSTGRES_SERVER", "postgresql")
        db_name = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
        
        # Use context manager to ensure connection is properly closed
        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        ) as conn:
            with conn.cursor() as cursor:
                # Update the onboarding table
                if status == "failed":
                    cursor.execute("""
                        UPDATE onboarding 
                        SET mapping_status = %s,
                            mapping_error = %s,
                            mapping_completed_at = %s,
                            current_step = 'connect'
                        WHERE business_id = %s
                    """, (status, error_message, datetime.now(timezone.utc), business_id))
                elif status == "completed":
                    cursor.execute("""
                        UPDATE onboarding 
                        SET mapping_status = %s,
                            mapping_completed_at = %s,
                            mapping_error = NULL,
                            current_step = 'mapping'
                        WHERE business_id = %s
                    """, (status, datetime.now(timezone.utc), business_id))
                else:
                    cursor.execute("""
                        UPDATE onboarding 
                        SET mapping_status = %s
                        WHERE business_id = %s
                    """, (status, business_id))
                
                conn.commit()
        
        print(f"✅ Database updated: status={status}, business_id={business_id}", flush=True)
        if error_message:
            print(f"   Error: {error_message}", flush=True)
            
    except Exception as db_error:
        print(f"⚠️  Failed to update database status: {db_error}", flush=True)
        # Don't re-raise - we want the original error to be the main one


def _save_auto_mappings_for_business(business_id: str, auto_mappings: dict):
    """
    Persist auto mappings to onboarding.auto_mappings for the latest in-progress row.
    """
    try:
        db_host = os.getenv("POSTGRES_SERVER", "10.5.0.5")
        db_name = os.getenv("POSTGRES_DATABASE_NAME", os.getenv("POSTGRES_DB", "pulse"))
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        payload = auto_mappings if isinstance(auto_mappings, dict) else {}

        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE onboarding
                    SET auto_mappings = %s::jsonb,
                        updated_at = NOW()
                    WHERE business_id = %s
                      AND is_completed = false
                    """,
                    (json.dumps(payload), business_id),
                )
                conn.commit()

        print(
            f"✅ Saved auto_mappings to DB for business '{business_id}' "
            f"({len(payload)} tables)",
            flush=True,
        )
    except Exception as db_error:
        print(f"⚠️  Could not persist auto_mappings to DB: {db_error}", flush=True)


def load_combined_mappings_for_business(business_id: str):
    """
    Load effective/combined mappings for a business from onboarding table.

    Source of truth:
      onboarding.combined_mappings (latest row for business)
    """
    business_found_in_db = False
    try:
        db_host = os.getenv("POSTGRES_SERVER", "10.5.0.5")
        db_name = os.getenv("POSTGRES_DATABASE_NAME", os.getenv("POSTGRES_DB", "pulse"))
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT business_id, combined_mappings
                    FROM onboarding
                    WHERE business_id = %s
                    ORDER BY is_completed DESC, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (business_id,),
                )
                row = cursor.fetchone()

        if row:
            business_found_in_db = True
        if row and row[1]:
            payload = row[1]
            combined_mappings = json.loads(payload) if isinstance(payload, str) else payload
            if isinstance(combined_mappings, dict) and combined_mappings:
                print(
                    f"✅ Retrieved combined mappings from DB: {list(combined_mappings.keys())}",
                    flush=True,
                )
                return combined_mappings
    except Exception as db_error:
        print(f"⚠️  Could not retrieve combined mappings from DB: {db_error}", flush=True)

    if business_found_in_db:
        print(
            f"ℹ️  No combined mappings found for business '{business_id}' in DB. Proceeding with automatic mapping from scratch.",
            flush=True,
        )
    else:
        print(
            f"ℹ️  Business '{business_id}' not found in onboarding DB. Proceeding with automatic mapping from scratch.",
            flush=True,
        )

    return None


def run_batch_mode(bucket_name: str):
    """
    Batch mode: Load data from MinIO ingested folder, process through mapping, 
    save to mapped folder.
    
    Args:
        bucket_name: Name of the MinIO bucket
    """
    print(f"\n{'='*60}", flush=True)
    print(f"BATCH MODE: Processing files from bucket '{bucket_name}/ingested'", flush=True)
    print(f"{'='*60}\n", flush=True)
    
    try:
        from map import (
            load_all_files_from_minio,
            process_all_dataframes,
            save_dataframes_to_minio,
            minio_client,
            spark,
            COLUMNS_INFO,
        )
        import List as mapping_list

        combined_mappings = load_combined_mappings_for_business(bucket_name)

        print(f"Loading files from {bucket_name}/ingested...", flush=True)
        all_dataframes = load_all_files_from_minio(minio_client, bucket_name, spark)

        if not all_dataframes:
            print("⚠️  No files found in ingested folder", flush=True)
            return

        print(f"\nProcessing {len(all_dataframes)} dataframes through mapping pipeline...", flush=True)
        results = process_all_dataframes(
            all_dataframes,
            COLUMNS_INFO,
            mapping_list,
            mode="batch",
            manual_mappings=combined_mappings,
        )

        auto_mappings = {}
        for _, result in results.items():
            table_name = result.get("table_name")
            mapped_cols = result.get("mapped_cols")
            if table_name and isinstance(mapped_cols, dict):
                filtered = {
                    str(k): _qualify_source_ref(table_name, str(v))
                    for k, v in mapped_cols.items()
                    if str(k).strip() and str(v).strip() and _qualify_source_ref(table_name, str(v))
                }
                if filtered:
                    auto_mappings[table_name] = filtered

        _save_auto_mappings_for_business(bucket_name, auto_mappings)

        mapping_results = {"missing_cols": [], "extra_cols": []}

        for _, result in results.items():
            table_name = result.get("table_name", "")
            missing_cols = result.get("missing_cols", [])
            extra_cols = result.get("extra_cols", [])

            for missing_col in missing_cols:
                mapping_results["missing_cols"].append({"column": missing_col, "table": table_name})

            for extra_col in extra_cols:
                mapping_results["extra_cols"].append({"column": extra_col, "table": table_name})

        try:
            redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
            )
            redis_client.setex(
                f"mapping_results:{bucket_name}",
                86400,
                json.dumps(mapping_results),
            )
            print("\n✅ Mapping results saved to Redis", flush=True)
            print(f"   Missing columns: {len(mapping_results['missing_cols'])}", flush=True)
            print(f"   Extra columns: {len(mapping_results['extra_cols'])}", flush=True)

            if len(mapping_results["missing_cols"]) == 0:
                print("\n🎉 SUCCESS: All required columns have been successfully mapped!", flush=True)
                print("   No missing columns detected.", flush=True)
            else:
                missing_count = len(mapping_results["missing_cols"])
                column_word = "column" if missing_count == 1 else "columns"
                print(f"\n⚠️  WARNING: {missing_count} required {column_word} missing.", flush=True)
                print(f"   {'This' if missing_count == 1 else 'These'} will need to be mapped manually.", flush=True)
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not save mapping results to Redis: {redis_error}", flush=True)

        has_missing_columns = len(mapping_results["missing_cols"]) > 0
        target_folder = "mapped-temp" if has_missing_columns else "mapped"

        print(f"\nSaving results to {bucket_name}/{target_folder}...", flush=True)
        if has_missing_columns:
            print("   💡 Saving to temporary location for manual mapping review", flush=True)
        save_dataframes_to_minio(results, minio_client, bucket_name, folder=target_folder)

        print(f"\n{'='*60}", flush=True)
        print("✅ BATCH MODE COMPLETE", flush=True)
        print(f"   Processed {len(results)} tables", flush=True)
        print(f"   Results saved to {bucket_name}/{target_folder}/", flush=True)
        if has_missing_columns:
            print("   📝 Awaiting manual mapping before moving to final location", flush=True)
        else:
            print("   🎉 All columns mapped - ready for use", flush=True)
        print(f"{'='*60}\n", flush=True)

        update_mapping_status(bucket_name, "completed")
        spark.stop()

    except Exception as e:
        error_msg = "Batch mode: Failed during data processing"
        print(f"❌ {error_msg}: {str(e)}", flush=True)
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)



def run_db_mode(config: dict):
    """
    DB mode — two sub-paths depending on ``trigger_once``.

    trigger_once=True  (initial onboarding)
    ----------------------------------------
    1. Deploy Debezium CDC connector with ``snapshot.mode=never`` so it
       registers the current WAL/oplog position without performing a full
       database snapshot (we do our own bulk read instead).
    2. Run a JDBC / PyMongo chunked initial load: every table is read in
       chunks of ≤200 k rows and written to
       ``ingested/{canonical_table}/chunk_NNNN.parquet`` inside the tenant
       bucket.
    3. Call ``run_batch_mode`` which reads ``ingested/``, runs the
       7-algorithm mapping pipeline, writes results to ``mapped-temp/`` (when
       there are missing columns) or ``mapped/`` (when fully auto-mapped), and
       sets ``mapping_status = 'completed'`` in the DB — making the results
       available to the frontend for user review.

    trigger_once=False  (Airflow continuous CDC streaming)
    -------------------------------------------------------
    The Debezium connector already exists from the initial-load phase.
    ``deploy_connector`` will verify it is RUNNING and update its config if
    needed (with ``snapshot.mode`` silently overridden to ``no_data`` on
    updates so no duplicate snapshot occurs).  Then the continuous Spark
    Structured Streaming job starts, reading CDC events from Kafka and
    applying confirmed combined mappings on every micro-batch loaded
    from onboarding.combined_mappings in Postgres by business_id.

    Subsequent batches and confirmed mappings
    -----------------------------------------
    The continuous Spark streaming job (``process_microbatch``) re-reads
    effective mappings from onboarding.combined_mappings in Postgres on every
    micro-batch and passes them to ``process_all_dataframes``. After
    ``confirm_mapping`` saves user-approved mappings, subsequent CDC batches
    are automatically mapped correctly — no further user action required.

    Args:
        config: Configuration dictionary with db_uri, db_tables, bucket_name,
                trigger_once.
    """
    from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager
    from streaming.spark_streaming import run_streaming

    db_uri      = config["db_uri"]
    tables      = config.get("db_tables", [])
    bucket_name = config["bucket_name"]
    trigger_once = config.get("trigger_once", False)

    print(f"\n{'='*60}", flush=True)
    print(f"DB MODE: {'Initial load (JDBC → batch mapping)' if trigger_once else 'Continuous CDC streaming'}", flush=True)
    print(f"{'='*60}\n", flush=True)

    manager        = DebeziumConnectorManager()
    connector_name = f"pulse-{bucket_name}-connector"

    # ── Wait for Kafka Connect to be ready ────────────────────────────────
    if not manager.wait_for_connect():
        error_msg = "DB mode error: Kafka Connect not available"
        print(f"❌ {error_msg}", flush=True)
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)

    # ── Build & deploy the Debezium connector ─────────────────────────────
    # For the initial load we use snapshot.mode=never so Debezium starts
    # tracking the WAL from NOW without duplicating the JDBC bulk snapshot.
    # For the continuous Airflow job we use the default (initial), which
    # deploy_connector silently overrides to no_data on connector updates to
    # prevent re-snapshoting an already-running connector.
    _snap_mode = "never" if trigger_once else "initial"
    try:
        connector_config = manager.create_connector_config(
            db_uri=db_uri,
            tables=tables,
            connector_name=connector_name,
            topic_prefix=bucket_name,
            snapshot_mode=_snap_mode,
        )
    except Exception as e:
        error_msg = "DB mode error: Failed to create connector configuration"
        print(f"❌ {error_msg}: {str(e)}", flush=True)
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)

    if not manager.deploy_connector(connector_config):
        error_msg = "DB mode error: Failed to deploy Debezium connector"
        print(f"❌ {error_msg}", flush=True)
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)

    print("\nDebezium connector deployed/verified", flush=True)

    # ════════════════════════════════════════════════════════════════════
    # PATH A — trigger_once: JDBC bulk snapshot → batch mapping pipeline
    # ════════════════════════════════════════════════════════════════════
    if trigger_once:
        print("\n📥 Starting JDBC initial load …\n", flush=True)
        try:
            from streaming.initial_load import run_jdbc_initial_load
            chunk_size = int(os.getenv("INITIAL_LOAD_CHUNK_SIZE", "200000"))
            row_counts = run_jdbc_initial_load(
                db_uri=db_uri,
                tables=tables,
                bucket_name=bucket_name,
                chunk_size=chunk_size,
            )
            if not row_counts:
                # DB was empty or no tables matched the canonical schema.
                # Still mark as completed so the user sees the mapping UI
                # (with zero missing-column suggestions).
                print("⚠️  No rows loaded — database may be empty.", flush=True)
        except Exception as e:
            error_msg = "DB mode error: JDBC initial load failed"
            print(f"❌ {error_msg}: {str(e)}", flush=True)
            traceback.print_exc()
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)

        print("\n🗺️  Running batch mapping pipeline on ingested data …\n", flush=True)
        try:
            run_batch_mode(bucket_name)
        except Exception as e:
            error_msg = "DB mode error: Batch mapping failed after initial load"
            print(f"❌ {error_msg}: {str(e)}", flush=True)
            traceback.print_exc()
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)
        # run_batch_mode already calls update_mapping_status('completed').
        return

    # ════════════════════════════════════════════════════════════════════
    # PATH B — continuous: Spark Structured Streaming (Airflow CDC job)
    # ════════════════════════════════════════════════════════════════════
    # Retrieve effective combined mappings from Postgres so they are passed
    # to every micro-batch via MANUAL_MAPPINGS env var read by run_streaming.
    combined_mappings = load_combined_mappings_for_business(bucket_name)

    os.environ["BUSINESS_ID"] = bucket_name
    if combined_mappings:
        os.environ["MANUAL_MAPPINGS"] = json.dumps(combined_mappings)

    topic_prefix = connector_config["config"].get("topic.prefix", bucket_name)
    try:
        run_streaming(
            trigger_once=False,
            output_bucket=bucket_name,
            topic_prefix=topic_prefix,
        )
    except Exception as e:
        error_msg = "DB mode error: Continuous streaming failed"
        print(f"❌ {error_msg}: {str(e)}", flush=True)
        traceback.print_exc()
        print(
            "ℹ️  Skipping onboarding mapping_status failure update for continuous streaming runtime error.",
            flush=True,
        )
        sys.exit(1)


def run_api_mode(api_url: str, bucket_name: str, poll_interval: int = 10,
                 kafka_bootstrap: Optional[str] = None,
                 poll_duration: int = 0, trigger_once: bool = False):
    """
    API mode — two sub-paths depending on ``trigger_once``.

    trigger_once=True  (initial onboarding)
    ----------------------------------------
    Polls the user's external API for *poll_duration* seconds and writes all
    collected records directly to ``ingested/{canonical_table}/chunk_NNNN.parquet``
    inside the tenant bucket — no Kafka, no Spark Streaming.  After the poll
    window closes, ``run_batch_mode`` runs the same 7-algorithm mapping
    pipeline used for uploaded-file batch mode.

    trigger_once=False  (Airflow continuous API streaming)
    -------------------------------------------------------
    Runs two parallel processes:
    1. API ingestion service — polls every *poll_interval* seconds → Kafka
    2. Spark Structured Streaming consumer — Kafka → MinIO mapped/
    Confirmed combined mappings are re-read by ``process_microbatch`` on every
    micro-batch from onboarding.combined_mappings in Postgres, so subsequent
    batches are mapped correctly without further user action.

    Downstream processing (cleaning → transformation → analysis → ML) is
    handled exclusively by the ``scheduled_batch`` Airflow DAG every 10 minutes.

    Args:
        api_url: API endpoint URL
        bucket_name: Name of the MinIO bucket
        poll_interval: Polling interval in seconds (initial-load or continuous)
        kafka_bootstrap: Kafka bootstrap servers (defaults to env var, continuous only)
        poll_duration: Seconds to poll in initial-load mode (trigger_once=True)
        trigger_once: True → initial onboarding load; False → continuous streaming
    """
    print(f"\n{'='*60}", flush=True)
    print(f"API MODE: {'Initial load (API poll → batch mapping)' if trigger_once else 'Continuous API streaming'}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"API URL: {api_url}", flush=True)
    print(f"Bucket:  {bucket_name}", flush=True)
    print(f"Poll interval: {poll_interval}s", flush=True)
    if trigger_once:
        print(f"Poll duration: {poll_duration}s\n", flush=True)

    # ══════════════════════════════════════════════════════════════════════════
    # PATH A — trigger_once: poll API → ingested/ → batch mapping pipeline
    # ══════════════════════════════════════════════════════════════════════════
    if trigger_once:
        _duration = poll_duration if poll_duration > 0 else 300
        print(f"\n📥 Polling API for {_duration}s → writing to ingested/ …\n", flush=True)
        try:
            from streaming.initial_load import run_api_initial_load
            row_counts = run_api_initial_load(
                api_url=api_url,
                poll_duration=_duration,
                bucket_name=bucket_name,
                poll_interval=poll_interval,
            )
            if not row_counts:
                print("⚠️  No records collected from API — mapping will proceed with empty ingested/", flush=True)
        except Exception as e:
            error_msg = "API mode error: Initial API load failed"
            print(f"❌ {error_msg}: {str(e)}", flush=True)
            traceback.print_exc()
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)

        print("\n🗺️  Running batch mapping pipeline on ingested data …\n", flush=True)
        try:
            run_batch_mode(bucket_name)
        except Exception as e:
            error_msg = "API mode error: Batch mapping failed after API initial load"
            print(f"❌ {error_msg}: {str(e)}", flush=True)
            traceback.print_exc()
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)
        # run_batch_mode already calls update_mapping_status('completed').
        return

    # ══════════════════════════════════════════════════════════════════════════
    # PATH B — continuous: parallel API ingestion + Spark Structured Streaming
    # ══════════════════════════════════════════════════════════════════════════
    # Retrieve effective combined mappings from Postgres so they are available
    # to Spark's process_microbatch on every micro-batch.
    combined_mappings = load_combined_mappings_for_business(bucket_name)

    if kafka_bootstrap is None:
        from dotenv import load_dotenv, find_dotenv
        load_dotenv(find_dotenv())
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")

    print(f"Using Kafka: {kafka_bootstrap}\n", flush=True)

    try:
        from streaming.ingestion.api_ingest_service import run as run_api_ingestion
        from streaming.spark_streaming import run_streaming

        def run_api_service():
            print("Starting API ingestion service…", flush=True)
            run_api_ingestion(
                api_url=api_url,
                poll_interval=poll_interval,
                kafka_bootstrap=kafka_bootstrap,
                business_id=bucket_name,
            )

        def run_spark_consumer():
            print("Starting Spark streaming consumer…", flush=True)
            os.environ["BUSINESS_ID"] = bucket_name
            if combined_mappings:
                os.environ["MANUAL_MAPPINGS"] = json.dumps(combined_mappings)
            run_streaming(
                trigger_once=False,
                output_bucket=bucket_name,
                topic_prefix=bucket_name,
            )

        print("Starting parallel processes:", flush=True)
        print("  1. API ingestion → Kafka", flush=True)
        print("  2. Spark streaming → MinIO mapped/\n", flush=True)

        api_process   = multiprocessing.Process(target=run_api_service)
        spark_process = multiprocessing.Process(target=run_spark_consumer)

        api_process.start()
        spark_process.start()

        print("Both processes started. Press Ctrl+C to stop.\n", flush=True)

        try:
            # Production streaming: monitor both processes concurrently.
            # If either one exits (crash or normal), terminate the other so
            # Airflow gets a non-zero exit and can restart the whole task.
            import time as _time
            while True:
                api_alive   = api_process.is_alive()
                spark_alive = spark_process.is_alive()
                if not api_alive or not spark_alive:
                    if not api_alive:
                        print("⚠️  API ingestion process exited — terminating Spark consumer", flush=True)
                        if spark_alive:
                            spark_process.terminate()
                            spark_process.join()
                    else:
                        print("⚠️  Spark consumer process exited — terminating API ingestion", flush=True)
                        api_process.terminate()
                        api_process.join()
                    break
                _time.sleep(5)
            exit_code = max(api_process.exitcode or 0, spark_process.exitcode or 0)
            print(f"❌ API MODE exited (exit code {exit_code}) — Airflow will restart", flush=True)
            sys.exit(exit_code if exit_code != 0 else 1)
        except KeyboardInterrupt:
            print("\n\nStopping processes…", flush=True)
            api_process.terminate()
            spark_process.terminate()
            api_process.join()
            spark_process.join()
            print("✅ API MODE STOPPED\n", flush=True)

    except Exception as e:
        error_msg = "API mode error: An unexpected error occurred"
        print(f"❌ {error_msg}: {str(e)}", flush=True)
        traceback.print_exc()
        print(
            "ℹ️  Skipping onboarding mapping_status failure update for continuous API streaming runtime error.",
            flush=True,
        )
        sys.exit(1)


def main():
    """
    Main entry point that reads configuration and executes the appropriate mode.
    In production, CONFIG values will be provided by the React frontend via command-line args.
    """
    import argparse
    
    # Parse command-line arguments if provided
    parser = argparse.ArgumentParser(description='Run mapping pipeline')
    parser.add_argument('--mode', type=str, help='Mode: batch, db, or api')
    parser.add_argument('--business-id', type=str, help='Business ID (used as bucket name)')
    parser.add_argument('--db-uri', type=str, help='Database URI (for db mode)')
    parser.add_argument('--db-tables', type=str, help='Comma-separated list of database tables (for db mode)')
    parser.add_argument('--api-url', type=str, help='API endpoint URL (for api mode)')
    parser.add_argument('--api-poll-interval', type=int, help='API polling interval in seconds (for api mode)')
    parser.add_argument('--trigger-once', action='store_true',
                        help='Process all available Kafka messages then exit (db/api mode, Airflow-friendly)')
    parser.add_argument('--poll-duration', type=int, default=0,
                        help='For api mode: run ingestion for N seconds then stop (0=run forever)')
    parser.add_argument('--deploy-connector-only', action='store_true',
                        help='(db mode) Deploy / verify the Debezium CDC connector and exit '
                             'immediately — no JDBC initial load, no Spark streaming. '
                             'Use this for Airflow connector health-check tasks so a '
                             'connector restart never triggers a full database re-snapshot.')

    args = parser.parse_args()

    # Use command-line args if provided, otherwise use CONFIG
    mode = args.mode if args.mode else CONFIG["mode"]
    bucket_name = args.business_id if args.business_id else CONFIG["bucket_name"]
    trigger_once = args.trigger_once
    poll_duration = args.poll_duration
    deploy_connector_only = args.deploy_connector_only
    
    # Wrap execution in try-except to catch any uncaught errors
    try:
        print(f"\n{'='*60}", flush=True)
        print(f"PULSE MAPPING - Starting in {mode.upper()} mode", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Configuration:", flush=True)
        print(f"  Mode: {mode}", flush=True)
        print(f"  Bucket: {bucket_name}", flush=True)
        
        # Validate and execute the appropriate mode
        if mode == "batch":
            print(f"{'='*60}\n", flush=True)
            run_batch_mode(bucket_name)
            
        elif mode == "db":
            # Use command-line args if provided, otherwise use CONFIG
            db_uri = args.db_uri if args.db_uri else CONFIG["db_uri"]
            # Handle db_tables: distinguish between omitted (None) and explicitly provided (including empty string)
            if args.db_tables is not None:
                db_tables = [t.strip() for t in args.db_tables.split(',') if t.strip()]
            else:
                db_tables = CONFIG["db_tables"]
            
            # Mask credentials in URI for display
            display_uri = db_uri.split("@")[-1] if "@" in db_uri else db_uri
            print(f"  Database: {display_uri}", flush=True)
            print(f"  Tables: {db_tables}", flush=True)

            # ── deploy-connector-only: Airflow connector health-check ──────────
            # Deploy (or verify) the Debezium connector with snapshot.mode=no_data
            # so that restarting a connector NEVER triggers a full DB re-snapshot.
            # Used by check_or_deploy_debezium in db_streaming_dag.py.
            if deploy_connector_only:
                print(f"{'='*60}", flush=True)
                print("  Mode: deploy-connector-only (no JDBC load, no Spark)", flush=True)
                print(f"{'='*60}\n", flush=True)
                try:
                    from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager
                    manager        = DebeziumConnectorManager()
                    connector_name = f"pulse-{bucket_name}-connector"
                    if not manager.wait_for_connect():
                        print("❌ Kafka Connect not available", flush=True)
                        sys.exit(1)
                    connector_config = manager.create_connector_config(
                        db_uri=db_uri,
                        tables=db_tables,
                        connector_name=connector_name,
                        topic_prefix=bucket_name,
                        snapshot_mode="no_data",  # NEVER re-snapshot on restart
                    )
                    if not manager.deploy_connector(connector_config):
                        print("❌ Failed to deploy Debezium connector", flush=True)
                        sys.exit(1)
                    print("✅ Connector deployed/verified (deploy-connector-only — exiting)", flush=True)
                except Exception as e:
                    print(f"❌ deploy-connector-only failed: {e}", flush=True)
                    traceback.print_exc()
                    sys.exit(1)
                return  # done — do NOT run JDBC load or Spark streaming

            print(f"{'='*60}\n", flush=True)
            run_db_mode({
                "db_uri": db_uri,
                "db_tables": db_tables,
                "bucket_name": bucket_name,
                "trigger_once": trigger_once,
            })

        elif mode == "api":
            api_url = args.api_url if args.api_url else CONFIG["api_url"]
            poll_interval = args.api_poll_interval if args.api_poll_interval else CONFIG["api_poll_interval"]
            kafka_bootstrap = CONFIG["kafka_bootstrap"]
            
            print(f"  API URL: {api_url}", flush=True)
            print(f"  Poll interval: {poll_interval}s", flush=True)
            print(f"{'='*60}\n", flush=True)
            
            run_api_mode(api_url, bucket_name, poll_interval, kafka_bootstrap,
                         poll_duration=poll_duration, trigger_once=trigger_once)

        else:
            error_msg = f"Invalid mode '{mode}'. Valid modes: batch, db, api"
            print(f"\n❌ ERROR: {error_msg}", flush=True)
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)
            
    except Exception as e:
        # Catch any uncaught errors from the main execution
        error_msg = "An unexpected error occurred in the mapping pipeline"
        print(f"\n❌ {error_msg}: {str(e)}", flush=True)
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
