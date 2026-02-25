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

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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
    
    # Import and run the batch processing
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
        
        # Try to retrieve manual mappings from Redis
        manual_mappings = None
        try:
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
            manual_mappings_str = redis_client.get(f"manual_mappings:{bucket_name}")
            if manual_mappings_str:
                manual_mappings = json.loads(manual_mappings_str)
                print(f"\n✅ Retrieved manual mappings from Redis", flush=True)
                print(f"   Tables with manual mappings: {list(manual_mappings.keys())}", flush=True)
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not retrieve manual mappings from Redis: {redis_error}", flush=True)
        
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
            manual_mappings=manual_mappings
        )
        
        # Collect mapping results (missing_cols and extra_cols)
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
        
        # Save mapping results to Redis for the API to retrieve
        try:
            redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
            redis_client.setex(
                f"mapping_results:{bucket_name}",
                86400,  # 24 hours
                json.dumps(mapping_results)
            )
            print(f"\n✅ Mapping results saved to Redis", flush=True)
            print(f"   Missing columns: {len(mapping_results['missing_cols'])}", flush=True)
            print(f"   Extra columns: {len(mapping_results['extra_cols'])}", flush=True)
            
            # Provide clear feedback about mapping completeness
            if len(mapping_results['missing_cols']) == 0:
                print(f"\n🎉 SUCCESS: All required columns have been successfully mapped!", flush=True)
                print(f"   No missing columns detected.", flush=True)
            else:
                missing_count = len(mapping_results['missing_cols'])
                column_word = "column" if missing_count == 1 else "columns"
                print(f"\n⚠️  WARNING: {missing_count} required {column_word} missing.", flush=True)
                print(f"   {'This' if missing_count == 1 else 'These'} will need to be mapped manually.", flush=True)
                
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not save mapping results to Redis: {redis_error}", flush=True)
        
        # Determine which folder to save to based on whether manual mapping is needed
        has_missing_columns = len(mapping_results['missing_cols']) > 0
        target_folder = "mapped-temp" if has_missing_columns else "mapped"
        
        print(f"\nSaving results to {bucket_name}/{target_folder}...", flush=True)
        if has_missing_columns:
            print(f"   💡 Saving to temporary location for manual mapping review", flush=True)
        save_dataframes_to_minio(results, minio_client, bucket_name, folder=target_folder)
        
        print(f"\n{'='*60}", flush=True)
        print(f"✅ BATCH MODE COMPLETE", flush=True)
        print(f"   Processed {len(results)} tables", flush=True)
        print(f"   Results saved to {bucket_name}/{target_folder}/", flush=True)
        if has_missing_columns:
            print(f"   📝 Awaiting manual mapping before moving to final location", flush=True)
        else:
            print(f"   🎉 All columns mapped - ready for use", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        # Update database with success status
        # Set current_step to 'mapping' so user can review results before continuing
        update_mapping_status(bucket_name, "completed")
        
        spark.stop()
        
    except Exception as e:
        error_msg = f"Batch mode: Failed during data processing - {str(e)}"
        print(f"❌ {error_msg}", flush=True)
        traceback.print_exc()
        
        # Update database with error status so frontend can display it
        update_mapping_status(bucket_name, "failed", error_msg)
        
        sys.exit(1)



def run_db_mode(config: dict):
    """
    DB mode: Deploy Debezium CDC connector, stream real-time changes to Kafka,
    consume via Spark Streaming -> mapped folder.

    Auto-detects the database type from the URI scheme and builds the correct
    Debezium connector configuration. Supports all Debezium source connectors:
    PostgreSQL, MySQL, MariaDB, MongoDB, SQL Server, Oracle, Db2,
    Vitess, Spanner, Informix, Cassandra.

    Args:
        config: Configuration dictionary with db_uri and db_tables
    """
    from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager
    from streaming.spark_streaming import run_streaming

    db_uri = config["db_uri"]
    tables = config.get("db_tables", [])
    bucket_name = config["bucket_name"]

    print(f"\n{'='*60}")
    print(f"DB MODE: Real-time CDC via Debezium")
    print(f"{'='*60}\n")
    
    # Try to retrieve manual mappings from Redis
    manual_mappings = None
    try:
        redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
        manual_mappings_str = redis_client.get(f"manual_mappings:{bucket_name}")
        if manual_mappings_str:
            manual_mappings = json.loads(manual_mappings_str)
            print(f"\n✅ Retrieved manual mappings from Redis")
            print(f"   Tables with manual mappings: {list(manual_mappings.keys())}")
    except Exception as redis_error:
        print(f"⚠️  Warning: Could not retrieve manual mappings from Redis: {redis_error}")

    manager = DebeziumConnectorManager()

    if not manager.wait_for_connect():
        error_msg = "DB mode error: Kafka Connect not available"
        print(f"❌ {error_msg}")
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)

    # Auto-detect database type from URI and build connector config
    try:
        connector_config = manager.create_connector_config(
            db_uri=db_uri,
            tables=tables,
        )
    except Exception as e:
        error_msg = f"DB mode error: Failed to create connector config: {str(e)}"
        print(f"❌ {error_msg}")
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)

    # Deploy connector and start streaming
    if manager.deploy_connector(connector_config):
        print("\nDebezium connector deployed")
        print("   Starting Spark streaming...\n")

        # Pass bucket name and manual mappings to streaming
        os.environ["OUTPUT_BUCKET"] = bucket_name
        os.environ["BUSINESS_ID"] = bucket_name  # For saving mapping results
        if manual_mappings:
            os.environ["MANUAL_MAPPINGS"] = json.dumps(manual_mappings)
        
        try:
            trigger_once = config.get("trigger_once", False)

            if trigger_once:
                # When running in trigger-once mode (initial onboarding mapping),
                # give Debezium time to start its initial snapshot before Spark
                # tries to read from Kafka.  Without this wait, availableNow may
                # process 0 messages and exit before the snapshot produces events.
                import time as _time
                default_snapshot_wait = 90
                env_wait = os.getenv("DEBEZIUM_SNAPSHOT_WAIT")
                if env_wait is not None:
                    try:
                        snapshot_wait = max(0, int(env_wait))
                    except ValueError:
                        print(
                            f"   Invalid DEBEZIUM_SNAPSHOT_WAIT='{env_wait}', "
                            f"falling back to default {default_snapshot_wait}s.",
                            flush=True,
                        )
                        snapshot_wait = default_snapshot_wait
                else:
                    snapshot_wait = default_snapshot_wait
                print(f"   Waiting {snapshot_wait}s for Debezium initial snapshot...", flush=True)
                _time.sleep(snapshot_wait)

            enable_downstream = config.get("enable_downstream", False)
            run_streaming(trigger_once=trigger_once,
                          enable_downstream=enable_downstream)

            # After trigger-once streaming finishes, update mapping status to
            # 'completed' if the streaming callback didn't already do it (e.g.
            # because no data was available from the snapshot yet).
            if trigger_once:
                try:
                    import psycopg2
                    db_host = os.getenv("POSTGRES_SERVER", "postgresql")
                    db_name_pg = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
                    db_user = os.getenv("POSTGRES_USER", "postgres")
                    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
                    with psycopg2.connect(host=db_host, database=db_name_pg,
                                          user=db_user, password=db_password) as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT mapping_status FROM onboarding WHERE business_id = %s",
                                (bucket_name,)
                            )
                            row = cur.fetchone()
                            if row and row[0] == "running":
                                from datetime import datetime, timezone
                                cur.execute("""
                                    UPDATE onboarding
                                    SET mapping_status = 'completed',
                                        mapping_completed_at = %s,
                                        mapping_error = NULL,
                                        current_step = 'mapping'
                                    WHERE business_id = %s
                                """, (datetime.now(timezone.utc), bucket_name))
                                conn.commit()
                                print("   Mapping status set to 'completed' (trigger-once fallback)")
                except Exception as status_err:
                    print(f"⚠️  Could not update mapping status after trigger-once: {status_err}")
        except Exception as e:
            error_msg = f"DB mode error: Streaming failed: {str(e)}"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)
    else:
        error_msg = "DB mode error: Failed to deploy Debezium connector"
        print(f"❌ {error_msg}")
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)


def run_api_mode(api_url: str, bucket_name: str, poll_interval: int = 10,
                 kafka_bootstrap: Optional[str] = None,
                 poll_duration: int = 0, trigger_once: bool = False,
                 enable_downstream: bool = False):
    """
    API mode: Ingest from API endpoint -> Kafka -> Spark Streaming -> mapped folder.
    
    This mode runs two processes:
    1. API ingestion service (polls API and sends to Kafka)
    2. Spark streaming consumer (consumes from Kafka, maps, saves to MinIO)
    
    Args:
        api_url: API endpoint URL
        bucket_name: Name of the MinIO bucket
        poll_interval: Polling interval in seconds
        kafka_bootstrap: Kafka bootstrap servers (defaults to env var)
    """
    print(f"\n{'='*60}")
    print(f"API MODE: Ingesting from API endpoint")
    print(f"{'='*60}")
    print(f"API URL: {api_url}")
    print(f"Bucket: {bucket_name}")
    print(f"Poll interval: {poll_interval}s\n")
    
    # Try to retrieve manual mappings from Redis
    manual_mappings = None
    try:
        redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)
        manual_mappings_str = redis_client.get(f"manual_mappings:{bucket_name}")
        if manual_mappings_str:
            manual_mappings = json.loads(manual_mappings_str)
            print(f"\n✅ Retrieved manual mappings from Redis")
            print(f"   Tables with manual mappings: {list(manual_mappings.keys())}")
    except Exception as redis_error:
        print(f"⚠️  Warning: Could not retrieve manual mappings from Redis: {redis_error}")
    
    # Get Kafka bootstrap from env if not provided
    if kafka_bootstrap is None:
        from dotenv import load_dotenv, find_dotenv
        load_dotenv(find_dotenv())
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
    
    print(f"Using Kafka: {kafka_bootstrap}\n")
    
    # Import the necessary modules
    try:
        from streaming.ingestion.api_ingest_service import run as run_api_ingestion
        from streaming.spark_streaming import run_streaming
        
        # Function to run API ingestion in a separate process
        def run_api_service():
            print("Starting API ingestion service...")
            run_api_ingestion(
                api_url=api_url,
                poll_interval=poll_interval,
                kafka_bootstrap=kafka_bootstrap,
            )

        # Function to run Spark streaming in a separate process
        def run_spark_consumer():
            print("Starting Spark streaming consumer...")
            # Update the output bucket in environment
            os.environ["OUTPUT_BUCKET"] = bucket_name
            os.environ["BUSINESS_ID"] = bucket_name  # For saving mapping results
            if manual_mappings:
                os.environ["MANUAL_MAPPINGS"] = json.dumps(manual_mappings)
            run_streaming(trigger_once=trigger_once,
                          enable_downstream=enable_downstream)
        
        # Run both processes
        print("Starting parallel processes:")
        print("  1. API ingestion -> Kafka")
        print("  2. Spark streaming -> MinIO mapped/\n")
        
        api_process = multiprocessing.Process(target=run_api_service)
        spark_process = multiprocessing.Process(target=run_spark_consumer)
        
        api_process.start()
        spark_process.start()
        
        print("Both processes started. Press Ctrl+C to stop.\n")

        try:
            if poll_duration > 0:
                # Run the API ingestion for a bounded window (e.g. initial schema
                # discovery during onboarding), then stop both processes cleanly.
                print(f"Bounded run: will stop API ingestion after {poll_duration}s.")
                api_process.join(timeout=poll_duration)
                if api_process.is_alive():
                    api_process.terminate()
                    api_process.join()
                # Give Spark a little time to drain remaining Kafka messages.
                spark_process.join(timeout=30)
                if spark_process.is_alive():
                    spark_process.terminate()
                    spark_process.join()
                print(f"✅ API MODE COMPLETED ({poll_duration}s window)\n")
            else:
                # Run indefinitely (production streaming via Airflow DAG).
                api_process.join()
                spark_process.join()
        except KeyboardInterrupt:
            print("\n\nStopping processes...")
            api_process.terminate()
            spark_process.terminate()
            api_process.join()
            spark_process.join()
            print("✅ API MODE STOPPED\n")
            
    except Exception as e:
        error_msg = f"API mode error: {str(e)}"
        print(f"❌ {error_msg}")
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
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
    parser.add_argument('--enable-downstream', action='store_true',
                        help='Run downstream pipeline (clean/transform/analyze/ML) inline '
                             'after each micro-batch for near-real-time latency (db/api mode)')
    
    args = parser.parse_args()
    
    # Use command-line args if provided, otherwise use CONFIG
    mode = args.mode if args.mode else CONFIG["mode"]
    bucket_name = args.business_id if args.business_id else CONFIG["bucket_name"]
    trigger_once = args.trigger_once
    poll_duration = args.poll_duration
    enable_downstream = args.enable_downstream
    
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
            print(f"{'='*60}\n", flush=True)

            run_db_mode({
                "db_uri": db_uri,
                "db_tables": db_tables,
                "bucket_name": bucket_name,
                "trigger_once": trigger_once,
                "enable_downstream": enable_downstream,
            })

        elif mode == "api":
            api_url = args.api_url if args.api_url else CONFIG["api_url"]
            poll_interval = args.api_poll_interval if args.api_poll_interval else CONFIG["api_poll_interval"]
            kafka_bootstrap = CONFIG["kafka_bootstrap"]
            
            print(f"  API URL: {api_url}", flush=True)
            print(f"  Poll interval: {poll_interval}s", flush=True)
            print(f"{'='*60}\n", flush=True)
            
            run_api_mode(api_url, bucket_name, poll_interval, kafka_bootstrap,
                         poll_duration=poll_duration, trigger_once=trigger_once,
                         enable_downstream=enable_downstream)

        else:
            error_msg = f"Invalid mode '{mode}'. Valid modes: batch, db, api"
            print(f"\n❌ ERROR: {error_msg}", flush=True)
            update_mapping_status(bucket_name, "failed", error_msg)
            sys.exit(1)
            
    except Exception as e:
        # Catch any uncaught errors from the main execution
        error_msg = f"Unexpected error in mapping pipeline: {str(e)}"
        print(f"\n❌ {error_msg}", flush=True)
        traceback.print_exc()
        update_mapping_status(bucket_name, "failed", error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
