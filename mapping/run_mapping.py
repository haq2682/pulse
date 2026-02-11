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
        except Exception as redis_error:
            print(f"⚠️  Warning: Could not save mapping results to Redis: {redis_error}", flush=True)
        
        print(f"\nSaving results to {bucket_name}/mapped...", flush=True)
        save_dataframes_to_minio(results, minio_client, bucket_name)
        
        print(f"\n{'='*60}", flush=True)
        print(f"✅ BATCH MODE COMPLETE", flush=True)
        print(f"   Processed {len(results)} tables", flush=True)
        print(f"   Results saved to {bucket_name}/mapped/", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        spark.stop()
        
    except Exception as e:
        print(f"❌ Error in batch mode: {e}", flush=True)
        import traceback
        traceback.print_exc()
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
        print("Kafka Connect not available")
        sys.exit(1)

    # Auto-detect database type from URI and build connector config
    connector_config = manager.create_connector_config(
        db_uri=db_uri,
        tables=tables,
    )

    # Deploy connector and start streaming
    if manager.deploy_connector(connector_config):
        print("\nDebezium connector deployed")
        print("   Starting Spark streaming...\n")

        # Pass bucket name and manual mappings to streaming
        os.environ["OUTPUT_BUCKET"] = bucket_name
        os.environ["BUSINESS_ID"] = bucket_name  # For saving mapping results
        if manual_mappings:
            os.environ["MANUAL_MAPPINGS"] = json.dumps(manual_mappings)
        
        run_streaming()
    else:
        print("Failed to deploy connector")
        sys.exit(1)


def run_api_mode(api_url: str, bucket_name: str, poll_interval: int = 10, kafka_bootstrap: Optional[str] = None):
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
                kafka_bootstrap=kafka_bootstrap
            )
        
        # Function to run Spark streaming in a separate process
        def run_spark_consumer():
            print("Starting Spark streaming consumer...")
            # Update the output bucket in environment
            os.environ["OUTPUT_BUCKET"] = bucket_name
            os.environ["BUSINESS_ID"] = bucket_name  # For saving mapping results
            if manual_mappings:
                os.environ["MANUAL_MAPPINGS"] = json.dumps(manual_mappings)
            run_streaming()
        
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
        print(f"❌ Error in api mode: {e}")
        import traceback
        traceback.print_exc()
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
    
    args = parser.parse_args()
    
    # Use command-line args if provided, otherwise use CONFIG
    mode = args.mode if args.mode else CONFIG["mode"]
    bucket_name = args.business_id if args.business_id else CONFIG["bucket_name"]
    
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
            "bucket_name": bucket_name
        })

    elif mode == "api":
        api_url = args.api_url if args.api_url else CONFIG["api_url"]
        poll_interval = args.api_poll_interval if args.api_poll_interval else CONFIG["api_poll_interval"]
        kafka_bootstrap = CONFIG["kafka_bootstrap"]
        
        print(f"  API URL: {api_url}", flush=True)
        print(f"  Poll interval: {poll_interval}s", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        run_api_mode(api_url, bucket_name, poll_interval, kafka_bootstrap)

    else:
        print(f"\n❌ ERROR: Invalid mode '{mode}'", flush=True)
        print(f"   Valid modes: batch, db, api", flush=True)
        print(f"   Edit CONFIG in run_mapping.py to change mode\n", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
