#!/usr/bin/env python3
"""
Unified entry point for the mapping phase with 3 modes:
1. batch: Load from MinIO ingested folder -> map -> save to mapped folder
2. db: Ingest from database URI -> map -> save to mapped folder  
3. api: Ingest from API endpoint -> map -> save to mapped folder

Configuration:
    Edit the CONFIG section below to set the mode and parameters.
    In production, these values will come from the React frontend.
"""

import sys
import os
import multiprocessing
from typing import Optional

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
    "db_uri": "postgresql://user:pass@localhost:5432/ecommerce",  # Database connection URI
    "db_poll_interval": 10,  # Polling interval in seconds
    
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
    print(f"\n{'='*60}")
    print(f"BATCH MODE: Processing files from bucket '{bucket_name}/ingested'")
    print(f"{'='*60}\n")
    
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
        
        print(f"Loading files from {bucket_name}/ingested...")
        all_dataframes = load_all_files_from_minio(minio_client, bucket_name, spark)
        
        if not all_dataframes:
            print("⚠️  No files found in ingested folder")
            return
        
        print(f"\nProcessing {len(all_dataframes)} dataframes through mapping pipeline...")
        results = process_all_dataframes(
            all_dataframes, 
            COLUMNS_INFO, 
            mapping_list,
            mode="batch"
        )
        
        print(f"\nSaving results to {bucket_name}/mapped...")
        save_dataframes_to_minio(results, minio_client, bucket_name)
        
        print(f"\n{'='*60}")
        print(f"✅ BATCH MODE COMPLETE")
        print(f"   Processed {len(results)} tables")
        print(f"   Results saved to {bucket_name}/mapped/")
        print(f"{'='*60}\n")
        
        spark.stop()
        
    except Exception as e:
        print(f"❌ Error in batch mode: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_db_mode(db_uri: str, bucket_name: str, poll_interval: int = 10, kafka_bootstrap: Optional[str] = None):
    """
    DB mode: Ingest from database URI -> Kafka -> Spark Streaming -> mapped folder.
    
    This mode runs two processes:
    1. DB ingestion service (ingests DB changes to Kafka)
    2. Spark streaming consumer (consumes from Kafka, maps, saves to MinIO)
    
    Args:
        db_uri: Database connection URI
        bucket_name: Name of the MinIO bucket
        poll_interval: Polling interval in seconds
        kafka_bootstrap: Kafka bootstrap servers (defaults to env var)
    """
    print(f"\n{'='*60}")
    print(f"DB MODE: Ingesting from database")
    print(f"{'='*60}")
    print(f"Database URI: {db_uri[:30]}...")
    print(f"Bucket: {bucket_name}")
    print(f"Poll interval: {poll_interval}s\n")
    
    # Get Kafka bootstrap from env if not provided
    if kafka_bootstrap is None:
        from dotenv import load_dotenv, find_dotenv
        load_dotenv(find_dotenv())
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
    
    print(f"Using Kafka: {kafka_bootstrap}\n")
    
    # Import the necessary modules
    try:
        from streaming.ingestion.db_ingest_service import ingest_from_uri
        from streaming.spark_streaming import run_streaming
        
        # Function to run DB ingestion in a separate process
        def run_db_ingestion():
            print("Starting DB ingestion service...")
            ingest_from_uri(
                db_uri=db_uri,
                poll_interval=poll_interval,
                kafka_bootstrap=kafka_bootstrap
            )
        
        # Function to run Spark streaming in a separate process
        def run_spark_consumer():
            print("Starting Spark streaming consumer...")
            # Update the output bucket in environment
            os.environ["OUTPUT_BUCKET"] = bucket_name
            run_streaming()
        
        # Run both processes
        print("Starting parallel processes:")
        print("  1. DB ingestion -> Kafka")
        print("  2. Spark streaming -> MinIO mapped/\n")
        
        db_process = multiprocessing.Process(target=run_db_ingestion)
        spark_process = multiprocessing.Process(target=run_spark_consumer)
        
        db_process.start()
        spark_process.start()
        
        print("Both processes started. Press Ctrl+C to stop.\n")
        
        try:
            db_process.join()
            spark_process.join()
        except KeyboardInterrupt:
            print("\n\nStopping processes...")
            db_process.terminate()
            spark_process.terminate()
            db_process.join()
            spark_process.join()
            print("✅ DB MODE STOPPED\n")
            
    except Exception as e:
        print(f"❌ Error in db mode: {e}")
        import traceback
        traceback.print_exc()
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
    In production, CONFIG values will be provided by the React frontend.
    """
    mode = CONFIG["mode"]
    bucket_name = CONFIG["bucket_name"]
    
    print(f"\n{'='*60}")
    print(f"PULSE MAPPING - Starting in {mode.upper()} mode")
    print(f"{'='*60}")
    print(f"Configuration:")
    print(f"  Mode: {mode}")
    print(f"  Bucket: {bucket_name}")
    
    # Validate and execute the appropriate mode
    if mode == "batch":
        print(f"{'='*60}\n")
        run_batch_mode(bucket_name)
        
    elif mode == "db":
        db_uri = CONFIG["db_uri"]
        poll_interval = CONFIG["db_poll_interval"]
        kafka_bootstrap = CONFIG["kafka_bootstrap"]
        
        # Mask password in URI for display
        display_uri = db_uri.split("@")[-1] if "@" in db_uri else db_uri
        print(f"  Database: {display_uri}")
        print(f"  Poll interval: {poll_interval}s")
        print(f"{'='*60}\n")
        
        run_db_mode(db_uri, bucket_name, poll_interval, kafka_bootstrap)
        
    elif mode == "api":
        api_url = CONFIG["api_url"]
        poll_interval = CONFIG["api_poll_interval"]
        kafka_bootstrap = CONFIG["kafka_bootstrap"]
        
        print(f"  API URL: {api_url}")
        print(f"  Poll interval: {poll_interval}s")
        print(f"{'='*60}\n")
        
        run_api_mode(api_url, bucket_name, poll_interval, kafka_bootstrap)
        
    else:
        print(f"\n❌ ERROR: Invalid mode '{mode}'")
        print(f"   Valid modes: batch, db, api")
        print(f"   Edit CONFIG in run_mapping.py to change mode\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
