"""
Streaming Cleaning Pipeline - Refactored to Functional Style with Code Reuse

This module implements Spark Structured Streaming for continuous data cleaning.
REFACTORED: Uses pure functions and imports from existing cleaning modules.

Key Changes:
- Removed StreamingCleaner class → pure functions
- Imports from cleaning.data_cleaning (drop_duplicates, drop_null_rows, etc.)
- Imports from cleaning.standardization (remove_outliers, etc.)
- No duplicate code - reuses existing batch cleaning logic
- Functional programming style for easier testing and maintenance

Features:
- Continuous monitoring of MinIO/mapped/
- 10-second micro-batch processing
- Reuses existing cleaning functions
- Checkpoint-based fault tolerance
- Real-time cleaning metrics
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Import existing cleaning functions - NO DUPLICATION!
from cleaning.data_cleaning import (
    drop_duplicates,
    drop_null_rows,
    clean_text_columns,
    fill_null_values,
    validate_all_cleaned_data
)
from cleaning.standardization import (
    remove_outliers,
    normalize_dates_and_timestamps
)


def create_cleaning_stream(spark, source_path, table_name, 
                          output_path=None,
                          checkpoint_path="/tmp/spark_checkpoints/cleaning",
                          trigger_interval="10 seconds",
                          file_format="csv"):
    """
    Create streaming cleaning pipeline using existing batch cleaning functions.
    Pure function - no class, no state, imports existing logic.
    
    Args:
        spark: SparkSession instance
        source_path: Path to source data (e.g., "s3a://bucket/mapped/orders/")
        table_name: Name of the table (e.g., "orders")
        output_path: Path to write cleaned data (optional)
        checkpoint_path: Path for Spark checkpoints
        trigger_interval: Micro-batch trigger interval (default: "10 seconds")
        file_format: Input file format (csv, parquet, json)
        
    Returns:
        StreamingQuery object
    """
    # Configure Spark for streaming
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_path)
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    
    # Read stream
    reader = spark.readStream.format(file_format)
    
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    
    # Configure for incremental file discovery
    reader = reader.option("maxFilesPerTrigger", 1)  # Process 1 file per micro-batch
    
    df = reader.load(source_path)
    
    print(f"✅ Created input stream from {source_path}")
    print(f"   Table: {table_name}")
    print(f"   Trigger: {trigger_interval}")
    
    # Apply cleaning using foreachBatch to use existing functions
    def apply_batch_cleaning(batch_df, batch_id):
        """
        Apply existing batch cleaning functions to each micro-batch.
        This is the key: REUSE existing functions, don't duplicate!
        """
        if batch_df.isEmpty():
            return
        
        print(f"\n📊 Processing batch {batch_id} for {table_name}")
        print(f"   Input rows: {batch_df.count()}")
        
        # Wrap batch_df in dict format expected by existing functions
        dataframes = {table_name: batch_df}
        
        # REUSE existing cleaning functions (no duplication!)
        # These are the same functions used in batch processing
        dataframes = drop_duplicates(dataframes)
        dataframes = drop_null_rows(dataframes, table_name, "id")
        dataframes = clean_text_columns(dataframes)
        dataframes = fill_null_values(dataframes)
        
        # Add processing timestamp
        cleaned_df = dataframes[table_name].withColumn(
            "streaming_processed_at", 
            current_timestamp()
        )
        
        print(f"   Output rows: {cleaned_df.count()}")
        print(f"   ✅ Batch {batch_id} cleaned successfully")
        
        # Write to output if specified
        if output_path:
            (cleaned_df.write
             .mode("append")
             .format("parquet")
             .save(output_path))
        
        return cleaned_df
    
    # Create streaming query with foreachBatch
    checkpoint_full_path = f"{checkpoint_path}/{table_name}"
    
    if output_path:
        # Write to output path
        query = (df.writeStream
                 .foreachBatch(apply_batch_cleaning)
                 .trigger(processingTime=trigger_interval)
                 .option("checkpointLocation", checkpoint_full_path)
                 .start())
    else:
        # Just process without writing (for testing)
        query = (df.writeStream
                 .foreachBatch(apply_batch_cleaning)
                 .trigger(processingTime=trigger_interval)
                 .option("checkpointLocation", checkpoint_full_path)
                 .format("console")
                 .start())
    
    print(f"✅ Streaming cleaning query started for {table_name}")
    return query


def create_all_cleaning_streams(spark, bucket_name="pulse-bucket-1", 
                                trigger_interval="10 seconds"):
    """
    Create streaming cleaning pipelines for all tables.
    Pure function that orchestrates multiple streams.
    
    Args:
        spark: SparkSession instance
        bucket_name: MinIO bucket name
        trigger_interval: Micro-batch trigger interval
        
    Returns:
        List of StreamingQuery objects
    """
    tables = ["orders", "customers", "products"]
    queries = []
    
    for table in tables:
        source_path = f"s3a://{bucket_name}/mapped/{table}/"
        output_path = f"s3a://{bucket_name}/cleaned_streaming/{table}/"
        checkpoint_path = f"/tmp/spark_checkpoints/cleaning/{table}"
        
        query = create_cleaning_stream(
            spark=spark,
            source_path=source_path,
            table_name=table,
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            trigger_interval=trigger_interval
        )
        
        queries.append(query)
    
    print(f"\n✅ All {len(queries)} cleaning streams started")
    return queries


def monitor_cleaning_queries(queries):
    """
    Monitor streaming queries and display status.
    Pure function for monitoring.
    
    Args:
        queries: List of StreamingQuery objects
    """
    print("\n📊 STREAMING CLEANING STATUS")
    print("=" * 60)
    
    for query in queries:
        if query.isActive:
            status = query.status
            print(f"\n🟢 Query: {query.name or 'unnamed'}")
            print(f"   Active: {query.isActive}")
            print(f"   ID: {query.id}")
            
            if status:
                print(f"   Message: {status.get('message', 'N/A')}")
                print(f"   Data Available: {status.get('isDataAvailable', False)}")
        else:
            print(f"\n🔴 Query: {query.name or 'unnamed'}")
            print(f"   Active: False")
    
    print("=" * 60)


def main():
    """
    Main entry point for streaming cleaning.
    Pure function - just orchestrates other functions.
    """
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Streaming Cleaning Pipeline')
    parser.add_argument('--bucket-name', required=True, help='Business ID / bucket name')
    parser.add_argument('--mode', default='batch', choices=['batch', 'db', 'api'], 
                       help='Data ingestion mode (batch, db, api)')
    parser.add_argument('--trigger-interval', default='10 seconds', 
                       help='Trigger interval for micro-batches')
    args = parser.parse_args()
    
    # Create Spark session
    spark = (SparkSession.builder
             .appName(f"StreamingCleaning-{args.bucket_name}")
             .config("spark.sql.streaming.schemaInference", "true")
             .getOrCreate())
    
    print(f"🚀 Starting Streaming Cleaning Pipeline (Functional Style)")
    print(f"   Business ID: {args.bucket_name}")
    print(f"   Mode: {args.mode}")
    print(f"   Trigger Interval: {args.trigger_interval}")
    print("=" * 60)
    
    # Start all cleaning streams
    queries = create_all_cleaning_streams(
        spark, 
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval
    )
    
    # Monitor queries
    try:
        while True:
            import time
            time.sleep(30)  # Monitor every 30 seconds
            monitor_cleaning_queries(queries)
    except KeyboardInterrupt:
        print("\n⚠️  Stopping streaming queries...")
        for query in queries:
            query.stop()
        print("✅ All queries stopped")
    
    spark.stop()


if __name__ == "__main__":
    main()
