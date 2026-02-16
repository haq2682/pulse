"""
Streaming Transformation Pipeline - Refactored to Functional Style with Code Reuse

This module implements Spark Structured Streaming for continuous transformations.
REFACTORED: Uses pure functions and imports from existing transformation modules.

Key Changes:
- Removed StreamingTransformer class → pure functions
- Imports from transformation.aggregations.* modules
- Imports from transformation.transformations.* modules
- No duplicate code - reuses existing batch transformation logic
- Functional programming style for easier testing and maintenance

Features:
- Continuous transformation of cleaned data
- 10-second micro-batch processing
- Reuses existing aggregation functions
- Stateful aggregations with watermarking
- Checkpoint-based fault tolerance
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Import existing aggregation functions - NO DUPLICATION!
from transformation.aggregations.customers import aggregate_customers
from transformation.aggregations.orders import aggregate_orders
from transformation.aggregations.products import aggregate_products
from transformation.aggregations.categories import aggregate_categories
from transformation.aggregations.inventory_health import aggregate_inventory_health
from transformation.aggregations.geographic import aggregate_geographic


def create_transformation_stream(spark, source_path, table_name,
                                output_path=None,
                                checkpoint_path="/tmp/spark_checkpoints/transformation",
                                trigger_interval="10 seconds",
                                file_format="parquet"):
    """
    Create streaming transformation pipeline using existing batch aggregation functions.
    Pure function - no class, no state, imports existing logic.
    
    Args:
        spark: SparkSession instance
        source_path: Path to cleaned data (e.g., "s3a://bucket/cleaned/orders/")
        table_name: Name of the table (e.g., "orders")
        output_path: Path to write transformed data (optional)
        checkpoint_path: Path for Spark checkpoints
        trigger_interval: Micro-batch trigger interval (default: "10 seconds")
        file_format: Input file format (parquet recommended)
        
    Returns:
        StreamingQuery object
    """
    # Configure Spark for streaming
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpoint_path)
    
    # Read stream
    df = (spark.readStream
          .format(file_format)
          .option("maxFilesPerTrigger", 1)
          .load(source_path))
    
    print(f"✅ Created transformation stream from {source_path}")
    print(f"   Table: {table_name}")
    print(f"   Trigger: {trigger_interval}")
    
    # Apply transformation using foreachBatch to use existing functions
    def apply_batch_transformation(batch_df, batch_id):
        """
        Apply existing batch transformation functions to each micro-batch.
        This is the key: REUSE existing functions, don't duplicate!
        """
        if batch_df.isEmpty():
            return
        
        print(f"\n📊 Processing batch {batch_id} for {table_name}")
        print(f"   Input rows: {batch_df.count()}")
        
        # Wrap batch_df in dict format expected by existing functions
        dataframes = {table_name: batch_df}
        
        # REUSE existing aggregation functions (no duplication!)
        # These are the same functions used in batch processing
        if table_name == "orders":
            aggregate_orders(dataframes)
        elif table_name == "customers":
            aggregate_customers(dataframes)
        elif table_name == "products":
            aggregate_products(dataframes)
        elif table_name == "categories":
            aggregate_categories(dataframes)
        elif table_name == "inventory":
            aggregate_inventory_health(dataframes)
        elif table_name == "geographic":
            aggregate_geographic(dataframes)
        
        # Add processing timestamp
        transformed_df = dataframes[table_name].withColumn(
            "streaming_transformed_at",
            current_timestamp()
        )
        
        print(f"   Output rows: {transformed_df.count()}")
        print(f"   ✅ Batch {batch_id} transformed successfully")
        
        # Write to output if specified
        if output_path:
            (transformed_df.write
             .mode("append")
             .format("parquet")
             .save(output_path))
        
        return transformed_df
    
    # Create streaming query with foreachBatch
    checkpoint_full_path = f"{checkpoint_path}/{table_name}"
    
    if output_path:
        query = (df.writeStream
                 .foreachBatch(apply_batch_transformation)
                 .trigger(processingTime=trigger_interval)
                 .option("checkpointLocation", checkpoint_full_path)
                 .start())
    else:
        query = (df.writeStream
                 .foreachBatch(apply_batch_transformation)
                 .trigger(processingTime=trigger_interval)
                 .option("checkpointLocation", checkpoint_full_path)
                 .format("console")
                 .start())
    
    print(f"✅ Streaming transformation query started for {table_name}")
    return query


def create_all_transformation_streams(spark, bucket_name="pulse-bucket-1",
                                     trigger_interval="10 seconds"):
    """
    Create streaming transformation pipelines for all tables.
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
        source_path = f"s3a://{bucket_name}/cleaned_streaming/{table}/"
        output_path = f"s3a://{bucket_name}/transformed_streaming/{table}/"
        checkpoint_path = f"/tmp/spark_checkpoints/transformation/{table}"
        
        query = create_transformation_stream(
            spark=spark,
            source_path=source_path,
            table_name=table,
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            trigger_interval=trigger_interval
        )
        
        queries.append(query)
    
    print(f"\n✅ All {len(queries)} transformation streams started")
    return queries


def monitor_transformation_queries(queries):
    """
    Monitor streaming queries and display status.
    Pure function for monitoring.
    
    Args:
        queries: List of StreamingQuery objects
    """
    print("\n📊 STREAMING TRANSFORMATION STATUS")
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
    Main entry point for streaming transformation.
    Pure function - just orchestrates other functions.
    """
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Streaming Transformation Pipeline')
    parser.add_argument('--bucket-name', required=True, help='Business ID / bucket name')
    parser.add_argument('--mode', default='batch', choices=['batch', 'db', 'api'], 
                       help='Data ingestion mode (batch, db, api)')
    parser.add_argument('--trigger-interval', default='10 seconds', 
                       help='Trigger interval for micro-batches')
    args = parser.parse_args()
    
    # Create Spark session
    spark = (SparkSession.builder
             .appName(f"StreamingTransformation-{args.bucket_name}")
             .config("spark.sql.streaming.schemaInference", "true")
             .getOrCreate())
    
    print(f"🚀 Starting Streaming Transformation Pipeline (Functional Style)")
    print(f"   Business ID: {args.bucket_name}")
    print(f"   Mode: {args.mode}")
    print(f"   Trigger Interval: {args.trigger_interval}")
    print("=" * 60)
    
    # Start all transformation streams
    queries = create_all_transformation_streams(
        spark, 
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval
    )
    
    # Monitor queries
    try:
        while True:
            import time
            time.sleep(30)  # Monitor every 30 seconds
            monitor_transformation_queries(queries)
    except KeyboardInterrupt:
        print("\n⚠️  Stopping streaming queries...")
        for query in queries:
            query.stop()
        print("✅ All queries stopped")
    
    spark.stop()


if __name__ == "__main__":
    main()
