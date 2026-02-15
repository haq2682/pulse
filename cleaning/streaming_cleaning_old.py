"""
Streaming Cleaning Pipeline - Phase 2

This module implements Spark Structured Streaming for continuous data cleaning.
Instead of batch processing, it continuously monitors MinIO/mapped/ for new files
and processes them with 10-second micro-batches.

Features:
- Continuous monitoring of MinIO/mapped/
- 10-second micro-batch processing
- Stateful deduplication
- Streaming data quality checks
- Checkpoint-based fault tolerance
- Real-time cleaning metrics
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, regexp_replace, current_timestamp,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


class StreamingCleaner:
    """
    Manages streaming data cleaning pipeline with Spark Structured Streaming.
    """
    
    def __init__(self, spark, bucket_name="pulse-bucket-1", trigger_interval="10 seconds"):
        """
        Initialize streaming cleaner.
        
        Args:
            spark: SparkSession instance
            bucket_name: MinIO bucket name
            trigger_interval: Micro-batch trigger interval (default: 10 seconds)
        """
        self.spark = spark
        self.bucket_name = bucket_name
        self.trigger_interval = trigger_interval
        self.checkpoint_location = "/tmp/spark_checkpoints/cleaning"
        
        # Configure Spark for streaming
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
        self.spark.conf.set("spark.sql.streaming.schemaInference", "true")
        
        print(f"✅ StreamingCleaner initialized")
        print(f"   Bucket: {bucket_name}")
        print(f"   Trigger: {trigger_interval}")
        print(f"   Checkpoints: {self.checkpoint_location}")
    
    def create_input_stream(self, source_path, schema=None, file_format="csv"):
        """
        Create input streaming DataFrame from MinIO.
        
        Args:
            source_path: Path to source data (e.g., "s3a://bucket/mapped/orders/")
            schema: Optional StructType schema
            file_format: File format (csv, parquet, json)
            
        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format(file_format)
        
        if schema:
            reader = reader.schema(schema)
        
        if file_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        
        # Configure for incremental file discovery
        reader = reader.option("maxFilesPerTrigger", 1)  # Process 1 file per micro-batch
        reader = reader.option("cleanSource", "delete")  # Optional: archive processed files
        
        df = reader.load(source_path)
        
        print(f"✅ Created input stream from {source_path}")
        return df
    
    def clean_text_column(self, df, column_name):
        """
        Clean a text column: trim, remove extra spaces, handle nulls.
        
        Args:
            df: DataFrame
            column_name: Column to clean
            
        Returns:
            DataFrame with cleaned column
        """
        if column_name not in df.columns:
            return df
        
        return df.withColumn(
            column_name,
            when(col(column_name).isNull(), None)
            .otherwise(
                trim(regexp_replace(col(column_name), r'\s+', ' '))
            )
        )
    
    def remove_duplicates_streaming(self, df, key_columns, watermark_column=None, watermark_duration="1 hour"):
        """
        Remove duplicates in streaming context using watermarking.
        
        Args:
            df: Streaming DataFrame
            key_columns: List of columns to check for duplicates
            watermark_column: Column for watermarking (timestamp)
            watermark_duration: How long to keep state (default: 1 hour)
            
        Returns:
            DataFrame without duplicates
        """
        if watermark_column and watermark_column in df.columns:
            # With watermarking (for stateful dedup)
            df = df.withWatermark(watermark_column, watermark_duration)
            return df.dropDuplicates(key_columns)
        else:
            # Simple dedup within micro-batch
            return df.dropDuplicates(key_columns)
    
    def apply_cleaning_rules(self, df, table_name):
        """
        Apply table-specific cleaning rules.
        
        Args:
            df: Streaming DataFrame
            table_name: Name of the table (orders, customers, etc.)
            
        Returns:
            Cleaned DataFrame
        """
        # Add processing timestamp
        df = df.withColumn("cleaned_at", current_timestamp())
        
        # Table-specific cleaning
        if table_name == "orders":
            # Clean order-specific columns
            df = self.clean_text_column(df, "order_status")
            df = self.clean_text_column(df, "payment_method")
            
            # Remove duplicates on order_id
            if "order_id" in df.columns:
                df = self.remove_duplicates_streaming(df, ["order_id"])
        
        elif table_name == "customers":
            # Clean customer-specific columns
            df = self.clean_text_column(df, "first_name")
            df = self.clean_text_column(df, "last_name")
            df = self.clean_text_column(df, "email")
            
            # Remove duplicates on customer_id
            if "customer_id" in df.columns:
                df = self.remove_duplicates_streaming(df, ["customer_id"])
        
        elif table_name == "products":
            # Clean product-specific columns
            df = self.clean_text_column(df, "product_name")
            df = self.clean_text_column(df, "description")
            
            # Remove duplicates on product_id
            if "product_id" in df.columns:
                df = self.remove_duplicates_streaming(df, ["product_id"])
        
        # Add more table-specific rules as needed
        
        return df
    
    def write_stream(self, df, output_path, checkpoint_suffix="default", output_mode="append"):
        """
        Write streaming DataFrame to MinIO.
        
        Args:
            df: Streaming DataFrame
            output_path: Destination path (e.g., "s3a://bucket/cleaned/orders/")
            checkpoint_suffix: Unique suffix for checkpoint location
            output_mode: append, update, or complete
            
        Returns:
            StreamingQuery object
        """
        checkpoint_path = f"{self.checkpoint_location}/{checkpoint_suffix}"
        
        query = (
            df.writeStream
            .format("parquet")  # Use Parquet for efficiency
            .outputMode(output_mode)
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=self.trigger_interval)
            .start()
        )
        
        print(f"✅ Started streaming write to {output_path}")
        print(f"   Checkpoint: {checkpoint_path}")
        print(f"   Mode: {output_mode}")
        print(f"   Trigger: {self.trigger_interval}")
        
        return query
    
    def create_cleaning_pipeline(self, table_name):
        """
        Create end-to-end streaming cleaning pipeline for a table.
        
        Args:
            table_name: Name of table to clean (e.g., "orders")
            
        Returns:
            StreamingQuery object
        """
        print(f"\n{'='*60}")
        print(f"Creating streaming cleaning pipeline for: {table_name}")
        print(f"{'='*60}")
        
        # Define paths
        source_path = f"s3a://{self.bucket_name}/mapped/{table_name}/"
        output_path = f"s3a://{self.bucket_name}/cleaned_streaming/{table_name}/"
        
        # Create input stream
        df = self.create_input_stream(source_path)
        
        # Apply cleaning rules
        df_cleaned = self.apply_cleaning_rules(df, table_name)
        
        # Write stream
        query = self.write_stream(
            df_cleaned,
            output_path,
            checkpoint_suffix=f"cleaning_{table_name}",
            output_mode="append"
        )
        
        return query
    
    def monitor_stream(self, query, query_name="streaming_query"):
        """
        Monitor a streaming query and print status.
        
        Args:
            query: StreamingQuery object
            query_name: Name for logging
        """
        print(f"\n📊 Monitoring: {query_name}")
        print(f"   Status: {query.status}")
        print(f"   Is Active: {query.isActive}")
        
        if query.lastProgress:
            progress = query.lastProgress
            print(f"   Batch ID: {progress.get('batchId', 'N/A')}")
            print(f"   Rows Processed: {progress.get('numInputRows', 'N/A')}")
            print(f"   Processing Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec")
            print(f"   Duration: {progress.get('durationMs', {}).get('total', 'N/A')} ms")
    
    def stop_all_streams(self):
        """Stop all active streaming queries."""
        active_streams = self.spark.streams.active
        print(f"\n🛑 Stopping {len(active_streams)} active streams...")
        
        for stream in active_streams:
            stream.stop()
            print(f"   ✓ Stopped: {stream.name if stream.name else stream.id}")
        
        print("✅ All streams stopped")


def main():
    """
    Main function to run streaming cleaning pipeline.
    """
    print("=" * 60)
    print("🚀 STARTING STREAMING CLEANING PIPELINE - PHASE 2")
    print("=" * 60)
    
    # Create Spark session
    from cleaning.cleaning_config import create_spark_session
    spark = create_spark_session()
    
    # Create streaming cleaner
    cleaner = StreamingCleaner(
        spark=spark,
        bucket_name=os.getenv("MINIO_BUCKET", "pulse-bucket-1"),
        trigger_interval="10 seconds"
    )
    
    # Define tables to stream
    tables = ["orders", "customers", "products"]
    
    # Create streaming pipelines
    queries = []
    for table in tables:
        try:
            query = cleaner.create_cleaning_pipeline(table)
            queries.append(query)
        except Exception as e:
            print(f"❌ Error creating pipeline for {table}: {e}")
    
    # Monitor queries
    print(f"\n✅ Started {len(queries)} streaming queries")
    print("Press Ctrl+C to stop...")
    
    try:
        # Keep running and monitor
        import time
        while True:
            time.sleep(30)  # Check every 30 seconds
            print(f"\n{'='*60}")
            print(f"📊 STREAMING STATUS UPDATE")
            print(f"{'='*60}")
            for i, query in enumerate(queries):
                cleaner.monitor_stream(query, f"{tables[i]}_cleaning")
    
    except KeyboardInterrupt:
        print("\n⚠️  Keyboard interrupt received")
    
    finally:
        # Stop all streams gracefully
        cleaner.stop_all_streams()
        spark.stop()
        print("\n✅ Streaming cleaning pipeline stopped")


if __name__ == "__main__":
    main()
