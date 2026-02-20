"""
Streaming Transformation Pipeline - Phase 2

This module implements Spark Structured Streaming for continuous data transformations
and aggregations. Processes cleaned data with 10-second micro-batches and maintains
stateful aggregations.

Features:
- Continuous monitoring of MinIO/cleaned_streaming/
- Stateful aggregations with watermarking
- 10-second micro-batch processing
- Windowed aggregations (hourly, daily)
- Real-time metrics computation
- Checkpoint-based fault tolerance
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    window, current_timestamp, to_date, date_format, hour, dayofweek,
    lit, when, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


class StreamingTransformer:
    """
    Manages streaming data transformation and aggregation pipeline.
    """
    
    def __init__(self, spark, bucket_name="pulse-bucket-1", trigger_interval="10 seconds"):
        """
        Initialize streaming transformer.
        
        Args:
            spark: SparkSession instance
            bucket_name: MinIO bucket name
            trigger_interval: Micro-batch trigger interval (default: 10 seconds)
        """
        self.spark = spark
        self.bucket_name = bucket_name
        self.trigger_interval = trigger_interval
        self.checkpoint_location = "/tmp/spark_checkpoints/transformation"
        
        # Configure Spark for streaming
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
        
        print(f"✅ StreamingTransformer initialized")
        print(f"   Bucket: {bucket_name}")
        print(f"   Trigger: {trigger_interval}")
        print(f"   Checkpoints: {self.checkpoint_location}")
    
    def create_input_stream(self, source_path, file_format="parquet"):
        """
        Create input streaming DataFrame from cleaned data.
        
        Args:
            source_path: Path to cleaned data
            file_format: File format (parquet, csv, json)
            
        Returns:
            Streaming DataFrame
        """
        reader = self.spark.readStream.format(file_format)
        
        if file_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        
        reader = reader.option("maxFilesPerTrigger", 1)
        
        df = reader.load(source_path)
        
        print(f"✅ Created input stream from {source_path}")
        return df
    
    def aggregate_orders_streaming(self, orders_df):
        """
        Create streaming aggregations for orders.
        
        Args:
            orders_df: Streaming orders DataFrame
            
        Returns:
            Aggregated DataFrame
        """
        # Ensure we have a timestamp column for watermarking
        if "order_date" not in orders_df.columns and "cleaned_at" in orders_df.columns:
            orders_df = orders_df.withColumn("order_date", col("cleaned_at"))
        
        # Add watermark to handle late data
        orders_df = orders_df.withWatermark("order_date", "1 hour")
        
        # Hourly aggregations
        hourly_agg = (
            orders_df
            .groupBy(
                window(col("order_date"), "1 hour"),
                col("order_status")
            )
            .agg(
                count("*").alias("order_count"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_min("total_amount").alias("min_order_value"),
                spark_max("total_amount").alias("max_order_value")
            )
            .withColumn("aggregation_type", lit("hourly"))
            .withColumn("computed_at", current_timestamp())
        )
        
        return hourly_agg
    
    def aggregate_customers_streaming(self, customers_df, orders_df):
        """
        Create streaming customer aggregations.
        
        Args:
            customers_df: Streaming customers DataFrame
            orders_df: Streaming orders DataFrame
            
        Returns:
            Aggregated DataFrame
        """
        # Join customers with their orders
        customer_orders = customers_df.join(
            orders_df,
            customers_df.customer_id == orders_df.customer_id,
            "left"
        )
        
        # Add watermark
        if "order_date" in customer_orders.columns:
            customer_orders = customer_orders.withWatermark("order_date", "1 hour")
        
        # Aggregate by customer
        customer_agg = (
            customer_orders
            .groupBy(
                window(col("order_date"), "1 hour") if "order_date" in customer_orders.columns else lit(None),
                col("customer_id"),
                col("customer_segment")
            )
            .agg(
                count("order_id").alias("total_orders"),
                spark_sum("total_amount").alias("total_spent"),
                avg("total_amount").alias("avg_order_value"),
                spark_max("order_date").alias("last_order_date")
            )
            .withColumn("computed_at", current_timestamp())
        )
        
        return customer_agg
    
    def aggregate_products_streaming(self, products_df, order_items_df):
        """
        Create streaming product aggregations.
        
        Args:
            products_df: Streaming products DataFrame
            order_items_df: Streaming order items DataFrame
            
        Returns:
            Aggregated DataFrame
        """
        # Join products with order items
        product_sales = products_df.join(
            order_items_df,
            products_df.product_id == order_items_df.product_id,
            "left"
        )
        
        # Aggregate by product
        product_agg = (
            product_sales
            .groupBy(
                col("product_id"),
                col("product_name"),
                col("category_id")
            )
            .agg(
                count("order_item_id").alias("units_sold"),
                spark_sum("quantity").alias("total_quantity"),
                spark_sum("total_price").alias("total_revenue"),
                avg("unit_price").alias("avg_price")
            )
            .withColumn("computed_at", current_timestamp())
        )
        
        return product_agg
    
    def create_time_based_aggregations(self, df, timestamp_col="order_date"):
        """
        Create time-based aggregations (hourly, daily, etc.).
        
        Args:
            df: Streaming DataFrame with timestamp column
            timestamp_col: Name of timestamp column
            
        Returns:
            Aggregated DataFrame
        """
        # Add watermark
        df = df.withWatermark(timestamp_col, "1 hour")
        
        # Add time dimensions
        df = df.withColumn("hour_of_day", hour(col(timestamp_col)))
        df = df.withColumn("day_of_week", dayofweek(col(timestamp_col)))
        df = df.withColumn("date", to_date(col(timestamp_col)))
        
        # Hourly aggregations
        hourly_agg = (
            df
            .groupBy(
                window(col(timestamp_col), "1 hour"),
                col("hour_of_day")
            )
            .agg(
                count("*").alias("event_count"),
                spark_sum("total_amount").alias("total_amount") if "total_amount" in df.columns else lit(0)
            )
            .withColumn("aggregation_window", lit("hourly"))
            .withColumn("computed_at", current_timestamp())
        )
        
        return hourly_agg
    
    def write_stream(self, df, output_path, checkpoint_suffix, output_mode="update"):
        """
        Write streaming DataFrame to MinIO.
        
        Args:
            df: Streaming DataFrame
            output_path: Destination path
            checkpoint_suffix: Unique suffix for checkpoint
            output_mode: append, update, or complete
            
        Returns:
            StreamingQuery object
        """
        checkpoint_path = f"{self.checkpoint_location}/{checkpoint_suffix}"
        
        query = (
            df.writeStream
            .format("parquet")
            .outputMode(output_mode)
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=self.trigger_interval)
            .start()
        )
        
        print(f"✅ Started streaming write to {output_path}")
        print(f"   Checkpoint: {checkpoint_path}")
        print(f"   Mode: {output_mode}")
        
        return query
    
    def create_transformation_pipeline(self, table_name):
        """
        Create end-to-end streaming transformation pipeline for a table.
        
        Args:
            table_name: Name of table to transform
            
        Returns:
            StreamingQuery object
        """
        print(f"\n{'='*60}")
        print(f"Creating streaming transformation for: {table_name}")
        print(f"{'='*60}")
        
        # Define paths
        source_path = f"s3a://{self.bucket_name}/cleaned_streaming/{table_name}/"
        output_path = f"s3a://{self.bucket_name}/transformed_streaming/{table_name}/"
        
        # Create input stream
        df = self.create_input_stream(source_path)
        
        # Apply transformations based on table
        if table_name == "orders":
            df_transformed = self.aggregate_orders_streaming(df)
            output_mode = "update"
        elif table_name == "customers":
            # Would need orders stream too - simplified for now
            df_transformed = df.withColumn("computed_at", current_timestamp())
            output_mode = "append"
        elif table_name == "products":
            # Would need order_items stream too - simplified for now
            df_transformed = df.withColumn("computed_at", current_timestamp())
            output_mode = "append"
        else:
            # Default: pass through with timestamp
            df_transformed = df.withColumn("computed_at", current_timestamp())
            output_mode = "append"
        
        # Write stream
        query = self.write_stream(
            df_transformed,
            output_path,
            checkpoint_suffix=f"transform_{table_name}",
            output_mode=output_mode
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
    Main function to run streaming transformation pipeline.
    """
    print("=" * 60)
    print("🚀 STARTING STREAMING TRANSFORMATION PIPELINE - PHASE 2")
    print("=" * 60)
    
    # Create Spark session
    from transformation.config.spark_config import create_spark_session
    spark = create_spark_session()
    
    # Create streaming transformer
    transformer = StreamingTransformer(
        spark=spark,
        bucket_name=os.getenv("MINIO_BUCKET", "pulse-bucket-1"),
        trigger_interval="10 seconds"
    )
    
    # Define tables to transform
    tables = ["orders", "customers", "products"]
    
    # Create streaming pipelines
    queries = []
    for table in tables:
        try:
            query = transformer.create_transformation_pipeline(table)
            queries.append((table, query))
        except Exception as e:
            print(f"❌ Error creating pipeline for {table}: {e}")
    
    # Monitor queries
    print(f"\n✅ Started {len(queries)} streaming queries")
    print("Press Ctrl+C to stop...")
    
    try:
        import time
        while True:
            time.sleep(30)
            print(f"\n{'='*60}")
            print(f"📊 STREAMING STATUS UPDATE")
            print(f"{'='*60}")
            for table, query in queries:
                transformer.monitor_stream(query, f"{table}_transform")
    
    except KeyboardInterrupt:
        print("\n⚠️  Keyboard interrupt received")
    
    finally:
        transformer.stop_all_streams()
        spark.stop()
        print("\n✅ Streaming transformation pipeline stopped")


if __name__ == "__main__":
    main()
