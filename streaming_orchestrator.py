"""
Streaming Pipeline Orchestrator - Refactored to Functional Style

REFACTORED: Pure functions for pipeline orchestration, no class needed.

Key Changes:
- Removed StreamingOrchestrator class → pure functions
- Imports from refactored streaming modules
- Functional composition of pipelines
- Simplified orchestration logic

Features:
- Start all streaming pipelines
- Monitor pipeline status
- Graceful shutdown
"""

import time
from pyspark.sql import SparkSession

# Import refactored streaming functions
from cleaning.streaming_cleaning import (
    create_all_cleaning_streams,
    monitor_cleaning_queries
)
from transformation.streaming_transformation import (
    create_all_transformation_streams,
    monitor_transformation_queries
)
from analysis.streaming_analysis import create_analysis_stream
from machine_learning.streaming_ml_inference import create_all_ml_inference_streams


def start_streaming_pipeline(spark, bucket_name="pulse-bucket-1",
                            trigger_interval="10 seconds",
                            enable_cleaning=True,
                            enable_transformation=True,
                            enable_analysis=False,
                            enable_ml=False):
    """
    Start complete streaming pipeline.
    Pure function that orchestrates all pipelines.
    
    Args:
        spark: SparkSession instance
        bucket_name: MinIO bucket name
        trigger_interval: Micro-batch trigger interval
        enable_cleaning: Start cleaning streams
        enable_transformation: Start transformation streams
        enable_analysis: Start analysis streams
        enable_ml: Start ML inference streams
        
    Returns:
        Dict with all streaming queries
    """
    queries = {
        'cleaning': [],
        'transformation': [],
        'analysis': [],
        'ml_inference': []
    }
    
    print("🚀 Starting Streaming Pipeline (Functional Style)")
    print("=" * 60)
    
    # Start cleaning streams
    if enable_cleaning:
        print("\n📋 Starting cleaning streams...")
        queries['cleaning'] = create_all_cleaning_streams(
            spark, bucket_name, trigger_interval
        )
    
    # Start transformation streams
    if enable_transformation:
        print("\n📊 Starting transformation streams...")
        queries['transformation'] = create_all_transformation_streams(
            spark, bucket_name, trigger_interval
        )
    
    # Start analysis streams
    if enable_analysis:
        print("\n📈 Starting analysis streams...")
        queries['analysis'] = create_analysis_stream(
            spark, bucket_name, trigger_interval
        )
    
    # Start ML inference streams
    if enable_ml:
        print("\n🔮 Starting ML inference streams...")
        queries['ml_inference'] = create_all_ml_inference_streams(
            spark, bucket_name, trigger_interval
        )
    
    print("\n" + "=" * 60)
    print("✅ All streaming pipelines started")
    
    return queries


def monitor_all_queries(queries, interval=30):
    """
    Monitor all streaming queries.
    Pure function for monitoring.
    
    Args:
        queries: Dict of query lists
        interval: Monitoring interval in seconds
    """
    print(f"\n📊 MONITORING ALL PIPELINES (every {interval}s)")
    print("=" * 60)
    
    # Monitor cleaning
    if queries.get('cleaning'):
        monitor_cleaning_queries(queries['cleaning'])
    
    # Monitor transformation
    if queries.get('transformation'):
        monitor_transformation_queries(queries['transformation'])
    
    # Monitor ML inference
    if queries.get('ml_inference'):
        print("\n🔮 ML INFERENCE STATUS")
        for query in queries['ml_inference']:
            if query and query.isActive:
                print(f"   🟢 {query.name or query.id}: Active")
            else:
                print(f"   🔴 {query.name or query.id}: Inactive")
    
    # Summary
    total_active = sum(
        len([q for q in qlist if q and q.isActive])
        for qlist in queries.values()
    )
    print(f"\n📈 Total Active Queries: {total_active}")
    print("=" * 60)


def stop_all_queries(queries):
    """
    Stop all streaming queries gracefully.
    Pure function for shutdown.
    
    Args:
        queries: Dict of query lists
    """
    print("\n⚠️  Stopping all streaming queries...")
    
    for category, query_list in queries.items():
        print(f"\n   Stopping {category}...")
        for query in query_list:
            if query and query.isActive:
                query.stop()
                print(f"      ✅ Stopped {query.name or query.id}")
    
    print("\n✅ All queries stopped")


def main():
    """
    Main entry point for streaming orchestrator.
    Pure function for CLI.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Streaming Pipeline Orchestrator")
    parser.add_argument("--bucket-name", default="pulse-bucket-1", help="MinIO bucket")
    parser.add_argument("--trigger-interval", default="10 seconds", help="Trigger interval")
    parser.add_argument("--monitor-interval", type=int, default=30, help="Monitor interval (seconds)")
    parser.add_argument("--cleaning-only", action="store_true", help="Only run cleaning")
    parser.add_argument("--transformation-only", action="store_true", help="Only run transformation")
    parser.add_argument("--enable-ml", action="store_true", help="Enable ML inference")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = (SparkSession.builder
             .appName("StreamingOrchestrator")
             .config("spark.sql.streaming.schemaInference", "true")
             .getOrCreate())
    
    # Determine what to enable
    enable_cleaning = not args.transformation_only
    enable_transformation = not args.cleaning_only
    enable_ml = args.enable_ml
    
    # Start pipelines
    queries = start_streaming_pipeline(
        spark=spark,
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval,
        enable_cleaning=enable_cleaning,
        enable_transformation=enable_transformation,
        enable_ml=enable_ml
    )
    
    # Monitor loop
    try:
        while True:
            time.sleep(args.monitor_interval)
            monitor_all_queries(queries, args.monitor_interval)
    except KeyboardInterrupt:
        stop_all_queries(queries)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
