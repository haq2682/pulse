"""
Streaming Pipeline Orchestrator - Phase 2

This module orchestrates the complete streaming pipeline:
1. Streaming Cleaning (MinIO/mapped/ → MinIO/cleaned_streaming/)
2. Streaming Transformation (MinIO/cleaned_streaming/ → MinIO/transformed_streaming/)
3. Streaming Analysis (MinIO/transformed_streaming/ → MinIO/analytics_streaming/)

Manages lifecycle of all streaming queries and provides unified monitoring.
"""

import os
import sys
import time
import argparse
from datetime import datetime
from pyspark.sql import SparkSession


class StreamingOrchestrator:
    """
    Orchestrates multiple streaming pipelines with unified management.
    """
    
    def __init__(self, bucket_name="pulse-bucket-1", trigger_interval="10 seconds"):
        """
        Initialize orchestrator.
        
        Args:
            bucket_name: MinIO bucket name
            trigger_interval: Micro-batch trigger interval
        """
        self.bucket_name = bucket_name
        self.trigger_interval = trigger_interval
        self.spark = None
        self.cleaning_queries = []
        self.transformation_queries = []
        self.all_queries = []
        
        print("=" * 60)
        print("🎬 STREAMING PIPELINE ORCHESTRATOR - PHASE 2")
        print("=" * 60)
        print(f"Bucket: {bucket_name}")
        print(f"Trigger Interval: {trigger_interval}")
    
    def initialize_spark(self):
        """Initialize Spark session with streaming configuration."""
        from cleaning.cleaning_config import create_spark_session
        
        self.spark = create_spark_session()
        
        # Additional streaming configurations
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints")
        self.spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
                           "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        
        print("✅ Spark session initialized for streaming")
    
    def start_cleaning_pipeline(self, tables=None):
        """
        Start streaming cleaning pipeline for specified tables.
        
        Args:
            tables: List of table names (default: orders, customers, products)
        """
        if tables is None:
            tables = ["orders", "customers", "products", "order_items", "payments"]
        
        print(f"\n{'='*60}")
        print(f"🧹 STARTING CLEANING PIPELINE")
        print(f"{'='*60}")
        print(f"Tables: {', '.join(tables)}")
        
        from cleaning.streaming_cleaning import StreamingCleaner
        
        cleaner = StreamingCleaner(
            spark=self.spark,
            bucket_name=self.bucket_name,
            trigger_interval=self.trigger_interval
        )
        
        for table in tables:
            try:
                print(f"\n▶ Starting cleaning for: {table}")
                query = cleaner.create_cleaning_pipeline(table)
                self.cleaning_queries.append({
                    'name': f'clean_{table}',
                    'table': table,
                    'query': query,
                    'type': 'cleaning',
                    'started_at': datetime.now()
                })
                print(f"✅ Cleaning pipeline started for {table}")
            except Exception as e:
                print(f"❌ Error starting cleaning for {table}: {e}")
        
        print(f"\n✅ Started {len(self.cleaning_queries)} cleaning queries")
    
    def start_transformation_pipeline(self, tables=None):
        """
        Start streaming transformation pipeline for specified tables.
        
        Args:
            tables: List of table names
        """
        if tables is None:
            tables = ["orders", "customers", "products"]
        
        print(f"\n{'='*60}")
        print(f"🔄 STARTING TRANSFORMATION PIPELINE")
        print(f"{'='*60}")
        print(f"Tables: {', '.join(tables)}")
        
        from transformation.streaming_transformation import StreamingTransformer
        
        transformer = StreamingTransformer(
            spark=self.spark,
            bucket_name=self.bucket_name,
            trigger_interval=self.trigger_interval
        )
        
        for table in tables:
            try:
                print(f"\n▶ Starting transformation for: {table}")
                query = transformer.create_transformation_pipeline(table)
                self.transformation_queries.append({
                    'name': f'transform_{table}',
                    'table': table,
                    'query': query,
                    'type': 'transformation',
                    'started_at': datetime.now()
                })
                print(f"✅ Transformation pipeline started for {table}")
            except Exception as e:
                print(f"❌ Error starting transformation for {table}: {e}")
        
        print(f"\n✅ Started {len(self.transformation_queries)} transformation queries")
    
    def get_all_queries(self):
        """Get list of all active queries."""
        self.all_queries = self.cleaning_queries + self.transformation_queries
        return self.all_queries
    
    def monitor_queries(self):
        """Print status of all queries."""
        queries = self.get_all_queries()
        
        if not queries:
            print("⚠️  No active queries to monitor")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 STREAMING PIPELINE STATUS")
        print(f"{'='*60}")
        print(f"Active Queries: {len(queries)}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Group by type
        cleaning_active = len([q for q in queries if q['type'] == 'cleaning' and q['query'].isActive])
        transform_active = len([q for q in queries if q['type'] == 'transformation' and q['query'].isActive])
        
        print(f"\nCleaning: {cleaning_active}/{len(self.cleaning_queries)} active")
        print(f"Transformation: {transform_active}/{len(self.transformation_queries)} active")
        
        # Detailed status for each query
        print(f"\n{'='*60}")
        print("QUERY DETAILS:")
        print(f"{'='*60}")
        
        for q_info in queries:
            query = q_info['query']
            name = q_info['name']
            
            status_icon = "🟢" if query.isActive else "🔴"
            print(f"\n{status_icon} {name}")
            print(f"   Type: {q_info['type']}")
            print(f"   Started: {q_info['started_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Active: {query.isActive}")
            
            if query.lastProgress:
                progress = query.lastProgress
                batch_id = progress.get('batchId', 'N/A')
                num_rows = progress.get('numInputRows', 0)
                processing_rate = progress.get('processedRowsPerSecond', 0)
                
                print(f"   Batch ID: {batch_id}")
                print(f"   Rows Processed: {num_rows}")
                print(f"   Rate: {processing_rate:.2f} rows/sec" if processing_rate else "   Rate: N/A")
            else:
                print(f"   Status: Waiting for data...")
    
    def get_metrics_summary(self):
        """Get summary metrics for all queries."""
        queries = self.get_all_queries()
        
        total_rows = 0
        total_batches = 0
        
        for q_info in queries:
            query = q_info['query']
            if query.lastProgress:
                total_rows += query.lastProgress.get('numInputRows', 0)
                total_batches += 1
        
        return {
            'total_queries': len(queries),
            'active_queries': len([q for q in queries if q['query'].isActive]),
            'total_rows_processed': total_rows,
            'total_batches': total_batches
        }
    
    def stop_all_queries(self):
        """Stop all streaming queries gracefully."""
        queries = self.get_all_queries()
        
        print(f"\n{'='*60}")
        print(f"🛑 STOPPING ALL STREAMING QUERIES")
        print(f"{'='*60}")
        print(f"Total queries: {len(queries)}")
        
        stopped_count = 0
        for q_info in queries:
            try:
                query = q_info['query']
                name = q_info['name']
                
                if query.isActive:
                    print(f"   Stopping {name}...")
                    query.stop()
                    stopped_count += 1
                    print(f"   ✅ Stopped {name}")
                else:
                    print(f"   ℹ️  {name} already stopped")
            except Exception as e:
                print(f"   ❌ Error stopping {q_info['name']}: {e}")
        
        print(f"\n✅ Stopped {stopped_count} queries")
    
    def run(self, cleaning_tables=None, transformation_tables=None, monitor_interval=30):
        """
        Run the complete streaming pipeline.
        
        Args:
            cleaning_tables: Tables for cleaning pipeline
            transformation_tables: Tables for transformation pipeline
            monitor_interval: Seconds between status updates
        """
        try:
            # Initialize Spark
            self.initialize_spark()
            
            # Start pipelines
            self.start_cleaning_pipeline(cleaning_tables)
            
            # Wait a bit for cleaning to produce data
            print(f"\n⏳ Waiting 30 seconds for cleaning pipeline to produce data...")
            time.sleep(30)
            
            self.start_transformation_pipeline(transformation_tables)
            
            # Monitor loop
            print(f"\n{'='*60}")
            print(f"✅ ALL PIPELINES RUNNING")
            print(f"{'='*60}")
            print(f"Monitoring interval: {monitor_interval} seconds")
            print(f"Press Ctrl+C to stop all pipelines")
            
            while True:
                time.sleep(monitor_interval)
                self.monitor_queries()
                
                # Print metrics summary
                metrics = self.get_metrics_summary()
                print(f"\n📈 METRICS SUMMARY:")
                print(f"   Total Queries: {metrics['total_queries']}")
                print(f"   Active: {metrics['active_queries']}")
                print(f"   Total Rows Processed: {metrics['total_rows_processed']}")
                print(f"   Total Batches: {metrics['total_batches']}")
        
        except KeyboardInterrupt:
            print(f"\n\n⚠️  KEYBOARD INTERRUPT RECEIVED")
        
        except Exception as e:
            print(f"\n\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Always stop queries
            self.stop_all_queries()
            
            # Stop Spark
            if self.spark:
                print("\n🛑 Stopping Spark session...")
                self.spark.stop()
                print("✅ Spark session stopped")
            
            print(f"\n{'='*60}")
            print(f"✅ STREAMING PIPELINE ORCHESTRATOR STOPPED")
            print(f"{'='*60}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Streaming Pipeline Orchestrator - Phase 2')
    parser.add_argument('--bucket-name', type=str, default='pulse-bucket-1',
                       help='MinIO bucket name')
    parser.add_argument('--trigger-interval', type=str, default='10 seconds',
                       help='Micro-batch trigger interval')
    parser.add_argument('--monitor-interval', type=int, default=30,
                       help='Status monitoring interval in seconds')
    parser.add_argument('--cleaning-only', action='store_true',
                       help='Run only cleaning pipeline')
    parser.add_argument('--transformation-only', action='store_true',
                       help='Run only transformation pipeline')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = StreamingOrchestrator(
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval
    )
    
    # Determine which pipelines to run
    cleaning_tables = None if not args.transformation_only else []
    transformation_tables = None if not args.cleaning_only else []
    
    # Run
    orchestrator.run(
        cleaning_tables=cleaning_tables,
        transformation_tables=transformation_tables,
        monitor_interval=args.monitor_interval
    )


if __name__ == "__main__":
    main()
