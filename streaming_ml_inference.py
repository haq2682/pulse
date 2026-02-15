"""
Streaming ML Inference Pipeline
Applies pre-trained ML models to streaming data in real-time
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from datetime import datetime
import argparse

# Add machine-learning directory to path
sys.path.insert(0, str(Path(__file__).parent / "machine-learning"))

class StreamingMLInference:
    """
    Real-time ML inference on streaming data
    Loads pre-trained models and applies predictions to micro-batches
    """
    
    def __init__(self, bucket_name="pulse-bucket-1", trigger_interval="10 seconds"):
        """
        Initialize streaming ML inference pipeline
        
        Args:
            bucket_name: MinIO bucket name
            trigger_interval: How often to process micro-batches
        """
        self.bucket_name = bucket_name
        self.trigger_interval = trigger_interval
        self.checkpoint_base = "/tmp/spark_checkpoints/ml_inference"
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("StreamingMLInference") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_base) \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        # MinIO configuration
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        self._configure_minio()
        
        # Model cache
        self.loaded_models = {}
        
        print(f"✅ StreamingMLInference initialized")
        print(f"   Bucket: {bucket_name}")
        print(f"   Trigger: {trigger_interval}")
    
    def _configure_minio(self):
        """Configure Spark to connect to MinIO"""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", self.minio_endpoint)
        hadoop_conf.set("fs.s3a.access.key", self.minio_access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.minio_secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    def load_model(self, model_name, model_type="classification"):
        """
        Load pre-trained model from MinIO
        
        Args:
            model_name: Name of the model
            model_type: Type (classification, regression, clustering)
        
        Returns:
            Loaded PipelineModel or None if not found
        """
        cache_key = f"{model_type}/{model_name}"
        
        if cache_key in self.loaded_models:
            print(f"   📦 Using cached model: {model_name}")
            return self.loaded_models[cache_key]
        
        try:
            model_path = f"s3a://{self.bucket_name}/models/general/{model_type}/{model_name}"
            print(f"   🔍 Loading model from: {model_path}")
            
            model = PipelineModel.load(model_path)
            self.loaded_models[cache_key] = model
            
            print(f"   ✅ Model loaded: {model_name}")
            return model
        
        except Exception as e:
            print(f"   ⚠️  Could not load model {model_name}: {e}")
            return None
    
    def run_classification_inference(self, model_name, input_table):
        """
        Run classification model inference
        
        Args:
            model_name: Name of classification model
            input_table: Input table name (e.g., 'agg_customers')
        """
        print(f"\n🔮 Starting classification inference: {model_name}")
        
        model = self.load_model(model_name, "classification")
        if not model:
            print(f"   ❌ Skipping {model_name} - model not found")
            return None
        
        # Read streaming data
        input_path = f"s3a://{self.bucket_name}/transformed_streaming/{input_table}"
        
        try:
            df_stream = self.spark.readStream \
                .format("parquet") \
                .option("maxFilesPerTrigger", "10") \
                .load(input_path)
            
            print(f"   📊 Reading from: {input_path}")
            
            # Apply model
            predictions = model.transform(df_stream)
            
            # Select relevant columns
            output_cols = [col for col in df_stream.columns]
            output_cols.extend(["prediction", "probability"])
            
            predictions_final = predictions.select(*output_cols)
            
            # Write predictions
            output_path = f"s3a://{self.bucket_name}/predictions_streaming/{model_name}"
            checkpoint_path = f"{self.checkpoint_base}/{model_name}"
            
            query = predictions_final.writeStream \
                .format("parquet") \
                .option("path", output_path) \
                .option("checkpointLocation", checkpoint_path) \
                .outputMode("append") \
                .trigger(processingTime=self.trigger_interval) \
                .start()
            
            print(f"   ✅ Inference query started: {model_name}")
            print(f"   📍 Output: {output_path}")
            
            return query
        
        except Exception as e:
            print(f"   ❌ Error starting inference for {model_name}: {e}")
            return None
    
    def run_regression_inference(self, model_name, input_table):
        """
        Run regression model inference
        
        Args:
            model_name: Name of regression model
            input_table: Input table name
        """
        print(f"\n📈 Starting regression inference: {model_name}")
        
        model = self.load_model(model_name, "regression")
        if not model:
            print(f"   ❌ Skipping {model_name} - model not found")
            return None
        
        input_path = f"s3a://{self.bucket_name}/transformed_streaming/{input_table}"
        
        try:
            df_stream = self.spark.readStream \
                .format("parquet") \
                .option("maxFilesPerTrigger", "10") \
                .load(input_path)
            
            print(f"   📊 Reading from: {input_path}")
            
            # Apply model
            predictions = model.transform(df_stream)
            
            # Select relevant columns
            output_cols = [col for col in df_stream.columns]
            output_cols.append("prediction")
            
            predictions_final = predictions.select(*output_cols)
            
            # Write predictions
            output_path = f"s3a://{self.bucket_name}/predictions_streaming/{model_name}"
            checkpoint_path = f"{self.checkpoint_base}/{model_name}"
            
            query = predictions_final.writeStream \
                .format("parquet") \
                .option("path", output_path) \
                .option("checkpointLocation", checkpoint_path) \
                .outputMode("append") \
                .trigger(processingTime=self.trigger_interval) \
                .start()
            
            print(f"   ✅ Inference query started: {model_name}")
            print(f"   📍 Output: {output_path}")
            
            return query
        
        except Exception as e:
            print(f"   ❌ Error starting inference for {model_name}: {e}")
            return None
    
    def run_all_inference(self):
        """
        Start inference for all available models
        """
        print("\n" + "="*70)
        print("🚀 STARTING ML INFERENCE PIPELINE")
        print("="*70)
        
        queries = []
        
        # Classification models
        classification_models = [
            ("customer_churn", "agg_customers.parquet"),
            ("customer_segments", "agg_customers.parquet"),
            ("cart_abandonment", "agg_sessions.parquet"),
            ("payment_success", "agg_orders.parquet"),
            ("stock_status", "agg_products.parquet"),
        ]
        
        print("\n📋 Classification Models:")
        for model_name, input_table in classification_models:
            query = self.run_classification_inference(model_name, input_table)
            if query:
                queries.append(query)
        
        # Regression models
        regression_models = [
            ("clv", "agg_customers.parquet"),
            ("revenue_forecast", "agg_daily_metrics.parquet"),
            ("safety_stock", "agg_products.parquet"),
            ("restock_quantity", "agg_products.parquet"),
            ("stockout_probability", "agg_product_inventory_health.parquet"),
        ]
        
        print("\n📊 Regression Models:")
        for model_name, input_table in regression_models:
            query = self.run_regression_inference(model_name, input_table)
            if query:
                queries.append(query)
        
        print("\n" + "="*70)
        print(f"✅ Started {len(queries)} inference queries")
        print("="*70)
        
        return queries
    
    def monitor_queries(self, queries, interval=30):
        """
        Monitor streaming queries
        
        Args:
            queries: List of StreamingQuery objects
            interval: Monitoring interval in seconds
        """
        import time
        
        print(f"\n📊 Monitoring {len(queries)} queries (refresh every {interval}s)")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                print(f"\n{'='*70}")
                print(f"⏰ Status at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*70}")
                
                active_count = 0
                for query in queries:
                    if query and query.isActive:
                        active_count += 1
                        progress = query.lastProgress
                        
                        if progress:
                            batch_id = progress.get("batchId", "N/A")
                            num_rows = progress.get("numInputRows", 0)
                            
                            print(f"🟢 {query.name or 'Query'}")
                            print(f"   Batch: {batch_id} | Rows: {num_rows}")
                        else:
                            print(f"🔄 {query.name or 'Query'} - Starting...")
                
                print(f"\n📈 Active Queries: {active_count}/{len(queries)}")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping monitoring...")
            for query in queries:
                if query and query.isActive:
                    print(f"   Stopping {query.name or 'query'}...")
                    query.stop()
            print("✅ All queries stopped")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Streaming ML Inference Pipeline')
    parser.add_argument('--bucket-name', type=str, default='pulse-bucket-1',
                       help='MinIO bucket name')
    parser.add_argument('--trigger-interval', type=str, default='10 seconds',
                       help='Trigger interval for micro-batches')
    parser.add_argument('--monitor-interval', type=int, default=30,
                       help='Monitoring interval in seconds')
    
    args = parser.parse_args()
    
    # Initialize inference pipeline
    inference = StreamingMLInference(
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval
    )
    
    # Start all inference queries
    queries = inference.run_all_inference()
    
    if queries:
        # Monitor queries
        inference.monitor_queries(queries, interval=args.monitor_interval)
    else:
        print("\n❌ No queries started. Check model availability.")


if __name__ == "__main__":
    main()
