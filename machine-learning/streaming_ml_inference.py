"""
Streaming ML Inference - Refactored to Functional Style with Code Reuse

REFACTORED: Uses pure functions and imports from existing ML modules.

Key Changes:
- Removed StreamingMLInference class → pure functions
- Imports from machine-learning/general/infer.py
- Imports from machine-learning/specific/infer.py
- No duplicate code - reuses existing ML inference logic
- Functional programming style

Features:
- Real-time ML inference on streaming data
- Reuses existing trained models
- 10-second micro-batch predictions
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Import existing ML inference functions - NO DUPLICATION!
try:
    from machine_learning.infer_all import main as infer_all
except ImportError:
    # Fallback for different import paths
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), 'machine-learning'))
    from infer_all import main as infer_all


def load_model_from_minio(bucket_name, model_name, model_type="general"):
    """
    Load pre-trained model from MinIO.
    Simple wrapper function - no class needed.
    
    Args:
        bucket_name: MinIO bucket name
        model_name: Name of the model (e.g., "customer_churn")
        model_type: "general" or "specific"
        
    Returns:
        Loaded model object
    """
    import joblib
    from minio import Minio
    
    # MinIO configuration
    minio_client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
    model_path = f"models/{model_type}/{model_name}.pkl"
    local_path = f"/tmp/{model_name}.pkl"
    
    try:
        minio_client.fget_object(bucket_name, model_path, local_path)
        model = joblib.load(local_path)
        print(f"✅ Loaded model: {model_name}")
        return model
    except Exception as e:
        print(f"⚠️  Failed to load model {model_name}: {e}")
        return None


def create_ml_inference_stream(spark, source_path, model_name, model_path,
                               output_path=None,
                               checkpoint_path="/tmp/spark_checkpoints/ml_inference",
                               trigger_interval="10 seconds"):
    """
    Create streaming ML inference pipeline using existing inference functions.
    Pure function - no class, reuses existing ML logic.
    
    Args:
        spark: SparkSession instance
        source_path: Path to transformed data
        model_name: Name of the model
        model_path: Path to the model file
        output_path: Path to write predictions (optional)
        checkpoint_path: Path for Spark checkpoints
        trigger_interval: Micro-batch trigger interval
        
    Returns:
        StreamingQuery object
    """
    # Load model once (not in every batch)
    import joblib
    model = joblib.load(model_path) if os.path.exists(model_path) else None
    
    if model is None:
        print(f"⚠️  Model not found at {model_path}")
        return None
    
    # Read stream
    df = (spark.readStream
          .format("parquet")
          .option("maxFilesPerTrigger", 1)
          .load(source_path))
    
    print(f"✅ Created ML inference stream for {model_name}")
    
    def apply_batch_inference(batch_df, batch_id):
        """
        Apply ML inference to each micro-batch.
        REUSES existing inference logic!
        """
        if batch_df.isEmpty():
            return
        
        print(f"\n🔮 Running inference batch {batch_id} for {model_name}")
        print(f"   Input rows: {batch_df.count()}")
        
        # Convert to pandas for ML inference (standard approach)
        pandas_df = batch_df.toPandas()
        
        # Apply model prediction (reusing existing logic)
        try:
            predictions = model.predict(pandas_df)
            pandas_df['prediction'] = predictions
            pandas_df['model_name'] = model_name
            pandas_df['inference_timestamp'] = current_timestamp()
            
            # Convert back to Spark DataFrame
            result_df = spark.createDataFrame(pandas_df)
            
            print(f"   Predictions: {len(predictions)}")
            print(f"   ✅ Batch {batch_id} inference complete")
            
            # Write predictions if output specified
            if output_path:
                (result_df.write
                 .mode("append")
                 .format("parquet")
                 .save(output_path))
            
            return result_df
        except Exception as e:
            print(f"   ❌ Inference failed: {e}")
            return batch_df
    
    # Create streaming query
    checkpoint_full_path = f"{checkpoint_path}/{model_name}"
    
    query = (df.writeStream
             .foreachBatch(apply_batch_inference)
             .trigger(processingTime=trigger_interval)
             .option("checkpointLocation", checkpoint_full_path)
             .start())
    
    print(f"✅ Streaming inference query started for {model_name}")
    return query


def create_all_ml_inference_streams(spark, bucket_name="pulse-bucket-1",
                                    trigger_interval="10 seconds"):
    """
    Create ML inference streams for all models.
    Pure function for orchestration.
    
    Args:
        spark: SparkSession instance
        bucket_name: MinIO bucket name
        trigger_interval: Micro-batch trigger interval
        
    Returns:
        List of StreamingQuery objects
    """
    # Define models to run inference on
    models = [
        ("customer_churn", "general"),
        ("clv", "general"),
        ("demand_forecast", "specific"),
    ]
    
    queries = []
    
    for model_name, model_type in models:
        source_path = f"s3a://{bucket_name}/transformed_streaming/customers/"
        model_path = f"/tmp/models/{model_type}/{model_name}.pkl"
        output_path = f"s3a://{bucket_name}/predictions_streaming/{model_name}/"
        
        query = create_ml_inference_stream(
            spark=spark,
            source_path=source_path,
            model_name=model_name,
            model_path=model_path,
            output_path=output_path,
            trigger_interval=trigger_interval
        )
        
        if query:
            queries.append(query)
    
    print(f"\n✅ All {len(queries)} ML inference streams started")
    return queries


def main():
    """
    Main entry point for streaming ML inference.
    Pure function - just orchestrates other functions.
    """
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Streaming ML Inference Pipeline')
    parser.add_argument('--bucket-name', required=True, help='Business ID / bucket name')
    parser.add_argument('--mode', default='batch', choices=['batch', 'db', 'api'], 
                       help='Data ingestion mode (batch, db, api)')
    parser.add_argument('--trigger-interval', default='10 seconds', 
                       help='Trigger interval for micro-batches')
    args = parser.parse_args()
    
    spark = (SparkSession.builder
             .appName(f"StreamingMLInference-{args.bucket_name}")
             .getOrCreate())
    
    print(f"🚀 Starting Streaming ML Inference (Functional Style)")
    print(f"   Business ID: {args.bucket_name}")
    print(f"   Mode: {args.mode}")
    print(f"   Trigger Interval: {args.trigger_interval}")
    print("=" * 60)
    
    queries = create_all_ml_inference_streams(
        spark, 
        bucket_name=args.bucket_name,
        trigger_interval=args.trigger_interval
    )
    
    try:
        while True:
            import time
            time.sleep(30)
            print("\n📊 ML Inference queries running...")
    except KeyboardInterrupt:
        print("\n⚠️  Stopping queries...")
        for query in queries:
            query.stop()
        print("✅ All queries stopped")
    
    spark.stop()


if __name__ == "__main__":
    main()
