"""
Multi-Bucket Data Loader for General ML Models

This module provides utilities for loading and aggregating data from multiple
buckets in MinIO for training general machine learning models across all
business data in the data lake.
"""

import os
import warnings
from typing import List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# Constants for General Model Training
GENERAL_MODEL_BUCKET = "pulse-bucket-1"
BUCKET_PREFIX = "pulse-bucket-"


def get_minio_buckets(spark: SparkSession, max_buckets: int = 100) -> List[str]:
    """
    Get list of all pulse buckets from MinIO.
    
    Args:
        spark: SparkSession with MinIO configuration
        max_buckets: Maximum number of buckets to scan (default 100)
    
    Returns:
        List of bucket names matching the pulse bucket pattern
    """
    buckets = []
    
    # Try to list buckets by checking if they exist
    for i in range(1, max_buckets + 1):
        bucket_name = f"{BUCKET_PREFIX}{i}"
        try:
            # Check if bucket exists by trying to list root path
            test_path = f"s3a://{bucket_name}/"
            # Use a quick existence check
            jvm = spark._jvm
            hadoop_conf = spark._jsc.hadoopConfiguration()
            path = jvm.org.apache.hadoop.fs.Path(test_path)
            fs = path.getFileSystem(hadoop_conf)
            if fs.exists(path):
                buckets.append(bucket_name)
        except Exception:
            # Bucket doesn't exist or not accessible
            continue
    
    if not buckets:
        # Fallback: at minimum, add the main bucket
        buckets = [GENERAL_MODEL_BUCKET]
    
    return buckets


def load_data_from_all_buckets(
    spark: SparkSession,
    relative_path: str,
    required_columns: List[str],
    filter_nulls: bool = True,
    union_mode: str = "permissive"
) -> Tuple[Optional[DataFrame], int]:
    """
    Load and aggregate data from all MinIO buckets.
    
    Args:
        spark: SparkSession with MinIO configuration
        relative_path: Path relative to bucket root (e.g., "transformed/agg_customers.parquet")
        required_columns: List of columns that must have non-null values
        filter_nulls: If True, filter out rows where required_columns have null values
        union_mode: How to handle schema differences ("permissive" or "strict")
    
    Returns:
        Tuple of (DataFrame with aggregated data, total record count)
    """
    buckets = get_minio_buckets(spark)
    
    print(f"📦 Loading data from {len(buckets)} bucket(s)...")
    print(f"   Buckets: {', '.join(buckets[:5])}{'...' if len(buckets) > 5 else ''}")
    
    all_dataframes = []
    total_loaded = 0
    
    for bucket in buckets:
        full_path = f"s3a://{bucket}/{relative_path}"
        try:
            df = spark.read.parquet(full_path)
            record_count = df.count()
            
            if record_count > 0:
                # Add bucket source for traceability (optional)
                df = df.withColumn("_source_bucket", F.lit(bucket))
                
                # Filter for non-null required columns if requested
                if filter_nulls and required_columns:
                    for col_name in required_columns:
                        if col_name in df.columns:
                            df = df.filter(F.col(col_name).isNotNull())
                
                filtered_count = df.count()
                if filtered_count > 0:
                    all_dataframes.append(df)
                    total_loaded += filtered_count
                    print(f"   ✓ {bucket}: {filtered_count} non-null records (from {record_count} total)")
                else:
                    print(f"   ⚠ {bucket}: 0 records after null filtering")
        except Exception as e:
            print(f"   ⚠ {bucket}: Could not load ({str(e)[:50]}...)")
            continue
    
    if not all_dataframes:
        print("   ✗ No data loaded from any bucket")
        return None, 0
    
    # Union all dataframes
    if len(all_dataframes) == 1:
        combined_df = all_dataframes[0]
    else:
        # Ensure schema compatibility
        combined_df = all_dataframes[0]
        for df in all_dataframes[1:]:
            try:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            except Exception as e:
                if union_mode == "permissive":
                    # Try with common columns only
                    common_cols = list(set(combined_df.columns) & set(df.columns))
                    combined_df = combined_df.select(common_cols).union(df.select(common_cols))
                else:
                    raise e
    
    # Drop the source bucket column before returning (it was for debugging)
    if "_source_bucket" in combined_df.columns:
        combined_df = combined_df.drop("_source_bucket")
    
    final_count = combined_df.count()
    print(f"   📊 Total aggregated: {final_count} records")
    
    return combined_df, final_count


def validate_training_data(
    df: Optional[DataFrame],
    record_count: int,
    min_records: int,
    max_records: int,
    model_name: str
) -> Tuple[bool, Optional[DataFrame]]:
    """
    Validate if training data meets requirements.
    
    Args:
        df: DataFrame to validate
        record_count: Number of records in the DataFrame
        min_records: Minimum required records for training
        max_records: Maximum records to use for training (sample if exceeded)
        model_name: Name of the model (for logging)
    
    Returns:
        Tuple of (is_valid, processed_df)
        - If records < min_records: (False, None) with warning
        - If records > max_records: (True, sampled_df)
        - Otherwise: (True, df)
    """
    if df is None or record_count == 0:
        warnings.warn(
            f"⚠️  [{model_name}] No data available for training. Skipping training.",
            UserWarning
        )
        print(f"⚠️  [{model_name}] No data available for training. Skipping training.")
        return False, None
    
    if record_count < min_records:
        warnings.warn(
            f"⚠️  [{model_name}] Insufficient training data: {record_count} records "
            f"(minimum required: {min_records}). Skipping training.",
            UserWarning
        )
        print(f"⚠️  [{model_name}] Insufficient training data: {record_count} records "
              f"(minimum required: {min_records}). Skipping training.")
        return False, None
    
    if record_count > max_records:
        sample_fraction = max_records / record_count
        print(f"   ℹ️  [{model_name}] Dataset exceeds maximum ({record_count} > {max_records}). "
              f"Sampling {max_records} records ({sample_fraction*100:.1f}%)")
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
        # Ensure we have exactly max_records (or close to it)
        df = df.limit(max_records)
    
    print(f"✓ [{model_name}] Training data validated: {df.count()} records "
          f"(window: {min_records} - {max_records})")
    
    return True, df


def get_general_model_output_path(model_type: str, model_category: str) -> str:
    """
    Get the output path for a general model.
    
    General models are always saved to the main pulse bucket (pulse-bucket-1).
    
    Args:
        model_type: Type of model (e.g., "classification", "regression", "clustering")
        model_category: Category/name of the model (e.g., "customer_churn", "clv")
    
    Returns:
        S3A path for model storage
    """
    return f"s3a://{GENERAL_MODEL_BUCKET}/machine-learning/{model_type}/models/{model_category}"


def get_general_model_input_path(model_type: str, model_category: str) -> str:
    """
    Get the input path for loading a trained general model.
    
    General models are always loaded from the main pulse bucket (pulse-bucket-1).
    
    Args:
        model_type: Type of model (e.g., "classification", "regression", "clustering")
        model_category: Category/name of the model (e.g., "customer_churn", "clv")
    
    Returns:
        S3A path for model loading
    """
    return f"s3a://{GENERAL_MODEL_BUCKET}/machine-learning/{model_type}/models/{model_category}"


# Model-specific training record windows
# Format: (min_records, max_records)
# These are reasonable defaults based on ML best practices
MODEL_TRAINING_WINDOWS = {
    # Classification models
    "cart_abandonment": (500, 1_000_000),
    "customer_churn": (200, 500_000),
    "customer_segments": (100, 500_000),
    "payment_success": (500, 1_000_000),
    "review_sentiment": (1000, 500_000),
    "stock_status": (100, 500_000),
    
    # Regression models
    "aov": (200, 500_000),
    "aov_v2": (200, 500_000),
    "clv": (100, 500_000),
    "restock_quantity": (100, 200_000),
    "revenue_forecast": (12, 1000),  # Time series - needs fewer but specific records
    "safety_stock": (100, 200_000),
    "session_conversion": (200, 500_000),
    "stockout_probability": (100, 200_000),
    
    # Clustering models
    "customer_segment": (100, 500_000),
    "geo_cluster": (30, 50_000),
    "session_behavior": (200, 500_000),
    "supplier_performance": (50, 10_000),
}


def get_training_window(model_name: str) -> Tuple[int, int]:
    """
    Get the training record window for a specific model.
    
    Args:
        model_name: Name of the model
    
    Returns:
        Tuple of (min_records, max_records)
    """
    return MODEL_TRAINING_WINDOWS.get(model_name, (100, 500_000))
