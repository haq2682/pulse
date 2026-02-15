import os
import sys
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, udf, struct, to_json, current_timestamp
)
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import (
    LogisticRegressionModel, RandomForestClassificationModel
)
from pyspark.ml.feature import StringIndexerModel
import findspark

findspark.init()



# Feature columns (must match training)
FEATURE_COLUMNS = [
    "days_since_last_purchase",
    "order_frequency",
    "customer_lifetime_value",
    "avg_days_between_orders",
    "total_orders",
    "total_revenue",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "days_since_last_login",
    "customer_tenure_days",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "avg_order_value",
    "cancellation_rate"
]


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("CustomerChurnInference") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "1000") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ) \
        .config("inferSchema", "true") \
        .config("mergeSchema", "true") \
        .getOrCreate()


def load_data(spark, path):
    """Load data from MinIO"""
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records from {path}")
        return df
    except Exception as e:
        print(f"✗ Failed to load data from {path}: {e}")
        return None


def load_model(spark, model_dir, model_name):
    """Load trained model and indexer from MinIO"""
    try:
        model_path = f"{model_dir}/{model_name}"
        indexer_path = f"{model_dir}/{model_name}_indexer"
        
        # Load model based on type
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {model_name}")
        
        # Load indexer
        indexer = StringIndexerModel.load(indexer_path)
        
        print(f"✓ Loaded model: {model_name}")
        return model, indexer
    except Exception as e:
        print(f"✗ Failed to load model from {model_dir}: {e}")
        return None, None


def validate_dataset(df, required_columns):
    """
    Validate dataset structure and content
    Returns: (is_valid, message)
    """
    if df is None:
        return False, "Dataset is None"
    
    # Check required columns exist
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    
    # Check columns have at least some non-null values
    for col_name in required_columns:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            print(f"⚠️  Warning: Column {col_name} is entirely null")
    
    return True, "Validation passed"


def prepare_features(df, feature_cols):
    """
    Prepare features for inference (must match training pipeline)
    - Fill nulls with 0
    - Assemble features into vector
    """
    # Fill nulls with 0 (same as training)
    df_filled = df.fillna(0, subset=feature_cols)
    
    # Assemble features into vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector = assembler.transform(df_filled)
    
    print(f"✓ Prepared features: {len(feature_cols)} columns vectorized")
    return df_vector


def extract_feature_importance(model, model_name, feature_cols):
    """Extract feature importance from model"""
    if model_name == "RandomForest":
        importances = model.featureImportances.toArray()
        feature_importance = {
            feature_cols[i]: float(importances[i]) 
            for i in range(len(feature_cols))
        }
        return feature_importance
    return {}


def get_top_contributing_factors(feature_importance, top_n=3):
    """Get top N contributing factors"""
    if not feature_importance:
        return {}
    
    sorted_features = sorted(
        feature_importance.items(), 
        key=lambda x: x[1], 
        reverse=True
    )
    return dict(sorted_features[:top_n])


def generate_predictions(spark, df, model, indexer, model_name, feature_importance, MODEL_VERSION):
    """
    Generate predictions and format output according to schema
    """
    # Make predictions
    predictions = model.transform(df)
    
    # Convert prediction index back to label
    labels = indexer.labels
    index_to_label = udf(lambda idx: labels[int(idx)], StringType())
    
    # Extract probability for predicted class
    extract_probability = udf(
        lambda prob, pred: float(prob[int(pred)]) if prob else 0.0,
        DoubleType()
    )
    
    # Calculate confidence score (max probability)
    calculate_confidence = udf(
        lambda prob: float(max(prob)) if prob else 0.0,
        DoubleType()
    )
    
    # Generate contributing factors JSON
    top_factors = get_top_contributing_factors(feature_importance)
    
    # Format predictions according to output schema
    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),  # Will be generated per row
        col("customer_id"),
        current_timestamp().alias("prediction_date"),
        index_to_label(col("prediction")).alias("predicted_churn_risk"),
        extract_probability(col("probability"), col("prediction")).alias("churn_probability"),
        calculate_confidence(col("probability")).alias("confidence_score"),
        lit(str(top_factors)).alias("contributing_factors"),  # JSON as string
        lit(MODEL_VERSION).alias("model_version")
    )
    
    # Generate unique prediction IDs
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    output_df = output_df.withColumn("prediction_id", generate_uuid())
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO as Parquet"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Saved predictions to {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {e}")
        return False


def main(BUCKET_NAME):
    # Configuration
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/customer_churn_predictions"
    MODEL_INPUT_DIR = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/classification/models/customer_churn"

    # Available options: "LogisticRegression", "RandomForest"
    SELECTED_MODEL = "RandomForest"

    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print("=" * 60)
    print("Customer Churn Prediction - Inference Pipeline")
    print("=" * 60)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load model
    model, indexer = load_model(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None or indexer is None:
        print("✗ Inference stopped: Failed to load model")
        return
    
    # Load data
    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("✗ Inference stopped: Failed to load data")
        return
    
    # Validate dataset
    required_cols = ["customer_id"] + FEATURE_COLUMNS
    is_valid, message = validate_dataset(df, required_cols)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return
    
    print(f"✓ Dataset validated")
    
    # Prepare features
    df_prepared = prepare_features(df, FEATURE_COLUMNS)
    
    # Extract feature importance
    feature_importance = extract_feature_importance(model, SELECTED_MODEL, FEATURE_COLUMNS)
    if feature_importance:
        print(f"✓ Extracted feature importance (top 3):")
        for feat, imp in sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:3]:
            print(f"  - {feat}: {imp:.4f}")
    
    # Generate predictions
    predictions_df = generate_predictions(
        spark, df_prepared, model, indexer, SELECTED_MODEL, feature_importance, MODEL_VERSION
    )
    
    # Show sample predictions
    print("\nSample predictions:")
    predictions_df.select(
        "customer_id", "predicted_churn_risk", "churn_probability", "confidence_score"
    ).show(5, truncate=False)
    
    # Save predictions
    success = save_predictions(predictions_df, OUTPUT_PATH)
    
    if success:
        print("\n" + "=" * 60)
        print("✓ Inference completed successfully")
        print("=" * 60)
    else:
        print("\n✗ Inference failed")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)