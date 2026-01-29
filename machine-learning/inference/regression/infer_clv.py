"""
Customer Lifetime Value (CLV) Prediction - Inference Script
Generates CLV predictions using trained regression models
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from datetime import datetime
import uuid

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/clv_predictions/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/clv/"

# ⚠️ MANUAL CONFIGURATION REQUIRED:
# Set MODEL_NAME to one of: "linear_regression", "random_forest", "gbt"
# Based on training results, select the best performing model
MODEL_NAME = "random_forest"  # <-- UPDATE THIS AFTER TRAINING

PREDICTION_HORIZON_DAYS = 365  # Predict CLV for next 1 year

# Feature columns (must match training)
FEATURE_COLUMNS = [
    "total_orders",
    "avg_order_value",
    "customer_tenure_days",
    "avg_days_between_orders",
    "order_frequency",
    "total_discount_received",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "recency_score",
    "frequency_score",
    "monetary_score"
]


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return (
        SparkSession.builder
        .appName("CLV_Model_Inference")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "1000")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("inferSchema", "true")
        .config("mergeSchema", "true")
        .getOrCreate()
    )


def load_model(model_name):
    """Load trained model from MinIO"""
    model_path = f"{MODEL_BASE_PATH}{model_name}"
    
    try:
        if model_name == "linear_regression":
            model = LinearRegressionModel.load(model_path)
        elif model_name == "random_forest":
            model = RandomForestRegressionModel.load(model_path)
        elif model_name == "gbt":
            model = GBTRegressionModel.load(model_path)
        else:
            print(f"✗ Unknown model type: {model_name}")
            return None
        
        print(f"✓ Model loaded: {model_path}")
        return model
    except Exception as e:
        print(f"✗ Failed to load model: {str(e)}")
        return None


def validate_dataset(spark, path):
    """Check if dataset exists and is readable"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ Dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ Dataset validation failed: {str(e)}")
        return None, 0


def validate_columns(df, required_columns):
    """Check if required columns exist"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False
    
    # Check for non-null values
    for col in required_columns:
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        total_count = df.count()
        
        if non_null_count == 0:
            print(f"✗ Column '{col}' is entirely null")
            return False
    
    print("✓ All required columns validated")
    return True


def prepare_inference_data(df):
    """
    Prepare data for inference:
    1. Keep customer_id for output
    2. Fill missing feature values with 0
    3. Create feature vector
    """
    # Fill missing feature values with 0 (same as training)
    df_filled = df.fillna(0, subset=FEATURE_COLUMNS)
    
    # Assemble features into vector
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep"
    )
    
    df_prepared = assembler.transform(df_filled).select("customer_id", "features")
    
    print(f"✓ Data prepared: {df_prepared.count()} records ready for inference")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions and format output"""
    # Generate predictions
    predictions_df = model.transform(df)
    
    # Generate unique prediction IDs
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    
    # Get current timestamp
    current_timestamp = F.lit(datetime.now())
    
    # Format output according to ml_clv_predictions schema
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("customer_id"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction").alias("predicted_clv"),
        F.lit(PREDICTION_HORIZON_DAYS).alias("prediction_horizon_days"),
        # Calculate confidence intervals (±20% of prediction as example)
        (F.col("prediction") * 0.8).alias("confidence_interval_lower"),
        (F.col("prediction") * 1.2).alias("confidence_interval_upper"),
        F.lit(0.85).alias("confidence_score"),  # Placeholder confidence score
        F.lit(model_name).alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO as Parquet"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_sample_predictions(df, n=5):
    """Display sample predictions for verification"""
    print("\n" + "="*60)
    print(f"Sample Predictions (first {n} records)")
    print("="*60)
    
    sample = df.select(
        "customer_id",
        "predicted_clv",
        "confidence_interval_lower",
        "confidence_interval_upper"
    ).limit(n).collect()
    
    for row in sample:
        print(
            f"Customer: {row['customer_id']:<30} "
            f"CLV: ${row['predicted_clv']:>10.2f} "
            f"(${row['confidence_interval_lower']:>10.2f} - ${row['confidence_interval_upper']:>10.2f})"
        )


def main():
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("CLV Prediction Model Inference")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        print("   Run training script first: train_clv_model.py")
        spark.stop()
        return
    
    # Step 2: Validate dataset
    print("\nStep 2: Dataset Validation")
    print("-" * 60)
    df, record_count = validate_dataset(spark, INPUT_PATH)
    
    if df is None:
        print("\n✗ Inference aborted: Dataset not found")
        spark.stop()
        return
    
    # Step 3: Validate columns
    print("\nStep 3: Column Validation")
    print("-" * 60)
    required_columns = ["customer_id"] + FEATURE_COLUMNS
    
    if not validate_columns(df, required_columns):
        print("\n✗ Inference aborted: Required columns missing or invalid")
        spark.stop()
        return
    
    # Step 4: Prepare data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_inference_data(df)
    
    # Step 5: Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Step 6: Display samples
    display_sample_predictions(predictions_df)
    
    # Step 7: Save predictions
    print("\nStep 6: Save Predictions")
    print("-" * 60)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed: Unable to save predictions")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()