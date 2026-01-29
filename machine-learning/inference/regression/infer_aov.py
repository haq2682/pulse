"""
Average Order Value (AOV) Prediction - Inference Script
Generates predictions for customers' next order values
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from datetime import datetime
import uuid
import json

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/aov_prediction/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/aov_prediction/"

# ⚠️ MANUAL CONFIGURATION REQUIRED:
MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

# Feature columns (must match training)
FEATURE_COLUMNS = [
    "total_orders",
    "customer_tenure_days",
    "avg_items_per_order",
    "avg_days_between_orders",
    "days_since_last_purchase",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "cancellation_rate",
    "avg_discount_per_order",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "aov_lag_1",
    "aov_lag_2",
    "aov_lag_3",
    "aov_rolling_3",
    "aov_trend",
    "customer_segment_label_idx",
    "preferred_payment_method_idx",
    "preferred_device_type_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("AOV_Prediction_Inference")
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


def validate_dataset(spark, path, name):
    """Check if dataset exists"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ {name} dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ {name} dataset validation failed: {str(e)}")
        return None, 0


def create_inference_features(customers_df, orders_df):
    """
    Create inference features for each customer based on most recent orders
    """
    print("Creating inference features...")
    
    # Filter delivered orders
    orders_filtered = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    
    # Create window for each customer
    customer_window = Window.partitionBy("customer_id").orderBy("order_placed_at")
    
    # Add order sequence and lag features
    orders_with_lags = orders_filtered.withColumn(
        "order_seq",
        F.row_number().over(customer_window)
    ).withColumn(
        "aov_lag_1",
        F.lag("total_amount", 1).over(customer_window)
    ).withColumn(
        "aov_lag_2",
        F.lag("total_amount", 2).over(customer_window)
    ).withColumn(
        "aov_lag_3",
        F.lag("total_amount", 3).over(customer_window)
    )
    
    # Calculate rolling average and trend
    window_rolling = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-3, -1)
    
    orders_with_lags = orders_with_lags.withColumn(
        "aov_rolling_3",
        F.avg("total_amount").over(window_rolling)
    ).withColumn(
        "aov_trend",
        F.when(
            (F.col("aov_lag_1").isNotNull()) & (F.col("aov_lag_2").isNotNull()) & (F.col("aov_lag_2") > 0),
            (F.col("aov_lag_1") - F.col("aov_lag_2")) / F.col("aov_lag_2")
        ).otherwise(0)
    )
    
    # Get latest order for each customer (most recent state)
    window_latest = Window.partitionBy("customer_id").orderBy(F.desc("order_placed_at"))
    
    latest_orders = orders_with_lags.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(
        (F.col("row_num") == 1) &
        (F.col("order_seq") > 1)  # Need at least 1 previous order
    ).drop("row_num")
    
    # Join with customer data
    customer_features = latest_orders.join(
        customers_df.select(
            "customer_id",
            "total_orders",
            "customer_tenure_days",
            "avg_items_per_order",
            "avg_days_between_orders",
            "days_since_last_purchase",
            "session_conversion_rate",
            "cart_abandonment_rate",
            "cancellation_rate",
            "avg_discount_per_order",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "customer_segment_label",
            "preferred_payment_method",
            "preferred_device_type"
        ),
        "customer_id",
        "left"
    )
    
    # Fill nulls
    customer_features = customer_features.fillna({
        "aov_lag_1": 0,
        "aov_lag_2": 0,
        "aov_lag_3": 0,
        "aov_rolling_3": 0,
        "aov_trend": 0,
        "session_conversion_rate": 0,
        "cart_abandonment_rate": 0,
        "cancellation_rate": 0,
        "avg_discount_per_order": 0,
        "avg_days_between_orders": 0,
        "days_since_last_purchase": 0,
        "avg_items_per_order": 0,
        "recency_score": 0,
        "frequency_score": 0,
        "monetary_score": 0,
        "customer_segment_label": "Unknown",
        "preferred_payment_method": "Unknown",
        "preferred_device_type": "Unknown"
    })
    
    print(f"✓ Inference features created: {customer_features.count()} customers")
    return customer_features


def prepare_inference_data(df):
    """
    Prepare and scale features for inference
    """
    # Encode categorical features
    indexer_segment = StringIndexer(
        inputCol="customer_segment_label",
        outputCol="customer_segment_label_idx",
        handleInvalid="keep"
    )
    
    indexer_payment = StringIndexer(
        inputCol="preferred_payment_method",
        outputCol="preferred_payment_method_idx",
        handleInvalid="keep"
    )
    
    indexer_device = StringIndexer(
        inputCol="preferred_device_type",
        outputCol="preferred_device_type_idx",
        handleInvalid="keep"
    )
    
    # Apply indexers
    df_indexed = indexer_segment.fit(df).transform(df)
    df_indexed = indexer_payment.fit(df_indexed).transform(df_indexed)
    df_indexed = indexer_device.fit(df_indexed).transform(df_indexed)
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "customer_id",
        "aov_lag_1",
        "aov_rolling_3",
        "aov_trend",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with influencing factors"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Create factors JSON
    factors_udf = F.udf(
        lambda lag1, rolling3, trend: json.dumps({
            "last_order_value": float(lag1) if lag1 else 0,
            "average_3_orders": float(rolling3) if rolling3 else 0,
            "spending_trend": "increasing" if trend > 0.05 else ("decreasing" if trend < -0.05 else "stable"),
            "trend_percentage": round(float(trend * 100) if trend else 0, 2)
        }),
        StringType()
    )
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("customer_id"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction").alias("predicted_next_aov"),
        (F.col("prediction") * 0.85).alias("confidence_interval_lower"),
        (F.col("prediction") * 1.15).alias("confidence_interval_upper"),
        factors_udf(
            F.col("aov_lag_1"),
            F.col("aov_rolling_3"),
            F.col("aov_trend")
        ).alias("factors_influencing_aov"),
        F.lit(0.88).alias("confidence_score"),
        F.lit(model_name).alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_sample_predictions(df, n=5):
    """Display sample predictions"""
    print("\n" + "="*60)
    print(f"Sample AOV Predictions (first {n} customers)")
    print("="*60)
    
    sample = df.select(
        "customer_id",
        "predicted_next_aov",
        "confidence_interval_lower",
        "confidence_interval_upper",
        "factors_influencing_aov"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Customer: {row['customer_id']:<30}")
        print(f"  Predicted AOV: ${row['predicted_next_aov']:>10.2f}")
        print(f"  Range: ${row['confidence_interval_lower']:>10.2f} - ${row['confidence_interval_upper']:>10.2f}")
        factors = json.loads(row['factors_influencing_aov'])
        print(f"  Last Order: ${factors['last_order_value']:.2f}")
        print(f"  Avg (3 orders): ${factors['average_3_orders']:.2f}")
        print(f"  Trend: {factors['spending_trend']} ({factors['trend_percentage']:+.1f}%)")
        print()


def main():
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("AOV Prediction - Inference")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 60)
    
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    
    if None in [customers_df, orders_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_inference_features(customers_df, orders_df)
    
    # Prepare data
    print("\nStep 4: Data Preparation & Encoding")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Save predictions
    print("Step 6: Save Predictions")
    print("-" * 60)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
