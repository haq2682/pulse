"""
Session Conversion Value Prediction - Inference Script
Predicts expected order value if a session converts
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
INPUT_SESSIONS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customer_sessions.parquet"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/session_conversion_value/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/session_conversion_value/"

# ⚠️ MANUAL CONFIGURATION REQUIRED:
MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

# Feature set (must match training)
NUMERIC_FEATURES = [
    "pages_viewed", "products_viewed", "session_duration_minutes",
    "pages_per_minute", "products_per_page", "session_engagement_score",
    "items_added_to_cart", "cart_value", "cart_add_rate", "avg_cart_item_value",
    "browse_to_cart_ratio",
    "customer_total_orders", "customer_lifetime_value", "customer_avg_order_value",
    "customer_recency_days", "customer_session_conversion_rate",
    "customer_cart_abandonment_rate", "is_new_customer", "is_repeat_customer",
    "rfm_overall_score", "recency_score", "frequency_score", "monetary_score",
    "hour_of_day", "day_of_week", "is_weekend", "is_business_hours",
    "device_type_idx", "referrer_source_idx", "customer_segment_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Session_Conversion_Value_Inference")
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
    """Load trained model"""
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


def create_inference_features(sessions_df, customers_df):
    """Create same features as training - for ALL sessions"""
    print("Creating inference features...")
    
    # Use ALL sessions (not just converted ones)
    # Join with customer data
    session_features = sessions_df.join(
        customers_df.select(
            "customer_id",
            F.col("total_orders").alias("customer_total_orders"),
            F.col("customer_lifetime_value"),
            F.col("avg_order_value").alias("customer_avg_order_value"),
            F.col("order_recency_days").alias("customer_recency_days"),
            F.col("session_conversion_rate").alias("customer_session_conversion_rate"),
            F.col("cart_abandonment_rate").alias("customer_cart_abandonment_rate"),
            F.col("is_repeat_customer"),
            F.col("rfm_overall_score"),
            F.col("recency_score"),
            F.col("frequency_score"),
            F.col("monetary_score"),
            F.col("customer_segment")
        ),
        "customer_id",
        "left"
    )
    
    # Fill nulls (new customers or missing data)
    session_features = session_features.fillna({
        "customer_total_orders": 0,
        "customer_lifetime_value": 0,
        "customer_avg_order_value": 0,
        "customer_recency_days": 999,
        "customer_session_conversion_rate": 0,
        "customer_cart_abandonment_rate": 0,
        "is_repeat_customer": 0,
        "rfm_overall_score": 0,
        "recency_score": 0,
        "frequency_score": 0,
        "monetary_score": 0,
        "customer_segment": "Unknown"
    })
    
    # Calculate derived features
    session_features = session_features.withColumn(
        "is_new_customer",
        F.when(F.col("customer_total_orders") <= 1, 1.0).otherwise(0.0)
    ).withColumn(
        "browse_to_cart_ratio",
        F.when(
            F.col("products_viewed") > 0,
            F.col("items_added_to_cart") / F.col("products_viewed")
        ).otherwise(0)
    ).withColumn(
        "hour_of_day",
        F.hour(F.col("session_start"))
    ).withColumn(
        "day_of_week",
        F.dayofweek(F.col("session_start"))
    ).withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("session_start")).isin([1, 7]), 1.0).otherwise(0.0)
    ).withColumn(
        "is_business_hours",
        F.when(
            (F.hour(F.col("session_start")) >= 9) &
            (F.hour(F.col("session_start")) <= 17),
            1.0
        ).otherwise(0.0)
    )
    
    # Fill nulls in session features
    session_features = session_features.fillna({
        "pages_viewed": 1,
        "products_viewed": 0,
        "session_duration_minutes": 1,
        "items_added_to_cart": 0,
        "cart_value": 0,
        "pages_per_minute": 0,
        "products_per_page": 0,
        "cart_add_rate": 0,
        "avg_cart_item_value": 0,
        "session_engagement_score": 0,
        "device_type": "Unknown",
        "referrer_source": "Unknown"
    })
    
    print(f"✓ Inference features created: {session_features.count()} sessions")
    return session_features


def prepare_inference_data(df):
    """Prepare and scale features"""
    device_indexer = StringIndexer(
        inputCol="device_type",
        outputCol="device_type_idx",
        handleInvalid="keep"
    )
    
    referrer_indexer = StringIndexer(
        inputCol="referrer_source",
        outputCol="referrer_source_idx",
        handleInvalid="keep"
    )
    
    segment_indexer = StringIndexer(
        inputCol="customer_segment",
        outputCol="customer_segment_idx",
        handleInvalid="keep"
    )
    
    df_indexed = device_indexer.fit(df).transform(df)
    df_indexed = referrer_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = segment_indexer.fit(df_indexed).transform(df_indexed)
    
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "session_id",
        "customer_id",
        "cart_value",
        "items_added_to_cart",
        "session_engagement_score",
        "customer_avg_order_value",
        "is_new_customer",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with business metrics"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Ensure predictions are non-negative
    predictions_df = predictions_df.withColumn(
        "predicted_conversion_value",
        F.greatest(F.lit(0), F.col("prediction"))
    )
    
    # Calculate conversion probability based on:
    # 1. Cart value (if they have items, higher probability)
    # 2. Session engagement
    # 3. Customer history
    predictions_df = predictions_df.withColumn(
        "base_conversion_prob",
        F.when(
            F.col("cart_value") > 0,
            F.lit(0.3)  # Base 30% if cart has items
        ).otherwise(
            F.lit(0.05)  # Base 5% if just browsing
        )
    ).withColumn(
        "engagement_boost",
        F.least(
            F.lit(0.4),
            F.col("session_engagement_score") / 100 * 0.4
        )
    ).withColumn(
        "history_boost",
        F.when(
            F.col("is_new_customer") == 1,
            F.lit(0.0)
        ).when(
            F.col("customer_avg_order_value") > 0,
            F.lit(0.2)
        ).otherwise(
            F.lit(0.0)
        )
    ).withColumn(
        "conversion_probability",
        F.least(
            F.lit(0.95),  # Cap at 95%
            F.col("base_conversion_prob") + 
            F.col("engagement_boost") + 
            F.col("history_boost")
        )
    )
    
    # Generate recommended interventions as JSON
    def generate_interventions(cart_value, items_in_cart, conv_prob, predicted_value):
        interventions = []
        
        if cart_value > 0 and conv_prob < 0.5:
            interventions.append({
                "type": "cart_abandonment_email",
                "priority": "high",
                "message": "Send personalized email about items in cart"
            })
        
        if items_in_cart > 0 and predicted_value > 100:
            interventions.append({
                "type": "discount_offer",
                "priority": "medium",
                "message": f"Offer 10% discount to encourage ${predicted_value:.2f} purchase"
            })
        
        if conv_prob > 0.7:
            interventions.append({
                "type": "upsell_opportunity",
                "priority": "low",
                "message": "Show complementary products"
            })
        
        if items_in_cart == 0 and conv_prob < 0.2:
            interventions.append({
                "type": "engagement_boost",
                "priority": "medium",
                "message": "Show personalized recommendations"
            })
        
        return json.dumps(interventions)
    
    interventions_udf = F.udf(generate_interventions, StringType())
    
    predictions_df = predictions_df.withColumn(
        "recommended_interventions",
        interventions_udf(
            F.col("cart_value"),
            F.col("items_added_to_cart"),
            F.col("conversion_probability"),
            F.col("predicted_conversion_value")
        )
    )
    
    # Calculate confidence score
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(
            F.col("is_new_customer") == 1,
            F.lit(0.70)  # Lower confidence for new customers
        ).when(
            F.col("customer_avg_order_value") > 0,
            F.lit(0.90)  # High confidence with history
        ).otherwise(
            F.lit(0.75)  # Medium confidence
        )
    )
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("session_id"),
        F.col("customer_id"),
        current_timestamp.alias("prediction_date"),
        F.col("predicted_conversion_value"),
        F.col("conversion_probability"),
        F.col("recommended_interventions"),
        F.col("confidence_score"),
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
    print("\n" + "="*80)
    print(f"Sample Session Conversion Predictions (first {n} sessions)")
    print("="*80)
    
    sample = df.select(
        "session_id",
        "customer_id",
        "predicted_conversion_value",
        "conversion_probability",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Session: {row['session_id']:<30}")
        print(f"  Customer: {row['customer_id']:<30}")
        print(f"  Predicted Value: ${row['predicted_conversion_value']:>8.2f}")
        print(f"  Conversion Probability: {row['conversion_probability']*100:>6.1f}%")
        print(f"  Confidence: {row['confidence_score']*100:>6.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("session_id").alias("total_sessions"),
        F.sum("predicted_conversion_value").alias("total_potential_value"),
        F.avg("predicted_conversion_value").alias("avg_predicted_value"),
        F.avg("conversion_probability").alias("avg_conversion_prob"),
        F.sum(
            F.when(F.col("conversion_probability") > 0.5, 1).otherwise(0)
        ).alias("high_probability_sessions"),
        F.sum(
            F.when(F.col("conversion_probability") > 0.7, 
                  F.col("predicted_conversion_value")
            ).otherwise(0)
        ).alias("high_confidence_value"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    print(f"Total Sessions: {stats['total_sessions']}")
    print(f"Total Potential Value: ${stats['total_potential_value']:,.2f}")
    print(f"Average Predicted Value: ${stats['avg_predicted_value']:.2f}")
    print(f"Average Conversion Probability: {stats['avg_conversion_prob']*100:.1f}%")
    print(f"High Probability Sessions (>50%): {stats['high_probability_sessions']}")
    print(f"High Confidence Value (>70%): ${stats['high_confidence_value']:,.2f}")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main():
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Session Conversion Value Prediction - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 80)
    model = load_model(MODEL_NAME)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 80)
    
    sessions_df, _ = validate_dataset(spark, INPUT_SESSIONS_PATH, "Customer Sessions")
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    
    if None in [sessions_df, customers_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(sessions_df, customers_df)
    
    # Prepare data
    print("\nStep 4: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
    print("\nStep 6: Save Predictions")
    print("-" * 80)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
