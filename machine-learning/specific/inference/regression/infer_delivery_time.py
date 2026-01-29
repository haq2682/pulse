"""
Delivery Time Prediction - Inference Script
Predicts order delivery time in days
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
from pyspark.ml.regression import RandomForestRegressionModel
from datetime import datetime, timedelta
import uuid
import json

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/delivery_time/"
MODEL_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/delivery_time/random_forest"

# Feature set (must match training)
NUMERIC_FEATURES = [
    "total_quantity", "total_amount", "shipping_cost", "unique_products_ordered", "avg_product_price",
    "order_placed_day_of_week", "order_placed_month", "order_placed_quarter", "order_placed_day_of_month",
    "is_weekend", "is_month_end", "is_holiday_season",
    "city_avg_delivery_days", "city_delivery_std", "state_avg_delivery_days",
    "country_avg_delivery_days", "location_delivery_consistency",
    "shipping_tier_avg_delivery", "shipping_cost_to_amount_ratio",
    "customer_total_orders", "customer_avg_delivery_days", "is_repeat_customer",
    "order_complexity_score", "order_size_tier",
    "is_major_city", "is_capital_city",
    "country_idx", "state_idx", "city_idx", "season_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Delivery_Time_Inference")
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


def load_model():
    """Load trained model"""
    try:
        model = RandomForestRegressionModel.load(MODEL_PATH)
        print(f"✓ Model loaded: {MODEL_PATH}")
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


def calculate_location_statistics(orders_df, customers_df):
    """Calculate location delivery statistics from historical data"""
    print("Calculating location statistics...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    ).join(
        customers_df.select("customer_id", "country", "state_province", "city"),
        "customer_id",
        "inner"
    )
    
    city_stats = delivered_orders.groupBy("country", "state_province", "city").agg(
        F.avg("delivery_days_diff").alias("city_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("city_delivery_std"),
        F.count("order_id").alias("city_order_count")
    ).fillna({"city_delivery_std": 0})
    
    state_stats = delivered_orders.groupBy("country", "state_province").agg(
        F.avg("delivery_days_diff").alias("state_avg_delivery_days")
    )
    
    country_stats = delivered_orders.groupBy("country").agg(
        F.avg("delivery_days_diff").alias("country_avg_delivery_days")
    )
    
    print(f"✓ Location statistics calculated")
    return city_stats, state_stats, country_stats


def calculate_shipping_tier_statistics(orders_df):
    """Calculate shipping tier statistics"""
    print("Calculating shipping tier statistics...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0) &
        (F.col("shipping_cost").isNotNull())
    )
    
    delivered_with_tiers = delivered_orders.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    tier_stats = delivered_with_tiers.groupBy("shipping_tier").agg(
        F.avg("delivery_days_diff").alias("shipping_tier_avg_delivery")
    )
    
    print(f"✓ Shipping tier statistics calculated")
    return tier_stats


def calculate_customer_history(orders_df):
    """Calculate customer delivery history"""
    print("Calculating customer history...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    )
    
    customer_stats = delivered_orders.groupBy("customer_id").agg(
        F.avg("delivery_days_diff").alias("customer_avg_delivery_days"),
        F.count("order_id").alias("customer_order_count")
    )
    
    print(f"✓ Customer history calculated")
    return customer_stats


def create_inference_features(orders_df, customers_df, city_stats_df, state_stats_df,
                              country_stats_df, tier_stats_df, customer_stats_df):
    """Create inference features matching training"""
    print("Creating inference features...")
    
    # Select orders for inference (typically new/pending orders)
    # For demo, we'll use all non-delivered orders or recent orders
    inference_orders = orders_df.filter(
        F.col("order_status").isin(["Processing", "Shipped", "Pending"])
    )
    
    # If no such orders, use recent delivered orders for demo
    if inference_orders.count() == 0:
        print("⚠  No pending orders found, using recent delivered orders for demo")
        inference_orders = orders_df.orderBy(F.desc("order_placed_at")).limit(1000)
    
    # Join with customer locations
    order_features = inference_orders.join(
        customers_df.select("customer_id", "country", "state_province", "city", "customer_segment"),
        "customer_id",
        "left"
    )
    
    # Join with location statistics
    order_features = order_features.join(
        city_stats_df,
        ["country", "state_province", "city"],
        "left"
    ).join(
        state_stats_df,
        ["country", "state_province"],
        "left"
    ).join(
        country_stats_df,
        "country",
        "left"
    )
    
    # Calculate shipping tier
    order_features = order_features.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    # Join with shipping tier and customer statistics
    order_features = order_features.join(tier_stats_df, "shipping_tier", "left") \
        .join(customer_stats_df, "customer_id", "left")
    
    # Create temporal features
    order_features = order_features.withColumn(
        "is_weekend",
        F.when(F.col("order_placed_day_of_week").isin([6, 7]), 1).otherwise(0)
    ).withColumn(
        "is_month_end",
        F.when(F.col("order_placed_day_of_month") >= 25, 1).otherwise(0)
    ).withColumn(
        "is_holiday_season",
        F.when(F.col("order_placed_month").isin([11, 12]), 1).otherwise(0)
    )
    
    # Additional features
    order_features = order_features.withColumn(
        "location_delivery_consistency",
        F.when(F.col("city_delivery_std") > 0, 1.0 / F.col("city_delivery_std")).otherwise(1.0)
    ).withColumn(
        "shipping_cost_to_amount_ratio",
        F.when(F.col("total_amount") > 0, F.col("shipping_cost") / F.col("total_amount")).otherwise(0)
    ).withColumn(
        "is_repeat_customer",
        F.when(F.col("customer_order_count") > 1, 1).otherwise(0)
    ).withColumn(
        "customer_total_orders",
        F.coalesce(F.col("customer_order_count"), F.lit(1))
    ).withColumn(
        "order_complexity_score",
        F.col("total_quantity") * F.coalesce(F.col("unique_products_ordered"), F.lit(1))
    ).withColumn(
        "avg_product_price",
        F.when(F.col("total_quantity") > 0, F.col("total_amount") / F.col("total_quantity")).otherwise(0)
    ).withColumn(
        "order_size_tier",
        F.when(F.col("total_amount") < 50, 1)
         .when(F.col("total_amount") < 150, 2)
         .when(F.col("total_amount") < 300, 3)
         .otherwise(4)
    ).withColumn(
        "is_major_city",
        F.when(F.col("city_order_count") > 100, 1).otherwise(0)
    ).withColumn(
        "is_capital_city",
        F.when(F.col("city_order_count") > 200, 1).otherwise(0)
    )
    
    # Fill nulls
    order_features = order_features.fillna({
        "total_quantity": 1,
        "total_amount": 50,
        "shipping_cost": 5,
        "unique_products_ordered": 1,
        "order_placed_day_of_week": 3,
        "order_placed_month": 6,
        "order_placed_quarter": 2,
        "order_placed_day_of_month": 15,
        "city_avg_delivery_days": 0,
        "city_delivery_std": 0,
        "state_avg_delivery_days": 0,
        "country_avg_delivery_days": 7,
        "shipping_tier_avg_delivery": 7,
        "customer_avg_delivery_days": 0,
        "season": "Summer",
        "country": "Unknown",
        "state_province": "Unknown",
        "city": "Unknown"
    })
    
    # Filter valid orders
    order_features = order_features.filter(
        F.col("order_id").isNotNull()
    )
    
    print(f"✓ Inference features created: {order_features.count()} orders")
    return order_features


def prepare_inference_data(df):
    """Prepare and scale features"""
    country_indexer = StringIndexer(inputCol="country", outputCol="country_idx", handleInvalid="keep")
    state_indexer = StringIndexer(inputCol="state_province", outputCol="state_idx", handleInvalid="keep")
    city_indexer = StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep")
    season_indexer = StringIndexer(inputCol="season", outputCol="season_idx", handleInvalid="keep")
    
    df_indexed = country_indexer.fit(df).transform(df)
    df_indexed = state_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = city_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = season_indexer.fit(df_indexed).transform(df_indexed)
    
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    
    assembler = VectorAssembler(inputCols=existing_features, outputCol="features_unscaled", handleInvalid="skip")
    df_assembled = assembler.transform(df_indexed)
    
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=False)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "order_id", "order_placed_at", "country", "city",
        "total_amount", "shipping_cost", "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df):
    """Generate comprehensive delivery time predictions"""
    predictions_df = model.transform(df)
    
    # Ensure non-negative predictions
    predictions_df = predictions_df.withColumn(
        "predicted_delivery_days",
        F.greatest(F.lit(1.0), F.round(F.col("prediction"), 1))
    )
    
    # Calculate predicted delivery date
    predictions_df = predictions_df.withColumn(
        "predicted_delivery_date",
        F.expr("date_add(to_date(order_placed_at), cast(predicted_delivery_days as int))")
    )
    
    # Calculate confidence intervals (±20% of prediction)
    predictions_df = predictions_df.withColumn(
        "confidence_interval_lower",
        F.greatest(F.lit(1.0), F.col("predicted_delivery_days") * 0.8)
    ).withColumn(
        "confidence_interval_upper",
        F.col("predicted_delivery_days") * 1.2
    )
    
    # Generate factors affecting delivery
    def generate_delivery_factors(days, shipping_cost, total_amount, country, city):
        factors = []
        
        if days > 10:
            factors.append({
                "factor": "remote_location",
                "impact": "high",
                "description": f"Delivery to {city}, {country} takes longer"
            })
        
        if shipping_cost < 5:
            factors.append({
                "factor": "economy_shipping",
                "impact": "medium",
                "description": "Economy shipping selected (slower service)"
            })
        elif shipping_cost > 25:
            factors.append({
                "factor": "express_shipping",
                "impact": "positive",
                "description": "Express shipping selected (faster delivery)"
            })
        
        if total_amount > 200:
            factors.append({
                "factor": "high_value_order",
                "impact": "low",
                "description": "High-value orders may require additional verification"
            })
        
        return json.dumps(factors)
    
    factors_udf = F.udf(generate_delivery_factors, StringType())
    
    predictions_df = predictions_df.withColumn(
        "factors_affecting_delivery",
        factors_udf(
            F.col("predicted_delivery_days"),
            F.col("shipping_cost"),
            F.col("total_amount"),
            F.col("country"),
            F.col("city")
        )
    )
    
    # Calculate confidence score
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(
            F.col("predicted_delivery_days") <= 5,
            F.lit(0.95)  # High confidence for short deliveries
        ).when(
            F.col("predicted_delivery_days") <= 10,
            F.lit(0.85)  # Medium confidence
        ).otherwise(
            F.lit(0.75)  # Lower confidence for long deliveries
        )
    )
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("order_id"),
        current_timestamp.alias("prediction_date"),
        F.col("predicted_delivery_days"),
        F.col("predicted_delivery_date"),
        F.col("confidence_interval_lower"),
        F.col("confidence_interval_upper"),
        F.col("factors_affecting_delivery"),
        F.col("confidence_score"),
        F.lit("random_forest").alias("model_version")
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
    print(f"Sample Delivery Time Predictions (first {n} orders)")
    print("="*80)
    
    sample = df.select(
        "order_id", "predicted_delivery_days", "predicted_delivery_date",
        "confidence_interval_lower", "confidence_interval_upper", "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Order: {row['order_id']:<30}")
        print(f"  Predicted Delivery: {row['predicted_delivery_days']:.1f} days")
        print(f"  Expected Date: {row['predicted_delivery_date']}")
        print(f"  Confidence Range: {row['confidence_interval_lower']:.1f} - {row['confidence_interval_upper']:.1f} days")
        print(f"  Confidence: {row['confidence_score']*100:.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("order_id").alias("total_orders"),
        F.avg("predicted_delivery_days").alias("avg_predicted_days"),
        F.min("predicted_delivery_days").alias("min_predicted_days"),
        F.max("predicted_delivery_days").alias("max_predicted_days"),
        F.sum(F.when(F.col("predicted_delivery_days") <= 3, 1).otherwise(0)).alias("express_deliveries"),
        F.sum(F.when(F.col("predicted_delivery_days") <= 7, 1).otherwise(0)).alias("standard_deliveries"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    print(f"Total Orders: {stats['total_orders']}")
    print(f"Average Predicted Delivery: {stats['avg_predicted_days']:.1f} days")
    print(f"Fastest Delivery: {stats['min_predicted_days']:.1f} days")
    print(f"Slowest Delivery: {stats['max_predicted_days']:.1f} days")
    print(f"Express Deliveries (≤3 days): {stats['express_deliveries']}")
    print(f"Standard Deliveries (≤7 days): {stats['standard_deliveries']}")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main():
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Delivery Time Prediction - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 80)
    model = load_model()
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 80)
    
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    
    if None in [orders_df, customers_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Calculate statistics
    print("\nStep 3: Calculate Location Statistics")
    print("-" * 80)
    city_stats, state_stats, country_stats = calculate_location_statistics(orders_df, customers_df)
    
    print("\nStep 4: Calculate Shipping Tier Statistics")
    print("-" * 80)
    tier_stats = calculate_shipping_tier_statistics(orders_df)
    
    print("\nStep 5: Calculate Customer History")
    print("-" * 80)
    customer_stats = calculate_customer_history(orders_df)
    
    # Create features
    print("\nStep 6: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(
        orders_df, customers_df, city_stats, state_stats,
        country_stats, tier_stats, customer_stats
    )
    
    # Prepare data
    print("\nStep 7: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 8: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(model, df_prepared)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
    print("\nStep 9: Save Predictions")
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
