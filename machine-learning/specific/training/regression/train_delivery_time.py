"""
Delivery Time Prediction - Training Script
Predicts order delivery time in days

Target Calculation:
- Use ONLY delivered orders with actual delivery dates
- Target = delivery_days_diff (already calculated in schema)
- delivery_days = (order_delivered_at - order_placed_at) in days
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/delivery_time/"
MIN_RECORDS_THRESHOLD = 100
MAX_NULL_PERCENTAGE = 95.0

# Configuration
USE_CROSS_VALIDATION = False

# Required columns
REQUIRED_ORDER_COLUMNS = ["order_id", "customer_id", "order_status", "order_placed_at", "delivery_days_diff"]
REQUIRED_CUSTOMER_COLUMNS = ["customer_id", "country", "state_province", "city"]

# Feature set
NUMERIC_FEATURES = [
    # Order characteristics
    "total_quantity",
    "total_amount",
    "shipping_cost",
    "unique_products_ordered",
    "avg_product_price",
    
    # Temporal features
    "order_placed_day_of_week",
    "order_placed_month",
    "order_placed_quarter",
    "order_placed_day_of_month",
    "is_weekend",
    "is_month_end",
    "is_holiday_season",
    
    # Location-based historical performance
    "city_avg_delivery_days",
    "city_delivery_std",
    "state_avg_delivery_days",
    "country_avg_delivery_days",
    "location_delivery_consistency",  # 1/std
    
    # Shipping cost tier performance
    "shipping_tier_avg_delivery",
    "shipping_cost_to_amount_ratio",
    
    # Customer history
    "customer_total_orders",
    "customer_avg_delivery_days",
    "is_repeat_customer",
    
    # Order complexity
    "order_complexity_score",  # quantity * unique_products
    "order_size_tier",  # Small/Medium/Large
    
    # Distance indicators (proxy)
    "is_major_city",
    "is_capital_city",
    
    # Categorical (indexed)
    "country_idx",
    "state_idx",
    "city_idx",
    "season_idx"
]

TARGET_COLUMN = "delivery_days_diff"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Delivery_Time_Training")
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


def validate_columns(df, required_columns, dataset_name):
    """Validate columns exist and are not entirely null"""
    print(f"\nValidating columns for {dataset_name}...")
    
    existing_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"✗ Missing columns in {dataset_name}: {', '.join(missing_columns)}")
        return False, missing_columns, []
    
    total_count = df.count()
    null_columns = []
    
    for col in required_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 100
        
        if null_pct > MAX_NULL_PERCENTAGE:
            print(f"✗ Column '{col}' is {null_pct:.1f}% null (threshold: {MAX_NULL_PERCENTAGE}%)")
            null_columns.append(col)
        elif null_pct > 50:
            print(f"⚠  Column '{col}' is {null_pct:.1f}% null (may affect accuracy)")
    
    if null_columns:
        return False, [], null_columns
    
    print(f"✓ All required columns validated for {dataset_name}")
    return True, [], []


def calculate_location_delivery_statistics(orders_df, customers_df):
    """
    Calculate historical delivery performance by location
    Uses PAST delivered orders to establish location baselines
    """
    print("Calculating location-based delivery statistics...")
    
    # Join orders with customer locations - only DELIVERED orders
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    ).join(
        customers_df.select("customer_id", "country", "state_province", "city"),
        "customer_id",
        "inner"
    )
    
    print(f"Delivered orders with locations: {delivered_orders.count()}")
    
    # City-level statistics
    city_stats = delivered_orders.groupBy("country", "state_province", "city").agg(
        F.avg("delivery_days_diff").alias("city_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("city_delivery_std"),
        F.count("order_id").alias("city_order_count")
    )
    
    # State-level statistics
    state_stats = delivered_orders.groupBy("country", "state_province").agg(
        F.avg("delivery_days_diff").alias("state_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("state_delivery_std"),
        F.count("order_id").alias("state_order_count")
    )
    
    # Country-level statistics
    country_stats = delivered_orders.groupBy("country").agg(
        F.avg("delivery_days_diff").alias("country_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("country_delivery_std"),
        F.count("order_id").alias("country_order_count")
    )
    
    # Fill nulls
    city_stats = city_stats.fillna({"city_delivery_std": 0})
    state_stats = state_stats.fillna({"state_delivery_std": 0})
    country_stats = country_stats.fillna({"country_delivery_std": 0})
    
    print(f"✓ Location statistics calculated")
    print(f"  Cities: {city_stats.count()}")
    print(f"  States: {state_stats.count()}")
    print(f"  Countries: {country_stats.count()}")
    
    return city_stats, state_stats, country_stats


def calculate_shipping_tier_statistics(orders_df):
    """Calculate delivery performance by shipping cost tier"""
    print("Calculating shipping tier statistics...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0) &
        (F.col("shipping_cost").isNotNull())
    )
    
    # Create shipping cost tiers
    delivered_with_tiers = delivered_orders.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    tier_stats = delivered_with_tiers.groupBy("shipping_tier").agg(
        F.avg("delivery_days_diff").alias("shipping_tier_avg_delivery"),
        F.count("order_id").alias("tier_order_count")
    )
    
    print(f"✓ Shipping tier statistics calculated")
    return tier_stats


def calculate_customer_delivery_history(orders_df):
    """Calculate historical delivery performance per customer"""
    print("Calculating customer delivery history...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    )
    
    customer_stats = delivered_orders.groupBy("customer_id").agg(
        F.avg("delivery_days_diff").alias("customer_avg_delivery_days"),
        F.count("order_id").alias("customer_order_count")
    )
    
    print(f"✓ Customer delivery history calculated")
    return customer_stats


def create_delivery_time_features(orders_df, customers_df, city_stats_df, state_stats_df, 
                                  country_stats_df, tier_stats_df, customer_stats_df):
    """
    Create comprehensive delivery time prediction features
    Only use features available at order placement time
    """
    print("Creating delivery time features...")
    
    # Start with delivered orders only for training
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    )
    
    print(f"Delivered orders for training: {delivered_orders.count()}")
    
    # Join with customer locations
    order_features = delivered_orders.join(
        customers_df.select("customer_id", "country", "state_province", "city", "customer_segment"),
        "customer_id",
        "inner"
    )
    
    # Join with location statistics
    order_features = order_features.join(
        city_stats_df,
        ["country", "state_province", "city"],
        "left"
    ).join(
        state_stats_df.select("country", "state_province", "state_avg_delivery_days"),
        ["country", "state_province"],
        "left"
    ).join(
        country_stats_df.select("country", "country_avg_delivery_days"),
        "country",
        "left"
    )
    
    # Calculate shipping tier for this order
    order_features = order_features.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    # Join with shipping tier statistics
    order_features = order_features.join(
        tier_stats_df,
        "shipping_tier",
        "left"
    )
    
    # Join with customer history
    order_features = order_features.join(
        customer_stats_df,
        "customer_id",
        "left"
    )
    
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
    
    # Location consistency metric
    order_features = order_features.withColumn(
        "location_delivery_consistency",
        F.when(
            F.col("city_delivery_std") > 0,
            1.0 / F.col("city_delivery_std")
        ).otherwise(1.0)
    )
    
    # Shipping cost ratio
    order_features = order_features.withColumn(
        "shipping_cost_to_amount_ratio",
        F.when(
            F.col("total_amount") > 0,
            F.col("shipping_cost") / F.col("total_amount")
        ).otherwise(0)
    )
    
    # Customer indicators
    order_features = order_features.withColumn(
        "is_repeat_customer",
        F.when(F.col("customer_order_count") > 1, 1).otherwise(0)
    ).withColumn(
        "customer_total_orders",
        F.coalesce(F.col("customer_order_count"), F.lit(1))
    )
    
    # Order complexity
    order_features = order_features.withColumn(
        "order_complexity_score",
        F.col("total_quantity") * F.coalesce(F.col("unique_products_ordered"), F.lit(1))
    ).withColumn(
        "avg_product_price",
        F.when(
            F.col("total_quantity") > 0,
            F.col("total_amount") / F.col("total_quantity")
        ).otherwise(0)
    )
    
    # Order size tier
    order_features = order_features.withColumn(
        "order_size_tier",
        F.when(F.col("total_amount") < 50, 1)
         .when(F.col("total_amount") < 150, 2)
         .when(F.col("total_amount") < 300, 3)
         .otherwise(4)
    )
    
    # Major city indicators (simplified - would need actual city list)
    # Using order volume as proxy
    order_features = order_features.withColumn(
        "is_major_city",
        F.when(F.col("city_order_count") > 100, 1).otherwise(0)
    ).withColumn(
        "is_capital_city",
        F.when(F.col("city_order_count") > 200, 1).otherwise(0)
    )
    
    # Fill nulls with reasonable defaults
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
        "country_avg_delivery_days": 7,  # Global average
        "shipping_tier_avg_delivery": 7,
        "customer_avg_delivery_days": 0,
        "season": "Summer"
    })
    
    print(f"✓ Delivery time features created: {order_features.count()} orders")
    return order_features


def prepare_training_data(df):
    """Prepare data with encoding and scaling"""
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0) &
        (F.col(TARGET_COLUMN) < 100)  # Reasonable delivery time limit
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
        return None
    
    # Encode categorical features
    country_indexer = StringIndexer(inputCol="country", outputCol="country_idx", handleInvalid="keep")
    state_indexer = StringIndexer(inputCol="state_province", outputCol="state_idx", handleInvalid="keep")
    city_indexer = StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep")
    season_indexer = StringIndexer(inputCol="season", outputCol="season_idx", handleInvalid="keep")
    
    df_indexed = country_indexer.fit(df_valid).transform(df_valid)
    df_indexed = state_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = city_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = season_indexer.fit(df_indexed).transform(df_indexed)
    
    # Filter features that exist
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    missing_features = [f for f in NUMERIC_FEATURES if f not in df_indexed.columns]
    
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} features for training")
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="skip"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Select final columns
    df_prepared = df_scaled.select(
        "order_id",
        "features",
        F.col(TARGET_COLUMN).alias("label")
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model, existing_features


def train_random_forest(train_df, test_df, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print("Training Random Forest")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=200,
        maxDepth=15,
        seed=42
    )
    
    if use_cv:
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [150, 200, 250]) \
            .addGrid(rf.maxDepth, [12, 15, 18]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=rf,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = rf.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "random_forest"


def evaluate_model(predictions, model_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name}...")
    
    rmse_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    mape_df = predictions.filter(F.col("label") > 0).withColumn(
        "ape",
        F.abs((F.col("label") - F.col("prediction")) / F.col("label")) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0] if mape_df.count() > 0 else 0
    
    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    
    print(f"  RMSE: {rmse:.2f} days")
    print(f"  MAE: {mae:.2f} days")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("Delivery Time Prediction - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    
    if None in [orders_df, customers_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    orders_valid, _, _ = validate_columns(orders_df, REQUIRED_ORDER_COLUMNS, "Orders")
    customers_valid, _, _ = validate_columns(customers_df, REQUIRED_CUSTOMER_COLUMNS, "Customers")
    
    if not (orders_valid and customers_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Calculate location statistics
    print("\nStep 3: Calculate Location Delivery Statistics")
    print("-" * 60)
    city_stats, state_stats, country_stats = calculate_location_delivery_statistics(orders_df, customers_df)
    
    # Calculate shipping tier statistics
    print("\nStep 4: Calculate Shipping Tier Statistics")
    print("-" * 60)
    tier_stats = calculate_shipping_tier_statistics(orders_df)
    
    # Calculate customer history
    print("\nStep 5: Calculate Customer Delivery History")
    print("-" * 60)
    customer_stats = calculate_customer_delivery_history(orders_df)
    
    # Create features
    print("\nStep 6: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_delivery_time_features(
        orders_df, customers_df, city_stats, state_stats, 
        country_stats, tier_stats, customer_stats
    )
    
    # Prepare data
    print("\nStep 7: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features)
    
    if result is None:
        print("\n✗ Training aborted: Insufficient data")
        spark.stop()
        return
    
    df_prepared, scaler, feature_list = result
    
    print(f"\n{'='*60}")
    print(f"Final Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    # Split data
    print("\nStep 8: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train model
    print("\nStep 9: Model Training")
    print("-" * 60)
    
    model, predictions, model_name = train_random_forest(train_df, test_df, USE_CROSS_VALIDATION)
    metrics = evaluate_model(predictions, model_name)
    save_model(model, model_name)
    
    print("\n" + "="*60)
    print(f"Best Model: {model_name} (R² = {metrics['r2']:.4f})")
    print("="*60)
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
