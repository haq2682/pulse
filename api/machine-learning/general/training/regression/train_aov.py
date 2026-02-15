"""
Average Order Value (AOV) Prediction - Improved Training Script
Enhanced with comprehensive validation and expanded feature set
"""

import os
import sys
import findspark
from dotenv import load_dotenv

findspark.init()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "aov"
INPUT_RELATIVE_PATH = "transformed/agg_customers.parquet"
INPUT_ORDERS_RELATIVE_PATH = "transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_RELATIVE_PATH = "transformed/agg_order_items.parquet"
INPUT_PRODUCTS_RELATIVE_PATH = "transformed/agg_products.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("regression", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
MAX_NULL_PERCENTAGE = 95.0  # Skip if column >95% null

# Configuration
USE_CROSS_VALIDATION = False

# Required columns for validation (REMOVED avg_days_between_orders - we calculate it)
REQUIRED_CUSTOMER_COLUMNS = [
    "customer_id", "total_orders", "customer_tenure_days", "total_items_purchased",
    "avg_items_per_order", "session_conversion_rate",
    "cart_abandonment_rate", "recency_score", "frequency_score", "monetary_score"
]

REQUIRED_ORDER_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_placed_at", "total_amount",
    "order_placed_month", "order_placed_day_of_week", "total_discount"
]

# Expanded feature set (REMOVED avg_days_between_orders - calculated manually)
NUMERIC_FEATURES = [
    # Customer profile
    "total_orders",
    "customer_tenure_days",
    "total_items_purchased",
    "avg_items_per_order",
    "days_since_last_purchase",
    
    # MANUALLY CALCULATED temporal features (replace null avg_days_between_orders)
    "calc_avg_days_between_orders",  # Calculated from actual order history
    "days_since_prev_order",  # Days since last order
    "order_frequency_per_month",  # Orders per month
    "avg_days_per_order",  # Customer tenure / total orders
    
    # Behavioral metrics
    "session_conversion_rate",
    "cart_abandonment_rate",
    "cancellation_rate",
    "total_reviews_written",
    "avg_review_rating",
    "customer_activity_score",
    
    # Engagement
    "total_pages_viewed",
    "total_products_viewed",
    "total_sessions",
    "wishlist_items_count",
    
    # RFM
    "recency_score",
    "frequency_score",
    "monetary_score",
    
    # Order history lags
    "aov_lag_1",
    "aov_lag_2",
    "aov_lag_3",
    "aov_rolling_3",
    "aov_rolling_6",
    "aov_trend",
    "aov_volatility",
    
    # Order patterns (ENHANCED)
    "avg_discount_per_order",
    "avg_order_discount_pct",
    "discount_rolling_3",  # NEW: Rolling discount average
    "discount_sensitivity",  # NEW: How often customer uses discounts
    "spending_acceleration",  # NEW: Is spending increasing?
    "avg_products_per_order",
    "avg_product_price",
    "avg_category_diversity",  # NEW: Category diversity in orders
    
    # Temporal
    "order_placed_month",
    "order_placed_day_of_week",
    "days_since_first_order"
]

CATEGORICAL_FEATURES = [
    "customer_segment_label",
    "preferred_payment_method",
    "preferred_device_type"
]

TARGET_COLUMN = "next_order_value"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("AOV_Prediction_Improved_Training")
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
    """
    Validate that required columns exist and are not entirely null
    Returns: (is_valid, missing_columns, null_columns)
    """
    print(f"\nValidating columns for {dataset_name}...")
    
    existing_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"✗ Missing columns in {dataset_name}: {', '.join(missing_columns)}")
        return False, missing_columns, []
    
    # Check null percentages
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


def create_enhanced_features(customers_df, orders_df, order_items_df, products_df):
    """
    Create enhanced features with manually calculated temporal metrics and category preferences
    """
    print("Creating enhanced features...")
    
    # Filter delivered orders
    orders_filtered = orders_df.filter(
        ((F.col("order_status") == "Delivered") | (F.col("order_status") == "delivered") | (F.col("order_status") == "Completed") | (F.col("order_status") == "completed") | (F.col("order_status") == "complete") | (F.col("order_status") == "Complete")) &
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    
    print(f"Filtered orders: {orders_filtered.count()} delivered orders")
    
    # Join orders with order_items and products to get category info
    orders_with_items = orders_filtered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "left"
    ).join(
        products_df.alias("p").select("product_id", "category", "sell_price"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "left"
    )
    
    # Aggregate order-level metrics with category info
    order_agg = orders_with_items.groupBy("o.order_id").agg(
        F.first("o.customer_id").alias("customer_id"),
        F.first("o.order_placed_at").alias("order_placed_at"),
        F.first("o.total_amount").alias("total_amount"),
        F.first("o.total_discount").alias("total_discount"),
        F.first("o.order_placed_month").alias("order_placed_month"),
        F.first("o.order_placed_day_of_week").alias("order_placed_day_of_week"),
        F.count("oi.order_item_id").alias("products_in_order"),
        F.avg("oi.product_price").alias("avg_product_price_order"),
        F.countDistinct("p.category").alias("unique_categories_in_order"),
        F.first("p.category").alias("primary_category_order")
    ).select(
        "customer_id", "order_placed_at", "total_amount", "total_discount",
        "order_placed_month", "order_placed_day_of_week", "products_in_order",
        "avg_product_price_order", "unique_categories_in_order", "primary_category_order"
    )
    
    print(f"Aggregated order metrics: {order_agg.count()} orders")
    
    # Create window for each customer
    customer_window = Window.partitionBy("customer_id").orderBy("order_placed_at")
    
    # Add sequence and calculate MANUAL temporal features
    orders_with_seq = order_agg.withColumn(
        "order_seq",
        F.row_number().over(customer_window)
    ).withColumn(
        "days_since_first_order",
        F.datediff(F.col("order_placed_at"), F.first("order_placed_at").over(customer_window))
    ).withColumn(
        # MANUAL: Days since previous order
        "days_since_prev_order",
        F.datediff(F.col("order_placed_at"), F.lag("order_placed_at", 1).over(customer_window))
    ).withColumn(
        # MANUAL: Total orders up to this point
        "orders_up_to_now",
        F.row_number().over(customer_window)
    )
    
    # Calculate MANUAL avg_days_between_orders from actual order history
    window_all_prev = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(Window.unboundedPreceding, -1)
    
    orders_with_seq = orders_with_seq.withColumn(
        # MANUAL: Average days between all previous orders
        "calc_avg_days_between_orders",
        F.when(
            F.col("order_seq") > 1,
            F.col("days_since_first_order") / (F.col("order_seq") - 1)
        ).otherwise(0)
    ).withColumn(
        # MANUAL: Order frequency (orders per month)
        "order_frequency_per_month",
        F.when(
            F.col("days_since_first_order") > 0,
            (F.col("order_seq") - 1) / (F.col("days_since_first_order") / 30.0)
        ).otherwise(0)
    )
    
    # Create lag features for AOV
    orders_with_lags = orders_with_seq.withColumn(
        "aov_lag_1",
        F.lag("total_amount", 1).over(customer_window)
    ).withColumn(
        "aov_lag_2",
        F.lag("total_amount", 2).over(customer_window)
    ).withColumn(
        "aov_lag_3",
        F.lag("total_amount", 3).over(customer_window)
    )
    
    # Calculate rolling averages
    window_rolling_3 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-3, -1)
    window_rolling_6 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-6, -1)
    
    orders_with_lags = orders_with_lags.withColumn(
        "aov_rolling_3",
        F.avg("total_amount").over(window_rolling_3)
    ).withColumn(
        "aov_rolling_6",
        F.avg("total_amount").over(window_rolling_6)
    ).withColumn(
        # Discount rolling average
        "discount_rolling_3",
        F.avg("total_discount").over(window_rolling_3)
    )
    
    # Calculate trend, volatility, and acceleration
    orders_with_lags = orders_with_lags.withColumn(
        "aov_trend",
        F.when(
            (F.col("aov_lag_1").isNotNull()) & (F.col("aov_lag_2").isNotNull()) & (F.col("aov_lag_2") > 0),
            (F.col("aov_lag_1") - F.col("aov_lag_2")) / F.col("aov_lag_2")
        ).otherwise(0)
    ).withColumn(
        "aov_volatility",
        F.stddev("total_amount").over(window_rolling_6)
    ).withColumn(
        # Spending acceleration (is customer spending more over time?)
        "spending_acceleration",
        F.when(
            (F.col("aov_rolling_3").isNotNull()) & (F.col("aov_rolling_6").isNotNull()) & (F.col("aov_rolling_6") > 0),
            (F.col("aov_rolling_3") - F.col("aov_rolling_6")) / F.col("aov_rolling_6")
        ).otherwise(0)
    )
    
    # Target: current order value
    orders_with_lags = orders_with_lags.withColumn(
        "next_order_value",
        F.col("total_amount")
    )
    
    # Calculate discount percentage and sensitivity
    orders_with_lags = orders_with_lags.withColumn(
        "order_discount_pct",
        F.when(
            F.col("total_amount") > 0,
            (F.col("total_discount") / F.col("total_amount")) * 100
        ).otherwise(0)
    ).withColumn(
        # Discount sensitivity
        "discount_sensitivity",
        F.avg(
            F.when(F.col("total_discount") > 0, 1).otherwise(0)
        ).over(window_rolling_6)
    )
    
    # Calculate category diversity (how many different categories per order on average)
    orders_with_lags = orders_with_lags.withColumn(
        "avg_category_diversity",
        F.avg("unique_categories_in_order").over(window_rolling_6)
    )
    
    print(f"Added temporal and pattern features")
    
    # Join with customer data
    customer_features = orders_with_lags.join(
        customers_df.select(
            "customer_id",
            "total_orders",
            "customer_tenure_days",
            "total_items_purchased",
            "avg_items_per_order",
            "session_conversion_rate",
            "cart_abandonment_rate",
            "cancellation_rate",
            "total_reviews_written",
            "avg_review_rating",
            "customer_activity_score",
            "total_pages_viewed",
            "total_products_viewed",
            "total_sessions",
            "wishlist_items_count",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "avg_discount_per_order",
            "customer_segment_label",
            "preferred_payment_method",
            "preferred_device_type"
        ),
        "customer_id",
        "left"
    )
    
    # Filter: need at least 1 previous order (not first order)
    customer_features = customer_features.filter(F.col("order_seq") > 1)
    
    print(f"After filtering (order_seq > 1): {customer_features.count()} records")
    
    # Calculate average metrics
    customer_features = customer_features.withColumn(
        "avg_products_per_order",
        F.when(
            F.col("total_orders") > 0,
            F.col("total_items_purchased") / F.col("total_orders")
        ).otherwise(0)
    ).withColumn(
        "avg_product_price",
        F.coalesce(F.col("avg_product_price_order"), F.lit(0))
    ).withColumn(
        "avg_order_discount_pct",
        F.coalesce(F.col("order_discount_pct"), F.lit(0))
    ).withColumn(
        # Customer purchase power (tenure / orders)
        "avg_days_per_order",
        F.when(
            F.col("total_orders") > 0,
            F.col("customer_tenure_days") / F.col("total_orders")
        ).otherwise(0)
    )
    
    # Fill nulls in lag features
    lag_cols = [
        "aov_lag_1", "aov_lag_2", "aov_lag_3", "aov_rolling_3", "aov_rolling_6",
        "aov_trend", "aov_volatility", "discount_rolling_3", "spending_acceleration",
        "discount_sensitivity", "avg_category_diversity"
    ]
    
    for col in lag_cols:
        customer_features = customer_features.fillna({col: 0})
    
    # Fill nulls in customer metrics (REMOVED avg_days_between_orders - we calculate manually)
    customer_features = customer_features.fillna({
        "total_items_purchased": 0,
        "session_conversion_rate": 0,
        "cart_abandonment_rate": 0,
        "cancellation_rate": 0,
        "total_reviews_written": 0,
        "avg_review_rating": 0,
        "customer_activity_score": 0,
        "total_pages_viewed": 0,
        "total_products_viewed": 0,
        "total_sessions": 0,
        "wishlist_items_count": 0,
        "avg_discount_per_order": 0,
        "avg_items_per_order": 0,
        "recency_score": 0,
        "frequency_score": 0,
        "monetary_score": 0,
        "customer_segment_label": "Unknown",
        "preferred_payment_method": "Unknown",
        "preferred_device_type": "Unknown",
        "days_since_first_order": 0,
        "days_since_prev_order": 0,
        "calc_avg_days_between_orders": 0,
        "order_frequency_per_month": 0,
        "avg_products_per_order": 0,
        "avg_product_price": 0,
        "avg_days_per_order": 0,
        "primary_category_order": "Unknown"
    })
    
    print(f"✓ Enhanced features created: {customer_features.count()} records")
    return customer_features


def prepare_training_data(df):
    """
    Prepare data with encoding and scaling
    """
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS}")
        return None
    
    # Encode categorical features
    indexers = {}
    indexed_cols = []
    
    for cat_col in CATEGORICAL_FEATURES:
        idx_col = f"{cat_col}_idx"
        indexed_cols.append(idx_col)
        indexers[cat_col] = StringIndexer(
            inputCol=cat_col,
            outputCol=idx_col,
            handleInvalid="keep"
        )
        df_valid = indexers[cat_col].fit(df_valid).transform(df_valid)
    
    # Combine all features
    all_features = NUMERIC_FEATURES + indexed_cols
    
    # Filter features that actually exist in dataframe
    existing_features = [f for f in all_features if f in df_valid.columns]
    missing_features = [f for f in all_features if f not in df_valid.columns]
    
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} features for training")
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_valid)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Select final columns
    df_prepared = df_scaled.select(
        "customer_id",
        "features",
        TARGET_COLUMN
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model, existing_features


def train_linear_regression(train_df, test_df, use_cv=False):
    """Train Linear Regression"""
    print("\n" + "="*60)
    print("Training Linear Regression")
    print("="*60)
    
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.001, 0.01, 0.1]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = lr.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "linear_regression"


def train_random_forest(train_df, test_df, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print("Training Random Forest")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        numTrees=200,
        maxDepth=15,
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [150, 200, 250]) \
            .addGrid(rf.maxDepth, [12, 15, 18]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
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


def train_gbt(train_df, test_df, use_cv=False):
    """Train GBT"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees")
    print("="*60)
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=150,
        maxDepth=8,
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(gbt.maxIter, [100, 150, 200]) \
            .addGrid(gbt.maxDepth, [6, 8, 10]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=gbt,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = gbt.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "gbt"


def evaluate_model(predictions, model_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name}...")
    
    rmse_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    mape_df = predictions.withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]
    
    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    
    print(f"  RMSE: ${rmse:.2f}")
    print(f"  MAE: ${mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_DIR}/{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("AOV Prediction - General Model Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print("="*60 + "\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Load datasets from all buckets
    print("Step 1: Loading data from all MinIO buckets...")
    print("-" * 60)
    
    customers_df, cust_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=REQUIRED_CUSTOMER_COLUMNS,
        filter_nulls=True
    )
    
    if customers_df is None:
        print("⚠️  No customer data available. Skipping training.")
        spark.stop()
        return
    
    orders_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_ORDERS_RELATIVE_PATH,
        required_columns=REQUIRED_ORDER_COLUMNS,
        filter_nulls=True
    )
    
    if orders_df is None:
        print("⚠️  No orders data available. Skipping training.")
        spark.stop()
        return
    
    order_items_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_ORDER_ITEMS_RELATIVE_PATH,
        required_columns=["order_id", "product_id"],
        filter_nulls=True
    )
    
    if order_items_df is None:
        print("⚠️  No order items data available. Skipping training.")
        spark.stop()
        return
    
    products_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_PRODUCTS_RELATIVE_PATH,
        required_columns=["product_id", "category"],
        filter_nulls=True
    )
    
    if products_df is None:
        print("⚠️  No products data available. Skipping training.")
        spark.stop()
        return
    
    # Step 2: Validate training data window
    print("\nStep 2: Validate Training Data Window")
    print("-" * 60)
    is_valid, customers_df = validate_training_data(
        customers_df, cust_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        spark.stop()
        return
    
    # Step 3: Column validation
    print("\nStep 3: Column Validation")
    print("-" * 60)
    
    cust_valid, cust_missing, cust_null = validate_columns(
        customers_df, REQUIRED_CUSTOMER_COLUMNS, "Customers"
    )
    
    ord_valid, ord_missing, ord_null = validate_columns(
        orders_df, REQUIRED_ORDER_COLUMNS, "Orders"
    )
    
    if not (cust_valid and ord_valid):
        print("⚠️  Training skipped due to required columns missing or entirely null")
        print(f"   Missing in Customers: {cust_missing}")
        print(f"   Null in Customers: {cust_null}")
        print(f"   Missing in Orders: {ord_missing}")
        print(f"   Null in Orders: {ord_null}")
        spark.stop()
        return
    
    # Step 4: Create features
    print("\nStep 4: Feature Engineering")
    print("-" * 60)
    df_features = create_enhanced_features(customers_df, orders_df, order_items_df, products_df)
    
    # Step 5: Prepare data
    print("\nStep 5: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features)
    
    if result is None:
        print("⚠️  Training skipped due to insufficient data")
        spark.stop()
        return
    
    df_prepared, scaler, feature_list = result
    
    print(f"\n{'='*60}")
    print(f"Final Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    # Step 6: Split data
    print("\nStep 6: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Step 7: Train models
    print("\nStep 7: Model Training")
    print("-" * 60)
    
    models_results = []
    
    lr_model, lr_pred, lr_name = train_linear_regression(train_df, test_df, USE_CROSS_VALIDATION)
    lr_metrics = evaluate_model(lr_pred, lr_name)
    save_model(lr_model, lr_name)
    models_results.append(lr_metrics)
    
    rf_model, rf_pred, rf_name = train_random_forest(train_df, test_df, USE_CROSS_VALIDATION)
    rf_metrics = evaluate_model(rf_pred, rf_name)
    save_model(rf_model, rf_name)
    models_results.append(rf_metrics)
    
    gbt_model, gbt_pred, gbt_name = train_gbt(train_df, test_df, USE_CROSS_VALIDATION)
    gbt_metrics = evaluate_model(gbt_pred, gbt_name)
    save_model(gbt_model, gbt_name)
    models_results.append(gbt_metrics)
    
    # Model comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<15} {'MAE':<15} {'R²':<10} {'MAPE':<10}")
    print("-" * 60)
    
    for m in models_results:
        print(f"{m['model']:<25} ${m['rmse']:<14.2f} ${m['mae']:<14.2f} {m['r2']:<10.4f} {m['mape']:<10.2f}%")
    
    best = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best['model']} (R² = {best['r2']:.4f})")
    print("="*60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Update MODEL_NAME in predict_aov.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()