"""
Average Order Value (AOV) Prediction - FIXED Training Script
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

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

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/aov_prediction/"
SCALER_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/aov_prediction/scaler"
MIN_RECORDS_THRESHOLD = 100
MAX_NULL_PERCENTAGE = 95.0

# Configuration
USE_CROSS_VALIDATION = False
TRAIN_SPLIT_RATIO = 0.8  # 80% train, 20% test (time-based)

# MAPE minimum denominator to prevent explosion on low AOV
MAPE_MIN_DENOMINATOR = 50.0

# Required columns for validation
REQUIRED_CUSTOMER_COLUMNS = [
    "customer_id", "total_orders", "customer_tenure_days", "total_items_purchased",
    "avg_items_per_order", "session_conversion_rate",
    "cart_abandonment_rate", "recency_score", "frequency_score", "monetary_score"
]

REQUIRED_ORDER_COLUMNS = [
    "order_id", "customer_id", "order_status", "order_placed_at", "total_amount",
    "order_placed_month", "order_placed_day_of_week", "total_discount"
]

# Reduced feature set (pruned weak features to prevent overfitting)
# Removed: days_since_last_purchase (leaky), redundant temporal features
NUMERIC_FEATURES = [
    # Customer profile (core)
    "total_orders",
    "customer_tenure_days",
    "total_items_purchased",
    "avg_items_per_order",
    
    # Calculated temporal features
    "calc_avg_days_between_orders",
    "days_since_prev_order",
    "order_frequency_per_month",
    
    # Behavioral metrics (pruned)
    "session_conversion_rate",
    "cart_abandonment_rate",
    "cancellation_rate",
    "customer_activity_score",
    
    # Engagement (pruned)
    "total_sessions",
    "wishlist_items_count",
    
    # RFM (keep all - strong predictors)
    "recency_score",
    "frequency_score",
    "monetary_score",
    
    # Order history lags (most important features)
    "aov_lag_1",
    "aov_lag_2",
    "aov_lag_3",
    "aov_rolling_3",
    "aov_rolling_6",
    "aov_trend",
    "aov_volatility",
    
    # Order patterns (improved)
    "avg_discount_per_order",
    "discount_rolling_3",
    "discount_sensitivity_weighted",  # FIXED: Weighted version
    "spending_acceleration",
    "avg_products_per_order",
    "avg_product_price",
    "avg_category_diversity",
    
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
        .appName("AOV_Prediction_Fixed_Training")
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
    """Validate that required columns exist and are not entirely null"""
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


def create_enhanced_features(customers_df, orders_df, order_items_df, products_df):
    """
    Create enhanced features with FIXED target leakage and improved calculations
    
    FIXES APPLIED:
    - Target is now NEXT order value (using lead)
    - Rolling windows include current row for stability
    - Discount sensitivity weighted by spend
    - Proper temporal feature calculation
    """
    print("Creating enhanced features (with leakage fixes)...")
    
    # Filter delivered orders
    orders_filtered = orders_df.filter(
        ((F.col("order_status") == "Delivered") | 
         (F.col("order_status") == "delivered") | 
         (F.col("order_status") == "Completed") | 
         (F.col("order_status") == "completed") | 
         (F.col("order_status") == "complete") | 
         (F.col("order_status") == "Complete")) &
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    
    print(f"Filtered orders: {orders_filtered.count()} delivered orders")
    
    # Join orders with order_items and products
    orders_with_items = orders_filtered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "left"
    ).join(
        products_df.alias("p").select("product_id", "category", "sell_price"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "left"
    )
    
    # Aggregate order-level metrics
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
    
    # Add sequence and temporal features
    orders_with_seq = order_agg.withColumn(
        "order_seq",
        F.row_number().over(customer_window)
    ).withColumn(
        "days_since_first_order",
        F.datediff(F.col("order_placed_at"), F.first("order_placed_at").over(customer_window))
    ).withColumn(
        "days_since_prev_order",
        F.datediff(F.col("order_placed_at"), F.lag("order_placed_at", 1).over(customer_window))
    ).withColumn(
        "orders_up_to_now",
        F.row_number().over(customer_window)
    )
    
    # Calculate avg_days_between_orders from actual history
    orders_with_seq = orders_with_seq.withColumn(
        "calc_avg_days_between_orders",
        F.when(
            F.col("order_seq") > 1,
            F.col("days_since_first_order") / (F.col("order_seq") - 1)
        ).otherwise(0)
    ).withColumn(
        "order_frequency_per_month",
        F.when(
            F.col("days_since_first_order") > 0,
            (F.col("order_seq") - 1) / (F.col("days_since_first_order") / 30.0)
        ).otherwise(0)
    )
    
    # Create lag features (all look backward only - no leakage)
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
    
    # FIXED: Rolling windows now use rowsBetween(-3, 0) to include current for stability
    # But for FEATURES, we still want to look backward to avoid leakage
    # The fix is about null handling, not including current in features
    window_rolling_3 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-3, -1)
    window_rolling_6 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-6, -1)
    
    # For early sequences, use a window that's more forgiving (includes current for calculation stability)
    window_rolling_3_stable = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-2, 0)
    window_rolling_6_stable = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-5, 0)
    
    orders_with_lags = orders_with_lags.withColumn(
        "aov_rolling_3_strict",
        F.avg("total_amount").over(window_rolling_3)
    ).withColumn(
        "aov_rolling_3_stable",
        F.avg(F.lag("total_amount", 1).over(customer_window)).over(
            Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-2, 0)
        )
    ).withColumn(
        # Use strict version when available, otherwise fall back to stable
        "aov_rolling_3",
        F.coalesce(F.col("aov_rolling_3_strict"), F.col("aov_lag_1"))
    ).withColumn(
        "aov_rolling_6",
        F.coalesce(
            F.avg("total_amount").over(window_rolling_6),
            F.col("aov_rolling_3"),
            F.col("aov_lag_1")
        )
    ).withColumn(
        "discount_rolling_3",
        F.coalesce(
            F.avg("total_discount").over(window_rolling_3),
            F.lag("total_discount", 1).over(customer_window),
            F.lit(0)
        )
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
        F.coalesce(
            F.stddev("total_amount").over(window_rolling_6),
            F.lit(0)
        )
    ).withColumn(
        "spending_acceleration",
        F.when(
            (F.col("aov_rolling_3").isNotNull()) & (F.col("aov_rolling_6").isNotNull()) & (F.col("aov_rolling_6") > 0),
            (F.col("aov_rolling_3") - F.col("aov_rolling_6")) / F.col("aov_rolling_6")
        ).otherwise(0)
    )
    
    # =========================================================================
    # FIX #1: TARGET LEAKAGE - Use lead() to predict NEXT order value
    # =========================================================================
    orders_with_lags = orders_with_lags.withColumn(
        "next_order_value",
        F.lead("total_amount", 1).over(customer_window)
    )
    print("✓ Target leakage fixed: Using lead() for next_order_value")
    
    # =========================================================================
    # FIX #6: Better discount sensitivity - weighted by spend
    # =========================================================================
    # Window to calculate cumulative sums up to current row (excluding current)
    window_cumsum = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(Window.unboundedPreceding, -1)
    
    orders_with_lags = orders_with_lags.withColumn(
        "cumsum_discount",
        F.coalesce(F.sum("total_discount").over(window_cumsum), F.lit(0))
    ).withColumn(
        "cumsum_amount",
        F.coalesce(F.sum("total_amount").over(window_cumsum), F.lit(1))  # Avoid division by zero
    ).withColumn(
        "discount_sensitivity_weighted",
        F.when(
            F.col("cumsum_amount") > 0,
            F.col("cumsum_discount") / F.col("cumsum_amount")
        ).otherwise(0)
    )
    print("✓ Discount sensitivity fixed: Using weighted version (sum_discount/sum_amount)")
    
    # Keep old version for comparison (can be removed later)
    orders_with_lags = orders_with_lags.withColumn(
        "order_discount_pct",
        F.when(
            F.col("total_amount") > 0,
            (F.col("total_discount") / F.col("total_amount")) * 100
        ).otherwise(0)
    ).withColumn(
        "discount_sensitivity_binary",
        F.avg(
            F.when(F.col("total_discount") > 0, 1).otherwise(0)
        ).over(window_rolling_6)
    )
    
    # Category diversity
    orders_with_lags = orders_with_lags.withColumn(
        "avg_category_diversity",
        F.coalesce(
            F.avg("unique_categories_in_order").over(window_rolling_6),
            F.col("unique_categories_in_order")
        )
    )
    
    print("Added temporal and pattern features")
    
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
    
    # Filter: need at least 1 previous order AND valid next_order_value (not null)
    customer_features = customer_features.filter(
        (F.col("order_seq") > 1) &
        (F.col("next_order_value").isNotNull()) &  # FIX: Filter out null targets
        (F.col("next_order_value") > 0)
    )
    
    print(f"After filtering (order_seq > 1 AND next_order_value not null): {customer_features.count()} records")
    
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
        "discount_sensitivity_weighted", "avg_category_diversity"
    ]
    
    for col in lag_cols:
        customer_features = customer_features.fillna({col: 0})
    
    # Fill nulls in customer metrics
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


def prepare_training_data(df, scale_features=True):
    """
    Prepare data with encoding and optional scaling
    
    FIX #7: scale_features parameter allows skipping scaling for tree models
    """
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
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
    
    # Filter features that actually exist
    existing_features = [f for f in all_features if f in df_valid.columns]
    missing_features = [f for f in all_features if f not in df_valid.columns]
    
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} features for training")
    
    if scale_features:
        # Assemble and scale for linear regression
        assembler = VectorAssembler(
            inputCols=existing_features,
            outputCol="features_unscaled",
            handleInvalid="keep"
        )
        
        df_assembled = assembler.transform(df_valid)
        
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
            "order_placed_at",  # Keep for time-based split
            "features",
            TARGET_COLUMN
        )
        
        print(f"✓ Data prepared with scaling: {df_prepared.count()} records")
        return df_prepared, scaler_model, existing_features
    else:
        # FIX #7: No scaling for tree models
        assembler = VectorAssembler(
            inputCols=existing_features,
            outputCol="features",
            handleInvalid="keep"
        )
        
        df_assembled = assembler.transform(df_valid)
        
        df_prepared = df_assembled.select(
            "customer_id",
            "order_placed_at",  # Keep for time-based split
            "features",
            TARGET_COLUMN
        )
        
        print(f"✓ Data prepared WITHOUT scaling (for tree models): {df_prepared.count()} records")
        return df_prepared, None, existing_features


def time_based_split(df, train_ratio=0.8):
    """
    FIX #2: Time-based train/test split
    
    Split data chronologically to avoid leaking future information into training.
    """
    print(f"\n✓ Using TIME-BASED split (ratio: {train_ratio:.0%} train)")
    
    # Get cutoff date based on quantile
    date_quantiles = df.select(
        F.expr(f"percentile_approx(order_placed_at, {train_ratio})").alias("cutoff_date")
    ).collect()
    
    cutoff_date = date_quantiles[0]["cutoff_date"]
    print(f"  Cutoff date: {cutoff_date}")
    
    # Split by date
    train_df = df.filter(F.col("order_placed_at") <= cutoff_date)
    test_df = df.filter(F.col("order_placed_at") > cutoff_date)
    
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"  Training set: {train_count} records (orders up to {cutoff_date})")
    print(f"  Test set: {test_count} records (orders after {cutoff_date})")
    
    # Validate test set has data
    if test_count < 10:
        print("  ⚠ Warning: Small test set. Falling back to random split.")
        return df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    
    return train_df, test_df


def train_linear_regression(train_df, test_df, use_cv=False):
    """Train Linear Regression with L1 regularization"""
    print("\n" + "="*60)
    print("Training Linear Regression (with L1/L2 regularization)")
    print("="*60)
    
    # FIX #4: Increased regularization to prevent overfitting
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.1,          # Increased from 0.01
        elasticNetParam=0.8    # More L1 (Lasso) for feature selection
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
            .addGrid(lr.elasticNetParam, [0.5, 0.8, 1.0]) \
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
    """Train Random Forest (no scaling needed)"""
    print("\n" + "="*60)
    print("Training Random Forest (unscaled features)")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        numTrees=200,
        maxDepth=12,       # Reduced from 15 to prevent overfitting
        minInstancesPerNode=5,  # Added to prevent overfitting
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [150, 200, 250]) \
            .addGrid(rf.maxDepth, [8, 10, 12]) \
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
    """Train GBT (no scaling needed)"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees (unscaled features)")
    print("="*60)
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,       # Reduced from 150
        maxDepth=6,        # Reduced from 8
        minInstancesPerNode=5,
        stepSize=0.1,      # Learning rate
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(gbt.maxIter, [50, 100, 150]) \
            .addGrid(gbt.maxDepth, [4, 6, 8]) \
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
    """
    Evaluate model with FIXED MAPE calculation
    
    FIX #3: Safe MAPE with clamped denominator
    """
    print(f"\nEvaluating {model_name}...")
    
    rmse_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    # FIX #3: Safe MAPE with clamped denominator
    mape_df = predictions.withColumn(
        "ape",
        F.abs(
            (F.col(TARGET_COLUMN) - F.col("prediction")) / 
            F.greatest(F.col(TARGET_COLUMN), F.lit(MAPE_MIN_DENOMINATOR))  # Clamp to minimum
        ) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]
    
    # Also calculate SMAPE as alternative
    smape_df = predictions.withColumn(
        "smape",
        F.abs(F.col(TARGET_COLUMN) - F.col("prediction")) / 
        ((F.abs(F.col(TARGET_COLUMN)) + F.abs(F.col("prediction"))) / 2 + 1) * 100
    )
    smape = smape_df.agg(F.avg("smape")).collect()[0][0]
    
    metrics = {
        "model": model_name, 
        "rmse": rmse, 
        "mae": mae, 
        "r2": r2, 
        "mape": mape,
        "smape": smape
    }
    
    print(f"  RMSE: ${rmse:.2f}")
    print(f"  MAE: ${mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE (clamped): {mape:.2f}%")
    print(f"  SMAPE: {smape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def print_feature_importance(model, feature_list, model_name):
    """Print feature importance for tree models"""
    if hasattr(model, 'featureImportances'):
        print(f"\n{'='*60}")
        print(f"Feature Importance ({model_name})")
        print(f"{'='*60}")
        
        importances = model.featureImportances.toArray()
        feature_importance = list(zip(feature_list, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        print(f"{'Feature':<40} {'Importance':>15}")
        print("-" * 55)
        for feat, imp in feature_importance[:15]:  # Top 15
            print(f"{feat:<40} {imp:>15.4f}")
        
        # Identify weak features
        weak_features = [f for f, i in feature_importance if i < 0.01]
        if weak_features:
            print(f"\n⚠ Weak features (importance < 0.01): {len(weak_features)}")
            print(f"  Consider removing: {', '.join(weak_features[:5])}...")


def main():
    """Main training pipeline with all fixes"""
    print("\n" + "="*60)
    print("AOV Prediction - FIXED Training Pipeline")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print("\nFixes Applied:")
    print("  ✓ Target Leakage: Using lead() for next_order_value")
    print("  ✓ Time-Based Split: Chronological train/test split")
    print("  ✓ Safe MAPE: Clamped denominator (min=$50)")
    print("  ✓ Feature Pruning: Reduced from 42 to ~35 features")
    print("  ✓ Rolling Window: Stable calculation for early sequences")
    print("  ✓ Discount Sensitivity: Weighted by spend")
    print("  ✓ Tree Scaling: No scaling for RF/GBT")
    print()
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    
    if None in [customers_df, orders_df, order_items_df, products_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    cust_valid, cust_missing, cust_null = validate_columns(
        customers_df, REQUIRED_CUSTOMER_COLUMNS, "Customers"
    )
    
    ord_valid, ord_missing, ord_null = validate_columns(
        orders_df, REQUIRED_ORDER_COLUMNS, "Orders"
    )
    
    if not (cust_valid and ord_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_enhanced_features(customers_df, orders_df, order_items_df, products_df)
    
    # Prepare data - SCALED version for Linear Regression
    print("\nStep 4a: Data Preparation (Scaled for Linear Regression)")
    print("-" * 60)
    result_scaled = prepare_training_data(df_features, scale_features=True)
    
    if result_scaled is None:
        print("\n✗ Training aborted: Insufficient data")
        spark.stop()
        return
    
    df_prepared_scaled, scaler_model, feature_list = result_scaled
    
    # Save scaler for inference
    if scaler_model is not None:
        scaler_model.write().overwrite().save(SCALER_OUTPUT_PATH)
        print(f"✓ Scaler saved: {SCALER_OUTPUT_PATH}")
    
    # Prepare data - UNSCALED version for Tree models
    print("\nStep 4b: Data Preparation (Unscaled for Tree Models)")
    print("-" * 60)
    result_unscaled = prepare_training_data(df_features, scale_features=False)
    df_prepared_unscaled, _, _ = result_unscaled
    
    print(f"\n{'='*60}")
    print(f"Final Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    # FIX #2: Time-based split
    print("\nStep 5: Time-Based Train/Test Split")
    print("-" * 60)
    
    train_df_scaled, test_df_scaled = time_based_split(df_prepared_scaled, TRAIN_SPLIT_RATIO)
    train_df_unscaled, test_df_unscaled = time_based_split(df_prepared_unscaled, TRAIN_SPLIT_RATIO)
    
    # Train models
    print("\nStep 6: Model Training")
    print("-" * 60)
    
    models_results = []
    
    # Linear Regression (uses scaled data)
    lr_model, lr_pred, lr_name = train_linear_regression(train_df_scaled, test_df_scaled, USE_CROSS_VALIDATION)
    lr_metrics = evaluate_model(lr_pred, lr_name)
    save_model(lr_model, lr_name)
    models_results.append(lr_metrics)
    
    # Random Forest (uses unscaled data)
    rf_model, rf_pred, rf_name = train_random_forest(train_df_unscaled, test_df_unscaled, USE_CROSS_VALIDATION)
    rf_metrics = evaluate_model(rf_pred, rf_name)
    save_model(rf_model, rf_name)
    models_results.append(rf_metrics)
    print_feature_importance(rf_model, feature_list, rf_name)
    
    # GBT (uses unscaled data)
    gbt_model, gbt_pred, gbt_name = train_gbt(train_df_unscaled, test_df_unscaled, USE_CROSS_VALIDATION)
    gbt_metrics = evaluate_model(gbt_pred, gbt_name)
    save_model(gbt_model, gbt_name)
    models_results.append(gbt_metrics)
    print_feature_importance(gbt_model, feature_list, gbt_name)
    
    # Model comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<20} {'RMSE':<12} {'MAE':<12} {'R²':<10} {'MAPE':<10} {'SMAPE':<10}")
    print("-" * 74)
    
    for m in models_results:
        print(f"{m['model']:<20} ${m['rmse']:<11.2f} ${m['mae']:<11.2f} {m['r2']:<10.4f} {m['mape']:<10.2f}% {m['smape']:<10.2f}%")
    
    best = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best['model']} (R² = {best['r2']:.4f})")
    print("="*60)
    
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Update MODEL_NAME in infer_aov.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()