"""
Inventory Restock Quantity Prediction - Training Script
Predicts optimal restock quantity using demand patterns and inventory optimization
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
import math

# Load environment variables
load_dotenv()

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "restock_quantity"
INPUT_RELATIVE_PATH = "transformed/agg_products.parquet"
INPUT_INVENTORY_RELATIVE_PATH = "transformed/agg_product_inventory_health.parquet"
INPUT_SUPPLIERS_RELATIVE_PATH = "transformed/agg_suppliers.parquet"
INPUT_ORDERS_RELATIVE_PATH = "transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_RELATIVE_PATH = "transformed/agg_order_items.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("regression", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
MAX_NULL_PERCENTAGE = 95.0
MIN_DEMAND_DAYS = 30  # Need at least 30 days of demand history

# Configuration
USE_CROSS_VALIDATION = False
Z_SCORE_SAFETY_STOCK = 1.65  # 95% service level

# Required columns
REQUIRED_PRODUCT_COLUMNS = [
    "product_id", "category", "cost_price", "sell_price", "current_stock"
]

REQUIRED_INVENTORY_COLUMNS = [
    "product_id", "current_stock", "minimum_stock_level",
    "storage_cost_per_unit", "cost_price"
]

# Feature set
NUMERIC_FEATURES = [
    # Current inventory status
    "current_stock",
    "minimum_stock_level",
    "available_stock",
    "stock_coverage_days",
    
    # Demand patterns (calculated from orders)
    "avg_daily_demand",
    "demand_std_dev",
    "demand_volatility",
    "max_daily_demand",
    "demand_trend",
    "days_with_demand",
    
    # Product economics
    "cost_price",
    "sell_price",
    "profit_margin",
    "storage_cost_per_unit",
    "holding_cost_per_day",
    
    # Supplier metrics
    "lead_time_days",
    "lead_time_demand",
    "supplier_reliability_score",
    
    # Stockout risk
    "stockout_frequency",
    "stockout_rate",
    "days_since_last_stockout",
    
    # Calculated safety metrics
    "safety_stock_calculated",
    "reorder_point_calculated",
    
    # Seasonality
    "seasonal_demand_factor",
    
    # Product performance
    "inventory_turnover_ratio",
    "total_revenue",
    
    # Categorical (indexed)
    "category_idx",
    "stock_status_idx"
]

TARGET_COLUMN = "optimal_restock_quantity"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Restock_Quantity_Training")
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


def calculate_demand_metrics(orders_df, order_items_df):
    """
    Calculate demand patterns from actual order history
    """
    print("Calculating demand metrics from order history...")
    
    # Filter delivered orders only (confirmed demand)
    orders_delivered = orders_df.filter(F.col("order_status") == "Delivered")
    
    # Join with order items to get product-level demand
    demand_data = orders_delivered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    ).select(
        F.col("oi.product_id"),
        F.col("o.order_placed_at").cast("date").alias("order_date"),
        F.col("oi.quantity").cast("double").alias("quantity")
    )
    
    # Aggregate daily demand per product
    daily_demand = demand_data.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity")
    )
    
    # Calculate demand statistics per product
    demand_stats = daily_demand.groupBy("product_id").agg(
        F.count("order_date").alias("days_with_demand"),
        F.avg("daily_quantity").alias("avg_daily_demand"),
        F.stddev("daily_quantity").alias("demand_std_dev"),
        F.max("daily_quantity").alias("max_daily_demand"),
        F.min("order_date").alias("first_demand_date"),
        F.max("order_date").alias("last_demand_date")
    )
    
    # Calculate total days in demand period
    demand_stats = demand_stats.withColumn(
        "total_demand_days",
        F.datediff(F.col("last_demand_date"), F.col("first_demand_date")) + 1
    )
    
    # Calculate demand volatility (coefficient of variation)
    demand_stats = demand_stats.withColumn(
        "demand_volatility",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("demand_std_dev") / F.col("avg_daily_demand")
        ).otherwise(0)
    )
    
    # Calculate demand trend (simple linear trend)
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    
    daily_with_seq = daily_demand.withColumn(
        "day_seq",
        F.row_number().over(window_spec)
    )
    
    # Calculate correlation between day_seq and quantity (trend)
    trend_data = daily_with_seq.groupBy("product_id").agg(
        F.corr("day_seq", "daily_quantity").alias("demand_trend")
    )
    
    # Join trend back to demand_stats
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    
    # Fill nulls
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_trend": 0
    })
    
    # Filter products with sufficient demand history
    demand_stats = demand_stats.filter(
        (F.col("total_demand_days") >= MIN_DEMAND_DAYS) &
        (F.col("avg_daily_demand") > 0)
    )
    
    print(f"✓ Demand metrics calculated for {demand_stats.count()} products with {MIN_DEMAND_DAYS}+ days history")
    return demand_stats


def create_restock_features(products_df, inventory_df, suppliers_df, demand_stats_df):
    """
    Create comprehensive restock features with calculated target
    """
    print("Creating restock features...")
    
    # Join products with inventory - SELECT SPECIFIC COLUMNS to avoid duplicates
    product_inventory = products_df.select(
        "product_id",
        "category",
        F.col("cost_price").alias("product_cost_price"),  # Rename to avoid conflict
        "sell_price",
        "supplier_id",
        "total_revenue"
    ).join(
        inventory_df.select(
            "product_id",
            "current_stock",
            "available_stock",
            "minimum_stock_level",
            "storage_cost_per_unit",
            F.col("cost_price").alias("inventory_cost_price"),  # Rename to avoid conflict
            "stock_status",
            "days_of_supply",
            "inventory_turnover_ratio",
            "stockout_frequency",
            "days_since_restock"
        ),
        "product_id",
        "inner"
    )
    
    # Use product_cost_price as primary, fallback to inventory_cost_price
    product_inventory = product_inventory.withColumn(
        "cost_price",
        F.coalesce(F.col("product_cost_price"), F.col("inventory_cost_price"), F.lit(0))
    ).drop("product_cost_price", "inventory_cost_price")
    
    # Join with suppliers to get lead time
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days"),
            "supplier_reliability_score",
            "stockout_rate"
        ),
        "supplier_id",
        "left"
    )
    
    # Join with demand metrics
    product_features = product_inventory.join(
        demand_stats_df,
        "product_id",
        "inner"
    )
    
    print(f"After joining: {product_features.count()} products")
    
    # Fill nulls in supplier metrics
    product_features = product_features.fillna({
        "lead_time_days": 7,  # Default 7 days lead time
        "supplier_reliability_score": 0.8,
        "stockout_rate": 0.05
    })
    
    # Calculate holding cost per day
    product_features = product_features.withColumn(
        "holding_cost_per_day",
        (F.col("cost_price") * 0.25) / 365  # 25% annual holding cost
    )
    
    # Calculate profit margin
    product_features = product_features.withColumn(
        "profit_margin",
        F.when(
            F.col("sell_price") > 0,
            ((F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")) * 100
        ).otherwise(0)
    )
    
    # Calculate stock coverage days (how long current stock will last)
    product_features = product_features.withColumn(
        "stock_coverage_days",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("current_stock") / F.col("avg_daily_demand")
        ).otherwise(999)
    )
    
    # Calculate lead time demand
    product_features = product_features.withColumn(
        "lead_time_demand",
        F.col("avg_daily_demand") * F.col("lead_time_days")
    )
    
    # Calculate safety stock (Z-score * std_dev * sqrt(lead_time))
    product_features = product_features.withColumn(
        "safety_stock_calculated",
        Z_SCORE_SAFETY_STOCK * F.col("demand_std_dev") * F.sqrt(F.col("lead_time_days"))
    )
    
    # Calculate reorder point
    product_features = product_features.withColumn(
        "reorder_point_calculated",
        F.col("lead_time_demand") + F.col("safety_stock_calculated")
    )
    
    # Calculate seasonal demand factor (month-based)
    product_features = product_features.withColumn(
        "seasonal_demand_factor",
        F.lit(1.0)  # Simplified; could be enhanced with actual seasonality
    )
    
    # Calculate days since last stockout
    product_features = product_features.withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    )
    
    # **CALCULATE TARGET: Optimal Restock Quantity**
    # Formula: Max(0, (reorder_point + EOQ/2) - current_stock)
    # Where EOQ = sqrt((2 * annual_demand * ordering_cost) / holding_cost)
    
    # Assume ordering cost = $50 per order
    ORDERING_COST = 50
    
    product_features = product_features.withColumn(
        "annual_demand",
        F.col("avg_daily_demand") * 365
    ).withColumn(
        "annual_holding_cost",
        F.col("cost_price") * 0.25  # 25% of cost
    ).withColumn(
        "eoq",
        F.sqrt(
            (2 * F.col("annual_demand") * ORDERING_COST) / 
            F.greatest(F.col("annual_holding_cost"), F.lit(0.01))
        )
    )
    
    # Optimal restock quantity considering current stock
    product_features = product_features.withColumn(
        TARGET_COLUMN,
        F.greatest(
            F.lit(0),
            (F.col("reorder_point_calculated") + (F.col("eoq") / 2)) - F.col("current_stock")
        )
    )
    
    # Round to nearest integer
    product_features = product_features.withColumn(
        TARGET_COLUMN,
        F.round(F.col(TARGET_COLUMN), 0)
    )
    
    # Fill remaining nulls
    product_features = product_features.fillna({
        "available_stock": 0,
        "minimum_stock_level": 0,
        "storage_cost_per_unit": 0,
        "inventory_turnover_ratio": 0,
        "stockout_frequency": 0,
        "total_revenue": 0,
        "stock_status": "Unknown"
    })
    
    print(f"✓ Restock features created: {product_features.count()} products")
    return product_features


def prepare_training_data(df):
    """Prepare data with encoding and scaling"""
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) >= 0) &
        (F.col("avg_daily_demand") > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS}")
        return None
    
    # Encode categorical features
    category_indexer = StringIndexer(
        inputCol="category",
        outputCol="category_idx",
        handleInvalid="keep"
    )
    
    stock_status_indexer = StringIndexer(
        inputCol="stock_status",
        outputCol="stock_status_idx",
        handleInvalid="keep"
    )
    
    df_indexed = category_indexer.fit(df_valid).transform(df_valid)
    df_indexed = stock_status_indexer.fit(df_indexed).transform(df_indexed)
    
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
    
    # Select final columns
    df_prepared = df_scaled.select(
        "product_id",
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
    
    mape_df = predictions.filter(F.col(TARGET_COLUMN) > 0).withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0] if mape_df.count() > 0 else 0
    
    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    
    print(f"  RMSE: {rmse:.2f} units")
    print(f"  MAE: {mae:.2f} units")
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
    print("Inventory Restock Quantity - General Model Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print(f"Safety Stock Z-Score: {Z_SCORE_SAFETY_STOCK} (95% service level)")
    print("="*60 + "\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Load datasets from all buckets
    print("Step 1: Loading data from all MinIO buckets...")
    print("-" * 60)
    
    products_df, prod_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=REQUIRED_PRODUCT_COLUMNS,
        filter_nulls=True
    )
    
    if products_df is None:
        print("⚠️  No products data available. Skipping training.")
        spark.stop()
        return
    
    inventory_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_INVENTORY_RELATIVE_PATH,
        required_columns=REQUIRED_INVENTORY_COLUMNS,
        filter_nulls=True
    )
    
    if inventory_df is None:
        print("⚠️  No inventory data available. Skipping training.")
        spark.stop()
        return
    
    suppliers_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_SUPPLIERS_RELATIVE_PATH,
        required_columns=["supplier_id"],
        filter_nulls=True
    )
    
    if suppliers_df is None:
        print("⚠️  No suppliers data available. Skipping training.")
        spark.stop()
        return
    
    orders_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_ORDERS_RELATIVE_PATH,
        required_columns=["order_id", "order_status"],
        filter_nulls=True
    )
    
    if orders_df is None:
        print("⚠️  No orders data available. Skipping training.")
        spark.stop()
        return
    
    order_items_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_ORDER_ITEMS_RELATIVE_PATH,
        required_columns=["order_id", "product_id", "quantity"],
        filter_nulls=True
    )
    
    if order_items_df is None:
        print("⚠️  No order items data available. Skipping training.")
        spark.stop()
        return
    
    # Step 2: Validate training data window
    print("\nStep 2: Validate Training Data Window")
    print("-" * 60)
    is_valid, products_df = validate_training_data(
        products_df, prod_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        spark.stop()
        return
    
    # Step 3: Column validation
    print("\nStep 3: Column Validation")
    print("-" * 60)
    
    prod_valid, _, _ = validate_columns(products_df, REQUIRED_PRODUCT_COLUMNS, "Products")
    inv_valid, _, _ = validate_columns(inventory_df, REQUIRED_INVENTORY_COLUMNS, "Inventory")
    
    if not (prod_valid and inv_valid):
        print("⚠️  Training skipped due to required columns missing or entirely null")
        spark.stop()
        return
    
    # Step 4: Calculate demand metrics
    print("\nStep 4: Calculate Demand Metrics from Order History")
    print("-" * 60)
    demand_stats = calculate_demand_metrics(orders_df, order_items_df)
    
    # Step 5: Create features
    print("\nStep 5: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_restock_features(products_df, inventory_df, suppliers_df, demand_stats)
    
    # Step 6: Prepare data
    print("\nStep 6: Data Preparation")
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
    
    # Step 7: Split data
    print("\nStep 7: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Step 8: Train models
    print("\nStep 8: Model Training")
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
        print(f"{m['model']:<25} {m['rmse']:<15.2f} {m['mae']:<15.2f} {m['r2']:<10.4f} {m['mape']:<10.2f}%")
    
    best = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best['model']} (R² = {best['r2']:.4f})")
    print("="*60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Update MODEL_NAME in predict_restock_quantity.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()