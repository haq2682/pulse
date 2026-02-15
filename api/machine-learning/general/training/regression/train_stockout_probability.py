"""
Stockout Probability Prediction - Training Script
Predicts probability and timing of stockouts for proactive inventory management

Target Calculation:
1. days_until_stockout = (current_stock + pending_orders) / projected_daily_demand
2. stockout_probability = based on historical stockout patterns + current trajectory
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
from datetime import datetime, timedelta
import math

# Load environment variables
load_dotenv()

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "stockout_probability"
INPUT_RELATIVE_PATH = "transformed/agg_products.parquet"
INPUT_INVENTORY_RELATIVE_PATH = "transformed/agg_product_inventory_health.parquet"
INPUT_SUPPLIERS_RELATIVE_PATH = "transformed/agg_suppliers.parquet"
INPUT_ORDERS_RELATIVE_PATH = "transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_RELATIVE_PATH = "transformed/agg_order_items.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("regression", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
MAX_NULL_PERCENTAGE = 95.0
MIN_DEMAND_DAYS = 30

# Configuration
USE_CROSS_VALIDATION = False
CRITICAL_DAYS_THRESHOLD = 3  # Critical if < 3 days of stock
HIGH_RISK_THRESHOLD = 7       # High risk if < 7 days
MEDIUM_RISK_THRESHOLD = 14    # Medium risk if < 14 days

# Required columns
REQUIRED_PRODUCT_COLUMNS = ["product_id", "category"]
REQUIRED_INVENTORY_COLUMNS = ["product_id", "current_stock", "stockout_frequency"]

# Feature set
NUMERIC_FEATURES = [
    # Current inventory status
    "current_stock",
    "available_stock",
    "reserved_quantity",
    "stock_utilization_rate",
    
    # Demand patterns (calculated from orders)
    "avg_daily_demand",
    "demand_std_dev",
    "demand_volatility",
    "demand_trend",  # Increasing/decreasing
    "demand_acceleration",  # Rate of change
    "max_daily_demand",
    "recent_7day_avg_demand",
    "recent_30day_avg_demand",
    
    # Supply chain metrics
    "lead_time_days",
    "pending_orders_quantity",
    "days_until_next_delivery",
    
    # Calculated stock metrics
    "current_days_of_supply",
    "projected_days_of_supply",  # With pending orders
    "safety_stock_coverage",
    "reorder_point_breach",  # Boolean: below reorder point?
    
    # Historical stockout patterns
    "stockout_frequency",
    "historical_stockout_rate",
    "days_since_last_stockout",
    "avg_stockout_duration",
    
    # Seasonal and promotional factors
    "seasonal_demand_multiplier",
    "promotion_impact_factor",
    "day_of_week_factor",
    "month_of_year_factor",
    
    # Product characteristics
    "inventory_turnover_ratio",
    "criticality_score",  # Based on revenue/popularity
    
    # Categorical (indexed)
    "category_idx",
    "stock_status_idx"
]

# We'll train TWO models:
# 1. Regression for days_until_stockout
# 2. Regression for stockout_probability (0-1)
TARGET_COLUMN_DAYS = "days_until_stockout"
TARGET_COLUMN_PROB = "stockout_probability"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Stockout_Probability_Training")
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


def calculate_demand_metrics_with_trends(orders_df, order_items_df):
    """
    Calculate comprehensive demand metrics including trends and acceleration
    """
    print("Calculating demand metrics with trends from order history...")
    
    # Filter delivered orders
    orders_delivered = orders_df.filter(F.col("order_status") == "Delivered")
    
    # Join with order items
    demand_data = orders_delivered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    ).select(
        F.col("oi.product_id"),
        F.col("o.order_placed_at").cast("date").alias("order_date"),
        F.col("oi.quantity").cast("double").alias("quantity")
    )
    
    # Aggregate daily demand
    daily_demand = demand_data.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity")
    )
    
    # Calculate basic statistics
    demand_stats = daily_demand.groupBy("product_id").agg(
        F.count("order_date").alias("days_with_demand"),
        F.avg("daily_quantity").alias("avg_daily_demand"),
        F.stddev("daily_quantity").alias("demand_std_dev"),
        F.max("daily_quantity").alias("max_daily_demand"),
        F.min("order_date").alias("first_demand_date"),
        F.max("order_date").alias("last_demand_date")
    )
    
    # Calculate total days
    demand_stats = demand_stats.withColumn(
        "total_demand_days",
        F.datediff(F.col("last_demand_date"), F.col("first_demand_date")) + 1
    )
    
    # Calculate demand volatility
    demand_stats = demand_stats.withColumn(
        "demand_volatility",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("demand_std_dev") / F.col("avg_daily_demand")
        ).otherwise(0)
    )
    
    # Calculate recent demand (7 and 30 days)
    window_7days = Window.partitionBy("product_id").orderBy(F.desc("order_date")).rowsBetween(0, 6)
    window_30days = Window.partitionBy("product_id").orderBy(F.desc("order_date")).rowsBetween(0, 29)
    
    recent_demand = daily_demand.withColumn(
        "recent_7day_sum",
        F.sum("daily_quantity").over(window_7days)
    ).withColumn(
        "recent_30day_sum",
        F.sum("daily_quantity").over(window_30days)
    ).withColumn(
        "recent_7day_count",
        F.count("order_date").over(window_7days)
    ).withColumn(
        "recent_30day_count",
        F.count("order_date").over(window_30days)
    )
    
    # Get latest values per product
    window_latest = Window.partitionBy("product_id").orderBy(F.desc("order_date"))
    recent_stats = recent_demand.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(
        F.col("row_num") == 1
    ).select(
        "product_id",
        (F.col("recent_7day_sum") / F.greatest(F.col("recent_7day_count"), F.lit(1))).alias("recent_7day_avg_demand"),
        (F.col("recent_30day_sum") / F.greatest(F.col("recent_30day_count"), F.lit(1))).alias("recent_30day_avg_demand")
    )
    
    # Calculate trend (correlation between day_seq and quantity)
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    daily_with_seq = daily_demand.withColumn(
        "day_seq",
        F.row_number().over(window_spec)
    )
    
    trend_data = daily_with_seq.groupBy("product_id").agg(
        F.corr("day_seq", "daily_quantity").alias("demand_trend")
    )
    
    # Calculate acceleration (is trend accelerating?)
    # Split data into first half and second half, compare trends
    daily_with_seq = daily_with_seq.withColumn(
        "total_days",
        F.count("order_date").over(Window.partitionBy("product_id"))
    ).withColumn(
        "is_first_half",
        F.col("day_seq") <= (F.col("total_days") / 2)
    )
    
    trend_first_half = daily_with_seq.filter(F.col("is_first_half")).groupBy("product_id").agg(
        F.avg("daily_quantity").alias("avg_first_half")
    )
    
    trend_second_half = daily_with_seq.filter(~F.col("is_first_half")).groupBy("product_id").agg(
        F.avg("daily_quantity").alias("avg_second_half")
    )
    
    acceleration_data = trend_first_half.join(trend_second_half, "product_id", "inner").withColumn(
        "demand_acceleration",
        F.when(
            F.col("avg_first_half") > 0,
            (F.col("avg_second_half") - F.col("avg_first_half")) / F.col("avg_first_half")
        ).otherwise(0)
    ).select("product_id", "demand_acceleration")
    
    # Join all metrics
    demand_stats = demand_stats.join(recent_stats, "product_id", "left")
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    demand_stats = demand_stats.join(acceleration_data, "product_id", "left")
    
    # Fill nulls
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_trend": 0,
        "demand_acceleration": 0,
        "recent_7day_avg_demand": 0,
        "recent_30day_avg_demand": 0
    })
    
    # Filter sufficient history
    demand_stats = demand_stats.filter(
        (F.col("total_demand_days") >= MIN_DEMAND_DAYS) &
        (F.col("avg_daily_demand") > 0)
    )
    
    print(f"✓ Demand metrics with trends for {demand_stats.count()} products")
    return demand_stats


def create_stockout_prediction_features(products_df, inventory_df, suppliers_df, demand_stats_df):
    """
    Create comprehensive stockout prediction features with calculated targets
    """
    print("Creating stockout prediction features...")
    
    # Join products with inventory - avoid column duplication
    # Include supplier_id from products table
    product_inventory = products_df.select(
        "product_id",
        "category",
        "supplier_id",  # Include for supplier join
        F.col("total_revenue").alias("product_revenue")
    ).join(
        inventory_df.select(
            "product_id",
            "current_stock",
            "available_stock",
            "minimum_stock_level",
            "stock_status",
            "stockout_frequency",
            "inventory_turnover_ratio",
            "days_since_restock"
        ),
        "product_id",
        "inner"
    )
    
    # Join with suppliers - handle null supplier_ids
    # Some products may not have supplier assigned
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days")
        ),
        "supplier_id",  # Join on supplier_id column
        "left"  # Left join to keep products without suppliers
    )
    
    # Join with demand metrics
    product_features = product_inventory.join(
        demand_stats_df,
        "product_id",
        "inner"
    )
    
    print(f"After joining: {product_features.count()} products")
    
    # Fill nulls
    product_features = product_features.fillna({
        "lead_time_days": 7,
        "available_stock": 0,
        "minimum_stock_level": 0,
        "stockout_frequency": 0,
        "inventory_turnover_ratio": 0,
        "days_since_restock": 0
    })
    
    # Calculate reserved quantity (difference between current and available)
    product_features = product_features.withColumn(
        "reserved_quantity",
        F.greatest(F.col("current_stock") - F.col("available_stock"), F.lit(0))
    )
    
    # Calculate stock utilization rate
    product_features = product_features.withColumn(
        "stock_utilization_rate",
        F.when(
            F.col("current_stock") > 0,
            F.col("reserved_quantity") / F.col("current_stock")
        ).otherwise(0)
    )
    
    # Estimate pending orders quantity (simplified - assume one order in transit)
    # In reality, this would come from purchase orders table
    product_features = product_features.withColumn(
        "pending_orders_quantity",
        F.when(
            F.col("current_stock") < F.col("minimum_stock_level"),
            F.col("avg_daily_demand") * F.col("lead_time_days") * 1.5
        ).otherwise(0)
    )
    
    # Calculate days until next delivery (simplified)
    product_features = product_features.withColumn(
        "days_until_next_delivery",
        F.when(
            F.col("pending_orders_quantity") > 0,
            F.col("lead_time_days") * 0.5  # Assume halfway through lead time
        ).otherwise(999)
    )
    
    # Calculate historical stockout rate
    product_features = product_features.withColumn(
        "historical_stockout_rate",
        F.when(
            F.col("total_demand_days") > 0,
            F.col("stockout_frequency") / F.col("total_demand_days")
        ).otherwise(0)
    )
    
    # Calculate days since last stockout
    product_features = product_features.withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    )
    
    # Estimate average stockout duration (simplified - based on lead time)
    product_features = product_features.withColumn(
        "avg_stockout_duration",
        F.col("lead_time_days") * 0.7
    )
    
    # Calculate seasonal demand multiplier (simplified - month-based)
    product_features = product_features.withColumn(
        "month_of_year_factor",
        F.month(F.current_date())
    ).withColumn(
        "day_of_week_factor",
        F.dayofweek(F.current_date())
    ).withColumn(
        "seasonal_demand_multiplier",
        F.lit(1.0)  # Would be enhanced with actual seasonality analysis
    )
    
    # Estimate promotion impact factor (simplified)
    product_features = product_features.withColumn(
        "promotion_impact_factor",
        F.lit(1.0)  # Would be 1.2-1.5 if promotion planned
    )
    
    # Calculate projected daily demand (with seasonal & promotion factors)
    product_features = product_features.withColumn(
        "projected_daily_demand",
        F.col("recent_7day_avg_demand") * 
        F.col("seasonal_demand_multiplier") * 
        F.col("promotion_impact_factor")
    )
    
    # **CALCULATE TARGET 1: Days Until Stockout**
    # Formula: (current_stock + pending_orders) / projected_daily_demand
    product_features = product_features.withColumn(
        TARGET_COLUMN_DAYS,
        F.when(
            F.col("projected_daily_demand") > 0,
            (F.col("available_stock") + 
             (F.col("pending_orders_quantity") * 
              F.when(F.col("days_until_next_delivery") < 30, 1.0).otherwise(0))
            ) / F.col("projected_daily_demand")
        ).otherwise(999)  # Cap at 999 days
    )
    
    # Cap at reasonable maximum
    product_features = product_features.withColumn(
        TARGET_COLUMN_DAYS,
        F.least(F.col(TARGET_COLUMN_DAYS), F.lit(999.0))
    )
    
    # Calculate current days of supply
    product_features = product_features.withColumn(
        "current_days_of_supply",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("available_stock") / F.col("avg_daily_demand")
        ).otherwise(999)
    )
    
    # Calculate projected days of supply (with pending orders)
    product_features = product_features.withColumn(
        "projected_days_of_supply",
        F.when(
            F.col("avg_daily_demand") > 0,
            (F.col("available_stock") + F.col("pending_orders_quantity")) / F.col("avg_daily_demand")
        ).otherwise(999)
    )
    
    # Calculate safety stock coverage
    product_features = product_features.withColumn(
        "safety_stock_coverage",
        F.when(
            F.col("minimum_stock_level") > 0,
            F.col("current_stock") / F.col("minimum_stock_level")
        ).otherwise(1.0)
    )
    
    # Check if below reorder point
    product_features = product_features.withColumn(
        "reorder_point_breach",
        F.when(
            F.col("current_stock") < F.col("minimum_stock_level"),
            1.0
        ).otherwise(0.0)
    )
    
    # **CALCULATE TARGET 2: Stockout Probability**
    # Based on multiple factors:
    # 1. Days until stockout (closer = higher probability)
    # 2. Historical stockout rate
    # 3. Demand volatility
    # 4. Whether below reorder point
    
    product_features = product_features.withColumn(
        "base_probability",
        F.when(
            F.col(TARGET_COLUMN_DAYS) <= CRITICAL_DAYS_THRESHOLD,
            F.lit(0.9)  # 90% if critical
        ).when(
            F.col(TARGET_COLUMN_DAYS) <= HIGH_RISK_THRESHOLD,
            F.lit(0.6)  # 60% if high risk
        ).when(
            F.col(TARGET_COLUMN_DAYS) <= MEDIUM_RISK_THRESHOLD,
            F.lit(0.3)  # 30% if medium risk
        ).otherwise(
            F.lit(0.05)  # 5% if low risk
        )
    ).withColumn(
        "historical_adjustment",
        F.col("historical_stockout_rate") * 0.5  # Adjust up to 50% based on history
    ).withColumn(
        "volatility_adjustment",
        F.col("demand_volatility") * 0.2  # Adjust up to 20% for volatility
    ).withColumn(
        TARGET_COLUMN_PROB,
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                F.col("base_probability") + 
                F.col("historical_adjustment") + 
                F.col("volatility_adjustment")
            )
        )
    )
    
    # Calculate criticality score (based on revenue)
    max_revenue = product_features.agg(F.max("product_revenue")).collect()[0][0] or 1
    product_features = product_features.withColumn(
        "criticality_score",
        F.col("product_revenue") / F.lit(max_revenue)
    )
    
    # Fill remaining nulls
    product_features = product_features.fillna({
        "stock_status": "Unknown"
    })
    
    print(f"✓ Stockout prediction features created: {product_features.count()} products")
    return product_features


def prepare_training_data(df, target_column):
    """Prepare data with encoding and scaling for specific target"""
    print(f"Preparing training data for target: {target_column}...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(target_column).isNotNull()) &
        (F.col("avg_daily_demand") > 0)
    )
    
    if target_column == TARGET_COLUMN_PROB:
        # For probability, ensure 0-1 range
        df_valid = df_valid.filter(
            (F.col(target_column) >= 0) &
            (F.col(target_column) <= 1)
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
        F.col(target_column).alias("label")
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model, existing_features


def train_random_forest(train_df, test_df, target_name, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print(f"Training Random Forest for {target_name}")
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
    return model, predictions


def evaluate_model(predictions, model_name, target_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name} for {target_name}...")
    
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
    
    metrics = {
        "model": model_name,
        "target": target_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape
    }
    
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
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
    print("Stockout Probability - General Model Training")
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
    
    # Step 4: Calculate demand metrics with trends
    print("\nStep 4: Calculate Demand Metrics with Trends")
    print("-" * 60)
    demand_stats = calculate_demand_metrics_with_trends(orders_df, order_items_df)
    
    # Step 5: Create features
    print("\nStep 5: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_stockout_prediction_features(
        products_df, inventory_df, suppliers_df, demand_stats
    )
    
    # Step 6: Train model for DAYS until stockout
    print("\n" + "="*60)
    print("Step 6: TRAINING - Days Until Stockout Model")
    print("="*60)
    
    result_days = prepare_training_data(df_features, TARGET_COLUMN_DAYS)
    
    if result_days is None:
        print("⚠️  Training skipped due to insufficient data for days model")
        spark.stop()
        return
    
    df_prepared_days, scaler_days, feature_list = result_days
    
    print(f"\n{'='*60}")
    print(f"Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    train_days, test_days = df_prepared_days.randomSplit([0.8, 0.2], seed=42)
    print(f"\nTraining set: {train_days.count()} records")
    print(f"Test set: {test_days.count()} records")
    
    model_days, pred_days = train_random_forest(
        train_days, test_days, "days_until_stockout", USE_CROSS_VALIDATION
    )
    metrics_days = evaluate_model(pred_days, "random_forest", "days_until_stockout")
    save_model(model_days, "days_until_stockout")
    
    # Step 7: Train model for PROBABILITY
    print("\n" + "="*60)
    print("Step 7: TRAINING - Stockout Probability Model")
    print("="*60)
    
    result_prob = prepare_training_data(df_features, TARGET_COLUMN_PROB)
    
    if result_prob is None:
        print("⚠️  Training skipped due to insufficient data for probability model")
        spark.stop()
        return
    
    df_prepared_prob, scaler_prob, _ = result_prob
    
    train_prob, test_prob = df_prepared_prob.randomSplit([0.8, 0.2], seed=42)
    print(f"\nTraining set: {train_prob.count()} records")
    print(f"Test set: {test_prob.count()} records")
    
    model_prob, pred_prob = train_random_forest(
        train_prob, test_prob, "stockout_probability", USE_CROSS_VALIDATION
    )
    metrics_prob = evaluate_model(pred_prob, "random_forest", "stockout_probability")
    save_model(model_prob, "stockout_probability")
    
    # Summary
    print("\n" + "="*60)
    print("Training Summary")
    print("="*60)
    print(f"\nDays Until Stockout Model:")
    print(f"  R²: {metrics_days['r2']:.4f}")
    print(f"  RMSE: {metrics_days['rmse']:.2f} days")
    print(f"  MAE: {metrics_days['mae']:.2f} days")
    
    print(f"\nStockout Probability Model:")
    print(f"  R²: {metrics_prob['r2']:.4f}")
    print(f"  RMSE: {metrics_prob['rmse']:.4f}")
    print(f"  MAE: {metrics_prob['mae']:.4f}")
    
    print(f"\n✓ Training completed")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()