"""
Safety Stock Level Prediction - Training Script
Calculates optimal safety stock levels to prevent stockouts while minimizing holding costs
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
import math

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
INPUT_INVENTORY_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
INPUT_SUPPLIERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/safety_stock/"
MIN_RECORDS_THRESHOLD = 100
MAX_NULL_PERCENTAGE = 95.0
MIN_DEMAND_DAYS = 30

# Service level targets and their Z-scores
SERVICE_LEVELS = {
    0.90: 1.28,  # 90% service level
    0.95: 1.65,  # 95% service level
    0.99: 2.33   # 99% service level
}

# Configuration
USE_CROSS_VALIDATION = False

# Required columns
REQUIRED_PRODUCT_COLUMNS = [
    "product_id", "category", "cost_price", "sell_price"
]

REQUIRED_INVENTORY_COLUMNS = [
    "product_id", "current_stock", "minimum_stock_level",
    "storage_cost_per_unit", "stockout_frequency"
]

# Feature set
NUMERIC_FEATURES = [
    # Demand patterns (calculated from orders)
    "avg_daily_demand",
    "demand_std_dev",
    "demand_volatility",
    "demand_coefficient_variation",
    "max_daily_demand",
    "demand_trend",
    "seasonal_index",
    
    # Lead time characteristics
    "lead_time_days",
    "lead_time_std_dev",
    "lead_time_reliability",
    "lead_time_demand",
    
    # Current inventory status
    "current_stock",
    "minimum_stock_level",
    "stock_coverage_days",
    "inventory_turnover_ratio",
    
    # Stockout history
    "stockout_frequency",
    "stockout_rate",
    "days_since_last_stockout",
    "avg_stockout_duration",
    
    # Economic factors
    "cost_price",
    "sell_price",
    "storage_cost_per_unit",
    "stockout_cost_per_unit",
    "holding_cost_per_day",
    "stockout_to_holding_ratio",
    
    # Service level (input)
    "service_level_target",
    "z_score",
    
    # Product characteristics
    "product_criticality_score",
    "demand_predictability_score",
    
    # Categorical (indexed)
    "category_idx",
    "stock_status_idx",
    "demand_pattern_idx"
]

TARGET_COLUMN = "required_safety_stock_units"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Safety_Stock_Training")
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
    """Calculate demand patterns from actual order history"""
    print("Calculating demand metrics from order history...")
    
    # Filter delivered orders only
    orders_delivered = orders_df.filter(F.col("order_status") == "Delivered")
    
    # Join with order items to get product-level demand
    demand_data = orders_delivered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    ).select(
        F.col("oi.product_id"),
        F.col("o.order_placed_at").cast("date").alias("order_date"),
        F.col("oi.quantity").cast("double").alias("quantity"),
        F.month(F.col("o.order_placed_at")).alias("order_month")
    )
    
    # Aggregate daily demand per product
    daily_demand = demand_data.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity"),
        F.first("order_month").alias("month")
    )
    
    # Calculate overall demand statistics
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
    ).withColumn(
        "demand_coefficient_variation",
        F.col("demand_volatility")
    )
    
    # Calculate demand trend
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    
    daily_with_seq = daily_demand.withColumn(
        "day_seq",
        F.row_number().over(window_spec)
    )
    
    trend_data = daily_with_seq.groupBy("product_id").agg(
        F.corr("day_seq", "daily_quantity").alias("demand_trend")
    )
    
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    
    # Calculate seasonal index (monthly variation)
    monthly_demand = demand_data.groupBy("product_id", "order_month").agg(
        F.sum("quantity").alias("monthly_quantity")
    )
    
    # Calculate average monthly demand per product
    avg_monthly = monthly_demand.groupBy("product_id").agg(
        F.avg("monthly_quantity").alias("avg_monthly_qty")
    )
    
    # Join back and calculate seasonal index
    monthly_with_avg = monthly_demand.join(avg_monthly, "product_id")
    monthly_with_avg = monthly_with_avg.withColumn(
        "month_index",
        F.col("monthly_quantity") / F.col("avg_monthly_qty")
    )
    
    # Get current month's seasonal index
    current_month = F.month(F.current_date())
    seasonal_data = monthly_with_avg.filter(
        F.col("order_month") == current_month
    ).select("product_id", F.col("month_index").alias("seasonal_index"))
    
    demand_stats = demand_stats.join(seasonal_data, "product_id", "left")
    
    # Fill nulls
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_coefficient_variation": 0,
        "demand_trend": 0,
        "seasonal_index": 1.0
    })
    
    # Filter products with sufficient demand history
    demand_stats = demand_stats.filter(
        (F.col("total_demand_days") >= MIN_DEMAND_DAYS) &
        (F.col("avg_daily_demand") > 0)
    )
    
    print(f"✓ Demand metrics calculated for {demand_stats.count()} products with {MIN_DEMAND_DAYS}+ days history")
    return demand_stats


def calculate_lead_time_variability(suppliers_df, orders_df):
    """
    Calculate lead time standard deviation from supplier historical performance
    For simplicity, estimate based on supplier reliability score
    """
    print("Calculating lead time variability...")
    
    # Create lead time std dev based on reliability
    # Lower reliability → higher variability
    suppliers_with_var = suppliers_df.withColumn(
        "lead_time_std_dev",
        F.when(
            F.col("avg_restock_lead_time").isNotNull(),
            # Estimate: std_dev = mean * (1 - reliability) * 0.3
            F.col("avg_restock_lead_time") * (1 - F.coalesce(F.col("supplier_reliability_score"), F.lit(0.8))) * 0.3
        ).otherwise(F.lit(2.0))  # Default 2 days std dev
    ).withColumn(
        "lead_time_reliability",
        F.coalesce(F.col("supplier_reliability_score"), F.lit(0.8))
    )
    
    print(f"✓ Lead time variability calculated for {suppliers_with_var.count()} suppliers")
    return suppliers_with_var


def classify_demand_pattern(demand_volatility):
    """
    Classify demand pattern based on volatility
    Low: CV < 0.3, Medium: 0.3 <= CV < 0.7, High: CV >= 0.7
    """
    if demand_volatility < 0.3:
        return "Stable"
    elif demand_volatility < 0.7:
        return "Variable"
    else:
        return "Erratic"


def create_safety_stock_features(products_df, inventory_df, suppliers_df, demand_stats_df):
    """
    Create comprehensive safety stock features with calculated target
    """
    print("Creating safety stock features...")
    
    # Join products with inventory - avoid column duplication
    product_inventory = products_df.select(
        "product_id",
        "category",
        F.col("cost_price").alias("product_cost_price"),
        "sell_price",
        "supplier_id",
        "total_revenue"
    ).join(
        inventory_df.select(
            "product_id",
            "current_stock",
            "minimum_stock_level",
            "storage_cost_per_unit",
            F.col("cost_price").alias("inventory_cost_price"),
            "stock_status",
            "inventory_turnover_ratio",
            "stockout_frequency",
            "days_since_restock"
        ),
        "product_id",
        "inner"
    )
    
    # Coalesce cost_price
    product_inventory = product_inventory.withColumn(
        "cost_price",
        F.coalesce(F.col("product_cost_price"), F.col("inventory_cost_price"), F.lit(0))
    ).drop("product_cost_price", "inventory_cost_price")
    
    # Join with suppliers (with lead time variability)
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days"),
            "lead_time_std_dev",
            "lead_time_reliability",
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
        "lead_time_days": 7,
        "lead_time_std_dev": 2.0,
        "lead_time_reliability": 0.8,
        "stockout_rate": 0.05,
        "storage_cost_per_unit": 0
    })
    
    # Calculate economic factors
    product_features = product_features.withColumn(
        "holding_cost_per_day",
        (F.col("cost_price") * 0.25) / 365
    ).withColumn(
        # Stockout cost = lost profit + reputation cost
        "stockout_cost_per_unit",
        (F.col("sell_price") - F.col("cost_price")) * 1.5  # 1.5x profit as penalty
    ).withColumn(
        "stockout_to_holding_ratio",
        F.when(
            F.col("holding_cost_per_day") > 0,
            F.col("stockout_cost_per_unit") / (F.col("holding_cost_per_day") * 365)
        ).otherwise(F.lit(10))  # Default ratio
    )
    
    # Calculate stock coverage days
    product_features = product_features.withColumn(
        "stock_coverage_days",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("current_stock") / F.col("avg_daily_demand")
        ).otherwise(F.lit(999))
    )
    
    # Calculate lead time demand
    product_features = product_features.withColumn(
        "lead_time_demand",
        F.col("avg_daily_demand") * F.col("lead_time_days")
    )
    
    # Calculate days since last stockout
    product_features = product_features.withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    )
    
    # Estimate average stockout duration (simplified)
    product_features = product_features.withColumn(
        "avg_stockout_duration",
        F.when(
            F.col("stockout_frequency") > 0,
            F.col("lead_time_days") * 0.5  # Assume half lead time on average
        ).otherwise(F.lit(0))
    )
    
    # Calculate product criticality score
    # Based on revenue, stockout frequency, and demand
    # Use approxQuantile to avoid single partition window operations
    revenue_quartiles = product_features.approxQuantile("total_revenue", [0.25, 0.5, 0.75], 0.01)
    stockout_quartiles = product_features.approxQuantile("stockout_frequency", [0.25, 0.5, 0.75], 0.01)
    
    product_features = product_features.withColumn(
        "revenue_score",
        F.when(F.col("total_revenue") >= revenue_quartiles[2], F.lit(100))
        .when(F.col("total_revenue") >= revenue_quartiles[1], F.lit(75))
        .when(F.col("total_revenue") >= revenue_quartiles[0], F.lit(50))
        .otherwise(F.lit(25))
    ).withColumn(
        "stockout_score",
        F.when(F.col("stockout_frequency") >= stockout_quartiles[2], F.lit(100))
        .when(F.col("stockout_frequency") >= stockout_quartiles[1], F.lit(75))
        .when(F.col("stockout_frequency") >= stockout_quartiles[0], F.lit(50))
        .otherwise(F.lit(25))
    ).withColumn(
        "product_criticality_score",
        (F.col("revenue_score") * 0.5 + F.col("stockout_score") * 0.5)
    ).drop("revenue_score", "stockout_score")
    
    # Calculate demand predictability score (inverse of volatility)
    product_features = product_features.withColumn(
        "demand_predictability_score",
        F.when(
            F.col("demand_volatility") < 0.3,
            F.lit(90)  # Stable demand
        ).when(
            F.col("demand_volatility") < 0.7,
            F.lit(60)  # Variable demand
        ).otherwise(F.lit(30))  # Erratic demand
    )
    
    # Classify demand pattern
    classify_udf = F.udf(classify_demand_pattern)
    product_features = product_features.withColumn(
        "demand_pattern",
        classify_udf(F.col("demand_volatility"))
    )
    
    # **CREATE TRAINING RECORDS FOR MULTIPLE SERVICE LEVELS**
    # Expand each product to 3 rows (one for each service level)
    service_level_data = []
    
    for service_level, z_score in SERVICE_LEVELS.items():
        sl_df = product_features.withColumn(
            "service_level_target",
            F.lit(service_level)
        ).withColumn(
            "z_score",
            F.lit(z_score)
        )
        service_level_data.append(sl_df)
    
    # Union all service levels
    expanded_features = service_level_data[0]
    for df in service_level_data[1:]:
        expanded_features = expanded_features.union(df)
    
    print(f"Expanded to {expanded_features.count()} records ({product_features.count()} products × {len(SERVICE_LEVELS)} service levels)")
    
    # **CALCULATE TARGET: Required Safety Stock**
    # Formula: safety_stock = Z × σ_demand × √(LT + LT_var²/LT)
    # Where: σ_demand = demand_std_dev, LT = lead_time_days, LT_var = lead_time_std_dev
    
    expanded_features = expanded_features.withColumn(
        "lead_time_variance_component",
        F.pow(F.col("lead_time_std_dev"), 2) / F.greatest(F.col("lead_time_days"), F.lit(1))
    ).withColumn(
        "effective_lead_time",
        F.col("lead_time_days") + F.col("lead_time_variance_component")
    ).withColumn(
        TARGET_COLUMN,
        F.col("z_score") * F.col("demand_std_dev") * F.sqrt(F.col("effective_lead_time"))
    )
    
    # Round to nearest integer
    expanded_features = expanded_features.withColumn(
        TARGET_COLUMN,
        F.round(F.col(TARGET_COLUMN), 0)
    )
    
    # Calculate additional targets
    expanded_features = expanded_features.withColumn(
        "minimum_stock_level",
        F.col("lead_time_demand") + F.col(TARGET_COLUMN)
    ).withColumn(
        "reorder_point",
        F.col("minimum_stock_level")
    )
    
    # Fill remaining nulls
    expanded_features = expanded_features.fillna({
        "inventory_turnover_ratio": 0,
        "stockout_frequency": 0,
        "total_revenue": 0,
        "stock_status": "Unknown",
        "demand_pattern": "Unknown"
    })
    
    print(f"✓ Safety stock features created: {expanded_features.count()} training records")
    return expanded_features


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
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
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
    
    demand_pattern_indexer = StringIndexer(
        inputCol="demand_pattern",
        outputCol="demand_pattern_idx",
        handleInvalid="keep"
    )
    
    df_indexed = category_indexer.fit(df_valid).transform(df_valid)
    df_indexed = stock_status_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = demand_pattern_indexer.fit(df_indexed).transform(df_indexed)
    
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
        "service_level_target",
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
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("Safety Stock Level Prediction - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print(f"Service Levels: {', '.join([f'{sl*100:.0f}%' for sl in SERVICE_LEVELS.keys()])}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    inventory_df, _ = validate_dataset(spark, INPUT_INVENTORY_PATH, "Inventory")
    suppliers_df, _ = validate_dataset(spark, INPUT_SUPPLIERS_PATH, "Suppliers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    
    if None in [products_df, inventory_df, suppliers_df, orders_df, order_items_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    prod_valid, _, _ = validate_columns(products_df, REQUIRED_PRODUCT_COLUMNS, "Products")
    inv_valid, _, _ = validate_columns(inventory_df, REQUIRED_INVENTORY_COLUMNS, "Inventory")
    
    if not (prod_valid and inv_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Calculate demand metrics
    print("\nStep 3: Calculate Demand Metrics from Order History")
    print("-" * 60)
    demand_stats = calculate_demand_metrics(orders_df, order_items_df)
    
    # Calculate lead time variability
    print("\nStep 4: Calculate Lead Time Variability")
    print("-" * 60)
    suppliers_with_var = calculate_lead_time_variability(suppliers_df, orders_df)
    
    # Create features
    print("\nStep 5: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_safety_stock_features(products_df, inventory_df, suppliers_with_var, demand_stats)
    
    # Prepare data
    print("\nStep 6: Data Preparation")
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
    print("\nStep 7: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train models
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
    print("   Update MODEL_NAME in predict_safety_stock.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
