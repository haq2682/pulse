"""
Product Price Optimization - Training Script
Predicts optimal product price for maximizing revenue

Target Calculation:
- Analyzes historical price-volume relationships
- Calculates revenue at different price points
- Target = price that historically maximized revenue per period
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

# Required columns
REQUIRED_PRODUCT_COLUMNS = ["product_id", "category", "cost_price", "sell_price"]
REQUIRED_ORDER_ITEM_COLUMNS = ["product_id", "quantity", "product_price"]

# Feature set
NUMERIC_FEATURES = [
    # Current pricing
    "current_price",
    "cost_price",
    "current_profit_margin",
    "price_to_cost_ratio",
    
    # Historical performance
    "total_units_sold",
    "total_revenue",
    "avg_units_per_order",
    "total_orders_count",
    
    # Price elasticity metrics
    "price_elasticity",
    "elasticity_category_avg",
    "elasticity_brand_avg",
    "demand_sensitivity",
    
    # Product quality indicators
    "avg_rating",
    "total_reviews",
    "review_sentiment_score",
    
    # Historical price range
    "historical_min_price",
    "historical_max_price",
    "historical_avg_price",
    "price_variance",
    "current_vs_historical_avg",
    
    # Revenue patterns at different price points
    "revenue_at_low_price",
    "revenue_at_medium_price",
    "revenue_at_high_price",
    "units_at_low_price",
    "units_at_medium_price",
    "units_at_high_price",
    
    # Category/brand context
    "category_avg_price",
    "category_price_position",
    "brand_avg_price",
    "brand_premium_factor",
    
    # Categorical (indexed)
    "category_idx",
    "brand_idx"
]

TARGET_COLUMN = "optimal_price"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Price_Optimization_Training")
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


def validate_columns(df, required_columns, dataset_name, MAX_NULL_PERCENTAGE):
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


def calculate_price_elasticity_and_optimal_price(orders_df, order_items_df, MIN_PRICE_POINTS):
    """
    Calculate price elasticity and find optimal price from historical data
    
    Price Elasticity = % change in quantity / % change in price
    Optimal Price = price point that historically maximized revenue
    """
    print("Calculating price elasticity and optimal prices...")
    
    # Join orders with items (only delivered orders)
    order_data = orders_df.filter(
        F.col("order_status") == "Delivered"
    ).alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    ).select(
        F.col("oi.product_id"),
        F.col("o.order_placed_at").cast("date").alias("order_date"),
        F.col("oi.product_price").alias("price"),  # Fixed: use product_price
        F.col("oi.quantity")
    )
    
    # Create price buckets for each product
    # Group by product and price to get volume at each price point
    price_volume_data = order_data.groupBy("product_id", "price").agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("order_date").alias("order_count"),
        F.countDistinct("order_date").alias("days_sold")
    ).withColumn(
        "revenue",
        F.col("price") * F.col("total_quantity")
    ).withColumn(
        "avg_daily_quantity",
        F.col("total_quantity") / F.greatest(F.col("days_sold"), F.lit(1))
    )
    
    # For each product, find the price that maximized revenue
    window_max_revenue = Window.partitionBy("product_id").orderBy(F.desc("revenue"))
    
    optimal_prices = price_volume_data.withColumn(
        "revenue_rank",
        F.row_number().over(window_max_revenue)
    ).filter(
        F.col("revenue_rank") == 1
    ).select(
        "product_id",
        F.col("price").alias("optimal_price_observed"),
        F.col("revenue").alias("max_revenue_observed"),
        F.col("avg_daily_quantity").alias("quantity_at_optimal")
    )
    
    # Calculate price elasticity per product
    # Need at least 2 price points to calculate elasticity
    price_points_count = price_volume_data.groupBy("product_id").agg(
        F.count("price").alias("num_price_points")
    )
    
    # For products with multiple price points, calculate elasticity
    multi_price_products = price_volume_data.join(
        price_points_count.filter(F.col("num_price_points") >= MIN_PRICE_POINTS),
        "product_id",
        "inner"
    )
    
    # Order by price for each product
    window_price_order = Window.partitionBy("product_id").orderBy("price")
    
    price_ordered = multi_price_products.withColumn(
        "price_rank",
        F.row_number().over(window_price_order)
    ).withColumn(
        "prev_price",
        F.lag("price").over(window_price_order)
    ).withColumn(
        "prev_quantity",
        F.lag("avg_daily_quantity").over(window_price_order)
    )
    
    # Calculate % changes
    elasticity_calcs = price_ordered.filter(
        F.col("prev_price").isNotNull()
    ).withColumn(
        "price_change_pct",
        (F.col("price") - F.col("prev_price")) / F.col("prev_price")
    ).withColumn(
        "quantity_change_pct",
        (F.col("avg_daily_quantity") - F.col("prev_quantity")) / F.greatest(F.col("prev_quantity"), F.lit(0.01))
    ).withColumn(
        "point_elasticity",
        F.when(
            F.col("price_change_pct") != 0,
            F.col("quantity_change_pct") / F.col("price_change_pct")
        ).otherwise(0)
    )
    
    # Average elasticity per product
    product_elasticity = elasticity_calcs.groupBy("product_id").agg(
        F.avg("point_elasticity").alias("price_elasticity"),
        F.count("point_elasticity").alias("elasticity_observations")
    )
    
    # Aggregate price statistics per product
    price_stats = price_volume_data.groupBy("product_id").agg(
        F.min("price").alias("historical_min_price"),
        F.max("price").alias("historical_max_price"),
        F.avg("price").alias("historical_avg_price"),
        F.stddev("price").alias("price_std_dev"),
        F.sum("total_quantity").alias("total_units_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("order_count").alias("total_orders_count"),
        F.count("price").alias("num_price_points")
    )
    
    # Calculate revenue at different price tiers (low, medium, high)
    price_tier_revenue = price_volume_data.join(
        price_stats.select("product_id", "historical_min_price", "historical_max_price"),
        "product_id",
        "inner"
    ).withColumn(
        "price_range",
        F.col("historical_max_price") - F.col("historical_min_price")
    ).withColumn(
        "price_tier",
        F.when(
            F.col("price") < (F.col("historical_min_price") + F.col("price_range") / 3),
            "low"
        ).when(
            F.col("price") > (F.col("historical_max_price") - F.col("price_range") / 3),
            "high"
        ).otherwise("medium")
    )
    
    tier_aggregates = price_tier_revenue.groupBy("product_id", "price_tier").agg(
        F.sum("revenue").alias("tier_revenue"),
        F.sum("total_quantity").alias("tier_units")
    )
    
    # Pivot to get columns for each tier - specify values to ensure consistent column names
    tier_pivoted = tier_aggregates.groupBy("product_id").pivot("price_tier", ["low", "medium", "high"]).agg(
        F.first("tier_revenue"),
        F.first("tier_units")
    )
    
    # Pivot creates columns like: low_first(tier_revenue), medium_first(tier_revenue), high_first(tier_revenue)
    tier_columns = tier_pivoted.columns
    tier_pivoted_renamed = tier_pivoted.select(
        "product_id",
        F.coalesce(F.col("low_first(tier_revenue)"), F.lit(0)).alias("revenue_at_low_price"),
        F.coalesce(F.col("medium_first(tier_revenue)"), F.lit(0)).alias("revenue_at_medium_price"),
        F.coalesce(F.col("high_first(tier_revenue)"), F.lit(0)).alias("revenue_at_high_price"),
        F.coalesce(F.col("low_first(tier_units)"), F.lit(0)).alias("units_at_low_price"),
        F.coalesce(F.col("medium_first(tier_units)"), F.lit(0)).alias("units_at_medium_price"),
        F.coalesce(F.col("high_first(tier_units)"), F.lit(0)).alias("units_at_high_price")
    )
    
    # Combine all metrics
    pricing_data = optimal_prices \
        .join(product_elasticity, "product_id", "left") \
        .join(price_stats, "product_id", "left") \
        .join(tier_pivoted_renamed, "product_id", "left")
    
    pricing_data = pricing_data.fillna({
        "price_elasticity": -1.5,  # Default elasticity (slightly elastic)
        "elasticity_observations": 0,
        "price_std_dev": 0,
        "revenue_at_low_price": 0,
        "revenue_at_medium_price": 0,
        "revenue_at_high_price": 0,
        "units_at_low_price": 0,
        "units_at_medium_price": 0,
        "units_at_high_price": 0
    })
    
    print(f"✓ Price elasticity calculated for {pricing_data.count()} products")
    return pricing_data


def calculate_category_brand_baselines(products_df, pricing_data_df):
    """Calculate category and brand pricing baselines"""
    print("Calculating category and brand baselines...")
    
    # Join products with pricing data
    product_pricing = products_df.join(
        pricing_data_df.select("product_id", "price_elasticity", "historical_avg_price"),
        "product_id",
        "inner"
    )
    
    # Category statistics
    category_stats = product_pricing.groupBy("category").agg(
        F.avg("historical_avg_price").alias("category_avg_price"),
        F.avg("price_elasticity").alias("elasticity_category_avg"),
        F.count("product_id").alias("category_product_count")
    )
    
    # Brand statistics
    brand_stats = product_pricing.groupBy("brand").agg(
        F.avg("historical_avg_price").alias("brand_avg_price"),
        F.avg("price_elasticity").alias("elasticity_brand_avg"),
        F.count("product_id").alias("brand_product_count")
    )
    
    category_stats = category_stats.fillna({
        "category_avg_price": 0,
        "elasticity_category_avg": -1.5
    })
    
    brand_stats = brand_stats.fillna({
        "brand_avg_price": 0,
        "elasticity_brand_avg": -1.5
    })
    
    print(f"✓ Baselines calculated for {category_stats.count()} categories, {brand_stats.count()} brands")
    return category_stats, brand_stats


def create_price_optimization_features(products_df, pricing_data_df, category_stats_df, brand_stats_df):
    """
    Create comprehensive price optimization features
    """
    print("Creating price optimization features...")
    
    # Select only needed columns from products_df to avoid ambiguity
    products_selected = products_df.select(
        "product_id",
        "category",
        "brand",
        "cost_price",
        "sell_price",
        "avg_rating",
        "total_reviews"
    )
    
    # Join products with pricing data
    product_features = products_selected.join(
        pricing_data_df,
        "product_id",
        "inner"
    )
    
    # Join with category baselines
    product_features = product_features.join(
        category_stats_df,
        "category",
        "left"
    )
    
    # Join with brand baselines
    product_features = product_features.join(
        brand_stats_df,
        "brand",
        "left"
    )
    
    print(f"After joins: {product_features.count()} products")
    
    # Calculate derived features
    product_features = product_features.withColumn(
        "current_price",
        F.col("sell_price")
    ).withColumn(
        "current_profit_margin",
        F.when(
            F.col("sell_price") > 0,
            ((F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")) * 100
        ).otherwise(0)
    ).withColumn(
        "price_to_cost_ratio",
        F.when(
            F.col("cost_price") > 0,
            F.col("sell_price") / F.col("cost_price")
        ).otherwise(1.0)
    ).withColumn(
        "price_variance",
        F.coalesce(F.col("price_std_dev"), F.lit(0))
    ).withColumn(
        "current_vs_historical_avg",
        F.when(
            F.col("historical_avg_price") > 0,
            (F.col("sell_price") - F.col("historical_avg_price")) / F.col("historical_avg_price")
        ).otherwise(0)
    ).withColumn(
        "category_price_position",
        F.when(
            F.col("category_avg_price") > 0,
            F.col("sell_price") / F.col("category_avg_price")
        ).otherwise(1.0)
    ).withColumn(
        "brand_premium_factor",
        F.when(
            F.col("brand_avg_price") > 0,
            F.col("sell_price") / F.col("brand_avg_price")
        ).otherwise(1.0)
    ).withColumn(
        "demand_sensitivity",
        F.abs(F.col("price_elasticity"))
    ).withColumn(
        "avg_units_per_order",
        F.when(
            F.col("total_orders_count") > 0,
            F.col("total_units_sold") / F.col("total_orders_count")
        ).otherwise(1.0)
    )
    
    # Calculate review metrics
    product_features = product_features.withColumn(
        "review_sentiment_score",
        F.when(
            F.col("avg_rating").isNotNull(),
            F.col("avg_rating") / 5.0  # Normalize to 0-1
        ).otherwise(0.7)  # Default neutral
    )
    
    # Fill nulls
    product_features = product_features.fillna({
        "cost_price": 0,
        "total_units_sold": 0,
        "total_revenue": 0,
        "total_orders_count": 0,
        "avg_rating": 0,
        "total_reviews": 0,
        "category_avg_price": 0,
        "brand_avg_price": 0,
        "elasticity_category_avg": -1.5,
        "elasticity_brand_avg": -1.5,
        "brand": "Unknown"
    })
    
    # **CALCULATE TARGET: Optimal Price**
    # Use observed optimal price as target
    # This is the price that historically maximized revenue
    product_features = product_features.withColumn(
        TARGET_COLUMN,
        F.col("optimal_price_observed")
    )
    
    print(f"✓ Price optimization features created: {product_features.count()} products")
    return product_features


def prepare_training_data(df, MIN_RECORDS_THRESHOLD):
    """Prepare data with encoding and scaling"""
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0) &
        (F.col("cost_price") > 0) &
        (F.col(TARGET_COLUMN) > F.col("cost_price"))  # Optimal price must be above cost
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
    
    brand_indexer = StringIndexer(
        inputCol="brand",
        outputCol="brand_idx",
        handleInvalid="keep"
    )
    
    df_indexed = category_indexer.fit(df_valid).transform(df_valid)
    df_indexed = brand_indexer.fit(df_indexed).transform(df_indexed)
    
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
    
    print(f"  RMSE: ${rmse:.2f}")
    print(f"  MAE: ${mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name, MODEL_OUTPUT_PATH):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main(BUCKET_NAME):
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/price_optimization/"
    MIN_RECORDS_THRESHOLD = 100
    MAX_NULL_PERCENTAGE = 95.0
    MIN_PRICE_POINTS = 2  # Need at least 2 different historical prices

    # Configuration
    USE_CROSS_VALIDATION = False
    """Main training pipeline"""
    print("\n" + "="*60)
    print("Product Price Optimization - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    
    if None in [products_df, orders_df, order_items_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    prod_valid, _, _ = validate_columns(products_df, REQUIRED_PRODUCT_COLUMNS, "Products", MAX_NULL_PERCENTAGE)
    item_valid, _, _ = validate_columns(order_items_df, REQUIRED_ORDER_ITEM_COLUMNS, "Order Items", MAX_NULL_PERCENTAGE)
    
    if not (prod_valid and item_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Calculate price elasticity and optimal prices
    print("\nStep 3: Calculate Price Elasticity & Optimal Prices")
    print("-" * 60)
    pricing_data = calculate_price_elasticity_and_optimal_price(orders_df, order_items_df, MIN_PRICE_POINTS)
    
    # Calculate baselines
    print("\nStep 4: Calculate Category & Brand Baselines")
    print("-" * 60)
    category_stats, brand_stats = calculate_category_brand_baselines(products_df, pricing_data)
    
    # Create features
    print("\nStep 5: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_price_optimization_features(
        products_df, pricing_data, category_stats, brand_stats
    )
    
    # Prepare data
    print("\nStep 6: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features, MIN_RECORDS_THRESHOLD)
    
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
    
    # Train model
    print("\nStep 8: Model Training")
    print("-" * 60)
    
    model, predictions, model_name = train_random_forest(train_df, test_df, USE_CROSS_VALIDATION)
    metrics = evaluate_model(predictions, model_name)
    save_model(model, model_name, MODEL_OUTPUT_PATH)
    
    print("\n" + "="*60)
    print(f"Best Model: {model_name} (R² = {metrics['r2']:.4f})")
    print("="*60)
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)