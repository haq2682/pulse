"""
Inventory Restock Quantity Prediction - Inference Script
Generates restock recommendations using demand patterns and inventory optimization
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

# Feature set (must match training)
NUMERIC_FEATURES = [
    "current_stock", "minimum_stock_level", "available_stock", "stock_coverage_days",
    "avg_daily_demand", "demand_std_dev", "demand_volatility", "max_daily_demand",
    "demand_trend", "days_with_demand",
    "cost_price", "sell_price", "profit_margin", "storage_cost_per_unit", "holding_cost_per_day",
    "lead_time_days", "lead_time_demand", "supplier_reliability_score",
    "stockout_frequency", "stockout_rate", "days_since_last_stockout",
    "safety_stock_calculated", "reorder_point_calculated",
    "seasonal_demand_factor",
    "inventory_turnover_ratio", "total_revenue",
    "category_idx", "stock_status_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Restock_Quantity_Inference")
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


def load_model(model_name, MODEL_BASE_PATH):
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


def calculate_demand_metrics(orders_df, order_items_df, MIN_DEMAND_DAYS):
    """Calculate demand patterns from actual order history"""
    print("Calculating demand metrics...")
    
    orders_delivered = orders_df.filter(F.col("order_status") == "Delivered")
    
    demand_data = orders_delivered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    ).select(
        F.col("oi.product_id"),
        F.col("o.order_placed_at").cast("date").alias("order_date"),
        F.col("oi.quantity").cast("double").alias("quantity")
    )
    
    daily_demand = demand_data.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity")
    )
    
    demand_stats = daily_demand.groupBy("product_id").agg(
        F.count("order_date").alias("days_with_demand"),
        F.avg("daily_quantity").alias("avg_daily_demand"),
        F.stddev("daily_quantity").alias("demand_std_dev"),
        F.max("daily_quantity").alias("max_daily_demand"),
        F.min("order_date").alias("first_demand_date"),
        F.max("order_date").alias("last_demand_date")
    )
    
    demand_stats = demand_stats.withColumn(
        "total_demand_days",
        F.datediff(F.col("last_demand_date"), F.col("first_demand_date")) + 1
    ).withColumn(
        "demand_volatility",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("demand_std_dev") / F.col("avg_daily_demand")
        ).otherwise(0)
    )
    
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    
    daily_with_seq = daily_demand.withColumn(
        "day_seq",
        F.row_number().over(window_spec)
    )
    
    trend_data = daily_with_seq.groupBy("product_id").agg(
        F.corr("day_seq", "daily_quantity").alias("demand_trend")
    )
    
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_trend": 0
    })
    
    demand_stats = demand_stats.filter(
        (F.col("total_demand_days") >= MIN_DEMAND_DAYS) &
        (F.col("avg_daily_demand") > 0)
    )
    
    print(f"✓ Demand metrics for {demand_stats.count()} products")
    return demand_stats


def create_inference_features(products_df, inventory_df, suppliers_df, demand_stats_df, Z_SCORE_SAFETY_STOCK):
    """Create same features as training"""
    print("Creating inference features...")
    
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
    
    # Join with suppliers
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
    
    product_features = product_features.fillna({
        "lead_time_days": 7,
        "supplier_reliability_score": 0.8,
        "stockout_rate": 0.05
    })
    
    product_features = product_features.withColumn(
        "holding_cost_per_day",
        (F.col("cost_price") * 0.25) / 365
    ).withColumn(
        "profit_margin",
        F.when(
            F.col("sell_price") > 0,
            ((F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")) * 100
        ).otherwise(0)
    ).withColumn(
        "stock_coverage_days",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("current_stock") / F.col("avg_daily_demand")
        ).otherwise(999)
    ).withColumn(
        "lead_time_demand",
        F.col("avg_daily_demand") * F.col("lead_time_days")
    ).withColumn(
        "safety_stock_calculated",
        Z_SCORE_SAFETY_STOCK * F.col("demand_std_dev") * F.sqrt(F.col("lead_time_days"))
    ).withColumn(
        "reorder_point_calculated",
        F.col("lead_time_demand") + F.col("safety_stock_calculated")
    ).withColumn(
        "seasonal_demand_factor",
        F.lit(1.0)
    ).withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    )
    
    # Calculate expected demand next 30 days
    product_features = product_features.withColumn(
        "expected_demand_next_30_days",
        F.col("avg_daily_demand") * 30 * F.col("seasonal_demand_factor")
    )
    
    product_features = product_features.fillna({
        "available_stock": 0,
        "minimum_stock_level": 0,
        "storage_cost_per_unit": 0,
        "inventory_turnover_ratio": 0,
        "stockout_frequency": 0,
        "total_revenue": 0,
        "stock_status": "Unknown"
    })
    
    print(f"✓ Inference features created: {product_features.count()} products")
    return product_features


def prepare_inference_data(df):
    """Prepare and scale features"""
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
    
    df_indexed = category_indexer.fit(df).transform(df)
    df_indexed = stock_status_indexer.fit(df_indexed).transform(df_indexed)
    
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
        "product_id",
        "current_stock",
        "avg_daily_demand",
        "lead_time_days",
        "cost_price",
        "safety_stock_calculated",
        "reorder_point_calculated",
        "expected_demand_next_30_days",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with detailed metrics"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Round predictions to nearest integer (can't order fractional units)
    predictions_df = predictions_df.withColumn(
        "prediction_rounded",
        F.round(F.col("prediction"), 0).cast("integer")
    )
    
    # Calculate optimal order point (reorder point)
    predictions_df = predictions_df.withColumn(
        "optimal_order_point",
        F.round(F.col("reorder_point_calculated"), 0).cast("integer")
    )
    
    # Calculate safety stock level
    predictions_df = predictions_df.withColumn(
        "safety_stock_level",
        F.round(F.col("safety_stock_calculated"), 0).cast("integer")
    )
    
    # Calculate estimated cost
    predictions_df = predictions_df.withColumn(
        "estimated_cost",
        F.col("prediction_rounded") * F.col("cost_price")
    )
    
    # Calculate confidence score (based on demand volatility)
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(
            F.col("prediction_rounded") > 0,
            F.greatest(
                F.lit(0.5),
                F.least(F.lit(0.95), 1.0 - F.col("expected_demand_next_30_days") / F.col("prediction_rounded"))
            )
        ).otherwise(0.5)
    )
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction_rounded").alias("recommended_restock_quantity"),
        F.col("expected_demand_next_30_days"),
        F.col("optimal_order_point"),
        F.col("safety_stock_level"),
        F.col("estimated_cost"),
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
    print(f"Sample Restock Quantity Predictions (first {n} products)")
    print("="*80)
    
    sample = df.select(
        "product_id",
        "recommended_restock_quantity",
        "expected_demand_next_30_days",
        "optimal_order_point",
        "safety_stock_level",
        "estimated_cost",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Product: {row['product_id']:<30}")
        print(f"  Recommended Restock: {row['recommended_restock_quantity']:>6} units")
        print(f"  Expected Demand (30d): {row['expected_demand_next_30_days']:>8.1f} units")
        print(f"  Reorder Point: {row['optimal_order_point']:>6} units")
        print(f"  Safety Stock: {row['safety_stock_level']:>6} units")
        print(f"  Estimated Cost: ${row['estimated_cost']:>10.2f}")
        print(f"  Confidence: {row['confidence_score']*100:>5.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics of predictions"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("product_id").alias("total_products"),
        F.sum("recommended_restock_quantity").alias("total_units_to_order"),
        F.sum("estimated_cost").alias("total_estimated_cost"),
        F.avg("recommended_restock_quantity").alias("avg_restock_qty"),
        F.avg("confidence_score").alias("avg_confidence"),
        F.sum(F.when(F.col("recommended_restock_quantity") > 0, 1).otherwise(0)).alias("products_need_restock")
    ).collect()[0]
    
    print(f"Total Products: {stats['total_products']}")
    print(f"Products Needing Restock: {stats['products_need_restock']}")
    print(f"Total Units to Order: {stats['total_units_to_order']:.0f}")
    print(f"Total Estimated Cost: ${stats['total_estimated_cost']:,.2f}")
    print(f"Average Restock Quantity: {stats['avg_restock_qty']:.1f} units")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_INVENTORY_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
    INPUT_SUPPLIERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/restock_quantity/"
    MODEL_BASE_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/restock_quantity/"

    # ⚠️ MANUAL CONFIGURATION REQUIRED:
    MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

    # Configuration
    Z_SCORE_SAFETY_STOCK = 1.65  # 95% service level
    ORDERING_COST = 50
    MIN_DEMAND_DAYS = 30
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Inventory Restock Quantity Prediction - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 80)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 80)
    
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    inventory_df, _ = validate_dataset(spark, INPUT_INVENTORY_PATH, "Inventory")
    suppliers_df, _ = validate_dataset(spark, INPUT_SUPPLIERS_PATH, "Suppliers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    
    if None in [products_df, inventory_df, suppliers_df, orders_df, order_items_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Calculate demand metrics
    print("\nStep 3: Calculate Demand Metrics")
    print("-" * 80)
    demand_stats = calculate_demand_metrics(orders_df, order_items_df, MIN_DEMAND_DAYS)
    
    # Create features
    print("\nStep 4: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(products_df, inventory_df, suppliers_df, demand_stats, Z_SCORE_SAFETY_STOCK)
    
    # Prepare data
    print("\nStep 5: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 6: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
    print("\nStep 7: Save Predictions")
    print("-" * 80)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)