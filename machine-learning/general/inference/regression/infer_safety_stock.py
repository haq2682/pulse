"""
Safety Stock Level Prediction - Inference Script
Generates safety stock recommendations using demand variability and lead time patterns
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

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
INPUT_INVENTORY_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
INPUT_SUPPLIERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/safety_stock/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/safety_stock/"

# ⚠️ MANUAL CONFIGURATION REQUIRED:
MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

# Configuration
DEFAULT_SERVICE_LEVEL = 0.95
Z_SCORE_95 = 1.65
Z_SCORE_99 = 2.33
MIN_DEMAND_DAYS = 30

# Feature set (must match training)
NUMERIC_FEATURES = [
    "avg_daily_demand", "demand_std_dev", "demand_volatility",
    "max_daily_demand", "demand_coefficient_of_variation", "days_with_demand",
    "lead_time_days", "lead_time_variability_factor",
    "service_level_target", "target_z_score",
    "current_stock", "stock_coverage_days", "inventory_turnover_ratio",
    "stockout_frequency", "historical_stockout_rate", "days_since_last_stockout",
    "storage_cost_per_unit", "stockout_cost_per_unit", "holding_cost_annual",
    "cost_price", "profit_margin",
    "theoretical_safety_stock", "lead_time_demand",
    "category_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Safety_Stock_Inference")
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


def calculate_demand_metrics(orders_df, order_items_df):
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
    ).withColumn(
        "demand_coefficient_of_variation",
        F.when(
            F.col("avg_daily_demand") > 0,
            (F.col("demand_std_dev") / F.col("avg_daily_demand")) * 100
        ).otherwise(0)
    )
    
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_coefficient_of_variation": 0
    })
    
    demand_stats = demand_stats.filter(
        (F.col("total_demand_days") >= MIN_DEMAND_DAYS) &
        (F.col("avg_daily_demand") > 0)
    )
    
    print(f"✓ Demand metrics for {demand_stats.count()} products")
    return demand_stats


def create_inference_features(products_df, inventory_df, suppliers_df, demand_stats_df):
    """Create same features as training"""
    print("Creating inference features...")
    
    # Join - avoid column duplication
    product_inventory = products_df.select(
        "product_id", "category",
        F.col("cost_price").alias("product_cost_price"),
        "sell_price", "supplier_id"
    ).join(
        inventory_df.select(
            "product_id", "current_stock", "minimum_stock_level",
            "stockout_frequency", "storage_cost_per_unit",
            F.col("cost_price").alias("inventory_cost_price"),
            "inventory_turnover_ratio", "days_since_restock"
        ),
        "product_id",
        "inner"
    )
    
    product_inventory = product_inventory.withColumn(
        "cost_price",
        F.coalesce(F.col("product_cost_price"), F.col("inventory_cost_price"), F.lit(0))
    ).drop("product_cost_price", "inventory_cost_price")
    
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days"),
            "supplier_reliability_score"
        ),
        "supplier_id",
        "left"
    )
    
    product_features = product_inventory.join(
        demand_stats_df,
        "product_id",
        "inner"
    )
    
    product_features = product_features.fillna({
        "lead_time_days": 7,
        "supplier_reliability_score": 0.8
    })
    
    # Calculate service level based on stockout history
    product_features = product_features.withColumn(
        "historical_stockout_rate",
        F.when(
            F.col("days_with_demand") > 0,
            F.col("stockout_frequency") / F.col("days_with_demand")
        ).otherwise(0)
    ).withColumn(
        "service_level_target",
        F.when(
            F.col("historical_stockout_rate") > 0.10,
            F.lit(0.99)
        ).when(
            F.col("historical_stockout_rate") > 0.05,
            F.lit(0.95)
        ).otherwise(
            F.lit(DEFAULT_SERVICE_LEVEL)
        )
    ).withColumn(
        "target_z_score",
        F.when(
            F.col("service_level_target") >= 0.99,
            F.lit(Z_SCORE_99)
        ).otherwise(
            F.lit(Z_SCORE_95)
        )
    )
    
    # Lead time variability
    product_features = product_features.withColumn(
        "lead_time_variability_factor",
        F.when(
            F.col("supplier_reliability_score") >= 0.9,
            F.lit(0.1)
        ).when(
            F.col("supplier_reliability_score") >= 0.7,
            F.lit(0.2)
        ).otherwise(
            F.lit(0.3)
        )
    )
    
    # Economic metrics
    product_features = product_features.withColumn(
        "profit_margin",
        F.when(
            F.col("sell_price") > 0,
            ((F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")) * 100
        ).otherwise(0)
    ).withColumn(
        "stockout_cost_per_unit",
        (F.col("sell_price") - F.col("cost_price")) * 1.5
    ).withColumn(
        "holding_cost_annual",
        F.col("cost_price") * 0.25
    )
    
    # Stock metrics
    product_features = product_features.withColumn(
        "stock_coverage_days",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("current_stock") / F.col("avg_daily_demand")
        ).otherwise(999)
    ).withColumn(
        "lead_time_demand",
        F.col("avg_daily_demand") * F.col("lead_time_days")
    ).withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    )
    
    # Theoretical safety stock
    product_features = product_features.withColumn(
        "theoretical_safety_stock",
        F.col("target_z_score") * F.sqrt(
            (F.col("lead_time_days") * F.pow(F.col("demand_std_dev"), 2)) +
            (F.pow(F.col("avg_daily_demand"), 2) * 
             F.pow(F.col("lead_time_variability_factor") * F.col("lead_time_days"), 2))
        )
    )
    
    product_features = product_features.fillna({
        "minimum_stock_level": 0,
        "storage_cost_per_unit": 0,
        "inventory_turnover_ratio": 0,
        "stockout_frequency": 0
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
    
    df_indexed = category_indexer.fit(df).transform(df)
    
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
        "service_level_target",
        "lead_time_demand",
        "theoretical_safety_stock",
        "demand_volatility",
        "lead_time_variability_factor",
        "historical_stockout_rate",
        "current_stock",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with detailed metrics"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Round predictions to nearest integer
    predictions_df = predictions_df.withColumn(
        "prediction_rounded",
        F.round(F.col("prediction"), 0).cast("integer")
    )
    
    # Calculate reorder point (lead time demand + safety stock)
    predictions_df = predictions_df.withColumn(
        "reorder_point",
        F.round(F.col("lead_time_demand") + F.col("prediction_rounded"), 0).cast("integer")
    )
    
    # Calculate minimum stock level (same as reorder point for safety stock model)
    predictions_df = predictions_df.withColumn(
        "minimum_stock_level",
        F.col("reorder_point")
    )
    
    # Calculate expected stockout probability
    predictions_df = predictions_df.withColumn(
        "expected_stockout_probability",
        F.when(
            F.col("service_level_target") >= 0.99,
            F.lit(0.01)
        ).when(
            F.col("service_level_target") >= 0.95,
            F.lit(0.05)
        ).otherwise(
            F.lit(0.10)
        )
    )
    
    # Calculate holding cost impact (annual cost of holding safety stock)
    predictions_df = predictions_df.withColumn(
        "holding_cost_impact",
        F.col("prediction_rounded") * (F.col("theoretical_safety_stock") * 0.25)
    )
    
    # Calculate confidence intervals (± 1 std dev)
    predictions_df = predictions_df.withColumn(
        "confidence_interval_lower",
        F.greatest(
            F.lit(0),
            F.round(F.col("prediction_rounded") - F.col("theoretical_safety_stock") * 0.2, 0)
        ).cast("integer")
    ).withColumn(
        "confidence_interval_upper",
        F.round(F.col("prediction_rounded") + F.col("theoretical_safety_stock") * 0.2, 0).cast("integer")
    )
    
    # Calculate confidence score (based on demand volatility)
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(
            F.col("demand_volatility") < 0.3,
            F.lit(0.95)
        ).when(
            F.col("demand_volatility") < 0.7,
            F.lit(0.80)
        ).otherwise(
            F.lit(0.65)
        )
    )
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction_rounded").alias("required_safety_stock_units"),
        F.col("minimum_stock_level"),
        F.col("reorder_point"),
        F.col("service_level_target"),
        F.col("demand_volatility").alias("demand_variability"),
        F.col("lead_time_variability_factor").alias("lead_time_variability"),
        F.col("expected_stockout_probability"),
        F.col("holding_cost_impact"),
        F.col("confidence_interval_lower"),
        F.col("confidence_interval_upper"),
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
    print(f"Sample Safety Stock Predictions (first {n} products)")
    print("="*80)
    
    sample = df.select(
        "product_id",
        "required_safety_stock_units",
        "minimum_stock_level",
        "reorder_point",
        "service_level_target",
        "demand_variability",
        "expected_stockout_probability",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Product: {row['product_id']:<30}")
        print(f"  Required Safety Stock: {row['required_safety_stock_units']:>6} units")
        print(f"  Minimum Stock Level: {row['minimum_stock_level']:>6} units")
        print(f"  Reorder Point: {row['reorder_point']:>6} units")
        print(f"  Service Level: {row['service_level_target']*100:>5.0f}%")
        print(f"  Demand Variability: {row['demand_variability']:>6.2f}")
        print(f"  Stockout Probability: {row['expected_stockout_probability']*100:>5.1f}%")
        print(f"  Confidence: {row['confidence_score']*100:>5.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics of predictions"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("product_id").alias("total_products"),
        F.sum("required_safety_stock_units").alias("total_safety_stock_units"),
        F.avg("required_safety_stock_units").alias("avg_safety_stock"),
        F.avg("service_level_target").alias("avg_service_level"),
        F.avg("expected_stockout_probability").alias("avg_stockout_prob"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    print(f"Total Products: {stats['total_products']}")
    print(f"Total Safety Stock Units: {stats['total_safety_stock_units']:.0f}")
    print(f"Average Safety Stock: {stats['avg_safety_stock']:.1f} units")
    print(f"Average Service Level: {stats['avg_service_level']*100:.1f}%")
    print(f"Average Stockout Probability: {stats['avg_stockout_prob']*100:.1f}%")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main():
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Safety Stock Level Prediction - Inference")
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
    demand_stats = calculate_demand_metrics(orders_df, order_items_df)
    
    # Create features
    print("\nStep 4: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(products_df, inventory_df, suppliers_df, demand_stats)
    
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
    main()
