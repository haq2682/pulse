"""
Safety Stock Adjustment Factor - Inference Script
Generates safety stock recommendations using adjustment factor model
Two-step process: 1) Calculate theoretical (formula), 2) Predict adjustment factor
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

load_dotenv()


# Feature set (MUST MATCH TRAINING - business context only, NO formula components)
NUMERIC_FEATURES = [
    # Product business metrics
    "product_criticality_score",
    "total_revenue",
    "sell_price",
    "cost_price",
    "profit_margin",
    
    # Stockout impact
    "stockout_frequency",
    "stockout_rate",
    "days_since_last_stockout",
    "avg_stockout_duration",
    "historical_stockout_cost",
    
    # Economic factors
    "holding_cost_per_day",
    "stockout_cost_per_unit",
    "stockout_to_holding_ratio",
    
    # Inventory performance
    "current_stock",
    "stock_coverage_days",
    "inventory_turnover_ratio",
    "overstock_frequency",
    
    # Demand stability indicators (NOT raw std dev)
    "demand_trend",
    "seasonal_index",
    "demand_pattern_score",
    
    # Supply chain reliability
    "supplier_reliability_score",
    "lead_time_reliability",
    
    # Categorical features (indexed)
    "category_idx",
    "stock_status_idx",
    "demand_pattern_idx",
    "service_level_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Safety_Stock_Adjustment_Inference")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", "100")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def load_model(model_name, MODEL_BASE_PATH):
    """Load trained adjustment factor model"""
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
        
        print(f"✓ Adjustment factor model loaded: {model_path}")
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
    """Calculate demand patterns from order history"""
    print("Calculating demand metrics...")
    
    orders_delivered = orders_df.filter(F.col("order_status") == "Delivered")
    
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
    
    daily_demand = demand_data.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity"),
        F.first("order_month").alias("month")
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
    )
    
    # Calculate volatility for internal classification only (NOT a feature)
    demand_stats = demand_stats.withColumn(
        "demand_volatility_internal",
        F.when(F.col("avg_daily_demand") > 0, F.col("demand_std_dev") / F.col("avg_daily_demand")).otherwise(0)
    )
    
    # Trend calculation
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    daily_with_seq = daily_demand.withColumn("day_seq", F.row_number().over(window_spec))
    trend_data = daily_with_seq.groupBy("product_id").agg(F.corr("day_seq", "daily_quantity").alias("demand_trend"))
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    
    # Seasonal index
    monthly_demand = demand_data.groupBy("product_id", "order_month").agg(F.sum("quantity").alias("monthly_quantity"))
    avg_monthly = monthly_demand.groupBy("product_id").agg(F.avg("monthly_quantity").alias("avg_monthly_qty"))
    monthly_with_avg = monthly_demand.join(avg_monthly, "product_id")
    monthly_with_avg = monthly_with_avg.withColumn("month_index", F.col("monthly_quantity") / F.col("avg_monthly_qty"))
    
    current_month = F.month(F.current_date())
    seasonal_data = monthly_with_avg.filter(F.col("order_month") == current_month).select("product_id", F.col("month_index").alias("seasonal_index"))
    demand_stats = demand_stats.join(seasonal_data, "product_id", "left")
    
    demand_stats = demand_stats.fillna({"demand_std_dev": 0, "demand_trend": 0, "seasonal_index": 1.0})
    demand_stats = demand_stats.filter((F.col("total_demand_days") >= MIN_DEMAND_DAYS) & (F.col("avg_daily_demand") > 0))
    
    print(f"✓ Demand metrics calculated for {demand_stats.count()} products")
    return demand_stats


def create_inference_features(products_df, inventory_df, suppliers_df, demand_stats_df, SERVICE_LEVELS):
    """
    Create features for adjustment factor inference
    Includes: business context + theoretical safety stock calculation
    """
    print("Creating inference features...")
    
    # Join datasets
    product_inventory = products_df.select(
        "product_id", "category",
        F.col("cost_price").alias("product_cost_price"),
        "sell_price", "supplier_id", "total_revenue"
    ).join(
        inventory_df.select(
            "product_id", "current_stock", "minimum_stock_level",
            "storage_cost_per_unit",
            F.col("cost_price").alias("inventory_cost_price"),
            "stock_status", "inventory_turnover_ratio",
            "stockout_frequency", "days_since_restock"
        ),
        "product_id", "inner"
    )
    
    product_inventory = product_inventory.withColumn(
        "cost_price",
        F.coalesce(F.col("product_cost_price"), F.col("inventory_cost_price"), F.lit(0))
    ).drop("product_cost_price", "inventory_cost_price")
    
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days"),
            "supplier_reliability_score",
            "stockout_rate"
        ),
        "supplier_id", "left"
    )
    
    product_features = product_inventory.join(demand_stats_df, "product_id", "inner")
    
    product_features = product_features.fillna({
        "lead_time_days": 7,
        "supplier_reliability_score": 0.8,
        "stockout_rate": 0.05,
        "storage_cost_per_unit": 0
    })
    
    # Calculate lead time std dev (for formula, not a feature)
    product_features = product_features.withColumn(
        "lead_time_std_dev",
        F.col("lead_time_days") * (1 - F.col("supplier_reliability_score")) * 0.3
    ).withColumn(
        "lead_time_reliability",
        F.col("supplier_reliability_score")
    )
    
    # Business metrics (NO formula components as features)
    product_features = product_features.withColumn(
        "profit_margin",
        F.when(F.col("sell_price") > 0, (F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")).otherwise(0)
    ).withColumn(
        "holding_cost_per_day", (F.col("cost_price") * 0.25) / 365
    ).withColumn(
        "stockout_cost_per_unit", (F.col("sell_price") - F.col("cost_price")) * 1.5
    ).withColumn(
        "stockout_to_holding_ratio",
        F.when(F.col("holding_cost_per_day") > 0,
               F.col("stockout_cost_per_unit") / (F.col("holding_cost_per_day") * 365)
        ).otherwise(F.lit(10))
    )
    
    product_features = product_features.withColumn(
        "stock_coverage_days",
        F.when(F.col("avg_daily_demand") > 0, F.col("current_stock") / F.col("avg_daily_demand")).otherwise(F.lit(999))
    ).withColumn(
        "days_since_last_stockout", F.coalesce(F.col("days_since_restock"), F.lit(365))
    ).withColumn(
        "avg_stockout_duration",
        F.when(F.col("stockout_frequency") > 0, F.col("lead_time_days") * 0.5).otherwise(F.lit(0))
    ).withColumn(
        "historical_stockout_cost",
        F.col("stockout_frequency") * F.col("stockout_cost_per_unit")
    ).withColumn(
        "overstock_frequency",
        F.when(F.col("stock_coverage_days") > 90, F.lit(1)).otherwise(F.lit(0))
    )
    
    # Product criticality score
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
        "product_criticality_score", (F.col("revenue_score") * 0.5 + F.col("stockout_score") * 0.5)
    ).drop("revenue_score", "stockout_score")
    
    # Demand pattern score (stability indicator, NOT raw volatility)
    product_features = product_features.withColumn(
        "demand_pattern_score",
        F.when(F.col("demand_volatility_internal") < 0.3, F.lit(90))
        .when(F.col("demand_volatility_internal") < 0.7, F.lit(60))
        .otherwise(F.lit(30))
    ).withColumn(
        "demand_pattern",
        F.when(F.col("demand_volatility_internal") < 0.3, F.lit("Stable"))
        .when(F.col("demand_volatility_internal") < 0.7, F.lit("Variable"))
        .otherwise(F.lit("Erratic"))
    )
    
    # Determine service level based on stockout history
    product_features = product_features.withColumn(
        "historical_stockout_rate",
        F.when(F.col("days_with_demand") > 0, F.col("stockout_frequency") / F.col("days_with_demand")).otherwise(0)
    ).withColumn(
        "service_level_numeric",
        F.when(F.col("historical_stockout_rate") > 0.10, F.lit(0.99))
        .when(F.col("historical_stockout_rate") > 0.05, F.lit(0.95))
        .otherwise(F.lit(0.90))
    )
    
    # Expand for each service level
    service_level_data = []
    for idx, (service_level, z_score) in enumerate(SERVICE_LEVELS.items()):
        sl_df = product_features.withColumn("service_level_target", F.lit(service_level)) \
            .withColumn("service_level_category", F.lit(f"level_{idx}")) \
            .withColumn("z_score_internal", F.lit(z_score))
        service_level_data.append(sl_df)
    
    expanded_features = service_level_data[0]
    for df in service_level_data[1:]:
        expanded_features = expanded_features.union(df)
    
    # CALCULATE THEORETICAL SAFETY STOCK (formula-based)
    expanded_features = expanded_features.withColumn(
        "lead_time_variance_component",
        F.pow(F.col("lead_time_std_dev"), 2) / F.greatest(F.col("lead_time_days"), F.lit(1))
    ).withColumn(
        "effective_lead_time", F.col("lead_time_days") + F.col("lead_time_variance_component")
    ).withColumn(
        "theoretical_safety_stock",
        F.col("z_score_internal") * F.col("demand_std_dev") * F.sqrt(F.col("effective_lead_time"))
    ).withColumn(
        "theoretical_safety_stock_rounded",
        F.round(F.col("theoretical_safety_stock"), 0).cast("integer")
    )
    
    # Calculate lead time demand
    expanded_features = expanded_features.withColumn(
        "lead_time_demand",
        F.col("avg_daily_demand") * F.col("lead_time_days")
    )
    
    expanded_features = expanded_features.fillna({
        "inventory_turnover_ratio": 0, "stockout_frequency": 0,
        "total_revenue": 0, "stock_status": "Unknown"
    })
    
    print(f"✓ Inference features created: {expanded_features.count()} records")
    return expanded_features


def prepare_inference_data(df):
    """Prepare and scale features for inference"""
    print("Preparing inference data...")
    
    # Encode categoricals
    category_indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    stock_status_indexer = StringIndexer(inputCol="stock_status", outputCol="stock_status_idx", handleInvalid="keep")
    demand_pattern_indexer = StringIndexer(inputCol="demand_pattern", outputCol="demand_pattern_idx", handleInvalid="keep")
    service_level_indexer = StringIndexer(inputCol="service_level_category", outputCol="service_level_idx", handleInvalid="keep")
    
    df_indexed = category_indexer.fit(df).transform(df)
    df_indexed = stock_status_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = demand_pattern_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = service_level_indexer.fit(df_indexed).transform(df_indexed)
    
    # Filter features that exist
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    missing_features = [f for f in NUMERIC_FEATURES if f not in df_indexed.columns]
    
    if missing_features:
        print(f"⚠  Missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} business features")
    
    # Assemble features
    assembler = VectorAssembler(inputCols=existing_features, outputCol="features_unscaled", handleInvalid="keep")
    df_assembled = assembler.transform(df_indexed)
    
    # Scale features
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "product_id",
        "service_level_target",
        "theoretical_safety_stock",
        "theoretical_safety_stock_rounded",
        "lead_time_demand",
        "demand_pattern",
        "product_criticality_score",
        "historical_stockout_rate",
        "current_stock",
        "features"
    )
    
    print(f"✓ Data prepared for inference")
    return df_prepared


def generate_predictions(model, df, model_name):
    """
    Generate adjustment factor predictions and calculate final safety stock
    prediction = adjustment_factor (0.5 - 2.0)
    final_safety_stock = theoretical_safety_stock × adjustment_factor
    """
    print("Generating predictions...")
    
    # Predict adjustment factor
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Clip adjustment factor to reasonable range
    predictions_df = predictions_df.withColumn(
        "adjustment_factor",
        F.when(F.col("prediction") < 0.5, F.lit(0.5))
        .when(F.col("prediction") > 2.0, F.lit(2.0))
        .otherwise(F.col("prediction"))
    )
    
    # Calculate FINAL safety stock (theoretical × adjustment)
    predictions_df = predictions_df.withColumn(
        "adjusted_safety_stock",
        F.round(F.col("theoretical_safety_stock") * F.col("adjustment_factor"), 0).cast("integer")
    )
    
    # Calculate reorder point
    predictions_df = predictions_df.withColumn(
        "reorder_point",
        F.round(F.col("lead_time_demand") + F.col("adjusted_safety_stock"), 0).cast("integer")
    )
    
    # Minimum stock level (same as reorder point)
    predictions_df = predictions_df.withColumn(
        "minimum_stock_level",
        F.col("reorder_point")
    )
    
    # Expected stockout probability based on service level
    predictions_df = predictions_df.withColumn(
        "expected_stockout_probability",
        F.when(F.col("service_level_target") >= 0.99, F.lit(0.01))
        .when(F.col("service_level_target") >= 0.95, F.lit(0.05))
        .otherwise(F.lit(0.10))
    )
    
    # Calculate adjustment reasoning
    predictions_df = predictions_df.withColumn(
        "adjustment_reasoning",
        F.when(F.col("adjustment_factor") > 1.2, F.lit("High risk: increase safety stock"))
        .when(F.col("adjustment_factor") > 1.05, F.lit("Moderate increase for business context"))
        .when(F.col("adjustment_factor") < 0.85, F.lit("Stable demand: reduce safety stock"))
        .when(F.col("adjustment_factor") < 0.95, F.lit("Moderate decrease for efficiency"))
        .otherwise(F.lit("Formula-based level appropriate"))
    )
    
    # Confidence score based on demand pattern
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(F.col("demand_pattern") == "Stable", F.lit(0.95))
        .when(F.col("demand_pattern") == "Variable", F.lit(0.80))
        .otherwise(F.lit(0.65))
    )
    
    # Calculate holding cost impact
    predictions_df = predictions_df.withColumn(
        "holding_cost_impact_annual",
        F.col("adjusted_safety_stock") * F.col("theoretical_safety_stock") * 0.25
    )
    
    # Select output columns
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        current_timestamp.alias("prediction_date"),
        
        # Main outputs
        F.col("theoretical_safety_stock_rounded").alias("theoretical_safety_stock"),
        F.col("adjustment_factor"),
        F.col("adjusted_safety_stock").alias("required_safety_stock_units"),
        F.col("minimum_stock_level"),
        F.col("reorder_point"),
        
        # Context
        F.col("service_level_target"),
        F.col("demand_pattern"),
        F.col("expected_stockout_probability"),
        F.col("confidence_score"),
        F.col("adjustment_reasoning"),
        
        # Metadata
        F.lit(model_name).alias("model_version"),
        F.lit("adjustment_factor").alias("model_type")
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
    print("\n" + "="*100)
    print(f"Sample Adjustment Factor Predictions (first {n} products)")
    print("="*100)
    
    sample = df.select(
        "product_id",
        "theoretical_safety_stock",
        "adjustment_factor",
        "required_safety_stock_units",
        "reorder_point",
        "service_level_target",
        "demand_pattern",
        "adjustment_reasoning",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Product: {row['product_id']:<30}")
        print(f"  Theoretical Safety Stock: {row['theoretical_safety_stock']:>6} units (formula-based)")
        print(f"  Adjustment Factor:        {row['adjustment_factor']:>6.2f}× ({row['adjustment_reasoning']})")
        print(f"  Final Safety Stock:       {row['required_safety_stock_units']:>6} units (ML-adjusted)")
        print(f"  Reorder Point:            {row['reorder_point']:>6} units")
        print(f"  Service Level:            {row['service_level_target']*100:>5.0f}%")
        print(f"  Demand Pattern:           {row['demand_pattern']}")
        print(f"  Confidence:               {row['confidence_score']*100:>5.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*100)
    print("Prediction Summary Statistics")
    print("="*100)
    
    stats = df.select(
        F.count("product_id").alias("total_products"),
        F.avg("theoretical_safety_stock").alias("avg_theoretical"),
        F.avg("adjustment_factor").alias("avg_adjustment"),
        F.avg("required_safety_stock_units").alias("avg_final"),
        F.sum("required_safety_stock_units").alias("total_safety_stock"),
        F.avg("service_level_target").alias("avg_service_level"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    # Adjustment distribution
    adj_dist = df.groupBy(
        F.when(F.col("adjustment_factor") > 1.2, "High increase (>1.2×)")
        .when(F.col("adjustment_factor") > 1.05, "Moderate increase (1.05-1.2×)")
        .when(F.col("adjustment_factor") < 0.85, "Significant decrease (<0.85×)")
        .when(F.col("adjustment_factor") < 0.95, "Moderate decrease (0.85-0.95×)")
        .otherwise("Near formula (0.95-1.05×)")
        .alias("adjustment_category")
    ).count().orderBy(F.desc("count")).collect()
    
    print(f"Total Products: {stats['total_products']}")
    print(f"Average Theoretical Safety Stock: {stats['avg_theoretical']:.1f} units")
    print(f"Average Adjustment Factor: {stats['avg_adjustment']:.3f}×")
    print(f"Average Final Safety Stock: {stats['avg_final']:.1f} units")
    print(f"Total Safety Stock Required: {stats['total_safety_stock']:.0f} units")
    print(f"Average Service Level: {stats['avg_service_level']*100:.1f}%")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    
    print("\nAdjustment Factor Distribution:")
    for row in adj_dist:
        print(f"  {row['adjustment_category']:<35} {row['count']:>6} products")
    
    print("="*100)


def main(BUCKET_NAME):
    """Main inference pipeline"""
    
    # Configuration
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_INVENTORY_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
    INPUT_SUPPLIERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/safety_stock_adjusted/"
    MODEL_BASE_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/safety_stock_adjustment/"
    
    # ⚠️ MANUAL CONFIGURATION:
    MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"
    
    SERVICE_LEVELS = {0.90: 1.28, 0.95: 1.65, 0.99: 2.33}
    MIN_DEMAND_DAYS = 30
    
    print("\n" + "="*100)
    print("Safety Stock Adjustment Factor - Inference")
    print("="*100)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}")
    print(f"Approach: Two-step (Formula → ML Adjustment → Final)")
    print("="*100 + "\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Adjustment Factor Model")
    print("-" * 100)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 100)
    
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
    print("-" * 100)
    demand_stats = calculate_demand_metrics(orders_df, order_items_df, MIN_DEMAND_DAYS)
    
    # Create features
    print("\nStep 4: Feature Engineering (Business Context + Theoretical Formula)")
    print("-" * 100)
    df_features = create_inference_features(products_df, inventory_df, suppliers_df, demand_stats, SERVICE_LEVELS)
    
    # Prepare data
    print("\nStep 5: Data Preparation & Scaling")
    print("-" * 100)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 6: Generate Adjustment Factor Predictions")
    print("-" * 100)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
    print("\nStep 7: Save Predictions")
    print("-" * 100)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
        print(f"   Method: Theoretical (formula) × Adjustment Factor (ML)")
    else:
        print("\n✗ Inference failed")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)