"""
Stockout Probability Prediction - Inference Script
Generates stockout risk predictions with timing and probability
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
from pyspark.ml.regression import RandomForestRegressionModel
from datetime import datetime, timedelta
import uuid

# Load environment variables
load_dotenv()

# Feature set (must match training)
NUMERIC_FEATURES = [
    "current_stock", "available_stock", "reserved_quantity", "stock_utilization_rate",
    "avg_daily_demand", "demand_std_dev", "demand_volatility", "demand_trend",
    "demand_acceleration", "max_daily_demand", "recent_7day_avg_demand",
    "recent_30day_avg_demand",
    "lead_time_days", "pending_orders_quantity", "days_until_next_delivery",
    "current_days_of_supply", "projected_days_of_supply", "safety_stock_coverage",
    "reorder_point_breach",
    "stockout_frequency", "historical_stockout_rate", "days_since_last_stockout",
    "avg_stockout_duration",
    "seasonal_demand_multiplier", "promotion_impact_factor", "day_of_week_factor",
    "month_of_year_factor",
    "inventory_turnover_ratio", "criticality_score",
    "category_idx", "stock_status_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Stockout_Probability_Inference")
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


def load_models(MODEL_BASE_PATH):
    """Load both trained models"""
    try:
        days_model = RandomForestRegressionModel.load(f"{MODEL_BASE_PATH}days_until_stockout")
        prob_model = RandomForestRegressionModel.load(f"{MODEL_BASE_PATH}stockout_probability")
        print(f"✓ Models loaded successfully")
        return days_model, prob_model
    except Exception as e:
        print(f"✗ Failed to load models: {str(e)}")
        return None, None


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


def calculate_demand_metrics_with_trends(orders_df, order_items_df, MIN_DEMAND_DAYS):
    """Calculate comprehensive demand metrics"""
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
    
    # Recent demand windows
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
    
    # Trend calculation
    window_spec = Window.partitionBy("product_id").orderBy("order_date")
    daily_with_seq = daily_demand.withColumn(
        "day_seq",
        F.row_number().over(window_spec)
    )
    
    trend_data = daily_with_seq.groupBy("product_id").agg(
        F.corr("day_seq", "daily_quantity").alias("demand_trend")
    )
    
    # Acceleration
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
    
    # Join all
    demand_stats = demand_stats.join(recent_stats, "product_id", "left")
    demand_stats = demand_stats.join(trend_data, "product_id", "left")
    demand_stats = demand_stats.join(acceleration_data, "product_id", "left")
    
    demand_stats = demand_stats.fillna({
        "demand_std_dev": 0,
        "demand_volatility": 0,
        "demand_trend": 0,
        "demand_acceleration": 0,
        "recent_7day_avg_demand": 0,
        "recent_30day_avg_demand": 0
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
    
    product_inventory = products_df.select(
        "product_id",
        "category",
        "supplier_id",  # Include for supplier join
        F.col("total_revenue").alias("product_revenue")
    ).join(
        inventory_df.select(
            "product_id", "current_stock", "available_stock",
            "minimum_stock_level", "stock_status", "stockout_frequency",
            "inventory_turnover_ratio", "days_since_restock"
        ),
        "product_id",
        "inner"
    )
    
    # Join with suppliers - handle null supplier_ids
    product_inventory = product_inventory.join(
        suppliers_df.select(
            "supplier_id",
            F.col("avg_restock_lead_time").alias("lead_time_days")
        ),
        "supplier_id",  # Join on supplier_id column
        "left"  # Left join to keep products without suppliers
    )
    
    product_features = product_inventory.join(
        demand_stats_df,
        "product_id",
        "inner"
    )
    
    product_features = product_features.fillna({
        "lead_time_days": 7,
        "available_stock": 0,
        "minimum_stock_level": 0,
        "stockout_frequency": 0,
        "inventory_turnover_ratio": 0,
        "days_since_restock": 0
    })
    
    # Calculate all features matching training
    product_features = product_features.withColumn(
        "reserved_quantity",
        F.greatest(F.col("current_stock") - F.col("available_stock"), F.lit(0))
    ).withColumn(
        "stock_utilization_rate",
        F.when(
            F.col("current_stock") > 0,
            F.col("reserved_quantity") / F.col("current_stock")
        ).otherwise(0)
    ).withColumn(
        "pending_orders_quantity",
        F.when(
            F.col("current_stock") < F.col("minimum_stock_level"),
            F.col("avg_daily_demand") * F.col("lead_time_days") * 1.5
        ).otherwise(0)
    ).withColumn(
        "days_until_next_delivery",
        F.when(
            F.col("pending_orders_quantity") > 0,
            F.col("lead_time_days") * 0.5
        ).otherwise(999)
    ).withColumn(
        "historical_stockout_rate",
        F.when(
            F.col("total_demand_days") > 0,
            F.col("stockout_frequency") / F.col("total_demand_days")
        ).otherwise(0)
    ).withColumn(
        "days_since_last_stockout",
        F.coalesce(F.col("days_since_restock"), F.lit(365))
    ).withColumn(
        "avg_stockout_duration",
        F.col("lead_time_days") * 0.7
    ).withColumn(
        "month_of_year_factor",
        F.month(F.current_date())
    ).withColumn(
        "day_of_week_factor",
        F.dayofweek(F.current_date())
    ).withColumn(
        "seasonal_demand_multiplier",
        F.lit(1.0)
    ).withColumn(
        "promotion_impact_factor",
        F.lit(1.0)
    ).withColumn(
        "projected_daily_demand",
        F.col("recent_7day_avg_demand") * 
        F.col("seasonal_demand_multiplier") * 
        F.col("promotion_impact_factor")
    ).withColumn(
        "current_days_of_supply",
        F.when(
            F.col("avg_daily_demand") > 0,
            F.col("available_stock") / F.col("avg_daily_demand")
        ).otherwise(999)
    ).withColumn(
        "projected_days_of_supply",
        F.when(
            F.col("avg_daily_demand") > 0,
            (F.col("available_stock") + F.col("pending_orders_quantity")) / F.col("avg_daily_demand")
        ).otherwise(999)
    ).withColumn(
        "safety_stock_coverage",
        F.when(
            F.col("minimum_stock_level") > 0,
            F.col("current_stock") / F.col("minimum_stock_level")
        ).otherwise(1.0)
    ).withColumn(
        "reorder_point_breach",
        F.when(
            F.col("current_stock") < F.col("minimum_stock_level"),
            1.0
        ).otherwise(0.0)
    )
    
    # Criticality score
    max_revenue = product_features.agg(F.max("product_revenue")).collect()[0][0] or 1
    product_features = product_features.withColumn(
        "criticality_score",
        F.col("product_revenue") / F.lit(max_revenue)
    )
    
    product_features = product_features.fillna({
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
        "current_days_of_supply",
        "projected_daily_demand",
        "minimum_stock_level",
        "criticality_score",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(days_model, prob_model, df, CRITICAL_DAYS_THRESHOLD, HIGH_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD):
    """Generate comprehensive stockout predictions"""
    # Predict days until stockout
    df_with_days = days_model.transform(df).withColumnRenamed("prediction", "days_until_stockout")
    
    # Predict stockout probability
    df_with_both = prob_model.transform(df_with_days).withColumnRenamed("prediction", "stockout_probability")
    
    # Clip probability to 0-1 range
    df_with_both = df_with_both.withColumn(
        "stockout_probability",
        F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("stockout_probability")))
    )
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Calculate expected stockout date
    df_with_both = df_with_both.withColumn(
        "expected_stockout_date",
        F.when(
            F.col("days_until_stockout") < 999,
            F.date_add(F.current_date(), F.col("days_until_stockout").cast("int"))
        ).otherwise(F.lit(None).cast("date"))
    )
    
    # Determine risk level
    df_with_both = df_with_both.withColumn(
        "stockout_risk_level",
        F.when(
            F.col("days_until_stockout") <= CRITICAL_DAYS_THRESHOLD,
            F.lit("Critical")
        ).when(
            F.col("days_until_stockout") <= HIGH_RISK_THRESHOLD,
            F.lit("High")
        ).when(
            F.col("days_until_stockout") <= MEDIUM_RISK_THRESHOLD,
            F.lit("Medium")
        ).otherwise(
            F.lit("Low")
        )
    )
    
    # Check if safety stock breached
    df_with_both = df_with_both.withColumn(
        "safety_stock_breach",
        F.col("current_stock") < F.col("minimum_stock_level")
    )
    
    # Recommend reorder
    df_with_both = df_with_both.withColumn(
        "reorder_recommended",
        (F.col("stockout_probability") > 0.3) |
        (F.col("days_until_stockout") <= HIGH_RISK_THRESHOLD) |
        F.col("safety_stock_breach")
    )
    
    # Calculate recommended reorder quantity
    df_with_both = df_with_both.withColumn(
        "recommended_reorder_quantity",
        F.when(
            F.col("reorder_recommended"),
            F.round(
                F.greatest(
                    F.col("minimum_stock_level") - F.col("current_stock"),
                    F.col("projected_daily_demand") * 7
                ),
                0
            ).cast("integer")
        ).otherwise(0)
    )
    
    # Calculate urgency score (0-100)
    df_with_both = df_with_both.withColumn(
        "urgency_score",
        F.least(
            F.lit(100.0),
            (F.col("stockout_probability") * 50) +
            (F.when(F.col("days_until_stockout") < 14, 
                   (14 - F.col("days_until_stockout")) / 14 * 30
            ).otherwise(0)) +
            (F.col("criticality_score") * 20)
        )
    )
    
    # Calculate confidence score
    df_with_both = df_with_both.withColumn(
        "confidence_score",
        F.when(
            F.col("current_days_of_supply") < 30,
            F.lit(0.90)  # High confidence for short-term
        ).when(
            F.col("current_days_of_supply") < 90,
            F.lit(0.75)  # Medium confidence
        ).otherwise(
            F.lit(0.60)  # Lower confidence for long-term
        )
    )
    
    output_df = df_with_both.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        current_timestamp.alias("prediction_date"),
        F.col("stockout_probability"),
        F.col("days_until_stockout"),
        F.col("expected_stockout_date"),
        F.col("stockout_risk_level"),
        F.col("current_days_of_supply"),
        F.col("safety_stock_breach"),
        F.col("reorder_recommended"),
        F.col("recommended_reorder_quantity"),
        F.col("urgency_score"),
        F.col("confidence_score"),
        F.lit("random_forest").alias("model_version")
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
    print(f"Sample Stockout Predictions (first {n} products)")
    print("="*80)
    
    sample = df.select(
        "product_id",
        "stockout_probability",
        "days_until_stockout",
        "expected_stockout_date",
        "stockout_risk_level",
        "current_days_of_supply",
        "reorder_recommended",
        "recommended_reorder_quantity",
        "urgency_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Product: {row['product_id']:<30}")
        print(f"  Stockout Probability: {row['stockout_probability']*100:>6.1f}%")
        print(f"  Days Until Stockout: {row['days_until_stockout']:>6.1f} days")
        print(f"  Expected Date: {row['expected_stockout_date']}")
        print(f"  Risk Level: {row['stockout_risk_level']}")
        print(f"  Current Supply: {row['current_days_of_supply']:>6.1f} days")
        print(f"  Reorder Recommended: {'Yes' if row['reorder_recommended'] else 'No'}")
        print(f"  Recommended Quantity: {row['recommended_reorder_quantity']:>6} units")
        print(f"  Urgency Score: {row['urgency_score']:>6.1f}/100")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("product_id").alias("total_products"),
        F.sum(F.when(F.col("stockout_risk_level") == "Critical", 1).otherwise(0)).alias("critical_risk"),
        F.sum(F.when(F.col("stockout_risk_level") == "High", 1).otherwise(0)).alias("high_risk"),
        F.sum(F.when(F.col("stockout_risk_level") == "Medium", 1).otherwise(0)).alias("medium_risk"),
        F.sum(F.when(F.col("reorder_recommended"), 1).otherwise(0)).alias("reorder_recommended"),
        F.sum("recommended_reorder_quantity").alias("total_reorder_quantity"),
        F.avg("stockout_probability").alias("avg_stockout_prob"),
        F.avg("days_until_stockout").alias("avg_days_until_stockout"),
        F.avg("urgency_score").alias("avg_urgency")
    ).collect()[0]
    
    print(f"Total Products: {stats['total_products']}")
    print(f"\nRisk Distribution:")
    print(f"  Critical Risk: {stats['critical_risk']} products")
    print(f"  High Risk: {stats['high_risk']} products")
    print(f"  Medium Risk: {stats['medium_risk']} products")
    print(f"\nReorder Recommendations:")
    print(f"  Products Needing Reorder: {stats['reorder_recommended']}")
    print(f"  Total Units to Reorder: {stats['total_reorder_quantity']:.0f}")
    print(f"\nAverages:")
    print(f"  Avg Stockout Probability: {stats['avg_stockout_prob']*100:.1f}%")
    print(f"  Avg Days Until Stockout: {stats['avg_days_until_stockout']:.1f} days")
    print(f"  Avg Urgency Score: {stats['avg_urgency']:.1f}/100")
    print("="*80)


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_INVENTORY_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
    INPUT_SUPPLIERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/stockout_probability/"
    MODEL_BASE_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/stockout_probability/"

    # Configuration
    MIN_DEMAND_DAYS = 30
    CRITICAL_DAYS_THRESHOLD = 3
    HIGH_RISK_THRESHOLD = 7
    MEDIUM_RISK_THRESHOLD = 14
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Stockout Probability Prediction - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load models
    print("Step 1: Load Models")
    print("-" * 80)
    days_model, prob_model = load_models(MODEL_BASE_PATH)
    
    if days_model is None or prob_model is None:
        print("\n✗ Inference aborted: Models not found")
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
    demand_stats = calculate_demand_metrics_with_trends(orders_df, order_items_df, MIN_DEMAND_DAYS)
    
    # Create features
    print("\nStep 4: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(products_df, inventory_df, suppliers_df, demand_stats, DEFAULT_SERVICE_LEVEL, Z_SCORE_95, Z_SCORE_99)
    
    # Prepare data
    print("\nStep 5: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 6: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(days_model, prob_model, df_prepared, CRITICAL_DAYS_THRESHOLD, HIGH_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD)
    
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
