"""
Product Price Optimization - Inference Script
Predicts optimal product prices to maximize revenue
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
from datetime import datetime
import uuid
import json

# Load environment variables
load_dotenv()

# Feature set (must match training)
NUMERIC_FEATURES = [
    "current_price", "cost_price", "current_profit_margin", "price_to_cost_ratio",
    "total_units_sold", "total_revenue", "avg_units_per_order", "total_orders_count",
    "price_elasticity", "elasticity_category_avg", "elasticity_brand_avg", "demand_sensitivity",
    "avg_rating", "total_reviews", "review_sentiment_score",
    "historical_min_price", "historical_max_price", "historical_avg_price",
    "price_variance", "current_vs_historical_avg",
    "revenue_at_low_price", "revenue_at_medium_price", "revenue_at_high_price",
    "units_at_low_price", "units_at_medium_price", "units_at_high_price",
    "category_avg_price", "category_price_position",
    "brand_avg_price", "brand_premium_factor",
    "category_idx", "brand_idx"
]

def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Price_Optimization_Inference")
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


def load_model(MODEL_PATH):
    """Load trained model"""
    try:
        model = RandomForestRegressionModel.load(MODEL_PATH)
        print(f"✓ Model loaded: {MODEL_PATH}")
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


def calculate_price_metrics(orders_df, order_items_df, MIN_PRICE_POINTS):
    """Calculate price metrics matching training"""
    print("Calculating price metrics...")
    
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
    
    # Get ALL products with sales (not just multi-price products)
    all_product_stats = price_volume_data.groupBy("product_id").agg(
        F.min("price").alias("historical_min_price"),
        F.max("price").alias("historical_max_price"),
        F.avg("price").alias("historical_avg_price"),
        F.stddev("price").alias("price_std_dev"),
        F.sum("total_quantity").alias("total_units_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("order_count").alias("total_orders_count"),
        F.count("price").alias("num_price_points")
    )
    
    # Price elasticity calculation - ONLY for products with 2+ price points
    price_points_count = price_volume_data.groupBy("product_id").agg(
        F.count("price").alias("num_price_points")
    )
    
    multi_price_products = price_volume_data.join(
        price_points_count.filter(F.col("num_price_points") >= MIN_PRICE_POINTS),
        "product_id",
        "inner"
    )
    
    # Only calculate elasticity if we have multi-price products
    if multi_price_products.count() > 0:
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
        
        product_elasticity = elasticity_calcs.groupBy("product_id").agg(
            F.avg("point_elasticity").alias("price_elasticity")
        )
    else:
        # No multi-price products - create empty dataframe with schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("price_elasticity", DoubleType(), True)
        ])
        product_elasticity = order_data.sql_ctx.createDataFrame([], schema)
    
    # Price tier revenues - calculate for all products
    price_tier_revenue = price_volume_data.join(
        all_product_stats.select("product_id", "historical_min_price", "historical_max_price"),
        "product_id",
        "inner"
    ).withColumn(
        "price_range",
        F.col("historical_max_price") - F.col("historical_min_price")
    ).withColumn(
        "price_tier",
        F.when(
            F.col("price_range") <= 0,  # Single price product
            "medium"
        ).when(
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
    tier_pivoted_renamed = tier_pivoted.select(
        "product_id",
        F.coalesce(F.col("low_first(tier_revenue)"), F.lit(0)).alias("revenue_at_low_price"),
        F.coalesce(F.col("medium_first(tier_revenue)"), F.lit(0)).alias("revenue_at_medium_price"),
        F.coalesce(F.col("high_first(tier_revenue)"), F.lit(0)).alias("revenue_at_high_price"),
        F.coalesce(F.col("low_first(tier_units)"), F.lit(0)).alias("units_at_low_price"),
        F.coalesce(F.col("medium_first(tier_units)"), F.lit(0)).alias("units_at_medium_price"),
        F.coalesce(F.col("high_first(tier_units)"), F.lit(0)).alias("units_at_high_price")
    )
    
    # Combine all metrics - LEFT JOIN to keep all products even without elasticity
    pricing_data = all_product_stats \
        .join(product_elasticity, "product_id", "left") \
        .join(tier_pivoted_renamed, "product_id", "left")
    
    # Fill nulls with defaults
    pricing_data = pricing_data.fillna({
        "price_elasticity": -1.5,  # Default elasticity
        "price_std_dev": 0,
        "revenue_at_low_price": 0,
        "revenue_at_medium_price": 0,
        "revenue_at_high_price": 0,
        "units_at_low_price": 0,
        "units_at_medium_price": 0,
        "units_at_high_price": 0
    })
    
    print(f"✓ Price metrics calculated for {pricing_data.count()} products")
    return pricing_data


def calculate_baselines(products_df, pricing_data_df):
    """Calculate category and brand baselines"""
    print("Calculating baselines...")
    
    product_pricing = products_df.join(
        pricing_data_df.select("product_id", "price_elasticity", "historical_avg_price"),
        "product_id",
        "inner"
    )
    
    category_stats = product_pricing.groupBy("category").agg(
        F.avg("historical_avg_price").alias("category_avg_price"),
        F.avg("price_elasticity").alias("elasticity_category_avg")
    )
    
    brand_stats = product_pricing.groupBy("brand").agg(
        F.avg("historical_avg_price").alias("brand_avg_price"),
        F.avg("price_elasticity").alias("elasticity_brand_avg")
    )
    
    category_stats = category_stats.fillna({"category_avg_price": 0, "elasticity_category_avg": -1.5})
    brand_stats = brand_stats.fillna({"brand_avg_price": 0, "elasticity_brand_avg": -1.5})
    
    print(f"✓ Baselines calculated")
    return category_stats, brand_stats


def create_inference_features(products_df, pricing_data_df, category_stats_df, brand_stats_df):
    """Create inference features matching training"""
    print("Creating inference features...")
    
    # Drop overlapping columns from products_df before joining to avoid ambiguity
    products_clean = products_df.drop("total_revenue", "total_units_sold", "total_orders_count") if "total_revenue" in products_df.columns else products_df
    
    # LEFT join so products without price history are kept
    product_features = products_clean.join(pricing_data_df, "product_id", "left") \
        .join(category_stats_df, "category", "left") \
        .join(brand_stats_df, "brand", "left")
    
    # Fill nulls for products without price history FIRST
    product_features = product_features.fillna({
        "price_elasticity": -1.5,
        "historical_min_price": 0,
        "historical_max_price": 0,
        "historical_avg_price": 0,
        "price_std_dev": 0,
        "total_units_sold": 0,
        "total_revenue": 0,
        "total_orders_count": 0,
        "revenue_at_low_price": 0,
        "revenue_at_medium_price": 0,
        "revenue_at_high_price": 0,
        "units_at_low_price": 0,
        "units_at_medium_price": 0,
        "units_at_high_price": 0,
        "cost_price": 0,
        "avg_rating": 0,
        "total_reviews": 0,
        "category_avg_price": 0,
        "brand_avg_price": 0,
        "elasticity_category_avg": -1.5,
        "elasticity_brand_avg": -1.5,
        "brand": "Unknown"
    })
    
    # If historical_avg_price is still 0, use current sell_price
    product_features = product_features.withColumn(
        "historical_avg_price",
        F.when(
            F.col("historical_avg_price") == 0,
            F.col("sell_price")
        ).otherwise(F.col("historical_avg_price"))
    )
    
    # Calculate derived features
    product_features = product_features.withColumn(
        "current_price", F.col("sell_price")
    ).withColumn(
        "current_profit_margin",
        F.when(F.col("sell_price") > 0,
              ((F.col("sell_price") - F.col("cost_price")) / F.col("sell_price")) * 100
        ).otherwise(0)
    ).withColumn(
        "price_to_cost_ratio",
        F.when(F.col("cost_price") > 0, F.col("sell_price") / F.col("cost_price")).otherwise(1.0)
    ).withColumn(
        "price_variance", F.coalesce(F.col("price_std_dev"), F.lit(0))
    ).withColumn(
        "current_vs_historical_avg",
        F.when(F.col("historical_avg_price") > 0,
              (F.col("sell_price") - F.col("historical_avg_price")) / F.col("historical_avg_price")
        ).otherwise(0)
    ).withColumn(
        "category_price_position",
        F.when(F.col("category_avg_price") > 0, F.col("sell_price") / F.col("category_avg_price")).otherwise(1.0)
    ).withColumn(
        "brand_premium_factor",
        F.when(F.col("brand_avg_price") > 0, F.col("sell_price") / F.col("brand_avg_price")).otherwise(1.0)
    ).withColumn(
        "demand_sensitivity", F.abs(F.col("price_elasticity"))
    ).withColumn(
        "avg_units_per_order",
        F.when(F.col("total_orders_count") > 0, F.col("total_units_sold") / F.col("total_orders_count")).otherwise(1.0)
    ).withColumn(
        "review_sentiment_score",
        F.when(F.col("avg_rating").isNotNull(), F.col("avg_rating") / 5.0).otherwise(0.7)
    )
    
    # Filter out invalid products
    product_features = product_features.filter(
        (F.col("sell_price").isNotNull()) &
        (F.col("sell_price") > 0)
    )
    
    print(f"✓ Inference features created: {product_features.count()} products")
    return product_features


def prepare_inference_data(df):
    """Prepare and scale features"""
    category_indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    brand_indexer = StringIndexer(inputCol="brand", outputCol="brand_idx", handleInvalid="keep")
    
    df_indexed = category_indexer.fit(df).transform(df)
    df_indexed = brand_indexer.fit(df_indexed).transform(df_indexed)
    
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    
    assembler = VectorAssembler(inputCols=existing_features, outputCol="features_unscaled", handleInvalid="keep")
    df_assembled = assembler.transform(df_indexed)
    
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "product_id", "current_price", "cost_price", "price_elasticity",
        "historical_avg_price", "category", "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df):
    """Generate comprehensive price optimization predictions"""
    predictions_df = model.transform(df)
    
    # Ensure optimal price is above cost
    predictions_df = predictions_df.withColumn(
        "optimal_price",
        F.greatest(
            F.col("cost_price") * 1.1,  # Minimum 10% markup
            F.col("prediction")
        )
    )
    
    # Round to 2 decimals
    predictions_df = predictions_df.withColumn(
        "optimal_price",
        F.round(F.col("optimal_price"), 2)
    )
    
    # Calculate expected revenue and units at optimal price
    # Using elasticity: Q_new = Q_old * (P_new / P_old) ^ elasticity
    predictions_df = predictions_df.withColumn(
        "price_change_ratio",
        F.when(
            F.col("current_price") > 0,
            F.col("optimal_price") / F.col("current_price")
        ).otherwise(1.0)
    ).withColumn(
        "expected_demand_multiplier",
        F.pow(F.col("price_change_ratio"), F.col("price_elasticity"))
    ).withColumn(
        "expected_units_at_optimal",
        F.round(
            F.col("expected_demand_multiplier") * 100  # Assume 100 base units
        , 0).cast("integer")
    ).withColumn(
        "expected_revenue_at_optimal",
        F.col("optimal_price") * F.col("expected_units_at_optimal")
    )
    
    # Calculate competitor price range (simplified - use category range)
    def generate_competitor_range(cat_avg, hist_min, hist_max):
        return json.dumps({
            "min": round(float(hist_min * 0.9), 2) if hist_min else 0,
            "max": round(float(hist_max * 1.1), 2) if hist_max else 0,
            "avg": round(float(cat_avg), 2) if cat_avg else 0
        })
    
    competitor_udf = F.udf(generate_competitor_range, StringType())
    
    predictions_df = predictions_df.withColumn(
        "competitor_price_range",
        competitor_udf(
            F.col("historical_avg_price"),
            F.lit(0),  # Simplified - would need competitor data
            F.lit(0)
        )
    )
    
    # Calculate confidence score
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        F.when(
            F.abs(F.col("price_elasticity")) > 1.0,
            F.lit(0.90)  # High confidence with elastic products
        ).when(
            F.abs(F.col("price_elasticity")) > 0.5,
            F.lit(0.80)  # Medium confidence
        ).otherwise(
            F.lit(0.70)  # Lower confidence for inelastic
        )
    )
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        current_timestamp.alias("prediction_date"),
        F.col("current_price"),
        F.col("optimal_price"),
        F.col("expected_revenue_at_optimal"),
        F.col("expected_units_at_optimal"),
        F.col("price_elasticity"),
        F.col("competitor_price_range"),
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
    print(f"Sample Price Optimization Predictions (first {n} products)")
    print("="*80)
    
    sample = df.select(
        "product_id", "current_price", "optimal_price",
        "expected_revenue_at_optimal", "price_elasticity", "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        price_change = ((row['optimal_price'] - row['current_price']) / row['current_price'] * 100) if row['current_price'] > 0 else 0
        print(f"Product: {row['product_id']:<30}")
        print(f"  Current Price: ${row['current_price']:>8.2f}")
        print(f"  Optimal Price: ${row['optimal_price']:>8.2f} ({price_change:+.1f}%)")
        print(f"  Expected Revenue: ${row['expected_revenue_at_optimal']:>10,.2f}")
        print(f"  Price Elasticity: {row['price_elasticity']:>6.2f}")
        print(f"  Confidence: {row['confidence_score']*100:>6.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("product_id").alias("total_products"),
        F.avg("current_price").alias("avg_current_price"),
        F.avg("optimal_price").alias("avg_optimal_price"),
        F.sum("expected_revenue_at_optimal").alias("total_expected_revenue"),
        F.sum(F.when(F.col("optimal_price") > F.col("current_price"), 1).otherwise(0)).alias("price_increase_recommended"),
        F.sum(F.when(F.col("optimal_price") < F.col("current_price"), 1).otherwise(0)).alias("price_decrease_recommended"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    print(f"Total Products: {stats['total_products']}")
    print(f"Average Current Price: ${stats['avg_current_price']:.2f}")
    print(f"Average Optimal Price: ${stats['avg_optimal_price']:.2f}")
    print(f"Total Expected Revenue: ${stats['total_expected_revenue']:,.2f}")
    print(f"Price Increase Recommended: {stats['price_increase_recommended']} products")
    print(f"Price Decrease Recommended: {stats['price_decrease_recommended']} products")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main(BUCKET_NAME):
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/price_optimization/"
    MODEL_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/price_optimization/random_forest"
    MIN_PRICE_POINTS = 2
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Product Price Optimization - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 80)
    model = load_model(MODEL_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 80)
    
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    
    if None in [products_df, orders_df, order_items_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Calculate price metrics
    print("\nStep 3: Calculate Price Metrics")
    print("-" * 80)
    pricing_data = calculate_price_metrics(orders_df, order_items_df, MIN_PRICE_POINTS)
    
    # Calculate baselines
    print("\nStep 4: Calculate Baselines")
    print("-" * 80)
    category_stats, brand_stats = calculate_baselines(products_df, pricing_data)
    
    # Create features
    print("\nStep 5: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(products_df, pricing_data, category_stats, brand_stats)
    
    # Prepare data
    print("\nStep 6: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 7: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(model, df_prepared)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
    print("\nStep 8: Save Predictions")
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