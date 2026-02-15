"""
Product Demand Forecasting V2 - Inference Script
Generates predictions using advanced features with temporal lags and category seasonality
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from datetime import datetime, timedelta
import uuid
import math

# Load environment variables
load_dotenv()


# Feature columns (must match training)
FEATURE_COLUMNS = [
    "sell_price",
    "days_since_launch",
    "avg_rating",
    "profit_margin",
    "total_orders",
    "avg_quantity_per_order",
    "order_placed_month",
    "order_placed_quarter",
    "order_placed_week_of_year",
    "order_placed_day_of_week",
    "month_sin",
    "month_cos",
    "quarter_sin",
    "quarter_cos",
    "category_growth_rate",
    "category_seasonal_current",
    "product_category_share",
    "demand_lag_1m",
    "demand_lag_3m",
    "demand_rolling_6m",
    "growth_rate_1m",
    "price_x_seasonality",
    "category_seasonal_x_month"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Demand_Forecast_V2_Inference")
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


def create_inference_features(orders_df, order_items_df, products_df, categories_df, monthly_agg_df):
    """
    Create same advanced features as training for inference
    """
    print("Creating inference features...")
    
    # Filter delivered orders
    orders_filtered = orders_df.filter(F.col("order_status") == "Delivered")
    
    # Join orders → order_items → products
    orders_items = orders_filtered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner"
    )
    
    full_data = orders_items.join(
        products_df.alias("p"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "inner"
    ).select(
        F.col("oi.product_id").alias("product_id"),
        "quantity",
        "order_placed_month",
        "order_placed_quarter",
        "order_placed_week_of_year",
        "order_placed_day_of_week",
        "order_placed_year",
        F.col("p.sell_price"),
        F.col("p.days_since_launch"),
        F.col("p.avg_rating"),
        F.col("p.profit_margin"),
        F.col("p.category"),
        F.col("p.total_units_sold")
    )
    
    # Create year_month
    full_data = full_data.withColumn(
        "year_month",
        F.concat(
            F.col("order_placed_year").cast("string"),
            F.lit("-"),
            F.lpad(F.col("order_placed_month").cast("string"), 2, "0")
        )
    )
    
    # Aggregate by product and month
    product_monthly = full_data.groupBy("product_id", "year_month", "order_placed_month").agg(
        F.sum("quantity").alias("monthly_demand"),
        F.count("*").alias("monthly_orders"),
        F.avg("quantity").alias("avg_quantity_per_order")
    )
    
    # Create lag features
    window_spec = Window.partitionBy("product_id").orderBy("year_month")
    
    product_monthly = product_monthly.withColumn(
        "demand_lag_1m",
        F.lag("monthly_demand", 1).over(window_spec)
    ).withColumn(
        "demand_lag_3m",
        F.lag("monthly_demand", 3).over(window_spec)
    ).withColumn(
        "demand_lag_6m",
        F.lag("monthly_demand", 6).over(window_spec)
    )
    
    # Rolling average
    window_rolling = Window.partitionBy("product_id").orderBy("year_month").rowsBetween(-6, -1)
    product_monthly = product_monthly.withColumn(
        "demand_rolling_6m",
        F.avg("monthly_demand").over(window_rolling)
    )
    
    # Growth rate
    product_monthly = product_monthly.withColumn(
        "prev_month_demand",
        F.lag("monthly_demand", 1).over(window_spec)
    ).withColumn(
        "growth_rate_1m",
        F.when(
            (F.col("prev_month_demand").isNotNull()) & (F.col("prev_month_demand") > 0),
            (F.col("monthly_demand") - F.col("prev_month_demand")) / F.col("prev_month_demand")
        ).otherwise(0)
    )
    
    # Get latest month data
    window_latest = Window.partitionBy("product_id").orderBy(F.desc("year_month"))
    
    product_features = product_monthly.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(F.col("row_num") == 1).drop("row_num", "prev_month_demand")
    
    # Join with products
    product_features = product_features.join(
        products_df.select(
            "product_id",
            "category",
            "sell_price",
            "days_since_launch",
            "avg_rating",
            "profit_margin",
            "total_units_sold"
        ),
        "product_id",
        "left"
    )
    
    # Join with categories
    product_features = product_features.join(
        categories_df.select(
            F.col("category"),
            F.col("avg_category_growth_rate").alias("category_growth_rate"),
            F.col("seasonal_index_spring"),
            F.col("seasonal_index_summer"),
            F.col("seasonal_index_fall"),
            F.col("seasonal_index_winter"),
            F.col("total_revenue").alias("category_revenue")
        ),
        "category",
        "left"
    )
    
    # Product category share
    product_features = product_features.withColumn(
        "product_revenue",
        F.col("total_units_sold") * F.col("sell_price")
    ).withColumn(
        "product_category_share",
        F.when(
            F.col("category_revenue") > 0,
            F.col("product_revenue") / F.col("category_revenue")
        ).otherwise(0)
    )
    
    # Current seasonal index
    product_features = product_features.withColumn(
        "category_seasonal_current",
        F.when(F.col("order_placed_month").isin([3, 4, 5]), F.col("seasonal_index_spring"))
         .when(F.col("order_placed_month").isin([6, 7, 8]), F.col("seasonal_index_summer"))
         .when(F.col("order_placed_month").isin([9, 10, 11]), F.col("seasonal_index_fall"))
         .otherwise(F.col("seasonal_index_winter"))
    )
    
    # Derive quarter from month before using it
    product_features = product_features.withColumn(
        "order_placed_quarter",
        F.when(F.col("order_placed_month").isin([1, 2, 3]), 1)
         .when(F.col("order_placed_month").isin([4, 5, 6]), 2)
         .when(F.col("order_placed_month").isin([7, 8, 9]), 3)
         .otherwise(4)
    )
    
    # Seasonal encoding (quarter now available)
    product_features = product_features.withColumn(
        "month_sin",
        F.sin(2 * math.pi * F.col("order_placed_month") / 12)
    ).withColumn(
        "month_cos",
        F.cos(2 * math.pi * F.col("order_placed_month") / 12)
    ).withColumn(
        "quarter_sin",
        F.sin(2 * math.pi * F.col("order_placed_quarter") / 4)
    ).withColumn(
        "quarter_cos",
        F.cos(2 * math.pi * F.col("order_placed_quarter") / 4)
    )
    
    # Interaction features
    product_features = product_features.withColumn(
        "price_x_seasonality",
        F.col("sell_price") * F.col("category_seasonal_current")
    ).withColumn(
        "category_seasonal_x_month",
        F.col("category_seasonal_current") * F.col("order_placed_month")
    )
    
    # Add remaining temporal columns
    product_features = product_features.withColumn(
        "order_placed_week_of_year",
        F.weekofyear(F.to_date(F.concat_ws("-", F.col("year_month"), F.lit("01"))))
    ).withColumn(
        "order_placed_day_of_week",
        F.dayofweek(F.to_date(F.concat_ws("-", F.col("year_month"), F.lit("01"))))
    )
    
    # Rename columns
    product_features = product_features.withColumnRenamed("monthly_orders", "total_orders")
    
    # Fill nulls
    for lag_col in ["demand_lag_1m", "demand_lag_3m", "demand_lag_6m", "demand_rolling_6m"]:
        product_features = product_features.fillna({lag_col: 0})
    
    print(f"✓ Inference features created: {product_features.count()} product records")
    return product_features


def prepare_inference_data(df):
    """
    Prepare and scale features for inference
    """
    # Fill missing values
    df_filled = df.fillna(0, subset=FEATURE_COLUMNS)
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_filled)
    
    # Apply StandardScaler (same as training)
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select("product_id", "features", "category_seasonal_current", "growth_rate_1m")
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared


def generate_predictions(model, df, model_name, FORECAST_HORIZON_DAYS):
    """Generate predictions with seasonal/trend factors"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    forecast_date = F.lit(datetime.now() + timedelta(days=FORECAST_HORIZON_DAYS))
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        forecast_date.cast("date").alias("forecast_date"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction").alias("predicted_demand_units"),
        (F.col("prediction") * 0.85).alias("confidence_interval_lower"),
        (F.col("prediction") * 1.15).alias("confidence_interval_upper"),
        F.lit(FORECAST_HORIZON_DAYS).alias("forecast_horizon_days"),
        F.col("category_seasonal_current").alias("seasonality_factor"),
        (1 + F.col("growth_rate_1m")).alias("trend_factor"),
        F.lit(0.90).alias("confidence_score"),
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
    print("\n" + "="*60)
    print(f"Sample Predictions (first {n} products)")
    print("="*60)
    
    sample = df.select(
        "product_id",
        "predicted_demand_units",
        "seasonality_factor",
        "trend_factor"
    ).limit(n).collect()
    
    for row in sample:
        print(
            f"Product: {row['product_id']:<30} "
            f"Demand: {row['predicted_demand_units']:>8.0f} "
            f"Seasonal: {row['seasonality_factor']:>5.2f} "
            f"Trend: {row['trend_factor']:>5.2f}"
        )


def main(BUCKET_NAME):
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    INPUT_CATEGORIES_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_categories.parquet"
    INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/demand_forecast/"
    MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/demand_forecast/"

    # ⚠️ MANUAL CONFIGURATION REQUIRED:
    MODEL_NAME = "gbt"  # Options: "linear_regression", "random_forest", "gbt"

    FORECAST_HORIZON_DAYS = 30
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("Demand Forecasting V2 - Inference")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 60)
    
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    categories_df, _ = validate_dataset(spark, INPUT_CATEGORIES_PATH, "Categories")
    monthly_agg_df, _ = validate_dataset(spark, INPUT_MONTHLY_AGG_PATH, "Monthly Aggregations")
    
    if None in [products_df, orders_df, order_items_df, categories_df, monthly_agg_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_inference_features(
        orders_df, order_items_df, products_df, categories_df, monthly_agg_df
    )
    
    # Prepare data
    print("\nStep 4: Data Preparation & Scaling")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME, FORECAST_HORIZON_DAYS)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Save predictions
    print("\nStep 6: Save Predictions")
    print("-" * 60)
    
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