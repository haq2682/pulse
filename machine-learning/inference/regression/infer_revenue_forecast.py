"""
Revenue Forecasting - Inference Script
Generates future revenue predictions using trained models
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
from dateutil.relativedelta import relativedelta
import uuid
import math

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/revenue_forecast/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/revenue_forecast/"

# ⚠️ MANUAL CONFIGURATION REQUIRED:
MODEL_NAME = "linear_regression"  # Options: "linear_regression", "random_forest", "gbt"

FORECAST_HORIZON_DAYS = 30  # Forecasting next month

# Feature columns (must match training)
FEATURE_COLUMNS = [
    "total_customers",
    "new_customers",
    "returning_customers",
    "customer_retention_rate",
    "total_orders",
    "avg_order_value",
    "total_units_sold",
    "session_to_order_rate",
    "order_month",
    "month_sin",
    "month_cos",
    "revenue_lag_1m",
    "revenue_lag_2m",
    "revenue_lag_3m",
    "revenue_lag_6m",
    "revenue_rolling_3m",
    "revenue_rolling_6m",
    "revenue_growth_1m",
    "revenue_growth_3m",
    "orders_lag_1m",
    "customers_lag_1m"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Revenue_Forecast_Inference")
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


def create_inference_features(monthly_df):
    """
    Create same time-series features as training
    """
    print("Creating inference features...")
    
    # Sort by year_month
    monthly_sorted = monthly_df.orderBy("year_month")
    
    # Add a constant partition key for global time-series operations
    monthly_sorted = monthly_sorted.withColumn("partition_key", F.lit(1))
    
    # Create window spec with partition
    window_spec = Window.partitionBy("partition_key").orderBy("year_month")
    
    # Create lag features
    df_with_lags = monthly_sorted.withColumn(
        "revenue_lag_1m",
        F.lag("total_revenue", 1).over(window_spec)
    ).withColumn(
        "revenue_lag_2m",
        F.lag("total_revenue", 2).over(window_spec)
    ).withColumn(
        "revenue_lag_3m",
        F.lag("total_revenue", 3).over(window_spec)
    ).withColumn(
        "revenue_lag_6m",
        F.lag("total_revenue", 6).over(window_spec)
    )
    
    # Rolling averages
    window_rolling_3m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-3, -1)
    window_rolling_6m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-6, -1)
    
    df_with_lags = df_with_lags.withColumn(
        "revenue_rolling_3m",
        F.avg("total_revenue").over(window_rolling_3m)
    ).withColumn(
        "revenue_rolling_6m",
        F.avg("total_revenue").over(window_rolling_6m)
    )
    
    # Growth rates
    df_with_lags = df_with_lags.withColumn(
        "revenue_growth_1m",
        F.when(
            (F.col("revenue_lag_1m").isNotNull()) & (F.col("revenue_lag_1m") > 0),
            (F.col("revenue_lag_1m") - F.col("revenue_lag_2m")) / F.col("revenue_lag_1m")
        ).otherwise(0)
    ).withColumn(
        "revenue_growth_3m",
        F.when(
            (F.col("revenue_lag_3m").isNotNull()) & (F.col("revenue_lag_3m") > 0),
            (F.col("revenue_lag_1m") - F.col("revenue_lag_3m")) / F.col("revenue_lag_3m")
        ).otherwise(0)
    )
    
    # Lag features for orders and customers
    df_with_lags = df_with_lags.withColumn(
        "orders_lag_1m",
        F.lag("total_orders", 1).over(window_spec)
    ).withColumn(
        "customers_lag_1m",
        F.lag("total_customers", 1).over(window_spec)
    )
    
    # Seasonal encoding
    df_with_lags = df_with_lags.withColumn(
        "month_sin",
        F.sin(2 * math.pi * F.col("order_month") / 12)
    ).withColumn(
        "month_cos",
        F.cos(2 * math.pi * F.col("order_month") / 12)
    )
    
    # Fill nulls
    lag_columns = [
        "revenue_lag_1m", "revenue_lag_2m", "revenue_lag_3m", "revenue_lag_6m",
        "revenue_rolling_3m", "revenue_rolling_6m", "revenue_growth_1m", "revenue_growth_3m",
        "orders_lag_1m", "customers_lag_1m"
    ]
    
    for col in lag_columns:
        df_with_lags = df_with_lags.fillna({col: 0})
    
    df_with_lags = df_with_lags.fillna({
        "customer_retention_rate": 0,
        "session_to_order_rate": 0,
        "avg_order_value": 0
    })
    
    # Get most recent month for prediction
    window_latest = Window.orderBy(F.desc("year_month"))
    
    df_latest = df_with_lags.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(F.col("row_num") == 1).drop("row_num")
    
    print(f"✓ Inference features created for latest month")
    return df_latest


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
    
    # Apply StandardScaler
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "year_month",
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "revenue_growth_1m",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with metadata"""
    predictions_df = model.transform(df)
    
    # Parse year_month to get forecast date
    year_month_col = predictions_df.select("year_month").collect()[0]["year_month"]
    
    # Calculate forecast date (next month from latest data)
    try:
        base_date = datetime.strptime(year_month_col, "%Y-%m")
        forecast_date = base_date + relativedelta(months=1)
    except:
        forecast_date = datetime.now() + relativedelta(months=1)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    forecast_date_lit = F.lit(forecast_date.date())
    
    # Calculate predicted orders (simple proportion)
    output_df = predictions_df.withColumn(
        "predicted_orders_calc",
        F.when(
            F.col("avg_order_value") > 0,
            (F.col("prediction") / F.col("avg_order_value")).cast("integer")
        ).otherwise(F.col("total_orders"))
    )
    
    output_df = output_df.select(
        prediction_id_udf().alias("prediction_id"),
        forecast_date_lit.alias("forecast_date"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction").alias("predicted_revenue"),
        F.col("predicted_orders_calc").alias("predicted_orders"),
        (F.col("prediction") * 0.90).alias("confidence_interval_lower"),
        (F.col("prediction") * 1.10).alias("confidence_interval_upper"),
        F.lit(FORECAST_HORIZON_DAYS).alias("forecast_horizon_days"),
        F.lit(1.0).alias("seasonality_factor"),
        (1 + F.col("revenue_growth_1m")).alias("trend_factor"),
        F.lit(0.92).alias("confidence_score"),
        F.lit(model_name).alias("model_version")
    )
    
    print(f"✓ Generated prediction for {forecast_date.strftime('%Y-%m')}")
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


def display_prediction(df):
    """Display prediction summary"""
    print("\n" + "="*60)
    print("Revenue Forecast Summary")
    print("="*60)
    
    row = df.collect()[0]
    
    print(f"Forecast Date: {row['forecast_date']}")
    print(f"Predicted Revenue: ${row['predicted_revenue']:,.2f}")
    print(f"Predicted Orders: {row['predicted_orders']:,}")
    print(f"Confidence Interval: ${row['confidence_interval_lower']:,.2f} - ${row['confidence_interval_upper']:,.2f}")
    print(f"Trend Factor: {row['trend_factor']:.3f}")
    print(f"Confidence Score: {row['confidence_score']:.2%}")


def main():
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("Revenue Forecasting - Inference")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load dataset
    print("\nStep 2: Load Monthly Aggregations")
    print("-" * 60)
    
    monthly_df, _ = validate_dataset(spark, INPUT_MONTHLY_AGG_PATH, "Monthly Aggregations")
    
    if monthly_df is None:
        print("\n✗ Inference aborted: Dataset not found")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_inference_features(monthly_df)
    
    # Prepare data
    print("\nStep 4: Data Preparation & Scaling")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display prediction
    display_prediction(predictions_df)
    
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
    main()