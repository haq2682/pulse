"""
Seasonal Trends - Inference Script (Specific Business Model)
Generates seasonal index predictions for upcoming months using trained models.

The seasonal index represents how much a given month's revenue deviates
from the trailing 12-month average:
  - Index > 1.0 means above-average month (e.g., holiday season)
  - Index < 1.0 means below-average month (e.g., slow season)
  - Index = 1.0 means average performance
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from pyspark.ml import PipelineModel
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import math

# Load environment variables
load_dotenv()


# Feature columns (must match training)
# Uses fields from agg_monthly_aggregations schema
FEATURE_COLUMNS = [
    # Seasonal encoding (cyclical, derived)
    "order_month",
    "month_sin",
    "month_cos",
    "quarter_sin",
    "quarter_cos",

    # Activity metrics (from agg_monthly_aggregations)
    "total_orders",
    "total_customers",
    "avg_order_value",
    "total_units_sold",
    "total_sessions",
    "total_conversions",
    "session_to_order_rate",

    # Customer metrics (from agg_monthly_aggregations)
    "new_customers",
    "returning_customers",
    "customer_retention_rate",
    "churn_rate",
    "prev_month_customers",

    # Revenue metrics (from agg_monthly_aggregations)
    "prev_month_revenue",
    "revenue_growth_rate",

    # Revenue lag features (derived from total_revenue)
    "revenue_lag_3m",
    "revenue_lag_6m",
    "revenue_lag_12m",

    # Rolling averages (derived)
    "orders_rolling_3m",
    "orders_rolling_6m",
    "customers_rolling_3m",

    # Growth features (derived)
    "revenue_growth_3m",
    "orders_growth_1m",

    # Year-over-year features (derived)
    "yoy_revenue_ratio",
    "yoy_orders_ratio",
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Seasonal_Trends_Inference")
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
    """Load trained pipeline model from MinIO"""
    model_path = f"{MODEL_BASE_PATH}{model_name}"

    try:
        # Load as PipelineModel (which includes the scaler)
        model = PipelineModel.load(model_path)
        print(f"✓ Pipeline model loaded: {model_path}")
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
    Create same seasonal features as training for the most recent month.

    Uses fields directly from agg_monthly_aggregations schema:
      - prev_month_revenue, revenue_growth_rate, prev_month_customers (schema fields)
      - total_sessions, total_conversions, session_to_order_rate, churn_rate (schema fields)

    Derives additional features via window functions:
      - revenue_lag_3m/6m/12m, rolling averages, YoY ratios, orders growth
    """
    print("Creating inference features...")

    # Sort by year_month
    monthly_sorted = monthly_df.orderBy("year_month")

    # Add partition key for window operations
    monthly_sorted = monthly_sorted.withColumn("partition_key", F.lit(1))

    # Window specs
    window_spec = Window.partitionBy("partition_key").orderBy("year_month")
    window_rolling_3m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-3, -1)
    window_rolling_6m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-6, -1)
    window_rolling_12m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-12, -1)

    # Revenue lag features (3m, 6m, 12m derived; 1m already in schema as prev_month_revenue)
    df = monthly_sorted.withColumn(
        "revenue_lag_3m",
        F.lag("total_revenue", 3).over(window_spec)
    ).withColumn(
        "revenue_lag_6m",
        F.lag("total_revenue", 6).over(window_spec)
    ).withColumn(
        "revenue_lag_12m",
        F.lag("total_revenue", 12).over(window_spec)
    )

    # Rolling average for revenue (12m, used for predictions)
    df = df.withColumn(
        "revenue_rolling_12m",
        F.avg("total_revenue").over(window_rolling_12m)
    )

    # Rolling averages for orders
    df = df.withColumn(
        "orders_rolling_3m",
        F.avg("total_orders").over(window_rolling_3m)
    ).withColumn(
        "orders_rolling_6m",
        F.avg("total_orders").over(window_rolling_6m)
    )

    # Rolling averages for customers
    df = df.withColumn(
        "customers_rolling_3m",
        F.avg("total_customers").over(window_rolling_3m)
    )

    # 3-month revenue growth (derived)
    # revenue_growth_rate (1m) already exists in schema
    df = df.withColumn(
        "revenue_growth_3m",
        F.when(
            (F.col("revenue_lag_3m").isNotNull()) & (F.col("revenue_lag_3m") > 0),
            (F.col("total_revenue") - F.col("revenue_lag_3m")) / F.col("revenue_lag_3m")
        ).otherwise(0)
    )

    # Orders growth (derived)
    orders_lag_1m = F.lag("total_orders", 1).over(window_spec)
    df = df.withColumn(
        "orders_lag_1m_temp",
        orders_lag_1m
    ).withColumn(
        "orders_growth_1m",
        F.when(
            (F.col("orders_lag_1m_temp").isNotNull()) & (F.col("orders_lag_1m_temp") > 0),
            (F.col("total_orders") - F.col("orders_lag_1m_temp")) / F.col("orders_lag_1m_temp")
        ).otherwise(0)
    ).drop("orders_lag_1m_temp")

    # Year-over-year features (derived)
    df = df.withColumn(
        "yoy_revenue_ratio",
        F.when(
            (F.col("revenue_lag_12m").isNotNull()) & (F.col("revenue_lag_12m") > 0),
            F.col("total_revenue") / F.col("revenue_lag_12m")
        ).otherwise(1.0)
    )

    orders_lag_12m = F.lag("total_orders", 12).over(window_spec)
    df = df.withColumn(
        "orders_lag_12m_temp",
        orders_lag_12m
    ).withColumn(
        "yoy_orders_ratio",
        F.when(
            (F.col("orders_lag_12m_temp").isNotNull()) & (F.col("orders_lag_12m_temp") > 0),
            F.col("total_orders") / F.col("orders_lag_12m_temp")
        ).otherwise(1.0)
    ).drop("orders_lag_12m_temp")

    # Seasonal encoding (derived)
    df = df.withColumn(
        "month_sin",
        F.sin(2 * math.pi * F.col("order_month") / 12)
    ).withColumn(
        "month_cos",
        F.cos(2 * math.pi * F.col("order_month") / 12)
    )

    # Quarter encoding
    df = df.withColumn(
        "order_quarter",
        F.when(F.col("order_month").isin([1, 2, 3]), 1)
         .when(F.col("order_month").isin([4, 5, 6]), 2)
         .when(F.col("order_month").isin([7, 8, 9]), 3)
         .otherwise(4)
    ).withColumn(
        "quarter_sin",
        F.sin(2 * math.pi * F.col("order_quarter") / 4)
    ).withColumn(
        "quarter_cos",
        F.cos(2 * math.pi * F.col("order_quarter") / 4)
    )

    # Fill nulls in derived lag/rolling features
    derived_lag_columns = [
        "revenue_lag_3m", "revenue_lag_6m", "revenue_lag_12m",
        "revenue_rolling_12m", "orders_rolling_3m", "orders_rolling_6m",
        "customers_rolling_3m", "revenue_growth_3m", "orders_growth_1m"
    ]

    for col in derived_lag_columns:
        df = df.fillna({col: 0})

    # Fill nulls in schema fields (nullable fields from agg_monthly_aggregations)
    df = df.fillna({
        "total_revenue": 0,
        "avg_order_value": 0,
        "session_to_order_rate": 0,
        "prev_month_revenue": 0,
        "revenue_growth_rate": 0,
        "customer_retention_rate": 0,
        "churn_rate": 0,
        "yoy_revenue_ratio": 1.0,
        "yoy_orders_ratio": 1.0,
    })

    # Get most recent month for prediction
    window_latest = Window.orderBy(F.desc("year_month"))

    df_latest = df.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(F.col("row_num") == 1).drop("row_num", "partition_key", "order_quarter")

    print(f"✓ Inference features created for latest month")
    return df_latest


def prepare_inference_data(df):
    """
    Prepare features for inference.
    Note: Feature scaling is handled by the Pipeline model.
    """
    # Fill missing values
    df_filled = df.fillna(0, subset=FEATURE_COLUMNS)

    # Select required columns (metadata columns + all feature columns)
    # Note: Some metadata columns may overlap with FEATURE_COLUMNS (e.g., total_orders)
    metadata_cols = ["year_month", "total_revenue", "revenue_rolling_12m", "revenue_growth_rate"]
    all_cols = metadata_cols + FEATURE_COLUMNS
    
    # Remove duplicates while preserving order
    seen = set()
    unique_cols = []
    for col in all_cols:
        if col not in seen:
            seen.add(col)
            unique_cols.append(col)
    
    df_prepared = df_filled.select(*unique_cols)

    print(f"✓ Data prepared for inference")
    return df_prepared


def generate_predictions(model, df, model_name, FORECAST_HORIZON_DAYS):
    """Generate seasonal index predictions with metadata"""
    predictions_df = model.transform(df)

    # Parse year_month to get forecast date
    year_month_col = predictions_df.select("year_month").collect()[0]["year_month"]

    # Calculate forecast date (next month from latest data)
    try:
        base_date = datetime.strptime(year_month_col, "%Y-%m")
        forecast_date = base_date + relativedelta(months=1)
        forecast_month = forecast_date.month
    except:
        forecast_date = datetime.now() + relativedelta(months=1)
        forecast_month = forecast_date.month

    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    forecast_date_lit = F.lit(forecast_date.date())

    # Clamp seasonal index to reasonable range (0.3 - 3.0)
    output_df = predictions_df.withColumn(
        "predicted_seasonal_index",
        F.greatest(F.lit(0.3), F.least(F.lit(3.0), F.col("prediction")))
    )

    # Classify season strength
    output_df = output_df.withColumn(
        "season_classification",
        F.when(F.col("predicted_seasonal_index") >= 1.3, "peak_season")
         .when(F.col("predicted_seasonal_index") >= 1.1, "above_average")
         .when(F.col("predicted_seasonal_index") >= 0.9, "average")
         .when(F.col("predicted_seasonal_index") >= 0.7, "below_average")
         .otherwise("low_season")
    )

    # Estimated revenue impact based on seasonal index
    output_df = output_df.withColumn(
        "estimated_revenue",
        F.when(
            (F.col("revenue_rolling_12m").isNotNull()) & (F.col("revenue_rolling_12m") > 0),
            F.col("predicted_seasonal_index") * F.col("revenue_rolling_12m")
        ).otherwise(F.col("total_revenue") * F.col("predicted_seasonal_index"))
    )

    output_df = output_df.select(
        prediction_id_udf().alias("prediction_id"),
        forecast_date_lit.alias("forecast_date"),
        F.lit(forecast_month).alias("forecast_month"),
        current_timestamp.alias("prediction_date"),
        F.col("predicted_seasonal_index"),
        F.col("season_classification"),
        F.col("estimated_revenue"),
        (F.col("predicted_seasonal_index") * 0.90).alias("confidence_interval_lower"),
        (F.col("predicted_seasonal_index") * 1.10).alias("confidence_interval_upper"),
        F.lit(FORECAST_HORIZON_DAYS).alias("forecast_horizon_days"),
        (1 + F.col("revenue_growth_rate")).alias("trend_factor"),
        F.lit(0.90).alias("confidence_score"),
        F.lit(model_name).alias("model_version")
    )

    print(f"✓ Generated seasonal prediction for {forecast_date.strftime('%Y-%m')}")
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
    print("Seasonal Trends Forecast Summary")
    print("="*60)

    row = df.collect()[0]

    print(f"Forecast Date: {row['forecast_date']}")
    print(f"Forecast Month: {row['forecast_month']}")
    print(f"Predicted Seasonal Index: {row['predicted_seasonal_index']:.3f}")
    print(f"Season Classification: {row['season_classification']}")
    print(f"Estimated Revenue: ${row['estimated_revenue']:,.2f}")
    print(f"Confidence Interval: {row['confidence_interval_lower']:.3f} - {row['confidence_interval_upper']:.3f}")
    print(f"Trend Factor: {row['trend_factor']:.3f}")
    print(f"Confidence Score: {row['confidence_score']:.2%}")


def main(BUCKET_NAME):
    INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/seasonal_trends/"
    MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/seasonal_trends/"

    # ⚠️ MANUAL CONFIGURATION REQUIRED:
    MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

    FORECAST_HORIZON_DAYS = 30  # Forecasting next month
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("Seasonal Trends - Inference (Specific Business Model)")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Model: {MODEL_NAME}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load model from business's own bucket
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)

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
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features)

    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME, FORECAST_HORIZON_DAYS)

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
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)
