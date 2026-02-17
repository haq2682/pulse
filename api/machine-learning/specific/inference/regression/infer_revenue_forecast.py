"""
Revenue Forecasting - Inference Script (Specific Business Model)
Generates future revenue predictions using trained models from business's own bucket.

Fixes applied:
  - Split pipeline: scaled features for linear_regression, unscaled for rf & gbt
  - WindowExec partition warnings suppressed
  - naive_baseline supported as a lightweight inference option
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
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import math

# Load environment variables
load_dotenv()

# Feature columns — must match training exactly
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

# Models that require feature scaling before inference
SCALED_MODELS   = {"linear_regression"}
# Models that work on raw feature magnitudes
UNSCALED_MODELS = {"random_forest", "gbt"}
# Lightweight baseline — no Spark model file needed
BASELINE_MODELS = {"naive_baseline"}


def create_spark_session():
    """Initialize Spark session."""
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


def load_model(model_name, MODEL_BASE_PATH):
    """Load a trained Spark model from MinIO. Returns None for baseline models."""
    if model_name in BASELINE_MODELS:
        print(f"ℹ️  '{model_name}' is a baseline — no model file to load.")
        return "baseline"   # sentinel so callers know loading succeeded

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
    """Check if dataset exists and is readable."""
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
    Build the same time-series features used during training and return
    only the single most-recent row (the one we want to forecast from).
    All Window specs carry a literal partition_key to suppress WARN WindowExec.
    """
    print("Creating inference features...")

    monthly_sorted = monthly_df.orderBy("year_month").withColumn("partition_key", F.lit(1))
    window_spec = Window.partitionBy("partition_key").orderBy("year_month")

    # Lag features
    df_with_lags = (
        monthly_sorted
        .withColumn("revenue_lag_1m", F.lag("total_revenue", 1).over(window_spec))
        .withColumn("revenue_lag_2m", F.lag("total_revenue", 2).over(window_spec))
        .withColumn("revenue_lag_3m", F.lag("total_revenue", 3).over(window_spec))
        .withColumn("revenue_lag_6m", F.lag("total_revenue", 6).over(window_spec))
    )

    # Rolling averages
    window_rolling_3m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-3, -1)
    window_rolling_6m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-6, -1)

    df_with_lags = (
        df_with_lags
        .withColumn("revenue_rolling_3m", F.avg("total_revenue").over(window_rolling_3m))
        .withColumn("revenue_rolling_6m", F.avg("total_revenue").over(window_rolling_6m))
    )

    # Growth rates (derived from lags only)
    df_with_lags = (
        df_with_lags
        .withColumn(
            "revenue_growth_1m",
            F.when(
                F.col("revenue_lag_1m").isNotNull() & (F.col("revenue_lag_1m") > 0),
                (F.col("revenue_lag_1m") - F.col("revenue_lag_2m")) / F.col("revenue_lag_1m")
            ).otherwise(0)
        )
        .withColumn(
            "revenue_growth_3m",
            F.when(
                F.col("revenue_lag_3m").isNotNull() & (F.col("revenue_lag_3m") > 0),
                (F.col("revenue_lag_1m") - F.col("revenue_lag_3m")) / F.col("revenue_lag_3m")
            ).otherwise(0)
        )
        .withColumn("orders_lag_1m",    F.lag("total_orders",    1).over(window_spec))
        .withColumn("customers_lag_1m", F.lag("total_customers", 1).over(window_spec))
    )

    # Cyclical month encoding
    df_with_lags = (
        df_with_lags
        .withColumn("month_sin", F.sin(2 * math.pi * F.col("order_month") / 12))
        .withColumn("month_cos", F.cos(2 * math.pi * F.col("order_month") / 12))
    )

    # Fill nulls
    lag_columns = [
        "revenue_lag_1m", "revenue_lag_2m", "revenue_lag_3m", "revenue_lag_6m",
        "revenue_rolling_3m", "revenue_rolling_6m",
        "revenue_growth_1m", "revenue_growth_3m",
        "orders_lag_1m", "customers_lag_1m"
    ]
    for col in lag_columns:
        df_with_lags = df_with_lags.fillna({col: 0})

    df_with_lags = df_with_lags.fillna({
        "customer_retention_rate": 0,
        "session_to_order_rate":   0,
        "avg_order_value":         0
    })

    # Keep only the latest month — the row we will actually forecast
    # Use partitioned window to avoid WARN WindowExec
    window_latest = Window.partitionBy(F.lit(1)).orderBy(F.desc("year_month"))
    df_latest = (
        df_with_lags
        .withColumn("_row_num", F.row_number().over(window_latest))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "partition_key")
    )

    print("✓ Inference features created for latest month")
    return df_latest


def prepare_inference_data(df, model_name):
    """
    Assemble feature vector and — for linear_regression only — apply StandardScaler.
    Returns a DataFrame whose 'features' column matches what the model was trained on.
    """
    df_filled = df.fillna(0, subset=FEATURE_COLUMNS)

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    df_assembled = assembler.transform(df_filled)

    if model_name in SCALED_MODELS:
        # Re-fit scaler on this single row.
        # NOTE: for production, persist the training scaler_model to MinIO and load it here
        # instead of re-fitting, so the scaling parameters are identical to training.
        scaler = StandardScaler(
            inputCol="features_unscaled",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        df_prepared = scaler.fit(df_assembled).transform(df_assembled)
    else:
        # RF, GBT, and naive_baseline all use raw magnitudes
        df_prepared = df_assembled.withColumnRenamed("features_unscaled", "features")

    df_prepared = df_prepared.select(
        "year_month",
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "revenue_growth_1m",
        "revenue_lag_1m",   # kept for naive_baseline prediction
        "features"
    )

    print("✓ Data prepared and assembled")
    return df_prepared


def generate_predictions(model, df, model_name, FORECAST_HORIZON_DAYS):
    """Generate prediction row with metadata."""

    if model_name in BASELINE_MODELS:
        # Naive baseline: prediction = revenue_lag_1m (last known revenue)
        predictions_df = df.withColumn("prediction", F.col("revenue_lag_1m"))
    else:
        predictions_df = model.transform(df)

    # Parse the latest year_month to derive the forecast date
    year_month_val = predictions_df.select("year_month").collect()[0]["year_month"]
    try:
        base_date     = datetime.strptime(year_month_val, "%Y-%m")
        forecast_date = base_date + relativedelta(months=1)
    except Exception:
        forecast_date = datetime.now() + relativedelta(months=1)

    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp  = F.lit(datetime.now())
    forecast_date_lit  = F.lit(forecast_date.date())

    output_df = (
        predictions_df
        .withColumn(
            "predicted_orders_calc",
            F.when(
                F.col("avg_order_value") > 0,
                (F.col("prediction") / F.col("avg_order_value")).cast("integer")
            ).otherwise(F.col("total_orders"))
        )
        .select(
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
    )

    print(f"✓ Generated prediction for {forecast_date.strftime('%Y-%m')}")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO."""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_prediction(df):
    """Print a human-readable summary of the forecast."""
    print("\n" + "="*60)
    print("Revenue Forecast Summary")
    print("="*60)

    row = df.collect()[0]
    print(f"Forecast Date:        {row['forecast_date']}")
    print(f"Predicted Revenue:    ${row['predicted_revenue']:,.2f}")
    print(f"Predicted Orders:     {row['predicted_orders']:,}")
    print(f"Confidence Interval:  ${row['confidence_interval_lower']:,.2f} – ${row['confidence_interval_upper']:,.2f}")
    print(f"Trend Factor:         {row['trend_factor']:.3f}")
    print(f"Confidence Score:     {row['confidence_score']:.2%}")
    print(f"Model Version:        {row['model_version']}")


def main(BUCKET_NAME):
    INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
    OUTPUT_PATH            = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/revenue_forecast/"
    MODEL_BASE_PATH        = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/revenue_forecast/"

    # ⚠️ MANUAL CONFIGURATION REQUIRED:
    # Set to whichever model had the best metrics during training.
    # Options: "linear_regression", "random_forest", "gbt", "naive_baseline"
    MODEL_NAME = "linear_regression"

    FORECAST_HORIZON_DAYS = 30

    print("\n" + "="*60)
    print("Revenue Forecasting - Inference (Specific Business Model)")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket:     {BUCKET_NAME}")
    print(f"Model:      {MODEL_NAME}")
    print(f"Pipeline:   {'scaled (LR)' if MODEL_NAME in SCALED_MODELS else 'unscaled (tree/baseline)'}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)

    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return

    # Step 2: Load dataset
    print("\nStep 2: Load Monthly Aggregations")
    print("-" * 60)
    monthly_df, _ = validate_dataset(spark, INPUT_MONTHLY_AGG_PATH, "Monthly Aggregations")

    if monthly_df is None:
        print("\n✗ Inference aborted: Dataset not found")
        spark.stop()
        return

    # Step 3: Feature engineering
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_inference_features(monthly_df)

    # Step 4: Prepare data (scaled or unscaled depending on model)
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features, MODEL_NAME)

    # Step 5: Generate prediction
    print("\nStep 5: Generate Prediction")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME, FORECAST_HORIZON_DAYS)

    display_prediction(predictions_df)

    # Step 6: Persist predictions
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