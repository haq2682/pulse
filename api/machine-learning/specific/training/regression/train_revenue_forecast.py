"""
Revenue Forecasting - Training Script (Specific Business Model)
Predicts future revenue using time-series features from monthly aggregations.
Trains on individual business data with time-based train/test split.

Fixes applied:
  - Split pipeline: Scaled → LinearRegression, Unscaled → RF & GBT
  - Reduced tree complexity (RF: numTrees=20/maxDepth=4, GBT: maxIter=30/maxDepth=3)
  - Increased MIN_RECORDS to 24
  - Added naive baseline (revenue_lag_1m)
  - Fixed WindowExec partition warnings
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime
import math

# Load environment variables
load_dotenv()

# Configuration
MODEL_NAME = "revenue_forecast"
MIN_RECORDS = 24   # Increased from 12 — need enough history for lag features to be meaningful
MAX_RECORDS = 1000
USE_CROSS_VALIDATION = False

# Feature columns (NO revenue-based features to prevent leakage)
FEATURE_COLUMNS = [
    # Customer metrics
    "total_customers",
    "new_customers",
    "returning_customers",
    "customer_retention_rate",

    # Operational metrics
    "total_orders",
    "avg_order_value",
    "total_units_sold",
    "session_to_order_rate",

    # Temporal features
    "order_month",
    "month_sin",
    "month_cos",

    # Lag features (revenue from past periods)
    "revenue_lag_1m",
    "revenue_lag_2m",
    "revenue_lag_3m",
    "revenue_lag_6m",
    "revenue_rolling_3m",
    "revenue_rolling_6m",

    # Growth features (derived from lags, not current revenue)
    "revenue_growth_1m",
    "revenue_growth_3m",
    "orders_lag_1m",
    "customers_lag_1m"
]

TARGET_COLUMN = "future_revenue"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return (
        SparkSession.builder
        .appName("Revenue_Forecast_Training")
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
    """Check if dataset exists and is readable"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ {name} dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ {name} dataset validation failed: {str(e)}")
        return None, 0


def create_time_series_features(monthly_df):
    """
    Create time-series features with lags for revenue forecasting.
    All Window operations use a literal partition_key to suppress the
    'No Partition Defined' warning and avoid a full data shuffle.
    """
    print("Creating time-series features...")

    # A single literal partition keeps all rows together while satisfying
    # Spark's requirement for a PARTITION BY clause — eliminates WARN WindowExec.
    monthly_sorted = monthly_df.withColumn("partition_key", F.lit(1)).orderBy("year_month")

    window_spec = Window.partitionBy("partition_key").orderBy("year_month")

    # Lag features for revenue
    df_with_lags = (
        monthly_sorted
        .withColumn("revenue_lag_1m", F.lag("total_revenue", 1).over(window_spec))
        .withColumn("revenue_lag_2m", F.lag("total_revenue", 2).over(window_spec))
        .withColumn("revenue_lag_3m", F.lag("total_revenue", 3).over(window_spec))
        .withColumn("revenue_lag_6m", F.lag("total_revenue", 6).over(window_spec))
    )

    # Rolling averages (rows preceding current, so no look-ahead)
    window_rolling_3m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-3, -1)
    window_rolling_6m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-6, -1)

    df_with_lags = (
        df_with_lags
        .withColumn("revenue_rolling_3m", F.avg("total_revenue").over(window_rolling_3m))
        .withColumn("revenue_rolling_6m", F.avg("total_revenue").over(window_rolling_6m))
    )

    # Growth rates derived purely from lag columns — no leakage
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

    # Target: NEXT month's revenue
    df_with_lags = df_with_lags.withColumn(
        "future_revenue",
        F.lead("total_revenue", 1).over(window_spec)
    )

    df_with_lags = df_with_lags.drop("partition_key")

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

    print(f"✓ Time-series features created: {df_with_lags.count()} records")
    return df_with_lags


def prepare_training_data(df):
    """
    Prepare data for training.
    Returns:
        df_scaled   — scaled features for LinearRegression
        df_unscaled — raw assembled features for RF & GBT
        scaler_model — fitted StandardScaler (persisted for inference)
    """
    df_valid = df.filter(
        F.col(TARGET_COLUMN).isNotNull() & (F.col(TARGET_COLUMN) > 0)
    )

    valid_count = df_valid.count()
    print(f"Records with valid future revenue: {valid_count}")

    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient training data: {valid_count} < {MIN_RECORDS}")
        return None

    df_filled = df_valid.fillna(0, subset=FEATURE_COLUMNS)

    # ── Assemble raw feature vector ──────────────────────────────────────────
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    df_assembled = assembler.transform(df_filled)

    # ── Scale (used only by LinearRegression) ────────────────────────────────
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features_scaled",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_assembled)
    df_with_both = scaler_model.transform(df_assembled)

    # Keep year_month for chronological splitting
    base_cols = ["year_month", TARGET_COLUMN]

    # Scaled version — LinearRegression expects "features" col
    df_scaled = (
        df_with_both
        .withColumnRenamed("features_scaled", "features")
        .select(base_cols + ["features"])
    )

    # Unscaled version — RF/GBT use raw magnitudes; rename for consistency
    df_unscaled = (
        df_with_both
        .withColumnRenamed("features_unscaled", "features")
        .select(base_cols + ["features"])
    )

    print(f"✓ Data prepared — scaled & unscaled sets: {df_scaled.count()} records each")
    return df_scaled, df_unscaled, scaler_model


def time_based_split(df, train_ratio=0.8):
    """
    Chronological train/test split — prevents temporal leakage.
    Uses a literal partition_key to avoid WindowExec warning.
    """
    total_count = df.count()
    split_point = int(total_count * train_ratio)

    window_spec = Window.partitionBy(F.lit(1)).orderBy("year_month")
    df_with_row = df.withColumn("_row_num", F.row_number().over(window_spec))

    train_df = df_with_row.filter(F.col("_row_num") <= split_point).drop("_row_num")
    test_df  = df_with_row.filter(F.col("_row_num") >  split_point).drop("_row_num")

    return train_df, test_df


# ── Baseline ─────────────────────────────────────────────────────────────────

def evaluate_naive_baseline(train_df_raw, test_df_raw, df_features):
    """
    Naive baseline: predict next month's revenue = last month's revenue (lag_1m).
    Works directly on the feature-engineered DataFrame before vector assembly.
    """
    print("\n" + "="*60)
    print("Training Naive Baseline (revenue_lag_1m)")
    print("="*60)

    # We need the raw feature columns — rebuild from the full feature df
    # filter to test rows by joining on year_month
    test_months = test_df_raw.select("year_month")
    baseline_df = df_features.join(test_months, on="year_month", how="inner")
    baseline_df = baseline_df.filter(
        F.col(TARGET_COLUMN).isNotNull() & (F.col(TARGET_COLUMN) > 0)
    )
    baseline_df = baseline_df.withColumn("prediction", F.col("revenue_lag_1m"))

    return evaluate_model(baseline_df, "naive_baseline")


# ── Model trainers ────────────────────────────────────────────────────────────

def train_linear_regression(train_df, test_df, use_cv=False):
    """Train LinearRegression on SCALED features."""
    print("\n" + "="*60)
    print("Training Linear Regression (scaled features)")
    print("="*60)

    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )

    if use_cv:
        param_grid = (
            ParamGridBuilder()
            .addGrid(lr.regParam,       [0.001, 0.01, 0.1])
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
            .build()
        )
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2"
        )
        model = CrossValidator(
            estimator=lr, estimatorParamMaps=param_grid,
            evaluator=evaluator, numFolds=3, seed=42
        ).fit(train_df).bestModel
    else:
        model = lr.fit(train_df)

    return model, model.transform(test_df), "linear_regression"


def train_random_forest(train_df, test_df, use_cv=False):
    """
    Train RandomForest on UNSCALED features.
    Reduced complexity: numTrees=20, maxDepth=4.
    """
    print("\n" + "="*60)
    print("Training Random Forest (unscaled features, reduced complexity)")
    print("="*60)

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        numTrees=20,    # was 150 — overfitting with tiny dataset
        maxDepth=4,     # was 12
        seed=42
    )

    if use_cv:
        param_grid = (
            ParamGridBuilder()
            .addGrid(rf.numTrees, [10, 20, 30])
            .addGrid(rf.maxDepth, [3, 4, 5])
            .build()
        )
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2"
        )
        model = CrossValidator(
            estimator=rf, estimatorParamMaps=param_grid,
            evaluator=evaluator, numFolds=3, seed=42
        ).fit(train_df).bestModel
    else:
        model = rf.fit(train_df)

    return model, model.transform(test_df), "random_forest"


def train_gbt(train_df, test_df, use_cv=False):
    """
    Train GBT on UNSCALED features.
    Reduced complexity: maxIter=30, maxDepth=3.
    """
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees (unscaled features, reduced complexity)")
    print("="*60)

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=30,   # was 100
        maxDepth=3,   # was 6
        seed=42
    )

    if use_cv:
        param_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxIter,  [20, 30, 50])
            .addGrid(gbt.maxDepth, [2, 3, 4])
            .build()
        )
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2"
        )
        model = CrossValidator(
            estimator=gbt, estimatorParamMaps=param_grid,
            evaluator=evaluator, numFolds=3, seed=42
        ).fit(train_df).bestModel
    else:
        model = gbt.fit(train_df)

    return model, model.transform(test_df), "gbt"


# ── Evaluation ────────────────────────────────────────────────────────────────

def evaluate_model(predictions, model_name):
    """Evaluate model with RMSE, MAE, R², MAPE."""
    print(f"\nEvaluating {model_name}...")

    rmse = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse").evaluate(predictions)
    mae  = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae" ).evaluate(predictions)
    r2   = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2"  ).evaluate(predictions)

    mape = (
        predictions
        .withColumn("ape", F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100)
        .agg(F.avg("ape"))
        .collect()[0][0]
    )

    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}

    print(f"  RMSE: {rmse:.2f}")
    print(f"  MAE:  {mae:.2f}")
    print(f"  R²:   {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")

    return metrics


def save_model(model, model_name, MODEL_OUTPUT_DIR):
    """Save model to MinIO."""
    model_path = f"{MODEL_OUTPUT_DIR}/{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(BUCKET_NAME):
    INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
    MODEL_OUTPUT_DIR       = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/revenue_forecast"

    print("\n" + "="*60)
    print("Revenue Forecasting Model Training - Specific Business Model")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print(f"Split strategy: Time-based (chronological)")
    print("="*60 + "\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Load data
    print("Step 1: Loading Monthly Aggregations from business bucket...")
    print("-" * 60)
    monthly_df, record_count = validate_dataset(spark, INPUT_MONTHLY_AGG_PATH, "Monthly Aggregations")

    if monthly_df is None:
        print("⚠️  No data available. Skipping training.")
        spark.stop()
        return

    monthly_df.select("year_month").orderBy("year_month").show(30, False)
    # Step 2: Validate training window
    print("\nStep 2: Validate Training Data Window")
    print("-" * 60)

    if record_count < MIN_RECORDS:
        print(f"⚠️  Insufficient training data: {record_count} records "
              f"(minimum required: {MIN_RECORDS}). Skipping training.")
        spark.stop()
        return

    if record_count > MAX_RECORDS:
        print(f"ℹ️  Dataset exceeds maximum ({record_count} > {MAX_RECORDS}). "
              f"Using most recent {MAX_RECORDS} records.")
        monthly_df = monthly_df.orderBy(F.desc("year_month")).limit(MAX_RECORDS)

    print(f"✓ Training data validated: {min(record_count, MAX_RECORDS)} records")

    # Step 3: Feature engineering
    print("\nStep 3: Time-Series Feature Engineering")
    print("-" * 60)
    df_features = create_time_series_features(monthly_df)

    # Step 4: Prepare training data (returns scaled + unscaled)
    print("\nStep 4: Data Preparation & Scaling")
    print("-" * 60)
    result = prepare_training_data(df_features)

    if result is None:
        print("⚠️  Training skipped due to insufficient data")
        spark.stop()
        return

    df_scaled, df_unscaled, scaler_model = result

    # Step 5: Time-based split — applied independently to each version
    print("\nStep 5: Time-Based Train/Test Split")
    print("-" * 60)

    train_scaled,   test_scaled   = time_based_split(df_scaled,   train_ratio=0.8)
    train_unscaled, test_unscaled = time_based_split(df_unscaled, train_ratio=0.8)

    train_count = train_scaled.count()
    test_count  = test_scaled.count()
    print(f"Training set: {train_count} records (earliest {train_count} months)")
    print(f"Test set:     {test_count} records (latest {test_count} months)")

    if train_count == 0 or test_count == 0:
        print("⚠️  Empty train/test split. Skipping training.")
        spark.stop()
        return

    # Step 6: Train models
    print("\nStep 6: Model Training")
    print("-" * 60)

    models_results = []

    # ── Naive baseline (no model save needed — it's a reference point) ──────
    baseline_metrics = evaluate_naive_baseline(train_scaled, test_scaled, df_features)
    models_results.append(baseline_metrics)

    # ── LinearRegression on SCALED features ─────────────────────────────────
    lr_model, lr_pred, lr_name = train_linear_regression(train_scaled, test_scaled, USE_CROSS_VALIDATION)
    lr_metrics = evaluate_model(lr_pred, lr_name)
    save_model(lr_model, lr_name, MODEL_OUTPUT_DIR)
    models_results.append(lr_metrics)

    # ── RandomForest on UNSCALED features ───────────────────────────────────
    rf_model, rf_pred, rf_name = train_random_forest(train_unscaled, test_unscaled, USE_CROSS_VALIDATION)
    rf_metrics = evaluate_model(rf_pred, rf_name)
    save_model(rf_model, rf_name, MODEL_OUTPUT_DIR)
    models_results.append(rf_metrics)

    # ── GBT on UNSCALED features ─────────────────────────────────────────────
    gbt_model, gbt_pred, gbt_name = train_gbt(train_unscaled, test_unscaled, USE_CROSS_VALIDATION)
    gbt_metrics = evaluate_model(gbt_pred, gbt_name)
    save_model(gbt_model, gbt_name, MODEL_OUTPUT_DIR)
    models_results.append(gbt_metrics)

    # Model comparison summary
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<15} {'MAE':<15} {'R²':<10} {'MAPE':<10}")
    print("-" * 75)

    for m in models_results:
        print(f"{m['model']:<25} {m['rmse']:<15.2f} {m['mae']:<15.2f} {m['r2']:<10.4f} {m['mape']:<10.2f}%")

    # Exclude naive baseline from auto-selection (it has no saveable model)
    trained_results = [m for m in models_results if m["model"] != "naive_baseline"]
    best = max(trained_results, key=lambda x: x["r2"])

    print("\n" + "="*60)
    print(f"Best Trained Model: {best['model']} (R² = {best['r2']:.4f})")

    baseline = next(m for m in models_results if m["model"] == "naive_baseline")
    if baseline["r2"] > best["r2"]:
        print("\n⚠️  WARNING: Naive baseline outperforms all trained models!")
        print("   The series is highly autoregressive. Consider using lag_1m directly,")
        print("   gathering more data, or adding exogenous features.")
    print("="*60)

    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Review metrics above and update MODEL_NAME in infer_revenue_forecast.py")
    print(f"   Available models: {', '.join([m['model'] for m in trained_results])}")
    print(f"   (naive_baseline is a reference — not a saveable Spark model)")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")

    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)