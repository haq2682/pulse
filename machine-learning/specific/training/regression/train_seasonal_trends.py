"""
Seasonal Trends - Training Script (Specific Business Model)
Predicts seasonal index multipliers for future months based on historical
monthly patterns. The seasonal index represents how much a given month's
revenue deviates from the trailing 12-month average.

Target: seasonal_index = month_revenue / rolling_12m_avg_revenue
  - Index > 1.0 means above-average month (e.g., holiday season)
  - Index < 1.0 means below-average month (e.g., slow season)
  - Index = 1.0 means average performance

Uses time-based train/test split to prevent temporal leakage.
"""

import os
import sys
import json

import findspark
from dotenv import load_dotenv
from pathlib import Path
# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
from datetime import datetime
import math
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()

# Configuration
MODEL_NAME = "seasonal_trends"
MIN_RECORDS = 12
MAX_RECORDS = 1000
USE_CROSS_VALIDATION = False
SEASONAL_INDEX_MIN = 0.3
SEASONAL_INDEX_MAX = 3.0

# Feature columns for seasonal trend prediction
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

TARGET_COLUMN = "seasonal_index"
PLOT_EXPORT_DIR = "/app/logs_for_report"
MAX_SCATTER_POINTS = 300


def _ensure_plot_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _sanitize_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in value.lower())


def _sample_indices(length: int, max_points: int):
    if length <= max_points:
        return list(range(length))
    return np.linspace(0, length - 1, num=max_points, dtype=int).tolist()


def _linear_trend_values(x_values, y_values):
    if not x_values or not y_values:
        return []
    if len(x_values) == 1:
        return [float(y_values[0])]

    slope, intercept = np.polyfit(np.array(x_values, dtype=float), np.array(y_values, dtype=float), 1)
    return [(slope * float(x)) + intercept for x in x_values]


def export_model_training_plot(train_predictions_df, model_name: str, export_plots: bool, export_dir: str = PLOT_EXPORT_DIR):
    """Export per-model training plot: actual dots + model fit line."""
    if not export_plots:
        return None

    rows = (
        train_predictions_df
        .select("year_month", TARGET_COLUMN, "prediction")
        .orderBy("year_month")
        .collect()
    )

    if not rows:
        print(f"⚠️  Plot export skipped for {model_name}: no training rows")
        return None

    x_vals = list(range(1, len(rows) + 1))
    labels = [str(r["year_month"]) for r in rows]
    actual = [float(r[TARGET_COLUMN]) for r in rows]
    predicted = [float(r["prediction"]) for r in rows]
    linear_predicted = _linear_trend_values(x_vals, predicted)
    scatter_idx = _sample_indices(len(x_vals), MAX_SCATTER_POINTS)
    scatter_x = [x_vals[i] for i in scatter_idx]
    scatter_actual = [actual[i] for i in scatter_idx]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.scatter(scatter_x, scatter_actual, s=34, alpha=0.70, edgecolor="black", linewidth=0.4, label="Sample data")
    ax.plot(x_vals, predicted, color="black", linewidth=2.0, label="Prediction line")
    ax.plot(x_vals, linear_predicted, color="red", linewidth=2.0, label="Linear prediction line")

    ax.set_title(f"Seasonal Trends - {model_name}")
    ax.set_xlabel("Training timeline (year_month)")
    ax.set_ylabel("Seasonal index")
    tick_step = max(1, len(x_vals) // 10)
    tick_positions = x_vals[::tick_step]
    tick_labels = labels[::tick_step]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=30, ha="right")
    ax.legend(loc="best")
    fig.tight_layout()

    plot_dir = _ensure_plot_dir(export_dir)
    file_name = f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-training-fit.png"
    output_path = os.path.join(plot_dir, file_name)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported training plot: {output_path}")
    return output_path


def create_feature_pipeline_stages():
    """
    Create common pipeline stages for feature assembly and scaling.
    Used by all model training functions to ensure consistency.
    """
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    return assembler, scaler


def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "Seasonal_Trends_Training",
        extra_configs={
                    "spark.sql.shuffle.partitions": "8"
                },
    )


def save_best_model_manifest(spark, model_output_dir, best_metrics, all_metrics):
    """Persist best-model metadata so inference can auto-select the best model."""
    manifest = {
        "best_model": best_metrics["model"],
        "best_r2": float(best_metrics["r2"]),
        "metric": "r2",
        "generated_at": datetime.now().isoformat(),
        "model_scores": {
            m["model"]: {
                "r2": float(m["r2"]),
                "rmse": float(m["rmse"]),
                "mae": float(m["mae"]),
                "mape": float(m["mape"]),
            }
            for m in all_metrics
        },
    }

    manifest_payload = json.dumps(manifest)
    manifest_path = f"{model_output_dir}/_best_model_manifest"
    spark.createDataFrame([(manifest_payload,)], ["value"]).coalesce(1).write.mode("overwrite").text(manifest_path)
    print(f"✓ Best-model manifest saved: {manifest_path}")
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


def create_seasonal_features(monthly_df):
    """
    Create features for seasonal trend prediction.

    Uses fields directly from agg_monthly_aggregations schema:
      - prev_month_revenue, revenue_growth_rate, prev_month_customers (schema fields)
      - total_sessions, total_conversions, session_to_order_rate, churn_rate (schema fields)

    Derives additional features via window functions:
      - revenue_lag_3m/6m/12m, rolling averages, YoY ratios, orders growth
    """
    print("Creating seasonal trend features...")

    # Add a partition key for global time-series operations
    monthly_sorted = monthly_df.withColumn("partition_key", F.lit(1)).orderBy("year_month")

    # Window specs
    window_spec = Window.partitionBy("partition_key").orderBy("year_month")
    window_rolling_3m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-3, -1)
    window_rolling_6m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-6, -1)
    window_rolling_12m = Window.partitionBy("partition_key").orderBy("year_month").rowsBetween(-12, -1)

    # --- Revenue lag features (3m, 6m, 12m derived; 1m already in schema as prev_month_revenue) ---
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

    # --- Rolling average for revenue (12m, used for target computation) ---
    df = df.withColumn(
        "revenue_rolling_12m",
        F.avg("total_revenue").over(window_rolling_12m)
    )

    # --- Rolling averages for orders ---
    df = df.withColumn(
        "orders_rolling_3m",
        F.avg("total_orders").over(window_rolling_3m)
    ).withColumn(
        "orders_rolling_6m",
        F.avg("total_orders").over(window_rolling_6m)
    )

    # --- Rolling averages for customers ---
    df = df.withColumn(
        "customers_rolling_3m",
        F.avg("total_customers").over(window_rolling_3m)
    )

    # --- 3-month revenue growth (derived) ---
    # revenue_growth_rate (1m) already exists in schema
    df = df.withColumn(
        "revenue_growth_3m",
        F.when(
            (F.col("revenue_lag_3m").isNotNull()) & (F.col("revenue_lag_3m") > 0),
            (F.col("total_revenue") - F.col("revenue_lag_3m")) / F.col("revenue_lag_3m")
        ).otherwise(0)
    )

    # --- Orders growth (derived) ---
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

    # --- Year-over-year features (derived) ---
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

    # --- Seasonal encoding (derived) ---
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

    # --- Target: Seasonal Index ---
    # seasonal_index = current month's revenue / trailing 12-month average revenue
    # This captures how much a month deviates from the rolling baseline
    df = df.withColumn(
        TARGET_COLUMN,
        F.when(
            (F.col("revenue_rolling_12m").isNotNull()) & (F.col("revenue_rolling_12m") > 0),
            F.col("total_revenue") / F.col("revenue_rolling_12m")
        ).otherwise(None)
    )

    df = df.filter(
        (F.col("revenue_rolling_12m").isNotNull()) &
        (F.col("revenue_lag_12m").isNotNull())
    )
    # Drop partition key and temporary columns
    df = df.drop("partition_key", "order_quarter")

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

    print(f"✓ Seasonal features created: {df.count()} records")
    return df


def prepare_training_data(df):
    """
    Prepare data for training.
    Filters out records without valid seasonal index
    (first ~12 months won't have enough history for 12m rolling average).
    Note: Feature scaling is now handled by Pipeline in train_* functions.
    """
    # Filter records where seasonal_index is valid
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0)
    )

    valid_count = df_valid.count()
    print(f"Records with valid seasonal index: {valid_count}")

    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient training data: {valid_count} < {MIN_RECORDS}")
        return None

    # Outlier treatment on target (winsorize via domain bounds)
    raw_outlier_count = df_valid.filter(
        (F.col(TARGET_COLUMN) < SEASONAL_INDEX_MIN) |
        (F.col(TARGET_COLUMN) > SEASONAL_INDEX_MAX)
    ).count()

    if raw_outlier_count > 0:
        print(
            f"⚠️  Detected {raw_outlier_count} target outliers. "
            f"Clamping {TARGET_COLUMN} to [{SEASONAL_INDEX_MIN}, {SEASONAL_INDEX_MAX}]"
        )

    df_capped = df_valid.withColumn(
        TARGET_COLUMN,
        F.greatest(F.lit(SEASONAL_INDEX_MIN), F.least(F.lit(SEASONAL_INDEX_MAX), F.col(TARGET_COLUMN)))
    )

    # Fill missing values with 0
    df_filled = df_capped.fillna(0, subset=FEATURE_COLUMNS)

    # Select final columns (keep year_month for time-based splitting)
    df_prepared = df_filled.select(
        "year_month",
        *FEATURE_COLUMNS,
        TARGET_COLUMN
    )

    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared


def time_based_split(df, train_ratio=0.8):
    """
    Split time-series data chronologically instead of randomly.
    Earlier records go to training, later records go to testing.
    This prevents temporal leakage from future data into training.
    """
    total_count = df.count()
    split_point = int(total_count * train_ratio)

    # Add row number based on chronological order
    window_spec = Window.partitionBy(F.lit(1)).orderBy("year_month")
    df_with_row = df.withColumn("_row_num", F.row_number().over(window_spec))

    train_df = df_with_row.filter(F.col("_row_num") <= split_point).drop("_row_num")
    test_df = df_with_row.filter(F.col("_row_num") > split_point).drop("_row_num")

    return train_df, test_df


def train_linear_regression(train_df, test_df, use_cv=False):
    """Train Linear Regression with Pipeline including scaler"""
    print("\n" + "="*60)
    print("Training Linear Regression")
    print("="*60)

    # Create common pipeline stages
    assembler, scaler = create_feature_pipeline_stages()

    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )

    # Build pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.001, 0.01, 0.1]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()

        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )

        model = cv.fit(train_df).bestModel
    else:
        model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    return model, predictions, "linear_regression"


def train_random_forest(train_df, test_df, use_cv=False):
    """Train Random Forest with Pipeline including scaler"""
    print("\n" + "="*60)
    print("Training Random Forest")
    print("="*60)

    # Create common pipeline stages
    assembler, scaler = create_feature_pipeline_stages()

    rf = RandomForestRegressor(
        featuresCol="features_unscaled",
        labelCol=TARGET_COLUMN,
        numTrees=200,
        maxDepth=15,
        seed=42
    )

    # Build pipeline
    pipeline = Pipeline(stages=[assembler, rf])

    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [100, 150, 200]) \
            .addGrid(rf.maxDepth, [10, 12, 15]) \
            .build()

        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )

        model = cv.fit(train_df).bestModel
    else:
        model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    return model, predictions, "random_forest"


def train_gbt(train_df, test_df, use_cv=False):
    """Train GBT with Pipeline including scaler"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees")
    print("="*60)

    # Create common pipeline stages
    assembler, scaler = create_feature_pipeline_stages()

    gbt = GBTRegressor(
        featuresCol="features_unscaled",
        labelCol=TARGET_COLUMN,
        maxIter=120,
        maxDepth=7,
        seed=42
    )

    # Build pipeline
    pipeline = Pipeline(stages=[assembler, gbt])

    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(gbt.maxIter, [50, 100, 150]) \
            .addGrid(gbt.maxDepth, [5, 6, 7]) \
            .build()

        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )

        model = cv.fit(train_df).bestModel
    else:
        model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    return model, predictions, "gbt"


def evaluate_model(predictions, model_name):
    """Evaluate model with comprehensive metrics"""
    print(f"\nEvaluating {model_name}...")

    rmse_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2")

    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)

    mape_df = predictions.withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]

    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}

    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")

    return metrics


def save_model(model, model_name, MODEL_OUTPUT_DIR):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_DIR}/{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main(BUCKET_NAME, EXPORT_PLOTS=False):
    """Main training pipeline"""
    INPUT_MONTHLY_AGG_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_monthly_aggregations.parquet"
    MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/seasonal_trends"

    print("\n" + "="*60)
    print("Seasonal Trends Model Training - Specific Business Model")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}")
    print(f"Split strategy: Time-based (chronological)")
    print(f"Outlier handling: clamp {TARGET_COLUMN} to [{SEASONAL_INDEX_MIN}, {SEASONAL_INDEX_MAX}]")
    print("="*60 + "\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Load data from business's own bucket
    print("Step 1: Loading Monthly Aggregations from business bucket...")
    print("-" * 60)

    monthly_df, record_count = validate_dataset(
        spark, INPUT_MONTHLY_AGG_PATH, "Monthly Aggregations"
    )

    if monthly_df is None:
        print("⚠️  No data available. Skipping training.")
        spark.stop()
        return

    # Step 2: Validate training data window
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

    # Step 3: Seasonal feature engineering
    print("\nStep 3: Seasonal Feature Engineering")
    print("-" * 60)
    df_features = create_seasonal_features(monthly_df)

    # Step 4: Prepare training data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_training_data(df_features)

    if df_prepared is None:
        print("⚠️  Training skipped due to insufficient data")
        spark.stop()
        return

    # Step 5: Time-based split (chronological ordering, not random)
    print("\nStep 5: Time-Based Train/Test Split")
    print("-" * 60)
    train_df, test_df = time_based_split(df_prepared, train_ratio=0.8)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"Training set: {train_count} records (earliest {train_count} months)")
    print(f"Test set: {test_count} records (latest {test_count} months)")

    if train_count == 0 or test_count == 0:
        print("⚠️  Empty train/test split. Skipping training.")
        spark.stop()
        return

    # Step 6: Train models
    print("\nStep 6: Model Training")
    print("-" * 60)

    models_results = []

    lr_model, lr_pred, lr_name = train_linear_regression(train_df, test_df, USE_CROSS_VALIDATION)
    lr_metrics = evaluate_model(lr_pred, lr_name)
    save_model(lr_model, lr_name, MODEL_OUTPUT_DIR)
    lr_train_pred = lr_model.transform(train_df)
    export_model_training_plot(lr_train_pred, lr_name, EXPORT_PLOTS)
    models_results.append(lr_metrics)

    rf_model, rf_pred, rf_name = train_random_forest(train_df, test_df, USE_CROSS_VALIDATION)
    rf_metrics = evaluate_model(rf_pred, rf_name)
    save_model(rf_model, rf_name, MODEL_OUTPUT_DIR)
    rf_train_pred = rf_model.transform(train_df)
    export_model_training_plot(rf_train_pred, rf_name, EXPORT_PLOTS)
    models_results.append(rf_metrics)

    gbt_model, gbt_pred, gbt_name = train_gbt(train_df, test_df, USE_CROSS_VALIDATION)
    gbt_metrics = evaluate_model(gbt_pred, gbt_name)
    save_model(gbt_model, gbt_name, MODEL_OUTPUT_DIR)
    gbt_train_pred = gbt_model.transform(train_df)
    export_model_training_plot(gbt_train_pred, gbt_name, EXPORT_PLOTS)
    models_results.append(gbt_metrics)

    # Model comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<15} {'MAE':<15} {'R²':<10} {'MAPE':<10}")
    print("-" * 60)

    for m in models_results:
        print(f"{m['model']:<25} {m['rmse']:<15.4f} {m['mae']:<15.4f} {m['r2']:<10.4f} {m['mape']:<10.2f}%")

    best = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best['model']} (R² = {best['r2']:.4f})")
    print("="*60)

    save_best_model_manifest(spark, MODEL_OUTPUT_DIR, best, models_results)
    print(f"\n✓ Automatic model selection metadata updated for inference")
    print(f"   Available models: {', '.join([m['model'] for m in models_results])}")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")

    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME, EXPORT_PLOTS=False)
