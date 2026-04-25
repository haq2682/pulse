"""
Customer Lifetime Value (CLV) Prediction - Training Script
Trains multiple regression models to predict customer lifetime value
"""

import os

import sys
import findspark
from dotenv import load_dotenv
from pathlib import Path
# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)
from utils.plot_exporter import export_training_metrics_plot
from general.model_registry import save_best_model_manifest

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "clv"
INPUT_RELATIVE_PATH = "transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("regression", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)

# Feature columns (avoiding leakage - no total_revenue or customer_lifetime_value)
FEATURE_COLUMNS = [
    "total_orders",
    "avg_order_value",
    "customer_tenure_days",
    "avg_days_between_orders",
    "order_frequency",
    "total_discount_received",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "recency_score",
    "frequency_score",
    "monetary_score"
]

TARGET_COLUMN = "customer_lifetime_value"
TARGET_LOG_COLUMN = "log_customer_lifetime_value"
USE_LOG_TARGET = True
GBT_MAX_ITER = int(os.getenv("CLV_GBT_MAX_ITER", "120"))
GBT_MAX_DEPTH = int(os.getenv("CLV_GBT_MAX_DEPTH", "7"))
GBT_STEP_SIZE = float(os.getenv("CLV_GBT_STEP_SIZE", "0.05"))
LEAKAGE_CORR_WARN_THRESHOLD = float(os.getenv("CLV_LEAKAGE_CORR_WARN_THRESHOLD", "0.95"))
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
    """Export per-model CLV training plot: sampled actual dots + prediction line + linear prediction line."""
    if not export_plots:
        return None

    rows = (
        train_predictions_df
        .select(TARGET_COLUMN, "prediction_clv")
        .orderBy("prediction_clv")
        .collect()
    )

    if not rows:
        print(f"⚠️  Plot export skipped for {model_name}: no training rows")
        return None

    x_vals = list(range(1, len(rows) + 1))
    actual = [float(r[TARGET_COLUMN]) for r in rows]
    predicted = [float(r["prediction_clv"]) for r in rows]
    linear_predicted = _linear_trend_values(x_vals, predicted)
    scatter_idx = _sample_indices(len(x_vals), MAX_SCATTER_POINTS)
    scatter_x = [x_vals[i] for i in scatter_idx]
    scatter_actual = [actual[i] for i in scatter_idx]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.scatter(scatter_x, scatter_actual, s=34, alpha=0.70, edgecolor="black", linewidth=0.4, label="Sample data")
    ax.plot(x_vals, predicted, color="black", linewidth=2.0, label="Prediction line")
    ax.plot(x_vals, linear_predicted, color="red", linewidth=2.0, label="Linear prediction line")

    ax.set_title(f"CLV - {model_name}")
    ax.set_xlabel("Training records (sorted by actual CLV)")
    ax.set_ylabel("Predicted CLV")
    ax.legend(loc="best")
    fig.tight_layout()

    plot_dir = _ensure_plot_dir(export_dir)
    file_name = f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-training-fit.png"
    output_path = os.path.join(plot_dir, file_name)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported training plot: {output_path}")
    return output_path


def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "CLV_Model_Training",
        extra_configs={
                    "spark.sql.shuffle.partitions": "8",
                    "inferSchema": "true",
                    "mergeSchema": "true"
                },
    )
def validate_dataset(spark, path):
    """Check if dataset exists and is readable"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ Dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ Dataset validation failed: {str(e)}")
        return None, 0


def validate_columns(df, required_columns):
    """Check if required columns exist and have non-null values"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False
    
    # Check for non-null values in each column
    for col in required_columns:
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        total_count = df.count()
        null_percentage = ((total_count - non_null_count) / total_count) * 100
        
        print(f"  {col}: {non_null_count}/{total_count} non-null ({null_percentage:.1f}% null)")
        
        if non_null_count == 0:
            print(f"✗ Column '{col}' is entirely null")
            return False
    
    print("✓ All required columns validated")
    return True


def check_monetary_score_leakage(df):
    """Run lightweight leakage diagnostics for monetary_score against target."""
    if "monetary_score" not in df.columns or TARGET_COLUMN not in df.columns:
        print("ℹ️  Leakage check skipped: required columns unavailable")
        return

    corr_df = (
        df.select(
            F.col("monetary_score").cast("double").alias("monetary_score"),
            F.col(TARGET_COLUMN).cast("double").alias(TARGET_COLUMN),
        )
        .filter(F.col("monetary_score").isNotNull() & F.col(TARGET_COLUMN).isNotNull())
    )

    pair_count = corr_df.count()
    if pair_count < 2:
        print("ℹ️  Leakage check skipped: insufficient rows for correlation")
        return

    corr_val = corr_df.stat.corr("monetary_score", TARGET_COLUMN)
    if corr_val is None:
        print("ℹ️  Leakage check skipped: correlation unavailable")
        return

    print(f"Monetary score correlation with CLV: {corr_val:.4f}")
    if abs(corr_val) >= LEAKAGE_CORR_WARN_THRESHOLD:
        print(
            "⚠️  Potential leakage risk detected: monetary_score is highly correlated with CLV. "
            "Verify this feature is computed only from information available at prediction time."
        )


def _with_output_scale_prediction(predictions_df):
    if USE_LOG_TARGET:
        return predictions_df.withColumn(
            "prediction_clv",
            F.greatest(F.lit(0.0), F.exp(F.col("prediction")) - F.lit(1.0)),
        )
    return predictions_df.withColumn("prediction_clv", F.col("prediction"))


def prepare_training_data(df):
    """
    Prepare data for training:
    1. Filter records with valid target values
    2. Fill missing feature values with 0 (conservative approach)
    3. Create feature vector
    """
    # Filter records where target is not null and > 0
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) & 
        (F.col(TARGET_COLUMN) > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid CLV: {valid_count}")
    
    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient training data: {valid_count} < {MIN_RECORDS}")
        return None
    
    # Fill missing feature values with 0
    df_filled = df_valid.fillna(0, subset=FEATURE_COLUMNS)
    
    # Assemble features into vector
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep"
    )
    
    df_with_target = df_filled.withColumn(
        TARGET_LOG_COLUMN,
        F.log1p(F.col(TARGET_COLUMN)) if USE_LOG_TARGET else F.col(TARGET_COLUMN),
    )

    df_prepared = assembler.transform(df_with_target).select("features", TARGET_COLUMN, TARGET_LOG_COLUMN)
    
    print(f"✓ Data prepared: {df_prepared.count()} records ready for training")
    return df_prepared


def train_linear_regression(train_df, test_df):
    """Train Linear Regression model"""
    print("\n" + "="*60)
    print("Training Linear Regression Model")
    print("="*60)
    
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_LOG_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )
    
    model = lr.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "linear_regression"


def train_random_forest(train_df, test_df):
    """Train Random Forest Regressor"""
    print("\n" + "="*60)
    print("Training Random Forest Regressor")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_LOG_COLUMN,
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    model = rf.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "random_forest"


def train_gbt(train_df, test_df):
    """Train Gradient Boosted Trees Regressor"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees Regressor")
    print("="*60)
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_LOG_COLUMN,
        maxIter=GBT_MAX_ITER,
        maxDepth=GBT_MAX_DEPTH,
        stepSize=GBT_STEP_SIZE,
        seed=42
    )
    
    model = gbt.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "gbt"


def evaluate_model(predictions, model_name):
    """Evaluate regression model using multiple metrics"""
    print(f"\nEvaluating {model_name}...")

    predictions_eval = _with_output_scale_prediction(predictions)
    
    # RMSE
    rmse_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction_clv",
        metricName="rmse"
    )
    rmse = rmse_evaluator.evaluate(predictions_eval)
    
    # MAE
    mae_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction_clv",
        metricName="mae"
    )
    mae = mae_evaluator.evaluate(predictions_eval)
    
    # R2
    r2_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction_clv",
        metricName="r2"
    )
    r2 = r2_evaluator.evaluate(predictions_eval)
    
    # MAPE (custom calculation)
    mape_df = predictions_eval.withColumn(
        "ape",
        F.when(
            F.col(TARGET_COLUMN) > 0,
            F.abs((F.col(TARGET_COLUMN) - F.col("prediction_clv")) / F.col(TARGET_COLUMN)) * 100,
        ).otherwise(None)
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]
    
    metrics = {
        "model": model_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape
    }
    
    print(f"  RMSE: {rmse:.2f}")
    print(f"  MAE: {mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save trained model to MinIO"""
    model_path = f"{MODEL_OUTPUT_DIR}/{model_name}"
    
    # Overwrite existing model
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main(EXPORT_PLOTS=False):
    """Main training pipeline"""
    print("\n" + "="*60)
    print("CLV Prediction Model Training - General Model")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print(f"Target transform: {'log1p(CLV)' if USE_LOG_TARGET else 'none'}")
    print(f"GBT config: maxIter={GBT_MAX_ITER}, maxDepth={GBT_MAX_DEPTH}, stepSize={GBT_STEP_SIZE}")
    print("="*60 + "\n")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Load data from all buckets
    print("Step 1: Loading data from all MinIO buckets...")
    print("-" * 60)
    all_required_columns = FEATURE_COLUMNS + [TARGET_COLUMN]
    df, record_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=all_required_columns,
        filter_nulls=True
    )
    
    if df is None:
        print("⚠️  No data available. Skipping training.")
        spark.stop()
        return
    
    # Step 2: Validate training data window
    print("\nStep 2: Validate Training Data Window")
    print("-" * 60)
    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        spark.stop()
        return
    
    # Step 3: Validate columns
    print("\nStep 3: Column Validation")
    print("-" * 60)
    
    if not validate_columns(df, all_required_columns):
        print("⚠️  Training skipped due to required columns missing or invalid")
        spark.stop()
        return

    # Step 3.1: Leakage diagnostics
    print("\nStep 3.1: Leakage Diagnostics")
    print("-" * 60)
    check_monetary_score_leakage(df)
    
    # Step 4: Prepare training data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_training_data(df)
    
    if df_prepared is None:
        print("⚠️  Training skipped due to insufficient training data")
        spark.stop()
        return
    
    # Step 4: Split data
    print("\nStep 4: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"Training set: {train_count} records")
    print(f"Test set: {test_count} records")
    
    # Step 5: Train models
    print("\nStep 5: Model Training")
    print("-" * 60)
    
    models_results = []
    
    # Train Linear Regression
    lr_model, lr_predictions, lr_name = train_linear_regression(train_df, test_df)
    lr_metrics = evaluate_model(lr_predictions, lr_name)
    save_model(lr_model, lr_name)
    export_model_training_plot(_with_output_scale_prediction(lr_model.transform(train_df)), lr_name, EXPORT_PLOTS)
    models_results.append(lr_metrics)
    
    # Train Random Forest
    rf_model, rf_predictions, rf_name = train_random_forest(train_df, test_df)
    rf_metrics = evaluate_model(rf_predictions, rf_name)
    save_model(rf_model, rf_name)
    export_model_training_plot(_with_output_scale_prediction(rf_model.transform(train_df)), rf_name, EXPORT_PLOTS)
    models_results.append(rf_metrics)
    
    # Train GBT
    gbt_model, gbt_predictions, gbt_name = train_gbt(train_df, test_df)
    gbt_metrics = evaluate_model(gbt_predictions, gbt_name)
    save_model(gbt_model, gbt_name)
    export_model_training_plot(_with_output_scale_prediction(gbt_model.transform(train_df)), gbt_name, EXPORT_PLOTS)
    models_results.append(gbt_metrics)
    
    # Step 6: Model Comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<12} {'MAE':<12} {'R²':<10} {'MAPE':<10}")
    print("-" * 60)
    
    for metrics in models_results:
        print(
            f"{metrics['model']:<25} "
            f"{metrics['rmse']:<12.2f} "
            f"{metrics['mae']:<12.2f} "
            f"{metrics['r2']:<10.4f} "
            f"{metrics['mape']:<10.2f}%"
        )
    
    # Find best model by R²
    best_model = max(models_results, key=lambda x: x['r2'])
    manifest_path = save_best_model_manifest(
        spark,
        MODEL_OUTPUT_DIR,
        best_model["model"],
        "r2",
        best_model["r2"],
        {m["model"]: m["r2"] for m in models_results},
    )
    print(f"✓ Saved best model manifest to: {manifest_path}")

    export_training_metrics_plot(
        model_name=MODEL_NAME,
        metrics=models_results,
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
    )
    print("\n" + "="*60)
    print(f"Best Model: {best_model['model']} (R² = {best_model['r2']:.4f})")
    print("="*60)
    print("\n✓ Best model is now auto-selected in inference via manifest")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed successfully\n")
    
    spark.stop()


if __name__ == "__main__":
    main(EXPORT_PLOTS=True)