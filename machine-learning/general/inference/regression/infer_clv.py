"""
Customer Lifetime Value (CLV) Prediction - Inference Script
Generates CLV predictions using trained regression models
"""

import os

import findspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from datetime import datetime
import uuid
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()

# Feature columns (must match training)
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
USE_LOG_TARGET = True
PLOT_EXPORT_DIR = "/app/logs_for_report"
MAX_SCATTER_POINTS = 300


import sys
from pathlib import Path

_ML_ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "machine-learning")
if str(_ML_ROOT) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT))

from spark_utils import create_ml_spark_session
from general.model_registry import resolve_best_model


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


def export_inference_prediction_plot(predictions_df, model_name: str, export_plots: bool, export_dir: str = PLOT_EXPORT_DIR):
    """Export CLV inference plot: sampled predicted dots + prediction line + linear prediction line."""
    if not export_plots:
        return None

    rows = (
        predictions_df
        .select("predicted_clv")
        .orderBy("predicted_clv")
        .collect()
    )

    if not rows:
        print("⚠️  Inference plot export skipped: no prediction rows")
        return None

    x_vals = list(range(1, len(rows) + 1))
    predicted = [float(r["predicted_clv"] or 0.0) for r in rows]
    linear_predicted = _linear_trend_values(x_vals, predicted)
    scatter_idx = _sample_indices(len(x_vals), MAX_SCATTER_POINTS)
    scatter_x = [x_vals[i] for i in scatter_idx]
    scatter_y = [predicted[i] for i in scatter_idx]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.scatter(scatter_x, scatter_y, s=34, alpha=0.70, edgecolor="black", linewidth=0.4, label="Sample data")
    ax.plot(x_vals, predicted, color="black", linewidth=2.0, label="Prediction line")
    ax.plot(x_vals, linear_predicted, color="red", linewidth=2.0, label="Linear prediction line")

    ax.set_title(f"CLV Forecast - {model_name}")
    ax.set_xlabel("Inference records (sorted by predicted CLV)")
    ax.set_ylabel("Predicted CLV")
    ax.legend(loc="best")
    fig.tight_layout()

    plot_dir = _ensure_plot_dir(export_dir)
    file_name = f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-forecast-fit.png"
    output_path = os.path.join(plot_dir, file_name)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported inference plot: {output_path}")
    return output_path

def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return create_ml_spark_session(
        "CLV_Model_Inference",
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "inferSchema": "true",
            "mergeSchema": "true",
        },
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
    """Check if required columns exist"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False
    
    # Check for non-null values
    for col in required_columns:
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        total_count = df.count()
        
        if non_null_count == 0:
            print(f"✗ Column '{col}' is entirely null")
            return False
    
    print("✓ All required columns validated")
    return True


def prepare_inference_data(df):
    """
    Prepare data for inference:
    1. Keep customer_id for output
    2. Fill missing feature values with 0
    3. Create feature vector
    """
    # Fill missing feature values with 0 (same as training)
    df_filled = df.fillna(0, subset=FEATURE_COLUMNS)
    
    # Assemble features into vector
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep"
    )
    
    df_prepared = assembler.transform(df_filled).select("customer_id", "features")
    
    print(f"✓ Data prepared: {df_prepared.count()} records ready for inference")
    return df_prepared


def generate_predictions(model, df, model_name, PREDICTION_HORIZON_DAYS):
    """Generate predictions and format output"""
    # Generate predictions
    predictions_df = model.transform(df)
    
    # Generate unique prediction IDs
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    
    # Get current timestamp
    current_timestamp = F.lit(datetime.now())

    predicted_clv_expr = (
        F.greatest(F.lit(0.0), F.exp(F.col("prediction")) - F.lit(1.0))
        if USE_LOG_TARGET
        else F.col("prediction")
    )
    
    # Format output according to ml_clv_predictions schema
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("customer_id"),
        current_timestamp.alias("prediction_date"),
        predicted_clv_expr.alias("predicted_clv"),
        F.lit(PREDICTION_HORIZON_DAYS).alias("prediction_horizon_days"),
        # Calculate confidence intervals (±20% of prediction as example)
        (predicted_clv_expr * 0.8).alias("confidence_interval_lower"),
        (predicted_clv_expr * 1.2).alias("confidence_interval_upper"),
        F.lit(0.85).alias("confidence_score"),  # Placeholder confidence score
        F.lit(model_name).alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO as Parquet"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_sample_predictions(df, n=5):
    """Display sample predictions for verification"""
    print("\n" + "="*60)
    print(f"Sample Predictions (first {n} records)")
    print("="*60)
    
    sample = df.select(
        "customer_id",
        "predicted_clv",
        "confidence_interval_lower",
        "confidence_interval_upper"
    ).limit(n).collect()
    
    for row in sample:
        print(
            f"Customer: {row['customer_id']:<30} "
            f"CLV: ${row['predicted_clv']:>10.2f} "
            f"(${row['confidence_interval_lower']:>10.2f} - ${row['confidence_interval_upper']:>10.2f})"
        )


def main(BUCKET_NAME, EXPORT_PLOTS=False):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/clv_predictions/"
    MODEL_BASE_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/clv/"

    MODEL_CANDIDATES = ["linear_regression", "random_forest", "gbt"]
    PREFERRED_MODEL = "random_forest"

    PREDICTION_HORIZON_DAYS = 365  # Predict CLV for next 1 year
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("CLV Prediction Model Inference")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Preferred model: {PREFERRED_MODEL}\n")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    MODEL_NAME, selection_source, _ = resolve_best_model(
        spark,
        MODEL_BASE_PATH,
        MODEL_CANDIDATES,
        preferred_model=PREFERRED_MODEL,
    )
    print(f"Selected model: {MODEL_NAME} (source: {selection_source})")
    
    # Step 1: Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        print("   Run training script first: train_clv_model.py")
        spark.stop()
        return
    
    # Step 2: Validate dataset
    print("\nStep 2: Dataset Validation")
    print("-" * 60)
    df, record_count = validate_dataset(spark, INPUT_PATH)
    
    if df is None:
        print("\n✗ Inference aborted: Dataset not found")
        spark.stop()
        return
    
    # Step 3: Validate columns
    print("\nStep 3: Column Validation")
    print("-" * 60)
    required_columns = ["customer_id"] + FEATURE_COLUMNS
    
    if not validate_columns(df, required_columns):
        print("\n✗ Inference aborted: Required columns missing or invalid")
        spark.stop()
        return
    
    # Step 4: Prepare data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_inference_data(df)
    
    # Step 5: Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME, PREDICTION_HORIZON_DAYS)
    
    # Step 6: Display samples
    display_sample_predictions(predictions_df)

    export_inference_prediction_plot(predictions_df, MODEL_NAME, EXPORT_PLOTS)
    
    # Step 7: Save predictions
    print("\nStep 6: Save Predictions")
    print("-" * 60)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed: Unable to save predictions")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME, EXPORT_PLOTS=False)
