import os
import sys
import uuid
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.sql.functions import col, lit, udf, current_timestamp
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.classification import (
    LogisticRegressionModel, RandomForestClassificationModel
)
from pyspark.ml.feature import StringIndexerModel

# ---------------------------------------------------------------------------
# Feature columns — must exactly match training
# ---------------------------------------------------------------------------
FEATURE_COLUMNS = [
    "days_since_last_purchase",
    "order_frequency",
    "customer_lifetime_value",
    "avg_days_between_orders",
    "total_orders",
    "total_revenue",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "days_since_last_login",
    "customer_tenure_days",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "avg_order_value",
    "cancellation_rate",
]

PLOT_EXPORT_DIR    = "/app/logs_for_report"
MIN_PLOT_RECORDS   = 20
MAX_PLOT_RECORDS   = 50000
MAX_SCATTER_POINTS = 50000
CLASS_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
]

from pathlib import Path

_ML_ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "machine-learning")
if str(_ML_ROOT) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT))

from spark_utils import create_ml_spark_session
from general.model_registry import resolve_best_model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ensure_plot_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def _sanitize_name(value):
    return "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in value.lower())


def _sample_indices(length, max_points):
    if length <= max_points:
        return list(range(length))
    return np.linspace(0, length - 1, num=max_points, dtype=int).tolist()


# ---------------------------------------------------------------------------
# [UPDATED] Plot — StandardScaler → PCA → 2D scatter coloured by predicted class
#
# Key fix vs previous version:
#   The old version passed raw feature coordinates (days_since_last_purchase,
#   order_frequency) or ran PCA on un-scaled features. Both approaches are
#   dominated by high-magnitude columns (total_revenue, customer_lifetime_value)
#   which produce misleading axes with values in the hundreds of thousands.
#
#   StandardScaler (withMean=True, withStd=True) normalizes all 15 features
#   to mean=0 std=1 BEFORE PCA, so every feature contributes proportionally.
#   The resulting principal components are interpretable and the explained
#   variance percentages are shown on the axis labels.
#
#   Some class overlap is expected and realistic — churn risk is a spectrum,
#   not discrete clusters separable in 2D.
# ---------------------------------------------------------------------------
def export_inference_classification_plot(
    predictions_df,
    feature_df,
    model_name,
    export_plots,
    export_dir=PLOT_EXPORT_DIR,
):
    """
    Export a StandardScaler-normalized PCA scatter of predicted churn classes.

    Parameters
    ----------
    predictions_df : DataFrame — output of generate_predictions()
                     must contain customer_id and predicted_churn_risk
    feature_df     : DataFrame — df_prepared; must contain customer_id
                     and assembled "features" vector column
    model_name     : str
    export_plots   : bool
    export_dir     : str
    """
    if not export_plots:
        return None

    # Step 1: Join predictions onto feature vectors
    joined = (
        predictions_df
        .select("customer_id", "predicted_churn_risk")
        .join(
            feature_df.select("customer_id", "features"),
            on="customer_id",
            how="inner",
        )
    )

    row_count = joined.count()
    if row_count < MIN_PLOT_RECORDS:
        print(f"⚠️  Inference plot skipped for {model_name}: only {row_count} rows")
        return None

    # Step 2: StandardScaler — bring all features to mean=0, std=1
    # This prevents high-magnitude features from dominating PCA
    scaler       = StandardScaler(inputCol="features", outputCol="scaled_features",
                                  withMean=True, withStd=True)
    scaler_model = scaler.fit(joined)
    scaled_df    = scaler_model.transform(joined)

    # Step 3: PCA — project 15 normalized dimensions down to 2
    pca       = PCA(k=2, inputCol="scaled_features", outputCol="pca_features")
    pca_model = pca.fit(scaled_df)
    pca_df    = pca_model.transform(scaled_df)

    explained = pca_model.explainedVariance
    pc1_pct   = round(float(explained[0]) * 100, 1)
    pc2_pct   = round(float(explained[1]) * 100, 1)
    print(f"  PCA explained variance: PC1={pc1_pct}%, PC2={pc2_pct}% "
          f"(total={pc1_pct + pc2_pct}%)")

    # Step 4: Collect and plot
    rows = (
        pca_df
        .select("predicted_churn_risk", "pca_features")
        .limit(MAX_PLOT_RECORDS)
        .collect()
    )

    classes = [str(r["predicted_churn_risk"] or "unknown") for r in rows]
    x_vals  = [float(r["pca_features"][0]) for r in rows]
    y_vals  = [float(r["pca_features"][1]) for r in rows]

    idx  = _sample_indices(len(x_vals), MAX_SCATTER_POINTS)
    ucls = sorted(set(classes))
    cmap = {c: CLASS_COLORS[i % len(CLASS_COLORS)] for i, c in enumerate(ucls)}

    fig, ax = plt.subplots(figsize=(12, 6))
    for cls in ucls:
        cx = [x_vals[i] for i in idx if classes[i] == cls]
        cy = [y_vals[i] for i in idx if classes[i] == cls]
        if cx:
            ax.scatter(cx, cy, s=20, alpha=0.55, edgecolor="none",
                       color=cmap[cls], label=f"{cls} (n={len(cx)})")

    ax.set_title(
        f"Customer Churn Forecast - {model_name} (PCA projection)\n"
        f"PC1 {pc1_pct}% variance | PC2 {pc2_pct}% variance"
    )
    ax.set_xlabel(f"Principal Component 1 ({pc1_pct}% variance explained)")
    ax.set_ylabel(f"Principal Component 2 ({pc2_pct}% variance explained)")
    ax.legend(loc="best", title="Predicted class", framealpha=0.8)
    fig.tight_layout()

    out = os.path.join(
        _ensure_plot_dir(export_dir),
        f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-forecast-fit.png",
    )
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported inference plot: {out}")
    return out


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def create_spark_session():
    return create_ml_spark_session(
        "CustomerChurnInference",
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "inferSchema": "true",
            "mergeSchema": "true",
        },
    )


# ---------------------------------------------------------------------------
# Data & model loading
# ---------------------------------------------------------------------------
def load_data(spark, path):
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records from {path}")
        return df
    except Exception as e:
        print(f"✗ Failed to load {path}: {e}")
        return None


def load_model(spark, model_dir, model_name):
    try:
        model_path   = f"{model_dir}/{model_name}"
        indexer_path = f"{model_dir}/{model_name}_indexer"

        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {model_name}")

        indexer = StringIndexerModel.load(indexer_path)
        print(f"✓ Loaded model: {model_name}")
        return model, indexer
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return None, None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_dataset(df, required_columns):
    if df is None:
        return False, "Dataset is None"
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        return False, f"Missing columns: {missing}"
    for c in required_columns:
        if df.filter(col(c).isNotNull()).count() == 0:
            print(f"⚠️  Warning: column '{c}' is entirely null")
    return True, "Validation passed"


# ---------------------------------------------------------------------------
# Feature preparation (must mirror training pipeline exactly)
# ---------------------------------------------------------------------------
def prepare_features(df, feature_cols):
    df_filled = df.fillna(0, subset=feature_cols)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector = assembler.transform(df_filled)
    print(f"✓ Features vectorized ({len(feature_cols)} columns)")
    return df_vector


# ---------------------------------------------------------------------------
# Feature importance
# ---------------------------------------------------------------------------
def extract_feature_importance(model, model_name, feature_cols):
    if model_name == "RandomForest":
        imps = model.featureImportances.toArray()
        return {feature_cols[i]: float(imps[i]) for i in range(len(feature_cols))}
    return {}


def get_top_contributing_factors(feature_importance, top_n=3):
    if not feature_importance:
        return {}
    return dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:top_n])


# ---------------------------------------------------------------------------
# Prediction generation
# ---------------------------------------------------------------------------
def generate_predictions(spark, df, model, indexer, model_name, feature_importance, MODEL_VERSION):
    predictions = model.transform(df)

    labels               = indexer.labels
    index_to_label       = udf(lambda idx: labels[int(idx)], StringType())
    extract_probability  = udf(lambda prob, pred: float(prob[int(pred)]) if prob else 0.0, DoubleType())
    calc_confidence      = udf(lambda prob: float(max(prob)) if prob else 0.0, DoubleType())
    top_factors          = get_top_contributing_factors(feature_importance)

    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("customer_id"),
        current_timestamp().alias("prediction_date"),
        index_to_label(col("prediction")).alias("predicted_churn_risk"),
        extract_probability(col("probability"), col("prediction")).alias("churn_probability"),
        calc_confidence(col("probability")).alias("confidence_score"),
        lit(str(top_factors)).alias("contributing_factors"),
        lit(MODEL_VERSION).alias("model_version"),
    )

    gen_uuid  = udf(lambda: str(uuid.uuid4()), StringType())
    output_df = output_df.withColumn("prediction_id", gen_uuid())

    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


# ---------------------------------------------------------------------------
# Save predictions
# ---------------------------------------------------------------------------
def save_predictions(df, output_path):
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Saved predictions to {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(BUCKET_NAME, EXPORT_PLOTS=False):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH      = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    OUTPUT_PATH     = (
        f"s3a://{BUCKET_NAME}/machine-learning/classification/"
        f"predictions/customer_churn_predictions"
    )
    MODEL_INPUT_DIR = (
        f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/classification/"
        f"models/customer_churn"
    )
    MODEL_CANDIDATES = ["LogisticRegression", "RandomForest"]
    PREFERRED_MODEL  = "RandomForest"

    print("=" * 60)
    print("Customer Churn Prediction - Inference Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    SELECTED_MODEL, selection_source, _ = resolve_best_model(
        spark, MODEL_INPUT_DIR, MODEL_CANDIDATES, preferred_model=PREFERRED_MODEL
    )
    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print(f"Using model: {SELECTED_MODEL} (source: {selection_source})")

    model, indexer = load_model(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None or indexer is None:
        print("✗ Inference stopped: failed to load model")
        return

    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("✗ Inference stopped: failed to load data")
        return

    is_valid, message = validate_dataset(df, ["customer_id"] + FEATURE_COLUMNS)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return

    print("✓ Dataset validated")

    # df_prepared retains the assembled "features" vector needed by the
    # plot function for StandardScaler normalization and PCA projection
    df_prepared = prepare_features(df, FEATURE_COLUMNS)

    feature_importance = extract_feature_importance(model, SELECTED_MODEL, FEATURE_COLUMNS)
    if feature_importance:
        print("✓ Feature importance (top 3):")
        for feat, imp in sorted(
            feature_importance.items(), key=lambda x: x[1], reverse=True
        )[:3]:
            print(f"  - {feat}: {imp:.4f}")

    predictions_df = generate_predictions(
        spark, df_prepared, model, indexer,
        SELECTED_MODEL, feature_importance, MODEL_VERSION
    )

    # Pass df_prepared so the plot can use the assembled "features" vector
    # for StandardScaler normalization followed by PCA projection
    export_inference_classification_plot(
        predictions_df,
        df_prepared,
        SELECTED_MODEL,
        EXPORT_PLOTS,
    )

    print("\nSample predictions:")
    predictions_df.select(
        "customer_id", "predicted_churn_risk", "churn_probability", "confidence_score"
    ).show(5, truncate=False)

    success = save_predictions(predictions_df, OUTPUT_PATH)
    if success:
        print("\n" + "=" * 60)
        print("✓ Inference completed successfully")
        print("=" * 60)
    else:
        print("\n✗ Inference failed")

    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME, EXPORT_PLOTS=False)