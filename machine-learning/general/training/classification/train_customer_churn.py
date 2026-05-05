import os
import sys
from pathlib import Path
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session

from pyspark.sql.functions import (
    col, when, lit, count, datediff, to_timestamp, udf,
    max as spark_max,
)
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer, PCA
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
)
from general.model_registry import save_best_model_manifest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_NAME           = "customer_churn"
INPUT_RELATIVE_PATH  = "transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR     = get_general_model_output_path("classification", MODEL_NAME)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)

# agg_orders supplies the forward-looking order events used to label churn.
# No separate label file or external table is needed.
ORDERS_RELATIVE_PATH = "transformed/agg_orders.parquet"

# How many days forward to look when deciding whether a customer churned.
#   0 orders in window  → "High"   (churned / at severe risk)
#   1-2 orders          → "Medium" (at risk)
#   3+ orders           → "Low"    (loyal, retained)
CHURN_LOOKAHEAD_DAYS = 90

# ---------------------------------------------------------------------------
# Feature columns
# All 15 columns are valid predictors because the churn label is derived
# from FUTURE order behaviour (a different time window), not from these values.
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

TARGET_COLUMN    = "churn_risk"
PLOT_EXPORT_DIR  = "/app/logs_for_report"
MIN_PLOT_RECORDS = 20
MAX_PLOT_RECORDS = 50000
MAX_SCATTER_POINTS = 50000
CLASS_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
]


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
# Ground-truth label computation  (replaces generate_churn_labels entirely)
#
# Why the old approach was wrong
# ──────────────────────────────
# generate_churn_labels() built the TARGET_COLUMN from the same three
# feature columns (days_since_last_purchase, order_frequency,
# cart_abandonment_rate) that are fed into the model. The model could
# therefore perfectly reconstruct the label from those features without
# learning any genuine signal, producing fake ~99 % accuracy.
#
# What this function does instead
# ────────────────────────────────
# 1. Treat the most-recent order date in agg_orders as the data horizon T.
# 2. Define a "future" window = (T − CHURN_LOOKAHEAD_DAYS, T].
# 3. Count each customer's non-cancelled orders inside that window.
# 4. Label:  0 orders → "High",  1-2 → "Medium",  3+ → "Low".
#
# The label now describes what the customer DID after the feature snapshot
# was recorded, making it genuinely independent of the feature values.
# ---------------------------------------------------------------------------
def compute_ground_truth_labels(spark, customers_df, orders_df):
    """
    Join forward-looking churn labels derived from agg_orders onto
    customers_df. Any pre-existing churn_risk column is discarded.

    Parameters
    ----------
    spark        : SparkSession
    customers_df : DataFrame — agg_customers (must contain customer_id)
    orders_df    : DataFrame — agg_orders (must contain customer_id,
                               order_placed_at, order_status, order_id)
    Returns
    -------
    DataFrame  — customers_df with a new churn_risk column.
    """
    print(f"\n  Computing forward-looking labels from agg_orders "
          f"(window = last {CHURN_LOOKAHEAD_DAYS} days of data)...")

    orders_df = orders_df.withColumn(
        "order_placed_at", to_timestamp(col("order_placed_at"))
    )

    # Determine the data horizon (latest timestamp in the orders table)
    latest_date = orders_df.select(spark_max("order_placed_at")).collect()[0][0]
    if latest_date is None:
        raise RuntimeError(
            "agg_orders contains no valid order_placed_at timestamps. "
            "Cannot compute forward-looking churn labels."
        )

    print(f"  Data horizon (latest order date) : {latest_date}")
    print(f"  Label window                     : last {CHURN_LOOKAHEAD_DAYS} days")

    # Count non-cancelled orders per customer in the label window
    future_orders = (
        orders_df
        .filter(col("order_status") != "Cancelled")
        .filter(datediff(lit(latest_date), col("order_placed_at")) >= 0)
        .filter(datediff(lit(latest_date), col("order_placed_at")) <= CHURN_LOOKAHEAD_DAYS)
        .groupBy("customer_id")
        .agg(count("order_id").alias("future_order_cnt"))
    )

    # Drop any pre-existing churn_risk so the join doesn't create conflicts
    if TARGET_COLUMN in customers_df.columns:
        print(f"  Dropping pre-existing '{TARGET_COLUMN}' from agg_customers "
              "(replacing with forward-looking label)")
        customers_df = customers_df.drop(TARGET_COLUMN)

    # Left-join so customers with NO orders in the window get cnt = 0 (High)
    labeled_df = (
        customers_df
        .join(future_orders, on="customer_id", how="left")
        .fillna(0, subset=["future_order_cnt"])
        .withColumn(
            TARGET_COLUMN,
            when(col("future_order_cnt") == 0, "High")
            .when(col("future_order_cnt") >= 3, "Low")
            .otherwise("Medium"),
        )
        .drop("future_order_cnt")
    )

    # Show distribution and warn if severely imbalanced
    print("\n  Label distribution (ground truth):")
    labeled_df.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False).show()

    total = labeled_df.count()
    high  = labeled_df.filter(col(TARGET_COLUMN) == "High").count()
    pct   = 100.0 * high / total if total > 0 else 0.0
    if pct > 80:
        print(
            f"  ⚠️  {pct:.1f}% of customers are 'High'. "
            f"Consider adjusting CHURN_LOOKAHEAD_DAYS (currently "
            f"{CHURN_LOOKAHEAD_DAYS}) or the order-count thresholds."
        )

    return labeled_df


# ---------------------------------------------------------------------------
# Stratified train / test split
# ---------------------------------------------------------------------------
def stratified_split(df, label_col, train_ratio=0.8, seed=42):
    """
    Split preserving per-class proportions.

    PySpark's randomSplit() does not stratify, so minority classes can
    vanish from the test fold on imbalanced datasets.
    """
    train_frames, test_frames = [], []
    for (label_val,) in df.select(label_col).distinct().collect():
        subset = df.filter(col(label_col) == label_val)
        tr, te = subset.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)
        train_frames.append(tr)
        test_frames.append(te)
    return (
        reduce(lambda a, b: a.union(b), train_frames),
        reduce(lambda a, b: a.union(b), test_frames),
    )


# ---------------------------------------------------------------------------
# Inverse-frequency class weights
# ---------------------------------------------------------------------------
def add_class_weights(spark, df, label_col="label"):
    """
    Append class_weight = total / (n_classes × class_count).

    Prevents the majority class from dominating gradient updates in LR and RF.
    """
    rows      = df.groupBy(label_col).count().collect()
    total     = df.count()
    n_classes = len(rows)
    wmap      = {r[label_col]: total / (n_classes * r["count"]) for r in rows}
    print(f"  Class weights (label index → weight): {wmap}")

    bc   = spark.sparkContext.broadcast(wmap)
    wudf = udf(lambda lbl: bc.value.get(lbl, 1.0), DoubleType())
    return df.withColumn("class_weight", wudf(col(label_col)))


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------
def export_training_classification_plot(predictions_df, model_name, export_plots, export_dir=PLOT_EXPORT_DIR):
    if not export_plots:
        return None

    # Reduce 15-D feature vector to 2 principal components for plotting
    pca = PCA(k=2, inputCol="features", outputCol="pca_features")
    pca_model = pca.fit(predictions_df)
    pca_df = pca_model.transform(predictions_df)

    rows = (
        pca_df
        .select(TARGET_COLUMN, "pca_features")
        .limit(MAX_PLOT_RECORDS)
        .collect()
    )
    if len(rows) < MIN_PLOT_RECORDS:
        return None

    classes = [str(r[TARGET_COLUMN] or "unknown") for r in rows]
    # Extract the two PCA components from the vector
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
            ax.scatter(cx, cy, s=34, alpha=0.75, edgecolor="black",
                       linewidth=0.35, color=cmap[cls], label=cls)

    ax.set_title(f"Customer Churn Classification - {model_name} (PCA projection)")
    ax.set_xlabel(FEATURE_COLUMNS[0])
    ax.set_ylabel(FEATURE_COLUMNS[1])
    ax.legend(loc="best", title="Class")
    fig.tight_layout()

    out = os.path.join(
        _ensure_plot_dir(export_dir),
        f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-training-fit.png",
    )
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported training plot: {out}")
    return out


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def create_spark_session():
    return create_ml_spark_session(
        "CustomerChurnTraining",
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "inferSchema": "true",
            "mergeSchema": "true",
        },
    )


# ---------------------------------------------------------------------------
# Data loading & validation
# ---------------------------------------------------------------------------
def load_data(spark, path):
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records from {path}")
        return df
    except Exception as e:
        print(f"✗ Failed to load {path}: {e}")
        return None


def validate_dataset(df, required_columns):
    if df is None:
        return False, "Dataset is None"
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        return False, f"Missing columns: {missing}"
    for c in required_columns:
        if df.filter(col(c).isNotNull()).count() == 0:
            return False, f"Column '{c}' is entirely null"
    return True, "Validation passed"


# ---------------------------------------------------------------------------
# Feature preparation
# ---------------------------------------------------------------------------
def prepare_features(df, feature_cols):
    df_filled  = df.fillna(0, subset=feature_cols)
    df_clean   = df_filled.filter(col(TARGET_COLUMN).isNotNull())
    dropped    = df_filled.count() - df_clean.count()
    if dropped:
        print(f"⚠️  Filtered {dropped} records with null {TARGET_COLUMN}")

    assembler     = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector     = assembler.transform(df_clean)
    indexer       = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    indexer_model = indexer.fit(df_vector)
    df_indexed    = indexer_model.transform(df_vector)

    print(f"✓ Features vectorized ({len(feature_cols)} columns)")
    print(f"  Label mapping: {dict(enumerate(indexer_model.labels))}")
    return df_indexed, indexer_model


# ---------------------------------------------------------------------------
# Model trainers (class_weight column used by both)
# ---------------------------------------------------------------------------
def train_logistic_regression(train_df):
    print("\n[1/2] Training Logistic Regression...")
    model = LogisticRegression(
        maxIter=100, regParam=0.01, elasticNetParam=0.5,
        weightCol="class_weight",
    ).fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_random_forest(train_df):
    print("\n[2/2] Training Random Forest...")
    model = RandomForestClassifier(
        numTrees=100, maxDepth=10, seed=42,
        weightCol="class_weight",
        featureSubsetStrategy="sqrt",
    ).fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
def evaluate_model(model, test_df, model_name):
    predictions = model.transform(test_df)
    ev = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    metrics = {
        "model_name": model_name,
        "accuracy":   ev.evaluate(predictions, {ev.metricName: "accuracy"}),
        "precision":  ev.evaluate(predictions, {ev.metricName: "weightedPrecision"}),
        "recall":     ev.evaluate(predictions, {ev.metricName: "weightedRecall"}),
        "f1_score":   ev.evaluate(predictions, {ev.metricName: "f1"}),
    }
    print(f"\n{model_name} Metrics:")
    for k, v in metrics.items():
        if k != "model_name":
            print(f"  {k.capitalize():12s}: {v:.4f}")
    return metrics, predictions


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
def save_model(model, indexer_model, output_dir, model_name):
    model.write().overwrite().save(f"{output_dir}/{model_name}")
    indexer_model.write().overwrite().save(f"{output_dir}/{model_name}_indexer")
    print(f"✓ Saved {model_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(EXPORT_PLOTS=False):
    print("=" * 60)
    print("Customer Churn Prediction - Training Pipeline")
    print("=" * 60)
    print(f"Training window : {MIN_RECORDS} – {MAX_RECORDS} records")
    print(f"Model output    : {MODEL_OUTPUT_DIR}")
    print(f"Label lookahead : {CHURN_LOOKAHEAD_DAYS} days (from agg_orders)")
    print("=" * 60)

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # Step 1 — Load customer features from agg_customers
    # ------------------------------------------------------------------
    print("\nStep 1: Loading customer features...")
    df, record_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=FEATURE_COLUMNS,
        filter_nulls=True,
    )
    if df is None:
        print("⚠️  No feature data. Skipping training.")
        spark.stop()
        return

    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    if not is_valid:
        print("⚠️  Training skipped: insufficient records.")
        spark.stop()
        return

    # ------------------------------------------------------------------
    # Step 2 — Load agg_orders and derive real churn labels
    #
    # agg_orders already exists in the data pipeline (same schema/bucket
    # pattern as agg_customers). We use order_placed_at, order_status,
    # order_id, and customer_id to count each customer's future orders
    # within a rolling CHURN_LOOKAHEAD_DAYS window from the data horizon.
    #
    # This completely replaces generate_churn_labels(), which caused data
    # leakage by building the label from the same feature columns.
    # ------------------------------------------------------------------
    print("\nStep 2: Loading agg_orders for ground-truth label computation...")
    orders_df, _ = load_data_from_all_buckets(
        spark,
        ORDERS_RELATIVE_PATH,
        required_columns=["customer_id", "order_placed_at", "order_id", "order_status"],
        filter_nulls=False,
    )
    if orders_df is None:
        print(
            f"✗ Training stopped: could not load agg_orders.\n"
            f"  Expected path: <bucket>/{ORDERS_RELATIVE_PATH}\n"
            f"  agg_orders is required to produce leakage-free churn labels."
        )
        spark.stop()
        return

    df = compute_ground_truth_labels(spark, df, orders_df)

    # ------------------------------------------------------------------
    # Step 3 — Validate
    # ------------------------------------------------------------------
    print("\nStep 3: Validating labeled dataset...")
    is_valid, message = validate_dataset(df, FEATURE_COLUMNS + [TARGET_COLUMN])
    if not is_valid:
        print(f"⚠️  Training skipped: {message}")
        spark.stop()
        return

    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_RECORDS:
        print(f"⚠️  Only {labeled_count} labeled records (< {MIN_RECORDS}). Skipping.")
        spark.stop()
        return

    print(f"✓ {labeled_count} labeled records ready")

    # ------------------------------------------------------------------
    # Step 4 — Prepare features
    # ------------------------------------------------------------------
    print("\nStep 4: Preparing features...")
    df_prepared, indexer_model = prepare_features(df, FEATURE_COLUMNS)

    # ------------------------------------------------------------------
    # Step 5 — Stratified split + class weights
    # ------------------------------------------------------------------
    print("\nStep 5: Stratified split and class weights...")
    train_df, test_df = stratified_split(df_prepared, label_col="label")
    print(f"  Train rows: {train_df.count()} | Test rows: {test_df.count()}")
    train_df = add_class_weights(spark, train_df, label_col="label")

    # ------------------------------------------------------------------
    # Step 6 — Train
    # ------------------------------------------------------------------
    models = [
        train_logistic_regression(train_df),
        train_random_forest(train_df),
    ]

    # ------------------------------------------------------------------
    # Step 7 — Evaluate, save, plot
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Evaluation & Comparison")
    print("=" * 60)

    all_metrics = []
    for model, model_name in models:
        metrics, predictions = evaluate_model(model, test_df, model_name)
        all_metrics.append(metrics)
        save_model(model, indexer_model, MODEL_OUTPUT_DIR, model_name)
        export_training_classification_plot(predictions, model_name, EXPORT_PLOTS)

    # ------------------------------------------------------------------
    # Step 8 — Best model manifest
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | Acc: {m['accuracy']:.4f}")

    best = max(all_metrics, key=lambda x: x["f1_score"])
    manifest_path = save_best_model_manifest(
        spark,
        MODEL_OUTPUT_DIR,
        best["model_name"],
        "f1_score",
        best["f1_score"],
        {m["model_name"]: m["f1_score"] for m in all_metrics},
    )
    print(f"✓ Best model manifest: {manifest_path}")

    print("\n" + "=" * 60)
    print("✓ Training completed successfully")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main(EXPORT_PLOTS=False)