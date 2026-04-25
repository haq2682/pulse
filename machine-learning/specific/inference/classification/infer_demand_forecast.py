"""
Demand Forecast Classification - Inference Script
Predicts demand class: high / not_high.
Gracefully skips inference when required input columns are missing.

Plot fixes applied:
- Replaced misleading rank-sorted scatter with histogram + class-balance pie
- Loads isotonic calibrator (if available) and applies it to raw model
  probabilities before thresholding and output — matching the fix in training

v3: Decision thresholds saved by training are now post-calibration values,
so inference correctly uses them against calibrated probabilities with no
further changes needed here.
"""

import os
import sys
import uuid
import pickle
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from dotenv import load_dotenv

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session
from general.utils.plot_exporter import export_inference_outputs_plot
from specific.model_registry import resolve_best_model

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
import math
import json


load_dotenv()


FEATURE_NUMERIC_COLUMNS = [
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
    "demand_lag_6m",
    "demand_rolling_3m",
    "demand_rolling_6m",
    "demand_volatility_6m",
    "demand_momentum_3m",
    "relative_demand_to_rolling_6m",
    "log_demand_lag_1m",
    "growth_rate_1m",
    "price_x_seasonality",
    "category_seasonal_x_month",
    "demand_acceleration_6m",
    "demand_rolling_3_to_6_ratio",
    "avg_quantity_lag_1m",
    "order_size_momentum_1m",
    "price_to_category_avg",
    "rating_x_price",
]

FEATURE_CATEGORICAL_COLUMNS = ["category"]

MODEL_CANDIDATES = ["logistic_regression", "random_forest", "random_forest_tuned", "decision_tree"]
PLOT_EXPORT_DIR = "/app/logs_for_report"
MIN_PLOT_RECORDS = 20
MAX_PLOT_RECORDS = 5000
CLASS_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
]

REQUIRED_COLUMNS = {
    "products": [
        "product_id",
        "category",
        "sell_price",
        "days_since_launch",
        "avg_rating",
        "profit_margin",
    ],
    "orders": [
        "order_id",
        "order_status",
        "order_placed_year",
        "order_placed_month",
        "order_placed_quarter",
        "order_placed_week_of_year",
        "order_placed_day_of_week",
    ],
    "order_items": ["order_id", "product_id", "quantity"],
    "categories": [
        "category",
        "avg_category_growth_rate",
        "seasonal_index_spring",
        "seasonal_index_summer",
        "seasonal_index_fall",
        "seasonal_index_winter",
    ],
}


def _ensure_plot_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _sanitize_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in value.lower())


# ---------------------------------------------------------------------------
# Plot export — histogram + class-balance pie (mirrors training version)
# ---------------------------------------------------------------------------

def export_inference_classification_plot(
    predictions_df,
    model_name: str,
    export_plots: bool,
    calibrator=None,
    export_dir: str = PLOT_EXPORT_DIR,
):
    """
    Export a two-panel diagnostic plot for inference predictions:
      Left  — overlapping density histogram of P(high demand) per predicted class.
              If calibration was applied, also overlays the pre-calibration
              (raw) distribution as a dashed line for comparison.
      Right — predicted class balance as a pie chart.

    This replaces the old rank-sorted scatter that produced misleading
    horizontal bands from probability discretisation in tree-based models.
    """
    if not export_plots:
        return None

    rows = (
        predictions_df
        .select("predicted_demand_class", "high_demand_probability", "raw_high_demand_probability")
        .orderBy(F.desc("high_demand_probability"))
        .limit(MAX_PLOT_RECORDS)
        .collect()
    )

    if len(rows) < MIN_PLOT_RECORDS:
        print(
            f"⚠️  Inference plot export skipped for {model_name}: "
            f"insufficient rows ({len(rows)} < {MIN_PLOT_RECORDS})"
        )
        return None

    classes = [str(r["predicted_demand_class"] or "unknown") for r in rows]
    cal_scores = np.array([float(r["high_demand_probability"] or 0.0) for r in rows])
    raw_scores = np.array([
        float(r["raw_high_demand_probability"] or 0.0) for r in rows
    ])
    has_calibration = calibrator is not None and not np.allclose(cal_scores, raw_scores, atol=1e-6)

    unique_classes = sorted(set(classes))
    color_map = {cls: CLASS_COLORS[i % len(CLASS_COLORS)] for i, cls in enumerate(unique_classes)}

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(
        f"Demand Forecast Classification Inference — {model_name}",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )

    # ---- Left panel: probability density histogram -------------------------
    ax = axes[0]
    bins = np.linspace(0.0, 1.0, 41)

    for cls in unique_classes:
        mask = np.array([c == cls for c in classes])
        cls_cal = cal_scores[mask]
        if cls_cal.size == 0:
            continue
        ax.hist(
            cls_cal,
            bins=bins,
            alpha=0.55,
            color=color_map[cls],
            label=f"{cls}" + (" (calibrated)" if has_calibration else ""),
            density=True,
            edgecolor="white",
            linewidth=0.4,
        )
        # Overlay raw (pre-calibration) distribution as dashed step histogram
        if has_calibration:
            cls_raw = raw_scores[mask]
            ax.hist(
                cls_raw,
                bins=bins,
                histtype="step",
                linestyle="--",
                linewidth=1.6,
                color=color_map[cls],
                label=f"{cls} (raw)",
                density=True,
            )

    ax.axvline(0.5, color="black", linestyle=":", linewidth=1.0, label="threshold 0.5")
    ax.set_title("P(high demand) distribution by predicted class")
    ax.set_xlabel("P(high demand)")
    ax.set_ylabel("Density")
    ax.set_xlim(0.0, 1.0)
    ax.legend(title="Predicted class", fontsize=8)

    p_min, p_max = float(cal_scores.min()), float(cal_scores.max())
    ax.text(
        0.02, 0.97,
        f"range [{p_min:.3f}, {p_max:.3f}]",
        transform=ax.transAxes,
        fontsize=8,
        va="top",
        color="gray",
    )

    # ---- Right panel: class balance pie ------------------------------------
    ax2 = axes[1]
    class_counts = {cls: int(np.sum(np.array(classes) == cls)) for cls in unique_classes}
    wedge_colors = [color_map[c] for c in class_counts]
    wedges, texts, autotexts = ax2.pie(
        class_counts.values(),
        labels=class_counts.keys(),
        colors=wedge_colors,
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    for at in autotexts:
        at.set_fontsize(10)
    ax2.set_title("Predicted class balance")
    ax2.text(
        0.5, -0.08,
        f"n = {len(rows):,}",
        transform=ax2.transAxes,
        ha="center",
        fontsize=9,
        color="gray",
    )

    fig.tight_layout()

    plot_dir = _ensure_plot_dir(export_dir)
    file_name = (
        f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-forecast-fit.png"
    )
    output_path = os.path.join(plot_dir, file_name)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported inference classification plot: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Isotonic calibrator loading
# ---------------------------------------------------------------------------

def load_isotonic_calibrator(spark, model_dir: str, model_name: str):
    """
    Load the isotonic calibrator persisted by the training script.
    Returns a fitted IsotonicRegression instance, or None if not found.
    """
    path = f"{model_dir.rstrip('/')}/_calibrators/{model_name}"
    try:
        row = spark.read.text(path).limit(1).collect()
        if not row or not row[0]["value"]:
            return None
        import base64
        calibrator = pickle.loads(base64.b64decode(row[0]["value"]))
        print(f"✓ Isotonic calibrator loaded for {model_name}")
        return calibrator
    except Exception as exc:
        print(f"ℹ️  No isotonic calibrator found for {model_name} ({exc}); using raw probabilities")
        return None


def apply_calibrator_to_predictions(predictions_df, calibrator, spark):
    """
    Apply a loaded isotonic calibrator to the high_demand_probability column.
    The original raw probability is preserved as raw_high_demand_probability.
    If no calibrator is provided, raw_high_demand_probability mirrors
    high_demand_probability so downstream code is uniform.
    """
    if calibrator is None:
        return predictions_df.withColumn(
            "raw_high_demand_probability", F.col("high_demand_probability")
        )

    rows = predictions_df.select("product_id", "high_demand_probability").collect()
    product_ids = [r["product_id"] for r in rows]
    raw_probs = np.array([float(r["high_demand_probability"] or 0.0) for r in rows])
    cal_probs = calibrator.predict(raw_probs).tolist()

    cal_df = spark.createDataFrame(
        list(zip(product_ids, raw_probs.tolist(), cal_probs)),
        ["product_id", "raw_high_demand_probability", "calibrated_high_demand_probability"],
    )

    return (
        predictions_df
        .join(cal_df, on="product_id", how="left")
        .withColumn(
            "raw_high_demand_probability",
            F.coalesce(F.col("raw_high_demand_probability"), F.col("high_demand_probability")),
        )
        .withColumn(
            "high_demand_probability",
            F.coalesce(
                F.col("calibrated_high_demand_probability").cast(DoubleType()),
                F.col("high_demand_probability"),
            ),
        )
        .drop("calibrated_high_demand_probability")
    )


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session():
    return create_ml_spark_session(
        "Demand_Forecast_Classification_Inference",
        extra_configs={"spark.sql.shuffle.partitions": "8"},
    )


# ---------------------------------------------------------------------------
# Data loading / validation
# ---------------------------------------------------------------------------

def load_dataset(spark, path, name):
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {name}: {df.count()} records")
        return df
    except Exception as exc:
        print(f"✗ Failed to load {name}: {exc}")
        return None


def missing_columns(df, required_columns):
    current = set(df.columns)
    return [column for column in required_columns if column not in current]


def validate_required_columns(dataset_map):
    errors = []
    for dataset_name, required in REQUIRED_COLUMNS.items():
        dataframe = dataset_map.get(dataset_name)
        if dataframe is None:
            errors.append((dataset_name, required))
            continue
        missing = missing_columns(dataframe, required)
        if missing:
            errors.append((dataset_name, missing))

    if errors:
        print("✗ Inference skipped: required columns are missing")
        for dataset_name, cols in errors:
            print(f"  - {dataset_name}: missing {cols}")
        return False
    return True


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def build_monthly_product_frame(orders_df, order_items_df, products_df):
    delivered_statuses = ["delivered", "complete", "completed"]

    orders_filtered = orders_df.filter(
        F.lower(F.coalesce(F.col("order_status"), F.lit(""))).isin(delivered_statuses)
    )

    orders_items = orders_filtered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "inner",
    )

    joined = orders_items.join(
        products_df.alias("p"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "inner",
    ).select(
        F.col("oi.product_id").alias("product_id"),
        F.col("oi.quantity").alias("quantity"),
        F.col("o.order_placed_year").cast("int").alias("order_placed_year"),
        F.col("o.order_placed_month").cast("int").alias("order_placed_month"),
        F.col("p.sell_price").cast("double").alias("sell_price"),
        F.col("p.days_since_launch").cast("double").alias("days_since_launch"),
        F.col("p.avg_rating").cast("double").alias("avg_rating"),
        F.col("p.profit_margin").cast("double").alias("profit_margin"),
        F.col("p.category").alias("category"),
    )

    joined = joined.withColumn(
        "year_month",
        F.concat(
            F.col("order_placed_year").cast("string"),
            F.lit("-"),
            F.lpad(F.col("order_placed_month").cast("string"), 2, "0"),
        ),
    ).withColumn(
        "ym_index", F.col("order_placed_year") * F.lit(100) + F.col("order_placed_month")
    )

    monthly = joined.groupBy(
        "product_id",
        "year_month",
        "ym_index",
        "order_placed_year",
        "order_placed_month",
        "sell_price",
        "days_since_launch",
        "avg_rating",
        "profit_margin",
        "category",
    ).agg(
        F.sum("quantity").alias("monthly_demand"),
        F.count("*").alias("total_orders"),
        F.avg("quantity").alias("avg_quantity_per_order"),
    )

    return monthly


def clean_source_data(orders_df, order_items_df, products_df, categories_df):
    orders_clean = (
        orders_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_placed_year").isNotNull())
        .filter(F.col("order_placed_month").between(1, 12))
    )

    order_items_clean = (
        order_items_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("quantity") > 0)
    )

    products_clean = (
        products_df
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("category").isNotNull())
        .filter(F.col("sell_price").isNotNull())
        .filter(F.col("sell_price") > 0)
        .withColumn("avg_rating", F.when(F.col("avg_rating").between(0, 5), F.col("avg_rating")).otherwise(F.lit(None)))
        .withColumn("days_since_launch", F.when(F.col("days_since_launch") >= 0, F.col("days_since_launch")).otherwise(F.lit(None)))
    )

    categories_clean = categories_df.filter(F.col("category").isNotNull()).dropDuplicates(["category"])
    return orders_clean, order_items_clean, products_clean, categories_clean


def clip_outliers(df, column_name: str, low_q: float = 0.01, high_q: float = 0.99):
    quantiles = df.approxQuantile(column_name, [low_q, high_q], 0.01)
    if len(quantiles) != 2:
        return df

    low_val, high_val = float(quantiles[0]), float(quantiles[1])
    if high_val < low_val:
        return df

    return df.withColumn(
        column_name,
        F.when(F.col(column_name) < F.lit(low_val), F.lit(low_val))
        .when(F.col(column_name) > F.lit(high_val), F.lit(high_val))
        .otherwise(F.col(column_name)),
    )


def filter_low_history_products(df, min_months: int = 6):
    history_df = df.groupBy("product_id").agg(F.countDistinct("year_month").alias("history_months"))
    keep_df = history_df.filter(F.col("history_months") >= F.lit(min_months)).select("product_id")
    return df.join(keep_df, "product_id", "inner")


# ---------------------------------------------------------------------------
# Threshold loading
# ---------------------------------------------------------------------------

def load_decision_thresholds(spark, model_dir: str):
    threshold_path = f"{model_dir.rstrip('/')}/_decision_thresholds"
    try:
        row = spark.read.text(threshold_path).limit(1).collect()
        if row and row[0]["value"]:
            payload = json.loads(row[0]["value"])
            return {str(k): float(v) for k, v in payload.items()}
    except Exception:
        return {}
    return {}


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def engineer_features(monthly_df, categories_df):
    category_lookup = categories_df.select(
        F.col("category"),
        F.col("avg_category_growth_rate").cast("double").alias("category_growth_rate"),
        F.col("seasonal_index_spring").cast("double").alias("seasonal_index_spring"),
        F.col("seasonal_index_summer").cast("double").alias("seasonal_index_summer"),
        F.col("seasonal_index_fall").cast("double").alias("seasonal_index_fall"),
        F.col("seasonal_index_winter").cast("double").alias("seasonal_index_winter"),
    )

    by_product_time = Window.partitionBy("product_id").orderBy("ym_index")
    cumulative_product_window = by_product_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cumulative_category_window = Window.partitionBy("category").orderBy("ym_index").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    rolling_6m = by_product_time.rowsBetween(-6, -1)
    rolling_3m = by_product_time.rowsBetween(-3, -1)
    category_month_window = Window.partitionBy("category", "ym_index")

    features = monthly_df.withColumn(
        "demand_lag_1m", F.lag("monthly_demand", 1).over(by_product_time)
    ).withColumn(
        "demand_lag_3m", F.lag("monthly_demand", 3).over(by_product_time)
    ).withColumn(
        "demand_lag_6m", F.lag("monthly_demand", 6).over(by_product_time)
    ).withColumn(
        "demand_rolling_3m", F.avg("monthly_demand").over(rolling_3m)
    ).withColumn(
        "demand_rolling_6m", F.avg("monthly_demand").over(rolling_6m)
    ).withColumn(
        "demand_volatility_6m", F.stddev("monthly_demand").over(rolling_6m)
    ).withColumn(
        "prev_month_demand", F.lag("monthly_demand", 1).over(by_product_time)
    ).withColumn(
        "growth_rate_1m",
        F.when(
            (F.col("prev_month_demand").isNotNull()) & (F.col("prev_month_demand") > 0),
            (F.col("monthly_demand") - F.col("prev_month_demand")) / F.col("prev_month_demand"),
        ).otherwise(F.lit(0.0)),
    ).withColumn(
        "demand_momentum_3m",
        F.col("demand_lag_1m") - F.col("demand_lag_3m"),
    ).withColumn(
        "avg_quantity_lag_1m", F.lag("avg_quantity_per_order", 1).over(by_product_time)
    )

    features = features.withColumn(
        "relative_demand_to_rolling_6m",
        F.when(F.col("demand_rolling_6m") > 0, F.col("demand_lag_1m") / F.col("demand_rolling_6m")).otherwise(F.lit(0.0)),
    ).withColumn(
        "log_demand_lag_1m",
        F.log1p(F.greatest(F.col("demand_lag_1m"), F.lit(0.0))),
    ).withColumn(
        "demand_acceleration_6m",
        (F.col("demand_lag_1m") - F.col("demand_lag_3m")) - (F.col("demand_lag_3m") - F.col("demand_lag_6m")),
    ).withColumn(
        "demand_rolling_3_to_6_ratio",
        F.when(F.col("demand_rolling_6m") > 0, F.col("demand_rolling_3m") / F.col("demand_rolling_6m")).otherwise(F.lit(0.0)),
    ).withColumn(
        "order_size_momentum_1m",
        F.when(F.col("avg_quantity_lag_1m").isNotNull(), F.col("avg_quantity_per_order") - F.col("avg_quantity_lag_1m")).otherwise(F.lit(0.0)),
    )

    features = features.join(category_lookup, "category", "left")

    features = features.withColumn(
        "order_placed_quarter",
        F.when(F.col("order_placed_month").isin([1, 2, 3]), 1)
        .when(F.col("order_placed_month").isin([4, 5, 6]), 2)
        .when(F.col("order_placed_month").isin([7, 8, 9]), 3)
        .otherwise(4),
    )

    month_start_date = F.to_date(F.concat_ws("-", F.col("year_month"), F.lit("01")))
    features = features.withColumn("order_placed_week_of_year", F.weekofyear(month_start_date)).withColumn(
        "order_placed_day_of_week", F.dayofweek(month_start_date)
    )

    features = features.withColumn(
        "monthly_revenue", F.col("monthly_demand") * F.col("sell_price")
    ).withColumn(
        "category_month_avg_price",
        F.avg("sell_price").over(category_month_window),
    ).withColumn(
        "product_cumulative_revenue",
        F.sum("monthly_revenue").over(cumulative_product_window),
    ).withColumn(
        "category_cumulative_revenue",
        F.sum("monthly_revenue").over(cumulative_category_window),
    ).withColumn(
        "product_category_share",
        F.when(F.col("category_cumulative_revenue") > 0, F.col("product_cumulative_revenue") / F.col("category_cumulative_revenue")).otherwise(F.lit(0.0)),
    ).withColumn(
        "price_to_category_avg",
        F.when(F.col("category_month_avg_price") > 0, F.col("sell_price") / F.col("category_month_avg_price")).otherwise(F.lit(1.0)),
    ).withColumn(
        "rating_x_price",
        F.coalesce(F.col("avg_rating"), F.lit(0.0)) * F.coalesce(F.col("sell_price"), F.lit(0.0)),
    )

    features = features.withColumn(
        "category_seasonal_current",
        F.when(F.col("order_placed_month").isin([3, 4, 5]), F.col("seasonal_index_spring"))
        .when(F.col("order_placed_month").isin([6, 7, 8]), F.col("seasonal_index_summer"))
        .when(F.col("order_placed_month").isin([9, 10, 11]), F.col("seasonal_index_fall"))
        .otherwise(F.col("seasonal_index_winter")),
    )

    features = features.withColumn(
        "month_sin", F.sin(2 * math.pi * F.col("order_placed_month") / F.lit(12))
    ).withColumn(
        "month_cos", F.cos(2 * math.pi * F.col("order_placed_month") / F.lit(12))
    ).withColumn(
        "quarter_sin", F.sin(2 * math.pi * F.col("order_placed_quarter") / F.lit(4))
    ).withColumn(
        "quarter_cos", F.cos(2 * math.pi * F.col("order_placed_quarter") / F.lit(4))
    )

    features = features.withColumn(
        "price_x_seasonality", F.col("sell_price") * F.coalesce(F.col("category_seasonal_current"), F.lit(0.0))
    ).withColumn(
        "category_seasonal_x_month",
        F.coalesce(F.col("category_seasonal_current"), F.lit(0.0)) * F.col("order_placed_month"),
    )

    fill_zero = [
        "demand_lag_1m",
        "demand_lag_3m",
        "demand_lag_6m",
        "demand_rolling_3m",
        "demand_rolling_6m",
        "demand_volatility_6m",
        "demand_momentum_3m",
        "relative_demand_to_rolling_6m",
        "log_demand_lag_1m",
        "growth_rate_1m",
        "category_growth_rate",
        "category_seasonal_current",
        "product_category_share",
        "demand_acceleration_6m",
        "demand_rolling_3_to_6_ratio",
        "avg_quantity_lag_1m",
        "order_size_momentum_1m",
        "price_to_category_avg",
        "rating_x_price",
    ]
    features = features.fillna(0, subset=fill_zero)

    return features


def validate_engineered_feature_columns(df, context):
    required = set(FEATURE_NUMERIC_COLUMNS + FEATURE_CATEGORICAL_COLUMNS)
    missing = sorted([column for column in required if column not in df.columns])
    if missing:
        raise ValueError(f"{context}: missing engineered features: {missing}")


# ---------------------------------------------------------------------------
# Latest feature row selection
# ---------------------------------------------------------------------------

def build_latest_feature_rows(features_df):
    latest_window = Window.partitionBy("product_id").orderBy(F.desc("ym_index"))

    latest = (
        features_df.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "prev_month_demand")
    )

    latest = latest.fillna(0, subset=FEATURE_NUMERIC_COLUMNS).fillna("unknown", subset=FEATURE_CATEGORICAL_COLUMNS)
    return latest


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

def load_model(model_path):
    try:
        model = PipelineModel.load(model_path)
        print(f"✓ Model loaded: {model_path}")
        return model
    except Exception as exc:
        print(f"✗ Failed to load model {model_path}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Prediction generation
# ---------------------------------------------------------------------------

def generate_predictions(model, features_df, model_name, forecast_horizon_days, decision_threshold):
    """
    Score features through the pipeline model and apply the decision threshold.
    Returns a DataFrame with raw probability in high_demand_probability.
    Calibration (if any) is applied after this step via apply_calibrator_to_predictions.
    """
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

    scored = model.transform(features_df)

    scored = scored.withColumn(
        "high_demand_probability",
        vector_to_array(F.col("probability")).getItem(1),
    )

    now_ts = datetime.now()
    output = scored.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("product_id"),
        F.lit((now_ts + timedelta(days=forecast_horizon_days)).date()).cast("date").alias("forecast_date"),
        F.lit(now_ts).alias("prediction_date"),
        F.col("high_demand_probability"),
        F.array_max(vector_to_array(F.col("probability"))).alias("confidence_score"),
        F.lit(forecast_horizon_days).alias("forecast_horizon_days"),
        F.lit(model_name).alias("model_version"),
    )

    print(f"✓ Generated raw scores: {output.count()} rows")
    return output


def apply_threshold_and_labels(predictions_df, decision_threshold: float):
    """
    Apply decision threshold to high_demand_probability (which may already be
    calibrated) and produce predicted_class_index / predicted_demand_class.
    Kept separate from generate_predictions so calibration can run in between.
    """
    return predictions_df.withColumn(
        "predicted_class_index",
        F.when(F.col("high_demand_probability") >= F.lit(float(decision_threshold)), F.lit(1))
        .otherwise(F.lit(0)),
    ).withColumn(
        "predicted_demand_class",
        F.when(F.col("predicted_class_index") == 1, F.lit("high"))
        .when(F.col("predicted_class_index") == 0, F.lit("not_high"))
        .otherwise(F.lit("unknown")),
    )


# ---------------------------------------------------------------------------
# Save predictions
# ---------------------------------------------------------------------------

def save_predictions(df, output_path):
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as exc:
        print(f"✗ Failed to save predictions: {exc}")
        return False


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def display_samples(df, limit_n=5):
    print("\n" + "=" * 70)
    print(f"Sample predictions (top {limit_n})")
    print("=" * 70)

    rows = (
        df.select(
            "product_id",
            "predicted_demand_class",
            "high_demand_probability",
            "raw_high_demand_probability",
            "confidence_score",
        )
        .orderBy(F.desc("high_demand_probability"))
        .limit(limit_n)
        .collect()
    )

    for row in rows:
        raw_str = (
            f"  raw={float(row['raw_high_demand_probability']):.3f}"
            if row["raw_high_demand_probability"] is not None
            else ""
        )
        print(
            f"Product: {row['product_id']:<30} "
            f"Class: {row['predicted_demand_class']:<8} "
            f"P(high): {float(row['high_demand_probability']):.3f}{raw_str} "
            f"Confidence: {float(row['confidence_score']):.3f}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(BUCKET_NAME, EXPORT_PLOTS=False):
    input_products_path = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    input_orders_path = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    input_order_items_path = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    input_categories_path = f"s3a://{BUCKET_NAME}/transformed/agg_categories.parquet"

    model_dir = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/demand_forecast/"
    output_path = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/demand_forecast/"

    preferred_model = os.getenv("DEMAND_FORECAST_CLASSIFIER", "random_forest_tuned")
    forecast_horizon_days = 30

    print("\n" + "=" * 70)
    print("Demand Forecast Classification - Inference")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Preferred model: {preferred_model}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    products_df = load_dataset(spark, input_products_path, "Products")
    orders_df = load_dataset(spark, input_orders_path, "Orders")
    order_items_df = load_dataset(spark, input_order_items_path, "Order Items")
    categories_df = load_dataset(spark, input_categories_path, "Categories")

    dataset_map = {
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
        "categories": categories_df,
    }

    if not validate_required_columns(dataset_map):
        spark.stop()
        return

    model_name, source, _ = resolve_best_model(
        spark,
        model_dir,
        MODEL_CANDIDATES,
        preferred_model=preferred_model,
    )

    if model_name is None:
        print("✗ Inference skipped: no model candidate available")
        spark.stop()
        return

    print(f"✓ Selected model: {model_name} (source: {source})")

    model = load_model(f"{model_dir}{model_name}")
    if model is None:
        print("✗ Inference skipped: selected model could not be loaded")
        spark.stop()
        return

    decision_thresholds = load_decision_thresholds(spark, model_dir)
    decision_threshold = float(decision_thresholds.get(model_name, 0.5))
    print(f"✓ Using decision threshold for {model_name}: {decision_threshold:.2f}")

    # Load calibrator — will be None for logistic_regression or if not saved
    calibrator = load_isotonic_calibrator(spark, model_dir, model_name)

    orders_df, order_items_df, products_df, categories_df = clean_source_data(
        orders_df, order_items_df, products_df, categories_df
    )

    monthly_df = build_monthly_product_frame(orders_df, order_items_df, products_df)
    monthly_df = clip_outliers(monthly_df, "monthly_demand", low_q=0.01, high_q=0.995)
    monthly_df = filter_low_history_products(monthly_df, min_months=int(os.getenv("DEMAND_FORECAST_MIN_HISTORY_MONTHS", "6")))

    features_df = engineer_features(monthly_df, categories_df)
    validate_engineered_feature_columns(features_df, "Inference feature validation")
    latest_features = build_latest_feature_rows(features_df)

    if latest_features.count() == 0:
        print("✗ Inference skipped: no rows available after feature engineering")
        spark.stop()
        return

    # 1. Score through Spark pipeline (raw probabilities)
    predictions_df = generate_predictions(
        model,
        latest_features,
        model_name,
        forecast_horizon_days,
        decision_threshold,
    )

    # 2. Apply isotonic calibration on the driver (pandas-side, small collect)
    predictions_df = apply_calibrator_to_predictions(predictions_df, calibrator, spark)

    # 3. Apply threshold to (possibly calibrated) probabilities
    predictions_df = apply_threshold_and_labels(predictions_df, decision_threshold)

    display_samples(predictions_df)

    export_inference_outputs_plot(
        model_name="demand_forecast_classification",
        predictions_df=predictions_df,
        label_column="predicted_demand_class",
        numeric_columns=["predicted_class_index", "confidence_score", "forecast_horizon_days"],
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
        run_name=model_name,
    )
    export_inference_classification_plot(
        predictions_df,
        model_name,
        EXPORT_PLOTS,
        calibrator=calibrator,
    )

    if save_predictions(predictions_df, output_path):
        print("✓ Inference completed successfully")
    else:
        print("✗ Inference failed")

    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    spark.stop()


if __name__ == "__main__":
    main("pulse-bucket-1", EXPORT_PLOTS=True)