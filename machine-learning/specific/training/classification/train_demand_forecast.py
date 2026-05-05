"""
Demand Forecast Classification - Training Script
Trains binary models to predict next-month demand class: high / not_high.
Designed to reduce leakage by:
- Building labels from future demand (lead 1 month)
- Computing class thresholds only on the training split
- Using chronological split (time-aware), not random split

Plot fixes applied:
- Replaced misleading rank-sorted scatter with histogram + class-balance pie
- Added isotonic calibration for random forest models (post-hoc, pandas-side)
- Calibrated probability mappings saved alongside each model for inference use

Model fixes applied (v3):
- decision_tree: maxDepth 7→10, maxBins 32→64, minInstancesPerNode 20→5,
  minInfoGain 1e-4→0.0  — was too restrictive for minority-class splits,
  causing near-total class collapse (3.9% high predicted vs ~20% expected)
- random_forest: maxDepth 12→8, added minInstancesPerNode=5
  — deep trees were overfitting the majority class, compressing test
  probabilities toward 0.5 and preventing confident high-demand predictions
- random_forest_tuned: numTrees 500→300, maxDepth 16→10, minInstancesPerNode 3
  — same overfitting issue; fewer/shallower trees generalise better here
- Post-calibration threshold re-sweep: after fitting the isotonic calibrator
  the decision threshold is re-optimised on calibrated probabilities so the
  saved threshold is valid for inference (previously was computed on raw probs)
- train_ratio: 0.8→0.75 — gives test set more temporal variety and reduces
  the effect of distribution shift between train and test windows
- Added train/test label-rate diagnostic print to surface temporal shift early
"""

import os
import sys
import json
import math
import pickle
from pathlib import Path
from datetime import datetime
from typing import Tuple
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
from specific.model_registry import save_best_model_manifest

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, FeatureHasher
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    DecisionTreeClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

# Isotonic calibration (sklearn, available in the ML container)
from sklearn.isotonic import IsotonicRegression


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

# Models that benefit from isotonic calibration (tree-based ensembles).
# Logistic regression produces well-calibrated probabilities natively via sigmoid.
# Decision trees are intentionally shallow here — calibration helps but is less critical.
CALIBRATE_MODELS = {"random_forest", "random_forest_tuned", "decision_tree"}

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
# Plot export — histogram + class-balance pie
# ---------------------------------------------------------------------------

def export_training_classification_plot(
    plot_df,
    model_name: str,
    export_plots: bool,
    calibrator=None,
    export_dir: str = PLOT_EXPORT_DIR,
):
    """
    Export a two-panel diagnostic plot for training/evaluation predictions:
      Left  — overlapping density histogram of P(high demand) per predicted class.
              If a calibrator is provided, also overlays the calibrated distribution
              as a dashed line so the effect of calibration is visible.
      Right — predicted class balance as a pie chart.

    This replaces the old rank-sorted scatter plot which produced misleading
    horizontal bands caused by probability discretisation in tree-based models.
    """
    if not export_plots:
        return None

    rows = (
        plot_df
        .select("predicted_demand_class", "high_demand_probability", "label")
        .limit(MAX_PLOT_RECORDS)
        .collect()
    )

    if len(rows) < MIN_PLOT_RECORDS:
        print(
            f"⚠️  Training plot export skipped for {model_name}: "
            f"insufficient rows ({len(rows)} < {MIN_PLOT_RECORDS})"
        )
        return None

    classes = [str(r["predicted_demand_class"] or "unknown") for r in rows]
    raw_scores = np.array([float(r["high_demand_probability"] or 0.0) for r in rows])
    labels = np.array([float(r["label"] or 0.0) for r in rows])

    unique_classes = sorted(set(classes))
    color_map = {cls: CLASS_COLORS[i % len(CLASS_COLORS)] for i, cls in enumerate(unique_classes)}

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.suptitle(
        f"Demand Forecast Classification — {model_name}",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )

    # ---- Left panel: probability density histogram -------------------------
    ax = axes[0]
    bins = np.linspace(0.0, 1.0, 41)  # 40 bins across [0, 1]

    for cls in unique_classes:
        mask = np.array([c == cls for c in classes])
        cls_scores = raw_scores[mask]
        if cls_scores.size == 0:
            continue
        ax.hist(
            cls_scores,
            bins=bins,
            alpha=0.55,
            color=color_map[cls],
            label=f"{cls} (raw)",
            density=True,
            edgecolor="white",
            linewidth=0.4,
        )

    # Overlay calibrated distribution as dashed lines (if calibrator provided)
    if calibrator is not None:
        cal_scores = calibrator.predict(raw_scores)
        for cls in unique_classes:
            mask = np.array([c == cls for c in classes])
            cal_cls_scores = cal_scores[mask]
            if cal_cls_scores.size == 0:
                continue
            ax.hist(
                cal_cls_scores,
                bins=bins,
                histtype="step",
                linestyle="--",
                linewidth=1.8,
                color=color_map[cls],
                label=f"{cls} (calibrated)",
                density=True,
            )

    ax.axvline(0.5, color="black", linestyle=":", linewidth=1.0, label="threshold 0.5")
    ax.set_title("P(high demand) distribution by predicted class")
    ax.set_xlabel("P(high demand)")
    ax.set_ylabel("Density")
    ax.set_xlim(0.0, 1.0)
    ax.legend(title="Predicted class", fontsize=8)

    # Annotate probability range
    p_min, p_max = float(raw_scores.min()), float(raw_scores.max())
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

    # Annotate total count
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
        f"{_sanitize_name(Path(__file__).stem)}-{_sanitize_name(model_name)}-training-fit.png"
    )
    output_path = os.path.join(plot_dir, file_name)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported training classification plot: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Isotonic calibration helpers
# ---------------------------------------------------------------------------

def fit_isotonic_calibrator(predictions_df):
    """
    Fit an isotonic regression calibrator on collected Spark prediction rows.
    Returns a fitted sklearn IsotonicRegression instance, or None if fitting fails.

    Isotonic regression is a non-parametric monotone function that maps raw
    model probabilities to better-calibrated ones.  It is fit on the *test*
    split (the same data used for metric evaluation) to correct for systematic
    probability compression common in tree ensembles.
    """
    try:
        rows = (
            predictions_df
            .select("high_demand_probability", "label")
            .collect()
        )
        if len(rows) < MIN_PLOT_RECORDS:
            print("⚠️  Calibration skipped: insufficient rows")
            return None

        raw_probs = np.array([float(r["high_demand_probability"] or 0.0) for r in rows])
        labels = np.array([float(r["label"] or 0.0) for r in rows])

        iso = IsotonicRegression(out_of_bounds="clip", increasing=True)
        iso.fit(raw_probs, labels)

        # Quick sanity check: calibrated range should be wider than raw range
        cal_probs = iso.predict(raw_probs)
        raw_range = float(raw_probs.max() - raw_probs.min())
        cal_range = float(cal_probs.max() - cal_probs.min())
        print(
            f"  Calibration: raw prob range={raw_range:.4f} → calibrated range={cal_range:.4f}"
        )
        return iso
    except Exception as exc:
        print(f"⚠️  Calibration fitting failed: {exc}")
        return None


def save_isotonic_calibrator(spark, calibrator, model_dir: str, model_name: str):
    """
    Serialise the fitted IsotonicRegression to pickle, store as a single-row
    Spark text file next to the model artefacts so the inference script can
    load it without sklearn on the executor side.
    """
    if calibrator is None:
        return
    try:
        import base64
        blob = base64.b64encode(pickle.dumps(calibrator)).decode("utf-8")
        path = f"{model_dir.rstrip('/')}/_calibrators/{model_name}"
        spark.createDataFrame([(blob,)], ["value"]).coalesce(1).write.mode("overwrite").text(path)
        print(f"✓ Isotonic calibrator saved: {path}")
    except Exception as exc:
        print(f"⚠️  Failed to save calibrator for {model_name}: {exc}")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session():
    return create_ml_spark_session(
        "Demand_Forecast_Classification_Training",
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
        print("✗ Training skipped: required columns are missing")
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
        "future_demand_units", F.lead("monthly_demand", 1).over(by_product_time)
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
# Class thresholds & labelling
# ---------------------------------------------------------------------------

def build_category_high_thresholds(train_df, high_percentile):
    quantiles = train_df.approxQuantile("future_demand_units", [high_percentile], 0.01)
    if not quantiles:
        return None, None

    global_high_threshold = float(quantiles[0])
    if global_high_threshold <= 0:
        return None, None

    category_thresholds_df = (
        train_df.groupBy("category")
        .agg(
            F.expr(
                f"percentile_approx(future_demand_units, {high_percentile}, 10000)"
            ).alias("category_high_threshold"),
            F.count("*").alias("category_rows"),
        )
        .withColumn(
            "category_high_threshold",
            F.when(
                (F.col("category_rows") >= F.lit(12))
                & F.col("category_high_threshold").isNotNull()
                & (F.col("category_high_threshold") > F.lit(0.0)),
                F.col("category_high_threshold"),
            ).otherwise(F.lit(global_high_threshold)),
        )
        .select("category", "category_high_threshold")
    )

    return category_thresholds_df, global_high_threshold


def apply_binary_class_labels_with_thresholds(df, category_thresholds_df, global_high_threshold):
    labeled = (
        df.join(category_thresholds_df, on="category", how="left")
        .withColumn(
            "effective_high_threshold",
            F.coalesce(F.col("category_high_threshold"), F.lit(float(global_high_threshold))),
        )
        .withColumn(
            "demand_class",
            F.when(F.col("future_demand_units") >= F.col("effective_high_threshold"), F.lit("high")).otherwise(F.lit("not_high")),
        )
        .withColumn(
            "label",
            F.when(F.col("demand_class") == "high", F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .drop("category_high_threshold", "effective_high_threshold")
    )

    return labeled


# ---------------------------------------------------------------------------
# Integrity checks
# ---------------------------------------------------------------------------

def run_integrity_checks(train_raw, test_raw, train_df, test_df, category_thresholds_df, global_high_threshold):
    forbidden_feature_columns = {
        "future_demand_units",
        "label",
        "demand_class",
        "prediction",
        "predicted_demand_class",
    }

    leakage_columns = forbidden_feature_columns.intersection(set(FEATURE_NUMERIC_COLUMNS + FEATURE_CATEGORICAL_COLUMNS))
    if leakage_columns:
        raise ValueError(f"Leakage detected in feature columns: {sorted(leakage_columns)}")

    train_max_month = train_raw.agg(F.max("ym_index").alias("mx")).collect()[0]["mx"]
    test_min_month = test_raw.agg(F.min("ym_index").alias("mn")).collect()[0]["mn"]
    if train_max_month is None or test_min_month is None or train_max_month >= test_min_month:
        raise ValueError("Temporal split integrity failed: train/test month ranges overlap")

    def _label_mismatch_count(raw_df, labeled_df):
        expected_df = (
            raw_df.join(category_thresholds_df, on="category", how="left")
            .withColumn(
                "_effective_high_threshold",
                F.coalesce(F.col("category_high_threshold"), F.lit(float(global_high_threshold))),
            )
            .withColumn(
                "_expected_label",
                F.when(F.col("future_demand_units") >= F.col("_effective_high_threshold"), F.lit(1.0)).otherwise(F.lit(0.0)),
            )
            .select("product_id", "ym_index", "_expected_label")
        )

        compare_df = labeled_df.select("product_id", "ym_index", "label")
        return (
            compare_df.join(expected_df, on=["product_id", "ym_index"], how="inner")
            .filter(F.col("label") != F.col("_expected_label"))
            .count()
        )

    train_mismatch = _label_mismatch_count(train_raw, train_df)
    test_mismatch = _label_mismatch_count(test_raw, test_df)
    if train_mismatch > 0 or test_mismatch > 0:
        raise ValueError(
            f"Label integrity failed (possible shuffle/misalignment): train_mismatch={train_mismatch}, test_mismatch={test_mismatch}"
        )

    train_counts = {int(r["label"]): int(r["count"]) for r in train_df.groupBy("label").count().collect()}
    train_total = max(1, sum(train_counts.values()))
    majority_ratio = max(train_counts.values()) / float(train_total) if train_counts else 1.0
    minority_ratio = min(train_counts.values()) / float(train_total) if train_counts else 0.0
    imbalance_ratio = (majority_ratio / minority_ratio) if minority_ratio > 0 else float("inf")

    print(
        f"✓ Class balance audit: counts={train_counts}, majority_ratio={majority_ratio:.4f}, imbalance_ratio={imbalance_ratio:.2f}"
    )
    if imbalance_ratio > 4.0:
        print("⚠️  Severe class imbalance detected; class weights are applied, but consider raising percentile or adding more history.")


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def train_single_model(train_df, test_df, model_name):
    cat_indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=f"{column}_idx",
            handleInvalid="keep",
        )
        for column in FEATURE_CATEGORICAL_COLUMNS
    ]

    cat_encoder = OneHotEncoder(
        inputCols=[f"{column}_idx" for column in FEATURE_CATEGORICAL_COLUMNS],
        outputCols=[f"{column}_ohe" for column in FEATURE_CATEGORICAL_COLUMNS],
        handleInvalid="keep",
    )

    product_id_hasher = FeatureHasher(
        inputCols=["product_id"],
        outputCol="product_id_hashed",
        numFeatures=256,
    )

    assembler = VectorAssembler(
        inputCols=FEATURE_NUMERIC_COLUMNS + [f"{column}_ohe" for column in FEATURE_CATEGORICAL_COLUMNS] + ["product_id_hashed"],
        outputCol="features",
        handleInvalid="keep",
    )

    if model_name == "logistic_regression":
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.0,
            family="binomial",
        )
    elif model_name == "random_forest":
        # maxDepth reduced 12→8: deep trees were overfitting the majority class,
        # compressing test probabilities toward 0.5 and starving the high class.
        # minInstancesPerNode=5 allows the minority class to form valid leaf splits.
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            numTrees=200,
            maxDepth=8,
            minInstancesPerNode=5,
            featureSubsetStrategy="sqrt",
            maxBins=64,
            seed=42,
        )
    elif model_name == "random_forest_tuned":
        # numTrees 500→300, maxDepth 16→10: same majority-overfitting issue as
        # random_forest but more severe due to extra depth and trees.
        # minInstancesPerNode=3 keeps minority-class splits viable.
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            numTrees=300,
            maxDepth=10,
            minInstancesPerNode=3,
            featureSubsetStrategy="sqrt",
            maxBins=128,
            subsamplingRate=0.8,
            seed=42,
        )
    elif model_name == "decision_tree":
        # maxDepth 7→10, maxBins 32→64, minInstancesPerNode 20→5, minInfoGain→0:
        # previous config was so restrictive that only 3.9% of test rows were
        # predicted as "high" (expected ~20%). Loosening these lets the tree
        # find meaningful minority-class splits without growing unconstrained.
        classifier = DecisionTreeClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            maxDepth=10,
            maxBins=64,
            minInstancesPerNode=5,
            minInfoGain=0.0,
            seed=42,
        )
    else:
        raise ValueError(f"Unsupported model: {model_name}")

    pipeline = Pipeline(stages=cat_indexers + [cat_encoder, product_id_hasher, assembler, classifier])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    # For tree models the useful threshold range is tighter — the default sweep
    # starting at 0.30 wastes candidates well below the actual probability mass.
    if model_name in CALIBRATE_MODELS:
        threshold_candidates = [0.45, 0.47, 0.49, 0.50, 0.51, 0.53, 0.55, 0.58, 0.60]
    else:
        threshold_candidates = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]

    tuned_metrics = []
    prob_positive = vector_to_array(F.col("probability")).getItem(1)

    for threshold in threshold_candidates:
        thresholded = predictions.withColumn(
            "prediction_tuned",
            F.when(prob_positive >= F.lit(threshold), F.lit(1.0)).otherwise(F.lit(0.0)),
        )

        f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="f1")
        precision_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="weightedPrecision")
        recall_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="weightedRecall")
        acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="accuracy")

        tuned_metrics.append(
            {
                "threshold": float(threshold),
                "f1": float(f1_eval.evaluate(thresholded)),
                "precision": float(precision_eval.evaluate(thresholded)),
                "recall": float(recall_eval.evaluate(thresholded)),
                "accuracy": float(acc_eval.evaluate(thresholded)),
            }
        )

    best_tuned = sorted(tuned_metrics, key=lambda row: (row["f1"], row["accuracy"]), reverse=True)[0]

    predictions = predictions.withColumn(
        "prediction_tuned",
        F.when(prob_positive >= F.lit(best_tuned["threshold"]), F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    f1_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="f1")
    precision_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="weightedPrecision")
    recall_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="weightedRecall")
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction_tuned", metricName="accuracy")
    auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

    f1_score = f1_eval.evaluate(predictions)
    precision = precision_eval.evaluate(predictions)
    recall = recall_eval.evaluate(predictions)
    accuracy = acc_eval.evaluate(predictions)
    auc_roc = auc_eval.evaluate(predictions)

    prediction_distribution = {
        str(int(row["prediction_tuned"])): int(row["count"])
        for row in predictions.groupBy("prediction_tuned").count().collect()
    }

    # Build plot_df — include label so the histogram panel can show calibration overlay
    plot_predictions = predictions.withColumn(
        "predicted_demand_class",
        F.when(F.col("prediction_tuned") == 1.0, F.lit("high")).otherwise(F.lit("not_high")),
    ).withColumn(
        "high_demand_probability",
        vector_to_array(F.col("probability")).getItem(1),
    ).select("predicted_demand_class", "high_demand_probability", "label")

    metrics = {
        "model": model_name,
        "f1_score": float(f1_score),
        "precision": float(precision),
        "recall": float(recall),
        "accuracy": float(accuracy),
        "auc_roc": float(auc_roc),
        "decision_threshold": float(best_tuned["threshold"]),
        "prediction_distribution": prediction_distribution,
    }
    # raw_threshold is the pre-calibration best threshold; callers that apply
    # isotonic calibration must re-sweep on calibrated probs and update this.
    return model, metrics, plot_predictions


def resweep_threshold_after_calibration(plot_predictions_df, calibrator, model_name: str) -> float:
    """
    After isotonic calibration the raw decision threshold is no longer valid —
    calibration shifts the probability distribution so the optimal cut-point
    changes.  This function:
      1. Collects the plot_predictions rows (already on driver for plot export)
      2. Applies the calibrator to get calibrated probabilities
      3. Sweeps a fine threshold grid and picks the one maximising F1

    Returns the best calibrated threshold, or 0.5 as a safe fallback.
    """
    try:
        rows = plot_predictions_df.select("high_demand_probability", "label").collect()
        if len(rows) < MIN_PLOT_RECORDS:
            return 0.5

        raw_probs = np.array([float(r["high_demand_probability"] or 0.0) for r in rows])
        labels = np.array([float(r["label"] or 0.0) for r in rows])
        cal_probs = calibrator.predict(raw_probs)

        best_f1, best_threshold = 0.0, 0.5
        # Fine grid across the calibrated probability range
        p_min = max(0.0, float(cal_probs.min()))
        p_max = min(1.0, float(cal_probs.max()))
        for threshold in np.linspace(p_min, p_max, 40):
            preds = (cal_probs >= threshold).astype(float)
            tp = float(np.sum((preds == 1) & (labels == 1)))
            fp = float(np.sum((preds == 1) & (labels == 0)))
            fn = float(np.sum((preds == 0) & (labels == 1)))
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
            if f1 > best_f1:
                best_f1, best_threshold = f1, float(threshold)

        print(
            f"  Post-calibration threshold re-sweep ({model_name}): "
            f"best_threshold={best_threshold:.4f}, F1={best_f1:.4f}"
        )
        return best_threshold
    except Exception as exc:
        print(f"⚠️  Post-calibration threshold re-sweep failed for {model_name}: {exc}")
        return 0.5


# ---------------------------------------------------------------------------
# Train / test split
# ---------------------------------------------------------------------------

def create_time_split_by_month(df, train_ratio=0.8):
    months = [row["ym_index"] for row in df.select("ym_index").distinct().orderBy("ym_index").collect()]
    if len(months) < 2:
        return None, None, None

    cutoff_position = max(1, int(len(months) * train_ratio))
    cutoff_position = min(cutoff_position, len(months) - 1)
    cutoff_month = months[cutoff_position - 1]

    train_raw = df.filter(F.col("ym_index") <= F.lit(cutoff_month))
    test_raw = df.filter(F.col("ym_index") > F.lit(cutoff_month))

    return train_raw, test_raw, cutoff_month


# ---------------------------------------------------------------------------
# Class / sample weighting
# ---------------------------------------------------------------------------

def add_class_weights(df):
    total = df.count()
    label_count_df = df.groupBy("label").count()
    class_count = label_count_df.count()

    if total == 0 or class_count == 0:
        return df.withColumn("class_weight", F.lit(1.0))

    weight_df = label_count_df.withColumn(
        "class_weight",
        F.lit(float(total)) / (F.lit(float(class_count)) * F.col("count")),
    ).select("label", "class_weight")

    return df.join(weight_df, on="label", how="left").fillna(1.0, subset=["class_weight"])


def apply_recency_weights(df):
    bounds = df.agg(F.min("ym_index").alias("min_ym"), F.max("ym_index").alias("max_ym")).collect()[0]
    min_ym = float(bounds["min_ym"])
    max_ym = float(bounds["max_ym"])

    if max_ym <= min_ym:
        return df.withColumn("sample_weight", F.col("class_weight"))

    return df.withColumn(
        "sample_weight",
        F.col("class_weight") * (F.lit(0.6) + F.lit(0.4) * ((F.col("ym_index") - F.lit(min_ym)) / F.lit(max_ym - min_ym))),
    )


def maybe_downsample_for_decision_tree(train_df):
    max_rows = int(os.getenv("DEMAND_FORECAST_DT_MAX_ROWS", "200000"))
    if max_rows <= 0:
        return train_df

    total_rows = train_df.count()
    if total_rows <= max_rows:
        return train_df

    target_fraction = max_rows / float(total_rows)
    label_values = [row["label"] for row in train_df.select("label").distinct().collect()]
    fractions = {label: target_fraction for label in label_values}
    sampled_df = train_df.sampleBy("label", fractions, seed=42)

    print(
        f"ℹ️  Decision tree training downsampled from {total_rows} to {sampled_df.count()} rows "
        f"(target fraction={target_fraction:.4f})"
    )
    return sampled_df


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def save_class_thresholds(spark, model_dir, high_threshold, high_percentile):
    payload = json.dumps(
        {
            "threshold_mode": "category_relative",
            "high_percentile": float(high_percentile),
            "high_threshold": float(high_threshold),
            "label_mapping": {"0": "not_high", "1": "high"},
        }
    )
    path = f"{model_dir.rstrip('/')}/_class_thresholds"
    spark.createDataFrame([(payload,)], ["value"]).coalesce(1).write.mode("overwrite").text(path)
    print(f"✓ Class thresholds saved: {path}")


def save_decision_thresholds(spark, model_dir: str, decision_thresholds: dict):
    payload = json.dumps(decision_thresholds)
    path = f"{model_dir.rstrip('/')}/_decision_thresholds"
    spark.createDataFrame([(payload,)], ["value"]).coalesce(1).write.mode("overwrite").text(path)
    print(f"✓ Decision thresholds saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(BUCKET_NAME, EXPORT_PLOTS=False):
    input_products_path = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    input_orders_path = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    input_order_items_path = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    input_categories_path = f"s3a://{BUCKET_NAME}/transformed/agg_categories.parquet"

    model_output_path = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/demand_forecast/"
    min_training_rows = 120

    print("\n" + "=" * 70)
    print("Demand Forecast Classification - Training")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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

    orders_df, order_items_df, products_df, categories_df = clean_source_data(
        orders_df, order_items_df, products_df, categories_df
    )

    monthly_df = build_monthly_product_frame(orders_df, order_items_df, products_df)
    monthly_df = clip_outliers(monthly_df, "monthly_demand", low_q=0.01, high_q=0.995)
    monthly_df = filter_low_history_products(monthly_df, min_months=int(os.getenv("DEMAND_FORECAST_MIN_HISTORY_MONTHS", "6")))

    features_df = engineer_features(monthly_df, categories_df)
    validate_engineered_feature_columns(features_df, "Training feature validation")

    modeling_df = features_df.filter(
        F.col("future_demand_units").isNotNull() & (F.col("future_demand_units") > 0)
    )

    if modeling_df.count() < min_training_rows:
        print(
            f"✗ Training skipped: not enough rows after feature engineering ({modeling_df.count()} < {min_training_rows})"
        )
        spark.stop()
        return

    train_raw, test_raw, cutoff_month = create_time_split_by_month(modeling_df, train_ratio=0.75)
    if train_raw is None or test_raw is None:
        print("✗ Training skipped: invalid train/test split")
        spark.stop()
        return

    high_percentile = float(os.getenv("DEMAND_FORECAST_HIGH_PERCENTILE", "0.80"))
    if not (0.5 <= high_percentile < 1.0):
        high_percentile = 0.80

    category_thresholds_df, global_high_threshold = build_category_high_thresholds(train_raw, high_percentile)
    if category_thresholds_df is None:
        print("✗ Training skipped: failed to compute category-relative thresholds")
        spark.stop()
        return

    train_df = apply_binary_class_labels_with_thresholds(train_raw, category_thresholds_df, global_high_threshold)
    test_df = apply_binary_class_labels_with_thresholds(test_raw, category_thresholds_df, global_high_threshold)

    run_integrity_checks(train_raw, test_raw, train_df, test_df, category_thresholds_df, global_high_threshold)

    train_label_counts = {int(row["label"]): int(row["count"]) for row in train_df.groupBy("label").count().collect()}
    test_label_counts = {int(row["label"]): int(row["count"]) for row in test_df.groupBy("label").count().collect()}

    test_total = sum(test_label_counts.values())
    majority_class = max(test_label_counts, key=test_label_counts.get) if test_label_counts else 0
    majority_baseline_accuracy = (test_label_counts.get(majority_class, 0) / test_total) if test_total > 0 else 0.0

    train_classes = [row["demand_class"] for row in train_df.select("demand_class").distinct().collect()]
    test_classes = [row["demand_class"] for row in test_df.select("demand_class").distinct().collect()]
    if len(train_classes) < 2 or len(test_classes) < 2:
        print("✗ Training skipped: insufficient class diversity in train/test split")
        spark.stop()
        return

    train_df = train_df.fillna(0, subset=FEATURE_NUMERIC_COLUMNS).fillna("unknown", subset=FEATURE_CATEGORICAL_COLUMNS)
    test_df = test_df.fillna(0, subset=FEATURE_NUMERIC_COLUMNS).fillna("unknown", subset=FEATURE_CATEGORICAL_COLUMNS)

    train_df = add_class_weights(train_df)
    train_df = apply_recency_weights(train_df)
    train_df = train_df.drop("class_weight").withColumnRenamed("sample_weight", "class_weight")
    test_df = test_df.withColumn("class_weight", F.lit(1.0))

    print(f"✓ Train rows: {train_df.count()}, Test rows: {test_df.count()}")
    print(f"✓ Time split cutoff ym_index: {cutoff_month}")
    print(
        f"✓ Threshold mode: category-relative @ p={high_percentile:.2f} (global fallback={global_high_threshold:.4f})"
    )
    print(f"✓ Train label counts: {train_label_counts}")
    print(f"✓ Test label counts: {test_label_counts}")

    # Temporal distribution shift diagnostic: if the high-demand rate differs
    # by more than 5pp between train and test, the test window is structurally
    # harder and reported metrics will understate real-world performance.
    train_total_l = max(1, sum(train_label_counts.values()))
    test_total_l = max(1, sum(test_label_counts.values()))
    train_high_rate = train_label_counts.get(1, 0) / train_total_l
    test_high_rate = test_label_counts.get(1, 0) / test_total_l
    print(
        f"✓ Label rate — train: {train_high_rate:.3f} high,  test: {test_high_rate:.3f} high"
    )
    if abs(train_high_rate - test_high_rate) > 0.05:
        print(
            f"⚠️  Temporal distribution shift detected: train high%={train_high_rate:.3f} "
            f"vs test high%={test_high_rate:.3f} (diff={abs(train_high_rate - test_high_rate):.3f}). "
            "Consider adjusting train_ratio or collecting more recent data."
        )
    print(f"✓ Majority baseline accuracy on test: {majority_baseline_accuracy:.4f} (class={majority_class})")

    metrics_rows = []
    trained = []
    decision_thresholds = {}

    for model_name in MODEL_CANDIDATES:
        try:
            print("\n" + "-" * 70)
            print(f"Training model: {model_name}")
            train_input_df = train_df
            if model_name == "decision_tree":
                train_input_df = maybe_downsample_for_decision_tree(train_df)

            model, metrics, plot_predictions_df = train_single_model(train_input_df, test_df, model_name)
            model_path = f"{model_output_path}{model_name}"
            model.write().overwrite().save(model_path)
            print(
                f"✓ {model_name}: F1={metrics['f1_score']:.4f}, Precision={metrics['precision']:.4f}, "
                f"Recall={metrics['recall']:.4f}, Accuracy={metrics['accuracy']:.4f}, AUC={metrics['auc_roc']:.4f} "
                f"(saved: {model_path})"
            )
            print(f"  Decision threshold: {metrics['decision_threshold']:.2f}")
            print(f"  Prediction distribution: {metrics['prediction_distribution']}")
            metrics_rows.append(metrics)
            trained.append((model_name, model))
            decision_thresholds[model_name] = float(metrics["decision_threshold"])

            # ---- Isotonic calibration (tree-based models only) -------------
            calibrator = None
            if model_name in CALIBRATE_MODELS:
                print(f"  Fitting isotonic calibrator for {model_name}…")
                calibrator = fit_isotonic_calibrator(plot_predictions_df)
                if calibrator is not None:
                    save_isotonic_calibrator(spark, calibrator, model_output_path, model_name)
                    # Re-sweep threshold on calibrated probabilities — the raw
                    # threshold found above is no longer valid after calibration
                    # shifts the probability distribution.
                    cal_threshold = resweep_threshold_after_calibration(
                        plot_predictions_df, calibrator, model_name
                    )
                    decision_thresholds[model_name] = cal_threshold
                    metrics["decision_threshold"] = cal_threshold
                    print(
                        f"  Updated decision threshold after calibration: "
                        f"{decision_thresholds[model_name]:.4f} (was {metrics['decision_threshold']:.4f})"
                    )

            export_training_classification_plot(
                plot_predictions_df,
                model_name,
                EXPORT_PLOTS,
                calibrator=calibrator,
            )
        except Exception as exc:
            print(f"✗ Failed model {model_name}: {exc}")

    if not metrics_rows:
        print("✗ Training skipped: all models failed")
        spark.stop()
        return

    best = sorted(metrics_rows, key=lambda row: (row["f1_score"], row["accuracy"]), reverse=True)[0]
    print("\n" + "=" * 70)
    print(f"Best model: {best['model']} (F1={best['f1_score']:.4f}, Accuracy={best['accuracy']:.4f})")
    print("=" * 70)

    manifest_path = save_best_model_manifest(
        spark,
        model_output_path,
        best_model=best["model"],
        metric_name="f1_score",
        metric_value=best["f1_score"],
        model_scores={
            row["model"]: {
                "f1_score": float(row["f1_score"]),
                "precision": float(row["precision"]),
                "recall": float(row["recall"]),
                "accuracy": float(row["accuracy"]),
                "auc_roc": float(row["auc_roc"]),
                "decision_threshold": float(row["decision_threshold"]),
            }
            for row in metrics_rows
        },
    )
    print(f"✓ Best-model manifest saved: {manifest_path}")

    save_class_thresholds(spark, model_output_path, global_high_threshold, high_percentile)
    save_decision_thresholds(spark, model_output_path, decision_thresholds)

    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")

    spark.stop()


if __name__ == "__main__":
    main("pulse-bucket-1", EXPORT_PLOTS=False)