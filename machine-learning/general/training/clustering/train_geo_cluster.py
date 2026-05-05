"""
Geographic Sales Clustering - Training Script
Clusters geographic regions by sales performance and market characteristics
"""

import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from scipy.spatial import ConvexHull
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)
from general.model_registry import save_best_model_manifest

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, log1p, concat_ws
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json

# Environment configuration
MODEL_NAME = "geo_cluster"
INPUT_RELATIVE_PATH = "transformed/agg_city_aggregations.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("clustering", MODEL_NAME)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"

# Feature columns for geographic clustering
NUMERIC_FEATURES = [
    "log_total_customers",
    "log_total_orders",
    "log_total_revenue",
    "avg_order_value",
    "avg_customer_lifetime_value",
    "revenue_per_customer",
    "orders_per_customer",
    "customer_density",
    "revenue_concentration_score",
    "market_efficiency_score",
]


# ---------------------------------------------------------------------------
# Plot configuration — mirrors conventions from other pipeline scripts
# ---------------------------------------------------------------------------
PLOT_EXPORT_DIR    = "/app/logs_for_report"
MIN_PLOT_RECORDS   = 20
MAX_PLOT_RECORDS   = 50000
MAX_SCATTER_POINTS = 50000

# Distinct palette — up to 10 clusters; extend if k_values ever exceeds 10
CLUSTER_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def _ensure_plot_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def _sanitize_name(value):
    return "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in value.lower())


def _sample_indices(length, max_points):
    if length <= max_points:
        return list(range(length))
    return np.linspace(0, length - 1, num=max_points, dtype=int).tolist()


def export_cluster_scatter_plot(
    predictions_df,
    model_type,
    k,
    export_plots,
    export_dir=PLOT_EXPORT_DIR,
    script_stem=None,
):
    """
    Export a PCA-space scatter plot with convex-hull cluster boundaries.

    Matches the reference image style:
      - One distinct colour per cluster
      - Semi-transparent filled convex hull per cluster
      - Solid hull border in the same colour
      - Scatter points inside each hull

    Parameters
    ----------
    predictions_df : Spark DataFrame
        Must contain "features" (PCA vector, first 2 components used)
        and "prediction" (integer cluster label).
    model_type : str  e.g. "kmeans", "gmm", "bisecting_kmeans"
    k          : int  number of clusters
    export_plots : bool  — no-op when False
    export_dir : str
    script_stem : str  base name for the output file (defaults to this file)
    """
    if not export_plots:
        return None

    rows = (
        predictions_df
        .select("prediction", "features")
        .limit(MAX_PLOT_RECORDS)
        .collect()
    )

    if len(rows) < MIN_PLOT_RECORDS:
        print(f"⚠️  Cluster plot skipped ({model_type} k={k}): "
              f"only {len(rows)} rows (< {MIN_PLOT_RECORDS})")
        return None

    # Extract PC1, PC2, and cluster ids
    idx         = _sample_indices(len(rows), MAX_SCATTER_POINTS)
    cluster_ids = [rows[i]["prediction"] for i in idx]
    x_vals      = [float(rows[i]["features"][0]) for i in idx]   # PC1
    y_vals      = [float(rows[i]["features"][1]) for i in idx]   # PC2

    unique_clusters = sorted(set(cluster_ids))
    color_map = {
        cid: CLUSTER_COLORS[j % len(CLUSTER_COLORS)]
        for j, cid in enumerate(unique_clusters)
    }

    fig, ax = plt.subplots(figsize=(12, 8))

    for cid in unique_clusters:
        color  = color_map[cid]
        mask   = [i for i, c in enumerate(cluster_ids) if c == cid]
        cx     = [x_vals[i] for i in mask]
        cy     = [y_vals[i] for i in mask]
        pts    = np.column_stack([cx, cy])

        # --- Convex hull boundary (filled + border) ----------------------
        if len(pts) >= 3:
            try:
                hull      = ConvexHull(pts)
                hull_verts = np.append(
                    pts[hull.vertices], [pts[hull.vertices[0]]], axis=0
                )
                ax.fill(
                    hull_verts[:, 0], hull_verts[:, 1],
                    alpha=0.15, color=color, zorder=1,
                )
                ax.plot(
                    hull_verts[:, 0], hull_verts[:, 1],
                    color=color, linewidth=1.8, alpha=0.85, zorder=2,
                )
            except Exception:
                pass  # degenerate set of points — skip hull silently

        # --- Scatter points -----------------------------------------------
        ax.scatter(
            cx, cy,
            s=40, alpha=0.75, color=color,
            edgecolor="white", linewidth=0.3,
            label=f"Cluster {cid} (n={len(cx)})",
            zorder=3,
        )

    ax.set_title(
        f"Geographic Clustering — {model_type.upper()} k={k}\n"
        f"(PCA projection: PC1 vs PC2)"
    )
    ax.set_xlabel("Principal Component 1")
    ax.set_ylabel("Principal Component 2")
    ax.legend(loc="best", title="Cluster", framealpha=0.8)
    fig.tight_layout()

    stem = script_stem or _sanitize_name(Path(__file__).stem)
    out  = os.path.join(
        _ensure_plot_dir(export_dir),
        f"{stem}-{_sanitize_name(model_type)}-k{k}-cluster-scatter.png",
    )
    fig.savefig(out, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ Exported cluster scatter plot: {out}")
    return out


def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "GeographicClusteringTraining",
        extra_configs={
                    "spark.sql.shuffle.partitions": "8",
                    "inferSchema": "true",
                    "mergeSchema": "true"
                },
    )
def load_and_validate_data(spark):
    """Load geographic data from city-level aggregations using multi-bucket loader"""
    # Load city-level data from all buckets
    required_cols = [
        "country",
        "state_province",
        "city",
        "total_customers",
        "total_orders",
        "total_revenue",
    ]

    df, record_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=required_cols,
        filter_nulls=False
    )

    if df is None:
        print("⚠️  No data available. Skipping training.")
        return None

    print(f"Loaded {record_count} cities/regions from all buckets")

    # Validate training data
    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )

    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        return None

    # Validate required columns exist
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print(f"⚠️  Training skipped - Missing required columns: {missing_cols}")
        return None

    # Check for non-null values
    for col_name in ["total_customers", "total_orders", "total_revenue"]:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            print(f"⚠️  Training skipped - Column '{col_name}' has all NULL values")
            return None
        print(f"Column '{col_name}': {non_null_count} non-null values")

    return df


def prepare_features(df):
    """Prepare features with derived metrics and log transformations"""
    print("Preparing features...")

    # Fill nulls for numeric features
    numeric_cols = [
        "total_customers",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "avg_customer_lifetime_value",
        "revenue_per_customer",
        "orders_per_customer",
        "customer_density",
    ]

    for col_name in numeric_cols:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Filter regions with activity (at least 1 customer and 1 order)
    df = df.filter((col("total_customers") > 0) & (col("total_orders") > 0))

    # Filter out extreme outliers
    df = df.filter((col("total_revenue") >= 0) & (col("total_revenue") <= 10000000))

    record_count = df.count()
    print(f"Filtered dataset: {record_count} geographic regions")

    if record_count < 30:
        print(f"ERROR: Insufficient data (need >= 30, got {record_count})")
        return None

    # Apply log transformations to skewed features
    print("Applying log transformations...")
    df = df.withColumn("log_total_customers", log1p(col("total_customers")))
    df = df.withColumn("log_total_orders", log1p(col("total_orders")))
    df = df.withColumn("log_total_revenue", log1p(col("total_revenue")))

    # Create derived features
    print("Creating derived performance metrics...")

    # Revenue concentration score (revenue / customers ratio normalized)
    df = df.withColumn(
        "revenue_concentration_score",
        when(col("total_customers") > 0, col("total_revenue") / col("total_customers")).otherwise(
            0.0
        ),
    )

    # Market efficiency score (orders per customer)
    df = df.withColumn(
        "market_efficiency_score",
        when(col("total_customers") > 0, col("total_orders") / col("total_customers")).otherwise(
            0.0
        ),
    )

    return df


def validate_clusterability(df):
    """Validate if data is suitable for clustering"""
    print("\nValidating data clusterability...")

    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_temp")
    df_temp = assembler.transform(df)

    scaler = StandardScaler(
        inputCol="features_temp", outputCol="features_scaled", withStd=True, withMean=True
    )
    df_temp = scaler.fit(df_temp).transform(df_temp)

    pca = PCA(k=5, inputCol="features_scaled", outputCol="pca_features")
    pca_model = pca.fit(df_temp)

    explained_variance = pca_model.explainedVariance.toArray()
    cumulative = sum(explained_variance)

    print(f"First 5 PCs explain {cumulative*100:.2f}% of variance")
    print(f"PC variance: {[f'{v*100:.2f}%' for v in explained_variance]}")

    if cumulative < 0.6:
        print("WARNING: Lower variance. Geographic clustering may be moderate quality.")
    else:
        print("Data appears well-suited for geographic clustering.")

    return True


def train_kmeans(df, features_col, k_values):
    """Train K-Means models"""
    print(f"\nTraining K-Means with k={k_values}...")
    models, metrics = [], []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"K-Means k={k}...", end=" ")
        kmeans = KMeans(featuresCol=features_col, predictionCol="prediction", k=k, seed=42)
        model = kmeans.fit(df)
        predictions = model.transform(df)
        silhouette = evaluator.evaluate(predictions)
        wssse = model.summary.trainingCost
        print(f"Silhouette={silhouette:.4f}, WSSSE={wssse:.2f}")

        models.append({"model": model, "k": k, "type": "kmeans"})
        metrics.append({"k": k, "type": "kmeans", "silhouette": silhouette, "wssse": wssse})

    return models, metrics


def train_gmm(df, features_col, k_values):
    """Train Gaussian Mixture Models"""
    print(f"\nTraining GMM with k={k_values}...")
    models, metrics = [], []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"GMM k={k}...", end=" ")
        try:
            gmm = GaussianMixture(
                featuresCol=features_col, predictionCol="prediction", k=k, seed=42
            )
            model = gmm.fit(df)
            predictions = model.transform(df)

            num_clusters = predictions.select("prediction").distinct().count()
            if num_clusters <= 1:
                print(f"Only {num_clusters} cluster. Skipping.")
                continue

            silhouette = evaluator.evaluate(predictions)
            log_likelihood = model.summary.logLikelihood
            print(f"Silhouette={silhouette:.4f}, LogLikelihood={log_likelihood:.2f}")

            models.append({"model": model, "k": k, "type": "gmm"})
            metrics.append({
                "k": k,
                "type": "gmm",
                "silhouette": silhouette,
                "log_likelihood": log_likelihood,
            })
        except Exception as e:
            print(f"Failed: {str(e)}")

    return models, metrics


def train_bisecting_kmeans(df, features_col, k_values):
    """Train Bisecting K-Means (hierarchical approach)"""
    print(f"\nTraining Bisecting K-Means with k={k_values}...")
    models, metrics = [], []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"Bisecting K-Means k={k}...", end=" ")
        bkm = BisectingKMeans(
            featuresCol=features_col, predictionCol="prediction", k=k, seed=42
        )
        model = bkm.fit(df)
        predictions = model.transform(df)

        num_clusters = predictions.select("prediction").distinct().count()
        if num_clusters <= 1:
            print(f"Only {num_clusters} cluster. Skipping.")
            continue

        silhouette = evaluator.evaluate(predictions)
        wssse = model.summary.trainingCost
        print(f"Silhouette={silhouette:.4f}, WSSSE={wssse:.2f}")

        models.append({"model": model, "k": k, "type": "bisecting_kmeans"})
        metrics.append({
            "k": k,
            "type": "bisecting_kmeans",
            "silhouette": silhouette,
            "wssse": wssse,
        })

    return models, metrics


def save_models(
    kmeans_models,
    kmeans_metrics,
    gmm_models,
    gmm_metrics,
    bkm_models,
    bkm_metrics,
    all_metrics,
    scaler_model,
    pca_model,
    spark,
):
    """Save models to MinIO"""
    print(f"\nSaving models to MinIO: {MODEL_OUTPUT_DIR}")

    # Save preprocessing models
    scaler_model.write().overwrite().save(f"{MODEL_OUTPUT_DIR}/geographic_scaler")
    pca_model.write().overwrite().save(f"{MODEL_OUTPUT_DIR}/geographic_pca")
    print("Saved preprocessing models")

    # Save best K-Means
    best_kmeans = max(kmeans_metrics, key=lambda x: x["silhouette"])
    best_kmeans_model = next(m for m in kmeans_models if m["k"] == best_kmeans["k"])
    best_kmeans_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_DIR}/geographic_kmeans")
    print(f"Saved K-Means k={best_kmeans['k']} (silhouette={best_kmeans['silhouette']:.4f})")

    # Save best GMM if exists
    best_gmm = None
    if gmm_metrics:
        best_gmm = max(gmm_metrics, key=lambda x: x["silhouette"])
        best_gmm_model = next(m for m in gmm_models if m["k"] == best_gmm["k"])
        best_gmm_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_DIR}/geographic_gmm")
        print(f"Saved GMM k={best_gmm['k']} (silhouette={best_gmm['silhouette']:.4f})")
    else:
        print("No valid GMM models to save")

    # Save best Bisecting K-Means if exists
    best_bkm = None
    if bkm_metrics:
        best_bkm = max(bkm_metrics, key=lambda x: x["silhouette"])
        best_bkm_model = next(m for m in bkm_models if m["k"] == best_bkm["k"])
        best_bkm_model["model"].write().overwrite().save(
            f"{MODEL_OUTPUT_DIR}/geographic_bisecting_kmeans"
        )
        print(f"Saved Bisecting K-Means k={best_bkm['k']} (silhouette={best_bkm['silhouette']:.4f})")
    else:
        print("No valid Bisecting K-Means models to save")

    # Save metrics
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    metrics_data = {
        "training_date": datetime.now().isoformat(),
        "best_models": {
            "kmeans": {"k": best_kmeans["k"], "silhouette": best_kmeans["silhouette"]},
            "gmm": {"k": best_gmm["k"], "silhouette": best_gmm["silhouette"]}
            if best_gmm
            else None,
            "bisecting_kmeans": {"k": best_bkm["k"], "silhouette": best_bkm["silhouette"]}
            if best_bkm
            else None,
        },
        "all_models": all_metrics,
        "features": NUMERIC_FEATURES,
    }

    with open(f"{LOCAL_METRICS_PATH}geographic_metrics.json", "w") as f:
        json.dump(metrics_data, f, indent=2)

    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(
        f"{MODEL_OUTPUT_DIR}/geographic_metrics.json"
    )
    print("Saved metrics")


def main(EXPORT_PLOTS=False):
    print("=" * 80)
    print("Geographic Sales Clustering - Training")
    print("=" * 80)

    spark = create_spark_session()

    # Load data
    df = load_and_validate_data(spark)
    if df is None:
        print("⚠️  Training skipped due to data validation failure")
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)
    if df is None:
        print("⚠️  Training skipped due to insufficient data")
        spark.stop()
        return

    # Validate clusterability
    validate_clusterability(df)

    # Assemble features
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df = assembler.transform(df)

    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features_scaled", withStd=True, withMean=True
    )
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Apply PCA
    print("\nApplying PCA...")
    pca = PCA(k=6, inputCol="features_scaled", outputCol="features")
    pca_model = pca.fit(df)
    df = pca_model.transform(df)

    explained = sum(pca_model.explainedVariance.toArray())
    print(f"6 components explain {explained*100:.2f}% of variance")

    df.cache()

    # Train models
    k_values = [3, 4, 5, 6]
    kmeans_models, kmeans_metrics = train_kmeans(df, "features", k_values)
    gmm_models, gmm_metrics = train_gmm(df, "features", k_values)
    bkm_models, bkm_metrics = train_bisecting_kmeans(df, "features", k_values)

    all_metrics = kmeans_metrics + gmm_metrics + bkm_metrics

    # ------------------------------------------------------------------
    # Export one cluster scatter plot per trained model × k combination.
    # Each plot shows PC1 vs PC2 (first two components of the 6-D PCA
    # already computed above) with filled convex-hull boundaries coloured
    # by cluster id — matching the reference scatter-cluster style.
    # ------------------------------------------------------------------
    if EXPORT_PLOTS:
        stem = _sanitize_name(Path(__file__).stem)
        all_trained = (
            [(m["model"], m["type"], m["k"]) for m in kmeans_models]
            + [(m["model"], m["type"], m["k"]) for m in gmm_models]
            + [(m["model"], m["type"], m["k"]) for m in bkm_models]
        )
        for trained_model, mtype, mk in all_trained:
            preds = trained_model.transform(df)   # df still has "features"
            export_cluster_scatter_plot(
                preds, mtype, mk, export_plots=True,
                export_dir=PLOT_EXPORT_DIR, script_stem=stem,
            )

    if not all_metrics:
        print("⚠️  Training skipped - No valid models")
        spark.stop()
        return

    best = max(all_metrics, key=lambda x: x["silhouette"])
    best_scores_by_type = {}
    for metric in all_metrics:
        model_type = metric["type"]
        best_scores_by_type[model_type] = max(best_scores_by_type.get(model_type, float("-inf")), metric["silhouette"])
    manifest_path = save_best_model_manifest(
        spark,
        MODEL_OUTPUT_DIR,
        f"geographic_{best['type']}",
        "silhouette",
        best["silhouette"],
        {f"geographic_{k}": v for k, v in best_scores_by_type.items()},
    )
    print(f"Saved best model manifest to: {manifest_path}")
    print(f"\n{'='*80}")
    print(f"Best: {best['type']} k={best['k']}, Silhouette={best['silhouette']:.4f}")
    print(f"{'='*80}")

    # Save models
    save_models(
        kmeans_models,
        kmeans_metrics,
        gmm_models,
        gmm_metrics,
        bkm_models,
        bkm_metrics,
        all_metrics,
        scaler_model,
        pca_model,
        spark,
    )

    print("\nTraining completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main(EXPORT_PLOTS=False)