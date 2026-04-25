"""
Geographic Sales Clustering - Inference Script
Generates geographic market segments and performance analysis
"""

import os
import sys
import matplotlib
from pathlib import Path
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial import ConvexHull

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    log1p,
    concat_ws,
    struct,
    to_json,
    avg,
    sum as _sum,
    percentile_approx,
)
from pyspark.ml.feature import VectorAssembler, StandardScalerModel, PCAModel
from pyspark.ml.clustering import KMeansModel, GaussianMixtureModel, BisectingKMeansModel
from datetime import datetime


# Feature columns (must match training)
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
# Plot configuration — same conventions as training and churn scripts
# ---------------------------------------------------------------------------
PLOT_EXPORT_DIR    = "/app/logs_for_report"
MIN_PLOT_RECORDS   = 20
MAX_PLOT_RECORDS   = 50000
MAX_SCATTER_POINTS = 50000

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
    Export a PCA-space scatter plot with convex-hull cluster boundaries
    for inference predictions.

    Parameters
    ----------
    predictions_df : Spark DataFrame
        Must contain "features" (PCA vector — first 2 components used)
        and "prediction" (integer cluster label).
        Call this BEFORE the final output-column select() in
        generate_predictions so "features" is still present.
    model_type : str  e.g. "kmeans", "gmm", "bisecting_kmeans"
    k          : int  number of clusters used
    export_plots : bool  — no-op when False
    export_dir : str
    script_stem : str  base name for the output file
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
        color = color_map[cid]
        mask  = [i for i, c in enumerate(cluster_ids) if c == cid]
        cx    = [x_vals[i] for i in mask]
        cy    = [y_vals[i] for i in mask]
        pts   = np.column_stack([cx, cy])

        # Convex hull boundary — filled + solid border
        if len(pts) >= 3:
            try:
                hull       = ConvexHull(pts)
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
                pass

        ax.scatter(
            cx, cy,
            s=40, alpha=0.75, color=color,
            edgecolor="white", linewidth=0.3,
            label=f"Cluster {cid} (n={len(cx)})",
            zorder=3,
        )

    ax.set_title(
        f"Geographic Clustering Forecast — {model_type.upper()} k={k}\n"
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



_ML_ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "machine-learning")
if str(_ML_ROOT) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT))

from spark_utils import create_ml_spark_session
from general.utils.plot_exporter import export_inference_outputs_plot
from general.model_registry import resolve_best_model

def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "GeographicClusteringInference",
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
            "inferSchema": "true",
            "mergeSchema": "true",
        },
    )

def load_data(spark, INPUT_PATH):
    """Load geographic data"""
    try:
        city_path = f"{INPUT_PATH}agg_city_aggregations.parquet"
        print(f"Loading city data from: {city_path}")
        df = spark.read.parquet(city_path)
        print(f"Loaded {df.count()} geographic regions")
        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features matching training preprocessing"""
    print("Preparing features...")

    # Fill nulls
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

    # Apply same filters as training
    df = df.filter((col("total_customers") > 0) & (col("total_orders") > 0))
    df = df.filter((col("total_revenue") >= 0) & (col("total_revenue") <= 10000000))

    # Apply log transformations
    df = df.withColumn("log_total_customers", log1p(col("total_customers")))
    df = df.withColumn("log_total_orders", log1p(col("total_orders")))
    df = df.withColumn("log_total_revenue", log1p(col("total_revenue")))

    # Create derived features
    df = df.withColumn(
        "revenue_concentration_score",
        when(col("total_customers") > 0, col("total_revenue") / col("total_customers")).otherwise(
            0.0
        ),
    )

    df = df.withColumn(
        "market_efficiency_score",
        when(col("total_customers") > 0, col("total_orders") / col("total_customers")).otherwise(
            0.0
        ),
    )

    print(f"Prepared {df.count()} geographic regions for clustering")
    return df


def load_models(spark, MODEL_PATH, SELECTED_MODEL_TYPE):
    """Load all required models"""
    try:
        # Load preprocessing models
        scaler = StandardScalerModel.load(f"{MODEL_PATH}geographic_scaler")
        pca = PCAModel.load(f"{MODEL_PATH}geographic_pca")
        print("Loaded preprocessing models")

        # Load metrics
        metrics_df = spark.read.json(f"{MODEL_PATH}geographic_metrics.json")
        metrics_row = metrics_df.select("best_models").first()
        best_models = metrics_row["best_models"]

        selected_info = best_models[SELECTED_MODEL_TYPE]
        if selected_info is None:
            print(f"ERROR: {SELECTED_MODEL_TYPE} model not available")
            return None, None, None, None

        k = selected_info["k"]
        print(f"Selected: {SELECTED_MODEL_TYPE} with k={k}")

        # Load clustering model
        model_path = f"{MODEL_PATH}geographic_{SELECTED_MODEL_TYPE}"
        if SELECTED_MODEL_TYPE == "kmeans":
            model = KMeansModel.load(model_path)
        elif SELECTED_MODEL_TYPE == "gmm":
            model = GaussianMixtureModel.load(model_path)
        elif SELECTED_MODEL_TYPE == "bisecting_kmeans":
            model = BisectingKMeansModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {SELECTED_MODEL_TYPE}")

        return model, scaler, pca, k

    except Exception as e:
        print(f"ERROR: Failed to load models: {str(e)}")
        return None, None, None, None


def compute_cluster_characteristics(predictions_df):
    """Compute characteristics for each geographic cluster"""
    print("Computing cluster characteristics...")

    cluster_stats = (
        predictions_df.groupBy("prediction")
        .agg(
            avg("total_revenue").alias("avg_revenue"),
            avg("total_customers").alias("avg_customers"),
            avg("total_orders").alias("avg_orders"),
            avg("avg_order_value").alias("avg_aov"),
            avg("revenue_per_customer").alias("avg_revenue_per_customer"),
            avg("market_efficiency_score").alias("avg_efficiency"),
        )
        .collect()
    )

    # Determine market segments based on cluster characteristics
    cluster_info = {}
    for row in cluster_stats:
        cluster_id = row["prediction"]
        avg_revenue = row["avg_revenue"]
        avg_customers = row["avg_customers"]
        avg_efficiency = row["avg_efficiency"]
        avg_revenue_per_customer = row["avg_revenue_per_customer"]

        # Assign market segment based on characteristics
        if avg_revenue > 50000 and avg_customers > 100:
            segment = "High Value Market"
            tier = "Top Performer"
        elif avg_revenue > 20000 and avg_efficiency > 3.0:
            segment = "Growth Market"
            tier = "Above Average"
        elif avg_customers > 50 and avg_revenue_per_customer > 200:
            segment = "Emerging Market"
            tier = "Above Average"
        elif avg_revenue < 5000:
            segment = "Developing Market"
            tier = "Below Average"
        else:
            segment = "Mature Market"
            tier = "Average"

        cluster_info[cluster_id] = {
            "market_segment": segment,
            "performance_tier": tier,
            "avg_revenue": round(avg_revenue, 2),
            "avg_customers": round(avg_customers, 2),
            "avg_efficiency": round(avg_efficiency, 2),
        }

        print(f"Cluster {cluster_id}: {segment} ({tier})")
        print(f"  Revenue: ${avg_revenue:.2f}, Customers: {avg_customers:.0f}")

    return cluster_info


def assign_market_segments(df, cluster_info):
    """Assign market segments and performance tiers"""
    print("Assigning market segments...")

    from functools import reduce

    # Assign market segment
    segment_expr = reduce(
        lambda acc, item: acc.when(
            col("prediction") == item[0], lit(item[1]["market_segment"])
        ),
        cluster_info.items(),
        when(lit(False), lit(None)),
    )
    df = df.withColumn("market_segment", segment_expr.otherwise(lit("Unknown")))

    # Assign performance tier
    tier_expr = reduce(
        lambda acc, item: acc.when(
            col("prediction") == item[0], lit(item[1]["performance_tier"])
        ),
        cluster_info.items(),
        when(lit(False), lit(None)),
    )
    df = df.withColumn("performance_tier", tier_expr.otherwise(lit("Unknown")))

    return df


def calculate_expansion_scores(df):
    """Calculate expansion opportunity scores"""
    print("Calculating expansion opportunity scores...")

    # Simple scoring based on market characteristics
    # High customers + low revenue per customer = expansion opportunity
    df = df.withColumn(
        "expansion_opportunity_score",
        when(
            (col("total_customers") > 50) & (col("revenue_per_customer") < 200),
            lit(0.8),
        )
        .when((col("total_customers") > 20) & (col("market_efficiency_score") > 2.0), lit(0.6))
        .when((col("total_revenue") < 10000) & (col("total_customers") > 10), lit(0.5))
        .otherwise(lit(0.3)),
    )

    return df


def generate_predictions(spark, df, model, scaler, pca, k, SELECTED_MODEL_TYPE, export_plots=False):
    """Apply model and generate predictions"""
    print("Generating predictions...")

    # Assemble features
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df = assembler.transform(df)

    # Scale and apply PCA
    df = scaler.transform(df)
    df = pca.transform(df)

    # Apply clustering model
    predictions = model.transform(df)

    # Compute cluster characteristics
    cluster_info = compute_cluster_characteristics(predictions)

    # Assign market segments and tiers
    predictions = assign_market_segments(predictions, cluster_info)

    # Calculate expansion scores
    predictions = calculate_expansion_scores(predictions)

    # Create segment characteristics JSON
    def make_characteristics(cluster_id):
        info = cluster_info.get(cluster_id, {})
        return to_json(
            struct(
                lit(info.get("avg_revenue", 0)).alias("avg_revenue"),
                lit(info.get("avg_customers", 0)).alias("avg_customers"),
                lit(info.get("avg_efficiency", 0)).alias("avg_efficiency"),
            )
        )

    # Add metadata
    predictions = predictions.withColumn("cluster_date", lit(datetime.now()))
    predictions = predictions.withColumn(
        "clustering_id",
        concat_ws("_", col("country"), col("state_province"), col("city"), lit("current")),
    )
    predictions = predictions.withColumn("model_version", lit(f"{SELECTED_MODEL_TYPE}_k{k}"))
    predictions = predictions.withColumn("cluster_centroid_distance", lit(0.0))

    # Create segment characteristics JSON for each row
    from functools import reduce

    char_expr = reduce(
        lambda acc, item: acc.when(
            col("prediction") == item[0],
            to_json(
                struct(
                    lit(item[1]["avg_revenue"]).alias("avg_revenue"),
                    lit(item[1]["avg_customers"]).alias("avg_customers"),
                    lit(item[1]["avg_efficiency"]).alias("avg_efficiency"),
                )
            ),
        ),
        cluster_info.items(),
        when(lit(False), lit(None)),
    )

    predictions = predictions.withColumn(
        "segment_characteristics", char_expr.otherwise(lit("{}"))
    )

    # Export cluster scatter plot while "features" (PCA vector) and
    # "prediction" are still both present on the DataFrame — before the
    # final select() drops them.
    export_cluster_scatter_plot(
        predictions,
        model_type=SELECTED_MODEL_TYPE,
        k=k,
        export_plots=export_plots,
        export_dir=PLOT_EXPORT_DIR,
        script_stem=_sanitize_name(Path(__file__).stem),
    )

    # Select output columns
    output_cols = [
        "clustering_id",
        "country",
        "state_province",
        "city",
        "cluster_date",
        col("prediction").alias("cluster_id"),
        "market_segment",
        "cluster_centroid_distance",
        "segment_characteristics",
        "expansion_opportunity_score",
        "model_version",
    ]

    return predictions.select(output_cols)


def save_predictions(predictions, output_path):
    """Save predictions to MinIO"""
    print(f"Saving predictions to: {output_path}")
    predictions.write.mode("overwrite").parquet(output_path)
    print(f"Saved {predictions.count()} predictions")


def main(BUCKET, EXPORT_PLOTS=False):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH = f"s3a://{BUCKET}/transformed/"
    MODEL_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/clustering/models/geo_cluster/"
    OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"

    MODEL_CANDIDATES = ["geographic_kmeans", "geographic_gmm", "geographic_bisecting_kmeans"]
    PREFERRED_MODEL = "geographic_kmeans"
    print("=" * 80)
    print("Geographic Sales Clustering - Inference")
    print("=" * 80)

    spark = create_spark_session()

    selected_artifact, selection_source, _ = resolve_best_model(
        spark,
        MODEL_PATH,
        MODEL_CANDIDATES,
        preferred_model=PREFERRED_MODEL,
    )
    SELECTED_MODEL_TYPE = selected_artifact.replace("geographic_", "")
    print(f"Model: {SELECTED_MODEL_TYPE.upper()} (source: {selection_source})")

    # Load data
    df = load_data(spark, INPUT_PATH)
    if df is None:
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)

    # Load models
    model, scaler, pca, k = load_models(spark, MODEL_PATH, SELECTED_MODEL_TYPE)
    if model is None:
        spark.stop()
        return

    # Generate predictions
    predictions = generate_predictions(spark, df, model, scaler, pca, k, SELECTED_MODEL_TYPE, export_plots=EXPORT_PLOTS)

    export_inference_outputs_plot(
        model_name=f"geo_cluster_{SELECTED_MODEL_TYPE}",
        predictions_df=predictions,
        label_column="market_segment",
        numeric_columns=["cluster_centroid_distance", "expansion_opportunity_score"],
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
        run_name=f"{SELECTED_MODEL_TYPE}_k{k}",
    )

    # Save
    save_predictions(predictions, f"{OUTPUT_PATH}geographic_clustering.parquet")

    print("\nInference completed successfully!")
    spark.stop()


if __name__ == "__main__":
    BUCKET= 'pulse-bucket-1'
    main(BUCKET, EXPORT_PLOTS=True)