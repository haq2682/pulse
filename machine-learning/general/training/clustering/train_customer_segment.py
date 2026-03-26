"""
Customer Segmentation Clustering - Training Script
Trains K-Means and Gaussian Mixture Models on RFM metrics
"""

import os

import sys
from pathlib import Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)
from utils.plot_exporter import export_training_metrics_plot

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json

# Environment configuration
MODEL_NAME = "customer_segment"
INPUT_RELATIVE_PATH = "transformed/agg_customers.parquet"
INPUT_RELATIVE_PATH_RFM = "transformed/agg_rfm_segmentation.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("clustering", MODEL_NAME)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"

# Required input columns
REQUIRED_COLS = [
    "customer_id",
    "days_since_last_order",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "customer_tenure_days",
    "session_conversion_rate",
]

# Feature columns for clustering (exclude ID)
FEATURE_COLS = [
    "days_since_last_order",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "customer_tenure_days",
    "session_conversion_rate",
]


def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "CustomerSegmentationTraining",
        extra_configs={
                    "spark.sql.shuffle.partitions": "8",
                    "inferSchema": "true",
                    "mergeSchema": "true"
                },
    )
def load_and_validate_data(spark):
    """Load data from multiple tables using multi-bucket loader and validate required columns"""
    # Load agg_customers from all buckets — keep _source_bucket for join safety
    customers_df, customers_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=["customer_id", "total_orders", "total_revenue", "avg_order_value", 
                         "customer_tenure_days", "session_conversion_rate"],
        filter_nulls=False,
        keep_source_bucket=True,
    )

    if customers_df is None:
        print("⚠️  No customer data available. Skipping training.")
        return None

    print(f"Loaded {customers_count} customers from all buckets")

    # Load agg_rfm_segmentation from all buckets — keep _source_bucket for join safety
    rfm_df, rfm_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH_RFM,
        required_columns=["customer_id", "days_since_last_order"],
        filter_nulls=False,
        keep_source_bucket=True,
    )

    if rfm_df is None:
        print("⚠️  No RFM data available. Skipping training.")
        return None

    print(f"Loaded {rfm_count} RFM records from all buckets")

    # Rename columns to avoid duplicates during join
    rfm_df = rfm_df.select(
        col("customer_id"),
        col("days_since_last_order"),
        col("_source_bucket"),
    )

    # Join tables on customer_id AND _source_bucket to prevent cross-tenant
    # ID collisions (two tenants can both have customer_id=1).
    customers_df = customers_df.withColumnRenamed("_source_bucket", "_bucket_cust")
    rfm_df = rfm_df.withColumnRenamed("_source_bucket", "_bucket_rfm")

    df = customers_df.join(
        rfm_df,
        (customers_df.customer_id == rfm_df.customer_id)
        & (customers_df._bucket_cust == rfm_df._bucket_rfm),
        how="inner",
    ).drop(rfm_df.customer_id).drop("_bucket_rfm").withColumnRenamed("_bucket_cust", "_source_bucket")

    record_count = df.count()
    print(f"Joined dataset shape: {record_count} rows, {len(df.columns)} columns")

    # Validate training data
    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )

    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        return None

    # Validate required columns exist
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        print(f"⚠️  Training skipped - Missing required columns: {missing_cols}")
        return None

    # Check for at least some non-null values in feature columns
    for col_name in FEATURE_COLS:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            print(f"⚠️  Training skipped - Column '{col_name}' has all NULL values")
            return None
        print(f"Column '{col_name}': {non_null_count} non-null values")

    return df


def prepare_features(df):
    """Prepare features for clustering by handling nulls and filtering"""
    print("Preparing features...")

    # Fill nulls with 0 for numeric features (business decision: missing = inactive)
    for col_name in FEATURE_COLS:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Filter out customers with no activity (total_orders = 0)
    df = df.filter(col("total_orders") > 0)

    # Filter out extreme outliers for better clustering
    df = df.filter(
        (col("total_revenue") >= 0) & (col("total_revenue") <= 1000000)
    )  # Cap at 1M

    record_count = df.count()
    print(f"Filtered dataset: {record_count} records")

    # Require minimum data for meaningful clustering
    if record_count < 100:
        print(f"ERROR: Insufficient data for clustering (need >= 100, got {record_count})")
        return None

    return df


def train_kmeans(df, features_col, k_values):
    """Train K-Means models with different k values"""
    print(f"\nTraining K-Means models with k={k_values}...")
    models = []
    metrics = []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"Training K-Means with k={k}...")
        kmeans = KMeans(featuresCol=features_col, predictionCol="prediction", k=k, seed=42)
        model = kmeans.fit(df)

        predictions = model.transform(df)
        silhouette = evaluator.evaluate(predictions)
        wssse = model.summary.trainingCost

        print(f"K-Means k={k}: Silhouette={silhouette:.4f}, WSSSE={wssse:.2f}")

        models.append({"model": model, "k": k, "type": "kmeans"})
        metrics.append({"k": k, "type": "kmeans", "silhouette": silhouette, "wssse": wssse})

    return models, metrics


def train_gmm(df, features_col, k_values):
    """Train Gaussian Mixture Models with different k values"""
    print(f"\nTraining GMM models with k={k_values}...")
    models = []
    metrics = []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"Training GMM with k={k}...")
        gmm = GaussianMixture(featuresCol=features_col, predictionCol="prediction", k=k, seed=42)
        model = gmm.fit(df)

        predictions = model.transform(df)
        silhouette = evaluator.evaluate(predictions)
        log_likelihood = model.summary.logLikelihood

        print(f"GMM k={k}: Silhouette={silhouette:.4f}, LogLikelihood={log_likelihood:.2f}")

        models.append({"model": model, "k": k, "type": "gmm"})
        metrics.append(
            {"k": k, "type": "gmm", "silhouette": silhouette, "log_likelihood": log_likelihood}
        )

    return models, metrics


def save_models_and_metrics(kmeans_models, kmeans_metrics, gmm_models, gmm_metrics, all_metrics, scaler, spark):
    """Save best k model for each algorithm type to MinIO"""
    print(f"\nSaving models to MinIO: {MODEL_OUTPUT_DIR}")

    # Save scaler to MinIO
    scaler_path = f"{MODEL_OUTPUT_DIR}/scaler"
    scaler.write().overwrite().save(scaler_path)
    print(f"Saved scaler to: {scaler_path}")

    # Find best k for K-Means
    best_kmeans = max(kmeans_metrics, key=lambda x: x["silhouette"])
    best_kmeans_model = next(m for m in kmeans_models if m["k"] == best_kmeans["k"])
    
    kmeans_path = f"{MODEL_OUTPUT_DIR}/kmeans"
    best_kmeans_model["model"].write().overwrite().save(kmeans_path)
    print(f"Saved K-Means (k={best_kmeans['k']}, silhouette={best_kmeans['silhouette']:.4f}) to: {kmeans_path}")

    # Find best k for GMM
    best_gmm = max(gmm_metrics, key=lambda x: x["silhouette"])
    best_gmm_model = next(m for m in gmm_models if m["k"] == best_gmm["k"])
    
    gmm_path = f"{MODEL_OUTPUT_DIR}/gmm"
    best_gmm_model["model"].write().overwrite().save(gmm_path)
    print(f"Saved GMM (k={best_gmm['k']}, silhouette={best_gmm['silhouette']:.4f}) to: {gmm_path}")

    # Save metrics as JSON to local temp, then upload to MinIO
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    local_metrics_file = f"{LOCAL_METRICS_PATH}metrics.json"
    
    metrics_data = {
        "training_date": datetime.now().isoformat(),
        "best_models": {
            "kmeans": {
                "k": best_kmeans["k"],
                "silhouette": best_kmeans["silhouette"],
                "wssse": best_kmeans["wssse"],
            },
            "gmm": {
                "k": best_gmm["k"],
                "silhouette": best_gmm["silhouette"],
                "log_likelihood": best_gmm["log_likelihood"],
            },
        },
        "all_models": all_metrics,
        "feature_columns": FEATURE_COLS,
    }
    
    with open(local_metrics_file, "w") as f:
        json.dump(metrics_data, f, indent=2)
    
    print(f"Saved metrics locally to: {local_metrics_file}")
    
    # Upload metrics to MinIO using Spark
    metrics_s3_path = f"{MODEL_OUTPUT_DIR}/metrics.json"
    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(metrics_s3_path)
    print(f"Uploaded metrics to MinIO: {metrics_s3_path}")
    
    print(f"\nAll models saved to MinIO bucket: {GENERAL_MODEL_BUCKET}")


def main(EXPORT_PLOTS=False):
    print("=" * 80)
    print("Customer Segmentation Clustering - Training")
    print("=" * 80)

    spark = create_spark_session()

    # Load and validate data
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

    # Assemble features into vector (drop _source_bucket — not a training feature)
    df_for_ml = df.drop("_source_bucket")
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    df_for_ml = assembler.transform(df_for_ml)

    # Normalize features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_for_ml)
    df_for_ml = scaler_model.transform(df_for_ml)

    # Cache for multiple model training
    df_for_ml.cache()

    # Train multiple K-Means models
    k_values = [3, 4, 5, 6]
    kmeans_models, kmeans_metrics = train_kmeans(df_for_ml, "features", k_values)

    # Train multiple GMM models
    gmm_models, gmm_metrics = train_gmm(df_for_ml, "features", k_values)

    # Combine all metrics for comparison
    all_metrics = kmeans_metrics + gmm_metrics

    export_training_metrics_plot(
        model_name=MODEL_NAME,
        metrics=all_metrics,
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
    )

    # Display best overall model
    best_overall = max(all_metrics, key=lambda x: x["silhouette"])
    print(f"\n{'='*80}")
    print(f"Best overall model: {best_overall['type']} with k={best_overall['k']}")
    print(f"Silhouette score: {best_overall['silhouette']:.4f}")
    print(f"{'='*80}")

    # Save all algorithm models (best k for each algorithm)
    save_models_and_metrics(kmeans_models, kmeans_metrics, gmm_models, gmm_metrics, all_metrics, scaler_model, spark)

    print("\n" + "=" * 80)
    print("Training completed successfully")
    print("=" * 80)
    print("\nSaved models:")
    print(f"- K-Means: {MODEL_OUTPUT_DIR}/kmeans")
    print(f"- GMM: {MODEL_OUTPUT_DIR}/gmm")
    print(f"- Scaler: {MODEL_OUTPUT_DIR}/scaler")
    print(f"- Metrics: {MODEL_OUTPUT_DIR}/metrics.json")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main(EXPORT_PLOTS=False)