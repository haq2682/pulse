"""
Customer Segmentation Clustering - Training Script
Trains K-Means and Gaussian Mixture Models on RFM metrics
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json

# Environment configuration
BUCKET = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET}/transformed/"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
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
    """Initialize Spark session with MinIO configuration"""
    return (
        SparkSession.builder.appName("CustomerSegmentationTraining")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "1000")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("inferSchema", "true")
        .config("mergeSchema", "true")
        .getOrCreate()
    )


def load_and_validate_data(spark):
    """Load data from multiple tables and validate required columns"""
    try:
        # Load agg_customers
        customers_path = f"{INPUT_PATH}agg_customers.parquet"
        print(f"Loading customers from: {customers_path}")
        customers_df = spark.read.parquet(customers_path)

        # Load agg_rfm_segmentation
        rfm_path = f"{INPUT_PATH}agg_rfm_segmentation.parquet"
        print(f"Loading RFM data from: {rfm_path}")
        rfm_df = spark.read.parquet(rfm_path)

        # Rename columns to avoid duplicates during join
        rfm_df = rfm_df.select(
            col("customer_id"),
            col("days_since_last_order"),
        )

        # Join tables on customer_id
        df = customers_df.join(rfm_df, on="customer_id", how="inner")

        print(f"Joined dataset shape: {df.count()} rows, {len(df.columns)} columns")

        # Validate required columns exist
        missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing_cols:
            print(f"ERROR: Missing required columns: {missing_cols}")
            return None

        # Check for at least some non-null values in feature columns
        for col_name in FEATURE_COLS:
            non_null_count = df.filter(col(col_name).isNotNull()).count()
            if non_null_count == 0:
                print(f"ERROR: Column '{col_name}' has all NULL values")
                return None
            print(f"Column '{col_name}': {non_null_count} non-null values")

        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


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
    print(f"\nSaving models to MinIO: {MODEL_OUTPUT_PATH}")

    # Save scaler to MinIO
    scaler_path = f"{MODEL_OUTPUT_PATH}scaler"
    scaler.write().overwrite().save(scaler_path)
    print(f"Saved scaler to: {scaler_path}")

    # Find best k for K-Means
    best_kmeans = max(kmeans_metrics, key=lambda x: x["silhouette"])
    best_kmeans_model = next(m for m in kmeans_models if m["k"] == best_kmeans["k"])
    
    kmeans_path = f"{MODEL_OUTPUT_PATH}kmeans"
    best_kmeans_model["model"].write().overwrite().save(kmeans_path)
    print(f"Saved K-Means (k={best_kmeans['k']}, silhouette={best_kmeans['silhouette']:.4f}) to: {kmeans_path}")

    # Find best k for GMM
    best_gmm = max(gmm_metrics, key=lambda x: x["silhouette"])
    best_gmm_model = next(m for m in gmm_models if m["k"] == best_gmm["k"])
    
    gmm_path = f"{MODEL_OUTPUT_PATH}gmm"
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
    metrics_s3_path = f"{MODEL_OUTPUT_PATH}metrics.json"
    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(metrics_s3_path)
    print(f"Uploaded metrics to MinIO: {metrics_s3_path}")
    
    print(f"\nAll models saved to MinIO bucket: {BUCKET}")


def main():
    print("=" * 80)
    print("Customer Segmentation Clustering - Training")
    print("=" * 80)

    spark = create_spark_session()

    # Load and validate data
    df = load_and_validate_data(spark)
    if df is None:
        print("Training aborted due to data validation failure")
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)
    if df is None:
        print("Training aborted due to insufficient data")
        spark.stop()
        return

    # Assemble features into vector
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    df = assembler.transform(df)

    # Normalize features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Cache for multiple model training
    df.cache()

    # Train multiple K-Means models
    k_values = [3, 4, 5, 6]
    kmeans_models, kmeans_metrics = train_kmeans(df, "features", k_values)

    # Train multiple GMM models
    gmm_models, gmm_metrics = train_gmm(df, "features", k_values)

    # Combine all metrics for comparison
    all_metrics = kmeans_metrics + gmm_metrics

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
    print("- K-Means: s3a://pulse-bucket-1/machine-learning/clustering/models/kmeans")
    print("- GMM: s3a://pulse-bucket-1/machine-learning/clustering/models/gmm")
    print("- Scaler: s3a://pulse-bucket-1/machine-learning/clustering/models/scaler")
    print("- Metrics: s3a://pulse-bucket-1/machine-learning/clustering/models/metrics.json")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()