"""
Geographic Sales Clustering - Training Script
Clusters geographic regions by sales performance and market characteristics
"""

import os
import sys
import findspark

findspark.init()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)

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


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder.appName("GeographicClusteringTraining")
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

    # Save best Bisecting K-Means
    best_bkm = max(bkm_metrics, key=lambda x: x["silhouette"])
    best_bkm_model = next(m for m in bkm_models if m["k"] == best_bkm["k"])
    best_bkm_model["model"].write().overwrite().save(
        f"{MODEL_OUTPUT_DIR}/geographic_bisecting_kmeans"
    )
    print(f"Saved Bisecting K-Means k={best_bkm['k']} (silhouette={best_bkm['silhouette']:.4f})")

    # Save metrics
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    metrics_data = {
        "training_date": datetime.now().isoformat(),
        "best_models": {
            "kmeans": {"k": best_kmeans["k"], "silhouette": best_kmeans["silhouette"]},
            "gmm": {"k": best_gmm["k"], "silhouette": best_gmm["silhouette"]}
            if best_gmm
            else None,
            "bisecting_kmeans": {"k": best_bkm["k"], "silhouette": best_bkm["silhouette"]},
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


def main():
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

    if not all_metrics:
        print("⚠️  Training skipped - No valid models")
        spark.stop()
        return

    best = max(all_metrics, key=lambda x: x["silhouette"])
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
    main()