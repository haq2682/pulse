"""
Product Affinity Clustering - Training Script (IMPROVED)
Clusters products based on co-purchase patterns and product attributes
WITH: Log transformations, PCA, better feature engineering
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, sum as _sum, avg, log1p
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json

# Feature columns (using log-transformed versions)
NUMERIC_FEATURES = [
    "log_sell_price",
    "avg_rating",
    "log_total_units_sold",
    "log_total_orders",
    "unique_customers",
    "profit_margin",
    "log_total_revenue",
    "avg_affinity_score",
    "log_total_co_occurrences",
    "avg_lift",
    "strong_affinity_count",
    "cross_category_ratio",
    "category_index",
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder.appName("ProductAffinityTraining")
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


def load_and_validate_data(spark, INPUT_PATH):
    """Load product and affinity data"""
    try:
        products_path = f"{INPUT_PATH}agg_products.parquet"
        print(f"Loading products from: {products_path}")
        products_df = spark.read.parquet(products_path)

        affinity_path = f"{INPUT_PATH}agg_product_affinity.parquet"
        print(f"Loading product affinity from: {affinity_path}")
        affinity_df = spark.read.parquet(affinity_path)

        print(f"Products: {products_df.count()} rows")
        print(f"Affinity pairs: {affinity_df.count()} rows")

        # Aggregate affinity metrics per product
        affinity_a = (
            affinity_df.groupBy("product_a_id")
            .agg(
                avg("affinity_score").alias("avg_affinity_score_a"),
                _sum("co_occurrence_count").alias("total_co_occurrences_a"),
                avg("avg_lift").alias("avg_lift_a"),
                _sum(when(col("affinity_strength") == "Strong", 1).otherwise(0)).alias(
                    "strong_affinity_count_a"
                ),
                avg(when(col("is_cross_category"), 1).otherwise(0)).alias("cross_category_ratio_a"),
            )
            .withColumnRenamed("product_a_id", "product_id")
        )

        affinity_b = (
            affinity_df.groupBy("product_b_id")
            .agg(
                avg("affinity_score").alias("avg_affinity_score_b"),
                _sum("co_occurrence_count").alias("total_co_occurrences_b"),
                avg("avg_lift").alias("avg_lift_b"),
                _sum(when(col("affinity_strength") == "Strong", 1).otherwise(0)).alias(
                    "strong_affinity_count_b"
                ),
                avg(when(col("is_cross_category"), 1).otherwise(0)).alias("cross_category_ratio_b"),
            )
            .withColumnRenamed("product_b_id", "product_id")
        )

        affinity_combined = affinity_a.join(affinity_b, on="product_id", how="outer")

        affinity_combined = affinity_combined.select(
            col("product_id"),
            coalesce(
                (col("avg_affinity_score_a") + col("avg_affinity_score_b")) / 2,
                col("avg_affinity_score_a"),
                col("avg_affinity_score_b"),
                lit(0.0),
            ).alias("avg_affinity_score"),
            coalesce(
                col("total_co_occurrences_a") + col("total_co_occurrences_b"),
                col("total_co_occurrences_a"),
                col("total_co_occurrences_b"),
                lit(0),
            ).alias("total_co_occurrences"),
            coalesce(
                (col("avg_lift_a") + col("avg_lift_b")) / 2,
                col("avg_lift_a"),
                col("avg_lift_b"),
                lit(1.0),
            ).alias("avg_lift"),
            coalesce(
                col("strong_affinity_count_a") + col("strong_affinity_count_b"),
                col("strong_affinity_count_a"),
                col("strong_affinity_count_b"),
                lit(0),
            ).alias("strong_affinity_count"),
            coalesce(
                (col("cross_category_ratio_a") + col("cross_category_ratio_b")) / 2,
                col("cross_category_ratio_a"),
                col("cross_category_ratio_b"),
                lit(0.0),
            ).alias("cross_category_ratio"),
        )

        df = products_df.join(affinity_combined, on="product_id", how="left")
        print(f"Joined dataset: {df.count()} rows, {len(df.columns)} columns")

        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features with log transformations"""
    print("Preparing features...")

    # Original features with null filling
    original_features = [
        "sell_price",
        "avg_rating",
        "total_units_sold",
        "total_orders",
        "unique_customers",
        "profit_margin",
        "total_revenue",
        "avg_affinity_score",
        "total_co_occurrences",
        "avg_lift",
        "strong_affinity_count",
        "cross_category_ratio",
    ]

    for col_name in original_features:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Filter products
    df = df.filter(col("total_orders") > 0)
    df = df.filter((col("sell_price") >= 0) & (col("sell_price") <= 10000))
    df = df.filter((col("total_revenue") >= 0) & (col("total_revenue") <= 1000000))
    df = df.filter(col("category").isNotNull())

    record_count = df.count()
    print(f"Filtered dataset: {record_count} records")

    if record_count < 50:
        print(f"ERROR: Insufficient data (need >= 50, got {record_count})")
        return None

    # Apply log transformations to skewed features
    print("Applying log transformations...")
    df = df.withColumn("log_sell_price", log1p(col("sell_price")))
    df = df.withColumn("log_total_units_sold", log1p(col("total_units_sold")))
    df = df.withColumn("log_total_orders", log1p(col("total_orders")))
    df = df.withColumn("log_total_revenue", log1p(col("total_revenue")))
    df = df.withColumn("log_total_co_occurrences", log1p(col("total_co_occurrences")))

    return df


def encode_and_validate(df):
    """Encode categories and validate clusterability"""
    print("\nEncoding categorical features...")
    
    # Use StringIndexer only (no one-hot to reduce dimensionality)
    category_indexer = StringIndexer(
        inputCol="category", outputCol="category_index", handleInvalid="keep"
    )
    category_indexer_model = category_indexer.fit(df)
    df = category_indexer_model.transform(df)

    # Validate clusterability using PCA
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
    cumulative_variance = sum(explained_variance)

    print(f"First 5 PCs explain {cumulative_variance*100:.2f}% of variance")
    print(f"PC variance: {[f'{v*100:.2f}%' for v in explained_variance]}")

    if cumulative_variance < 0.5:
        print("WARNING: Low variance. Data may not cluster well.")
    else:
        print("Data appears suitable for clustering.")

    return df, category_indexer_model


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
    """Train GMM models"""
    print(f"\nTraining GMM with k={k_values}...")
    models, metrics = [], []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"GMM k={k}...", end=" ")
        try:
            gmm = GaussianMixture(featuresCol=features_col, predictionCol="prediction", k=k, seed=42)
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
    """Train Bisecting K-Means"""
    print(f"\nTraining Bisecting K-Means with k={k_values}...")
    models, metrics = [], []

    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"Bisecting K-Means k={k}...", end=" ")
        bkm = BisectingKMeans(featuresCol=features_col, predictionCol="prediction", k=k, seed=42)
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
    kmeans_models, kmeans_metrics, gmm_models, gmm_metrics, bkm_models, bkm_metrics,
    all_metrics, scaler_model, pca_model, category_indexer_model, spark, MODEL_OUTPUT_PATH, LOCAL_METRICS_PATH
):
    """Save models to MinIO"""
    print(f"\nSaving models to MinIO: {MODEL_OUTPUT_PATH}")

    # Save preprocessing models
    scaler_model.write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_scaler")
    pca_model.write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_pca")
    category_indexer_model.write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_category_indexer")
    print("Saved preprocessing models")

    # Save best K-Means
    best_kmeans = max(kmeans_metrics, key=lambda x: x["silhouette"])
    best_kmeans_model = next(m for m in kmeans_models if m["k"] == best_kmeans["k"])
    best_kmeans_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_kmeans")
    print(f"Saved K-Means k={best_kmeans['k']} (silhouette={best_kmeans['silhouette']:.4f})")

    # Save best GMM if exists
    best_gmm = None
    if gmm_metrics:
        best_gmm = max(gmm_metrics, key=lambda x: x["silhouette"])
        best_gmm_model = next(m for m in gmm_models if m["k"] == best_gmm["k"])
        best_gmm_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_gmm")
        print(f"Saved GMM k={best_gmm['k']} (silhouette={best_gmm['silhouette']:.4f})")
    else:
        print("No valid GMM models to save")

    # Save best Bisecting K-Means
    best_bkm = max(bkm_metrics, key=lambda x: x["silhouette"])
    best_bkm_model = next(m for m in bkm_models if m["k"] == best_bkm["k"])
    best_bkm_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_affinity_bisecting_kmeans")
    print(f"Saved Bisecting K-Means k={best_bkm['k']} (silhouette={best_bkm['silhouette']:.4f})")

    # Save metrics
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    metrics_data = {
        "training_date": datetime.now().isoformat(),
        "best_models": {
            "kmeans": {"k": best_kmeans["k"], "silhouette": best_kmeans["silhouette"]},
            "gmm": {"k": best_gmm["k"], "silhouette": best_gmm["silhouette"]} if best_gmm else None,
            "bisecting_kmeans": {"k": best_bkm["k"], "silhouette": best_bkm["silhouette"]},
        },
        "all_models": all_metrics,
        "features": NUMERIC_FEATURES,
    }

    with open(f"{LOCAL_METRICS_PATH}product_affinity_metrics.json", "w") as f:
        json.dump(metrics_data, f, indent=2)

    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(f"{MODEL_OUTPUT_PATH}product_affinity_metrics.json")
    print("Saved metrics")


def main(BUCKET):
    INPUT_PATH = f"s3a://{BUCKET}/transformed/"
    MODEL_OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
    LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"
    print("=" * 80)
    print("Product Affinity Clustering - Training (IMPROVED)")
    print("=" * 80)

    spark = create_spark_session()

    # Load data
    df = load_and_validate_data(spark, INPUT_PATH)
    if df is None:
        print("Training aborted")
        spark.stop()
        return

    # Prepare features with log transformations
    df = prepare_features(df)
    if df is None:
        print("Training aborted")
        spark.stop()
        return

    # Encode and validate
    df, category_indexer_model = encode_and_validate(df)

    # Assemble features
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df = assembler.transform(df)

    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features_scaled", withStd=True, withMean=True
    )
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Apply PCA for dimensionality reduction
    print("\nApplying PCA...")
    pca = PCA(k=8, inputCol="features_scaled", outputCol="features")
    pca_model = pca.fit(df)
    df = pca_model.transform(df)

    explained = sum(pca_model.explainedVariance.toArray())
    print(f"8 components explain {explained*100:.2f}% of variance")

    df.cache()

    # Train models
    k_values = [4, 5, 6, 7, 8]
    kmeans_models, kmeans_metrics = train_kmeans(df, "features", k_values)
    gmm_models, gmm_metrics = train_gmm(df, "features", k_values)
    bkm_models, bkm_metrics = train_bisecting_kmeans(df, "features", k_values)

    all_metrics = kmeans_metrics + gmm_metrics + bkm_metrics

    if not all_metrics:
        print("ERROR: No valid models")
        spark.stop()
        return

    best = max(all_metrics, key=lambda x: x["silhouette"])
    print(f"\n{'='*80}")
    print(f"Best: {best['type']} k={best['k']}, Silhouette={best['silhouette']:.4f}")
    print(f"{'='*80}")

    # Save models
    save_models(
        kmeans_models, kmeans_metrics, gmm_models, gmm_metrics,
        bkm_models, bkm_metrics, all_metrics, scaler_model, pca_model,
        category_indexer_model, spark, MODEL_OUTPUT_PATH, LOCAL_METRICS_PATH
    )

    print("\nTraining completed successfully!")
    spark.stop()


if __name__ == "__main__":
    BUCKET = "pulse-bucket-1"
    main(BUCKET)