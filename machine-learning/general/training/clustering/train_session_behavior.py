"""
Session Behavior Clustering - ENHANCED Training Script
Clusters sessions by user behavior patterns with stability validation
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
from pyspark.sql.functions import col, when, lit, coalesce, log1p, expr, count
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, StringIndexer
from pyspark.ml.clustering import KMeans, GaussianMixture, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json
import numpy as np
from sklearn.metrics import adjusted_rand_score
import hdbscan

# Environment configuration
MODEL_NAME = "session_behavior"
INPUT_RELATIVE_PATH = "transformed/agg_customer_sessions.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("clustering", MODEL_NAME)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)
LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"

# Numeric features for session behavior
NUMERIC_FEATURES = [
    "session_duration_minutes",
    "pages_viewed",
    "products_viewed",
    "items_added_to_cart",
    "conversion_flag",
    "cart_abandonment_flag",
    "pages_per_minute",
    "products_per_page",
    "cart_add_rate",
    "session_engagement_score",
    "log_cart_value",
    "device_type_index",
    "referrer_source_index",
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder.appName("SessionBehaviorClustering")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
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
        .getOrCreate()
    )


def load_and_validate_data(spark):
    """Load session data using multi-bucket loader"""
    df, record_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=["session_id", "session_duration_minutes", "pages_viewed", 
                         "products_viewed", "items_added_to_cart", "conversion_flag"],
        filter_nulls=False
    )

    if df is None:
        print("⚠️  No data available. Skipping training.")
        return None

    print(f"Loaded {record_count} sessions from all buckets")

    # Validate training data
    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )

    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        return None

    return df


def prepare_features(df):
    """Prepare features with enhanced preprocessing"""
    print("Preparing features...")

    # Fill nulls for numeric features
    numeric_cols = [
        "session_duration_minutes", "pages_viewed", "products_viewed",
        "items_added_to_cart", "conversion_flag", "cart_abandonment_flag",
        "pages_per_minute", "products_per_page", "cart_add_rate",
        "session_engagement_score", "cart_value"
    ]
    
    for col_name in numeric_cols:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Fill nulls for categorical features
    df = df.withColumn("device_type", coalesce(col("device_type"), lit("Unknown")))
    df = df.withColumn("referrer_source", coalesce(col("referrer_source"), lit("Unknown")))

    # Filter out invalid sessions (need at least 1 page view or 30 seconds duration)
    df = df.filter(
        (col("pages_viewed") > 0) | (col("session_duration_minutes") >= 0.5)
    )
    
    # Filter outliers (sessions over 24 hours are likely data quality issues)
    df = df.filter(col("session_duration_minutes") <= 1440)  # 24 hours

    record_count = df.count()
    print(f"Filtered dataset: {record_count} sessions")

    if record_count < 100:
        print(f"ERROR: Insufficient data (need >= 100, got {record_count})")
        return None

    # Log transformation for skewed features
    print("Applying log transformations...")
    df = df.withColumn("log_cart_value", log1p(col("cart_value")))

    # Encode categorical features
    print("Encoding categorical features...")
    
    device_indexer = StringIndexer(
        inputCol="device_type", 
        outputCol="device_type_index",
        handleInvalid="keep"
    )
    df = device_indexer.fit(df).transform(df)
    
    referrer_indexer = StringIndexer(
        inputCol="referrer_source",
        outputCol="referrer_source_index",
        handleInvalid="keep"
    )
    df = referrer_indexer.fit(df).transform(df)

    return df


def validate_clusterability(df):
    """Validate if data is suitable for clustering"""
    print("\n" + "="*80)
    print("CLUSTERABILITY VALIDATION")
    print("="*80)

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
    for i, v in enumerate(explained_variance, 1):
        print(f"  PC{i}: {v*100:.2f}%")

    if cumulative < 0.6:
        print("⚠️  WARNING: Low variance - clustering may be moderate quality")
    else:
        print("✅ Data appears suitable for session behavior clustering")
    
    print("="*80 + "\n")
    return True


def train_kmeans_with_stability(df, features_col, k_values, n_runs=3):
    """Train K-Means with stability testing"""
    print(f"\n{'='*80}")
    print(f"K-MEANS CLUSTERING WITH STABILITY TESTING")
    print(f"{'='*80}")
    
    models, metrics = [], []
    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"\nK-Means k={k}:")
        run_predictions = []
        
        for run in range(n_runs):
            seed = 42 + run * 100
            kmeans = KMeans(featuresCol=features_col, predictionCol="prediction", k=k, seed=seed)
            model = kmeans.fit(df)
            predictions = model.transform(df)
            
            pred_list = predictions.select("prediction").rdd.map(lambda x: x[0]).collect()
            run_predictions.append(pred_list)
            
            if run == 0:
                silhouette = evaluator.evaluate(predictions)
                wssse = model.summary.trainingCost
                first_model = model
        
        # Calculate stability
        if n_runs >= 2:
            ari_01 = adjusted_rand_score(run_predictions[0], run_predictions[1])
            ari_02 = adjusted_rand_score(run_predictions[0], run_predictions[2]) if n_runs >= 3 else 0
            avg_ari = (ari_01 + ari_02) / 2 if n_runs >= 3 else ari_01
        else:
            avg_ari = 1.0
        
        print(f"  Silhouette: {silhouette:.4f}")
        print(f"  WSSSE: {wssse:.2f}")
        print(f"  Stability (ARI): {avg_ari:.4f} {'✅ Stable' if avg_ari > 0.7 else '⚠️ Moderate' if avg_ari > 0.5 else '❌ Unstable'}")

        models.append({"model": first_model, "k": k, "type": "kmeans"})
        metrics.append({
            "k": k,
            "type": "kmeans",
            "silhouette": silhouette,
            "wssse": wssse,
            "stability_ari": avg_ari,
        })

    print(f"{'='*80}\n")
    return models, metrics


def train_gmm(df, features_col, k_values):
    """Train Gaussian Mixture Models"""
    print(f"\n{'='*80}")
    print(f"GAUSSIAN MIXTURE MODEL CLUSTERING")
    print(f"{'='*80}")
    
    models, metrics = [], []
    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"\nGMM k={k}:", end=" ")
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

    print(f"{'='*80}\n")
    return models, metrics


def train_bisecting_kmeans(df, features_col, k_values):
    """Train Bisecting K-Means"""
    print(f"\n{'='*80}")
    print(f"BISECTING K-MEANS CLUSTERING")
    print(f"{'='*80}")
    
    models, metrics = [], []
    evaluator = ClusteringEvaluator(
        featuresCol=features_col, metricName="silhouette", distanceMeasure="squaredEuclidean"
    )

    for k in k_values:
        print(f"\nBisecting K-Means k={k}:", end=" ")
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

    print(f"{'='*80}\n")
    return models, metrics


def train_hdbscan_clustering(df, features_col):
    """Train HDBSCAN (finds natural cluster count)"""
    print(f"\n{'='*80}")
    print(f"HDBSCAN CLUSTERING (Auto-detects cluster count)")
    print(f"{'='*80}")
    
    features_np = np.array(df.select(features_col).rdd.map(lambda x: x[0].toArray()).collect())
    
    results = []
    for min_size in [30, 50, 75]:
        print(f"\nHDBSCAN min_cluster_size={min_size}:")
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_size, min_samples=10)
        labels = clusterer.fit_predict(features_np)
        
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)
        
        print(f"  Clusters found: {n_clusters}")
        print(f"  Noise points: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
        
        if n_clusters >= 2:
            from sklearn.metrics import silhouette_score
            mask = labels != -1
            if sum(mask) > 0:
                silhouette = silhouette_score(features_np[mask], labels[mask])
                print(f"  Silhouette: {silhouette:.4f}")
            else:
                silhouette = -1
        else:
            silhouette = -1
            print(f"  Silhouette: N/A")
        
        results.append({
            "min_size": min_size,
            "n_clusters": n_clusters,
            "n_noise": n_noise,
            "silhouette": silhouette,
            "labels": labels,
            "model": clusterer,
        })
    
    print(f"{'='*80}\n")
    
    valid_results = [r for r in results if r["n_clusters"] >= 3 and r["silhouette"] > 0]
    if valid_results:
        best = max(valid_results, key=lambda x: x["silhouette"])
        return best, results
    return None, results


def profile_session_clusters(df, predictions, k):
    """Profile clusters with session behavior metrics"""
    print(f"\n{'='*80}")
    print(f"SESSION BEHAVIOR PROFILING (k={k})")
    print(f"{'='*80}")
    
    df_with_pred = df.join(
        predictions.select("session_id", "prediction"),
        on="session_id",
        how="inner"
    )
    
    cluster_profiles = []
    
    for cluster_id in range(k):
        cluster_data = df_with_pred.filter(col("prediction") == cluster_id)
        count = cluster_data.count()
        
        if count == 0:
            continue
        
        stats = cluster_data.agg({
            "session_duration_minutes": "avg",
            "pages_viewed": "avg",
            "products_viewed": "avg",
            "items_added_to_cart": "avg",
            "conversion_flag": "avg",
            "cart_abandonment_flag": "avg",
            "pages_per_minute": "avg",
            "session_engagement_score": "avg",
        }).collect()[0]
        
        profile = {
            "cluster_id": cluster_id,
            "count": count,
            "avg_duration": float(stats["avg(session_duration_minutes)"]),
            "avg_pages": float(stats["avg(pages_viewed)"]),
            "avg_products": float(stats["avg(products_viewed)"]),
            "avg_cart_adds": float(stats["avg(items_added_to_cart)"]),
            "conversion_rate": float(stats["avg(conversion_flag)"]),
            "abandonment_rate": float(stats["avg(cart_abandonment_flag)"]),
            "avg_engagement": float(stats["avg(pages_per_minute)"]),
            "avg_score": float(stats["avg(session_engagement_score)"]),
        }
        
        # Assign behavior persona
        persona = assign_behavior_persona(profile)
        profile["persona"] = persona
        
        cluster_profiles.append(profile)
        
        print(f"\nCluster {cluster_id}: {persona}")
        print(f"  Size: {count} sessions ({count/df.count()*100:.1f}%)")
        print(f"  Avg Duration: {profile['avg_duration']:.1f} min")
        print(f"  Avg Pages: {profile['avg_pages']:.1f}")
        print(f"  Avg Products: {profile['avg_products']:.1f}")
        print(f"  Avg Cart Adds: {profile['avg_cart_adds']:.2f}")
        print(f"  Conversion Rate: {profile['conversion_rate']*100:.1f}%")
        print(f"  Abandonment Rate: {profile['abandonment_rate']*100:.1f}%")
        print(f"  Engagement: {profile['avg_engagement']:.2f} pages/min")
    
    print(f"{'='*80}\n")
    return cluster_profiles


def assign_behavior_persona(profile):
    """Assign behavior persona based on session characteristics"""
    duration = profile["avg_duration"]
    pages = profile["avg_pages"]
    products = profile["avg_products"]
    cart_adds = profile["avg_cart_adds"]
    conversion = profile["conversion_rate"]
    abandonment = profile["abandonment_rate"]
    
    # Quick Buyers: Low browsing, high conversion
    if conversion > 0.5 and pages < 5 and duration < 10:
        return "Quick Buyers"
    
    # Researchers: High browsing, no conversion
    elif pages > 10 and products > 8 and conversion < 0.1 and cart_adds < 0.5:
        return "Researchers"
    
    # Abandoners: Cart adds but no conversion
    elif cart_adds > 0.5 and conversion < 0.2 and abandonment > 0.5:
        return "Cart Abandoners"
    
    # Engaged Shoppers: High engagement, some cart activity
    elif pages > 8 and products > 5 and cart_adds > 0.3:
        return "Engaged Shoppers"
    
    # Browsers: Moderate pages, low products, no cart
    elif pages > 3 and products < 3 and cart_adds < 0.2:
        return "Casual Browsers"
    
    # Window Shoppers: High products, low pages
    elif products > 5 and pages < 8 and cart_adds < 0.5:
        return "Window Shoppers"
    
    # Converters: Moderate activity with conversion
    elif conversion > 0.3:
        return "Successful Converters"
    
    else:
        return "Standard Sessions"


def save_enhanced_metrics(all_metrics, cluster_profiles, spark):
    """Save comprehensive training metrics"""
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    
    kmeans_metrics = [m for m in all_metrics if m["type"] == "kmeans"]
    best_kmeans = max(kmeans_metrics, key=lambda x: x["silhouette"]) if kmeans_metrics else None
    
    metrics_data = {
        "training_date": datetime.now().isoformat(),
        "best_models": {
            "kmeans": {
                "k": best_kmeans["k"],
                "silhouette": best_kmeans["silhouette"],
                "stability_ari": best_kmeans.get("stability_ari", 0),
            } if best_kmeans else None,
        },
        "all_models": all_metrics,
        "cluster_profiles": cluster_profiles,
        "features": NUMERIC_FEATURES,
        "production_readiness": {
            "best_silhouette": max(m["silhouette"] for m in all_metrics),
            "stability_passed": best_kmeans.get("stability_ari", 0) > 0.7 if best_kmeans else False,
            "personas_defined": len(cluster_profiles) > 0,
        }
    }
    
    with open(f"{LOCAL_METRICS_PATH}session_behavior_metrics.json", "w") as f:
        json.dump(metrics_data, f, indent=2)
    
    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(
        f"{MODEL_OUTPUT_DIR}/session_behavior_metrics.json"
    )
    
    print(f"✅ Saved enhanced metrics to MinIO")


def main():
    print("="*80)
    print("ENHANCED Session Behavior Clustering - Training")
    print("="*80)

    spark = create_spark_session()

    df = load_and_validate_data(spark)
    if df is None:
        print("⚠️  Training skipped due to data validation failure")
        spark.stop()
        return

    df = prepare_features(df)
    if df is None:
        print("⚠️  Training skipped due to insufficient data")
        spark.stop()
        return

    validate_clusterability(df)

    # Assemble and scale features
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df = assembler.transform(df)

    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features_scaled", withStd=True, withMean=True
    )
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Apply PCA
    print("Applying PCA...")
    pca = PCA(k=8, inputCol="features_scaled", outputCol="features")
    pca_model = pca.fit(df)
    df = pca_model.transform(df)

    df.cache()

    # Train models
    k_values = [4, 5, 6]
    kmeans_models, kmeans_metrics = train_kmeans_with_stability(df, "features", k_values, n_runs=3)
    gmm_models, gmm_metrics = train_gmm(df, "features", k_values)
    bkm_models, bkm_metrics = train_bisecting_kmeans(df, "features", k_values)
    hdbscan_best, hdbscan_results = train_hdbscan_clustering(df, "features")

    all_metrics = kmeans_metrics + gmm_metrics + bkm_metrics

    # Profile best model
    best_model = max(kmeans_models, key=lambda m: next(
        met["silhouette"] for met in kmeans_metrics if met["k"] == m["k"]
    ))
    best_k = best_model["k"]
    
    predictions = best_model["model"].transform(df)
    predictions = predictions.withColumn("session_id", col("session_id"))
    
    cluster_profiles = profile_session_clusters(df, predictions, best_k)

    # Save models
    print("\nSaving models...")
    scaler_model.write().overwrite().save(f"{MODEL_OUTPUT_DIR}/session_behavior_scaler")
    pca_model.write().overwrite().save(f"{MODEL_OUTPUT_DIR}/session_behavior_pca")
    best_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_DIR}/session_behavior_kmeans")
    
    save_enhanced_metrics(all_metrics, cluster_profiles, spark)

    # Final report
    print("\n" + "="*80)
    print("TRAINING SUMMARY")
    print("="*80)
    best_metric = max(all_metrics, key=lambda x: x["silhouette"])
    print(f"Best Model: {best_metric['type']} k={best_metric['k']}")
    print(f"Silhouette: {best_metric['silhouette']:.4f}")
    
    if "stability_ari" in best_metric:
        print(f"Stability: {best_metric['stability_ari']:.4f}")
    
    print(f"\nBehavior Personas: {len(cluster_profiles)}")
    for profile in cluster_profiles:
        print(f"  - {profile['persona']}: {profile['count']} sessions")
    
    print("="*80)

    spark.stop()


if __name__ == "__main__":
    main()