"""
Supplier Performance Clustering - ENHANCED Training Script
Includes: Stability validation, HDBSCAN, comprehensive profiling
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, log1p, sqrt, pow as _pow
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture, BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import json
import numpy as np
from sklearn.metrics import adjusted_rand_score
import hdbscan

# Environment configuration
BUCKET = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET}/transformed/"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"

# Enhanced feature set with business-critical metrics
NUMERIC_FEATURES = [
    "supplier_rating",
    "log_total_revenue_generated",
    "avg_profit_margin",
    "stockout_rate",
    "supplier_reliability_score",
    "avg_restock_lead_time",
    "log_total_products_supplied",
    "log_total_units_sold",
    "log_total_orders_fulfilled",
    "supplier_performance_score",
    "stock_efficiency_ratio",
    "breach_rate",
    "supplier_inventory_health_score",
    "revenue_per_product",
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder.appName("SupplierClusteringEnhanced")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.dynamicAllocation.enabled", "true")
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
    """Load supplier data from multiple tables"""
    try:
        suppliers_path = f"{INPUT_PATH}agg_suppliers.parquet"
        inv_health_path = f"{INPUT_PATH}agg_supplier_inventory_health.parquet"
        
        print(f"Loading suppliers from: {suppliers_path}")
        suppliers_df = spark.read.parquet(suppliers_path)
        
        print(f"Loading inventory health from: {inv_health_path}")
        inv_health_df = spark.read.parquet(inv_health_path)

        # Select specific columns to avoid duplicates
        inv_health_df = inv_health_df.select(
            col("supplier_id"),
            col("breach_rate"),
            col("supplier_inventory_health_score").alias("inv_health_score"),
        )

        df = suppliers_df.join(inv_health_df, on="supplier_id", how="left")
        
        print(f"Joined dataset: {df.count()} suppliers, {len(df.columns)} columns")
        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features with enhanced preprocessing"""
    print("Preparing features...")

    # Fill nulls
    original_features = [
        "supplier_rating", "total_revenue_generated", "avg_profit_margin",
        "stockout_rate", "supplier_reliability_score", "avg_restock_lead_time",
        "total_products_supplied", "total_units_sold", "total_orders_fulfilled",
        "supplier_performance_score", "stock_efficiency_ratio", "breach_rate",
        "revenue_per_product",
    ]

    for col_name in original_features:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    df = df.withColumn("supplier_inventory_health_score", coalesce(col("inv_health_score"), lit(0.0)))

    # Filters
    df = df.filter(col("total_products_supplied") > 0)
    df = df.filter((col("total_revenue_generated") >= 0) & (col("total_revenue_generated") <= 10000000))
    df = df.filter((col("stockout_rate") >= 0) & (col("stockout_rate") <= 100))

    record_count = df.count()
    print(f"Filtered dataset: {record_count} suppliers")

    if record_count < 30:
        print(f"ERROR: Insufficient data (need >= 30, got {record_count})")
        return None

    # Log transformations
    print("Applying log transformations...")
    df = df.withColumn("log_total_revenue_generated", log1p(col("total_revenue_generated")))
    df = df.withColumn("log_total_products_supplied", log1p(col("total_products_supplied")))
    df = df.withColumn("log_total_units_sold", log1p(col("total_units_sold")))
    df = df.withColumn("log_total_orders_fulfilled", log1p(col("total_orders_fulfilled")))

    return df


def validate_clusterability(df):
    """Enhanced clusterability validation"""
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
        print("⚠️  WARNING: Low variance - clustering may be weak")
    else:
        print("✅ Data appears suitable for clustering")
    
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
            
            # Collect predictions for stability analysis
            pred_list = predictions.select("prediction").rdd.map(lambda x: x[0]).collect()
            run_predictions.append(pred_list)
            
            if run == 0:  # Use first run for metrics
                silhouette = evaluator.evaluate(predictions)
                wssse = model.summary.trainingCost
                first_model = model
        
        # Calculate stability (ARI between runs)
        if n_runs >= 2:
            ari_01 = adjusted_rand_score(run_predictions[0], run_predictions[1])
            ari_02 = adjusted_rand_score(run_predictions[0], run_predictions[2]) if n_runs >= 3 else 0
            avg_ari = (ari_01 + ari_02) / 2 if n_runs >= 3 else ari_01
        else:
            avg_ari = 1.0
        
        print(f"  Silhouette: {silhouette:.4f}")
        print(f"  WSSSE: {wssse:.2f}")
        print(f"  Stability (ARI): {avg_ari:.4f} {'✅ Stable' if avg_ari > 0.7 else '⚠️ Unstable'}")

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

def train_hdbscan_clustering(df, features_col):
    """Train HDBSCAN (finds natural cluster count)"""
    print(f"\n{'='*80}")
    print(f"HDBSCAN CLUSTERING (Auto-detects cluster count)")
    print(f"{'='*80}")
    
    features_np = np.array(df.select(features_col).rdd.map(lambda x: x[0].toArray()).collect())
    
    # Try different min_cluster_size values
    results = []
    for min_size in [15, 20, 30]:
        print(f"\nHDBSCAN min_cluster_size={min_size}:")
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_size, min_samples=5)
        labels = clusterer.fit_predict(features_np)
        
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = list(labels).count(-1)
        
        print(f"  Clusters found: {n_clusters}")
        print(f"  Noise points: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
        
        if n_clusters >= 2:
            from sklearn.metrics import silhouette_score
            # Filter out noise points for silhouette
            mask = labels != -1
            if sum(mask) > 0:
                silhouette = silhouette_score(features_np[mask], labels[mask])
                print(f"  Silhouette: {silhouette:.4f}")
            else:
                silhouette = -1
        else:
            silhouette = -1
            print(f"  Silhouette: N/A (insufficient clusters)")
        
        results.append({
            "min_size": min_size,
            "n_clusters": n_clusters,
            "n_noise": n_noise,
            "silhouette": silhouette,
            "labels": labels,
            "model": clusterer,
        })
    
    print(f"{'='*80}\n")
    
    # Return best result (highest silhouette with >=3 clusters)
    valid_results = [r for r in results if r["n_clusters"] >= 3 and r["silhouette"] > 0]
    if valid_results:
        best = max(valid_results, key=lambda x: x["silhouette"])
        return best, results
    return None, results


def profile_clusters(df, predictions, k):
    """Comprehensive cluster profiling for business validation"""
    print(f"\n{'='*80}")
    print(f"CLUSTER BUSINESS PROFILING (k={k})")
    print(f"{'='*80}")
    
    # Join predictions with original data
    df_with_pred = df.join(
        predictions.select("supplier_id", "prediction"),
        on="supplier_id",
        how="inner"
    )
    
    cluster_profiles = []
    
    for cluster_id in range(k):
        cluster_data = df_with_pred.filter(col("prediction") == cluster_id)
        count = cluster_data.count()
        
        if count == 0:
            continue
        
        # Aggregate key metrics
        stats = cluster_data.agg({
            "supplier_rating": "avg",
            "total_revenue_generated": "avg",
            "avg_profit_margin": "avg",
            "stockout_rate": "avg",
            "supplier_reliability_score": "avg",
            "avg_restock_lead_time": "avg",
            "total_products_supplied": "avg",
        }).collect()[0]
        
        profile = {
            "cluster_id": cluster_id,
            "count": count,
            "avg_rating": float(stats["avg(supplier_rating)"]),
            "avg_revenue": float(stats["avg(total_revenue_generated)"]),
            "avg_margin": float(stats["avg(avg_profit_margin)"]),
            "avg_stockout": float(stats["avg(stockout_rate)"]),
            "avg_reliability": float(stats["avg(supplier_reliability_score)"]),
            "avg_lead_time": float(stats["avg(avg_restock_lead_time)"]),
            "avg_products": float(stats["avg(total_products_supplied)"]),
        }
        
        # Assign business persona
        persona = assign_business_persona(profile)
        profile["persona"] = persona
        
        cluster_profiles.append(profile)
        
        # Print profile
        print(f"\nCluster {cluster_id}: {persona}")
        print(f"  Size: {count} suppliers ({count/df.count()*100:.1f}%)")
        print(f"  Rating: {profile['avg_rating']:.2f}★")
        print(f"  Revenue: ${profile['avg_revenue']:,.0f}")
        print(f"  Margin: {profile['avg_margin']:.1f}%")
        print(f"  Stockout: {profile['avg_stockout']:.1f}%")
        print(f"  Reliability: {profile['avg_reliability']:.3f}")
        print(f"  Lead Time: {profile['avg_lead_time']:.1f} days")
        print(f"  Avg Products: {profile['avg_products']:.1f}")
    
    print(f"{'='*80}\n")
    return cluster_profiles


def assign_business_persona(profile):
    """Assign business-oriented persona to cluster"""
    rating = profile["avg_rating"]
    revenue = profile["avg_revenue"]
    stockout = profile["avg_stockout"]
    reliability = profile["avg_reliability"]
    products = profile["avg_products"]
    
    # Strategic Partners: High revenue, high reliability, low stockout
    if revenue > 150000 and reliability > 0.75 and stockout < 15:
        return "Strategic Partners"
    
    # High-Growth Suppliers: Good metrics with high product count
    elif products > 10 and rating >= 4.0 and stockout < 20:
        return "High-Growth Suppliers"
    
    # Reliable Performers: Consistent moderate performance
    elif rating >= 3.5 and reliability > 0.7 and stockout < 25:
        return "Reliable Performers"
    
    # Long-Tail Suppliers: Low volume but acceptable quality
    elif revenue < 50000 and rating >= 3.5:
        return "Long-Tail Suppliers"
    
    # Risk Suppliers: High stockout or low reliability
    elif stockout > 30 or reliability < 0.6:
        return "Risk Suppliers"
    
    # Emerging Suppliers: Low metrics but potential
    elif revenue < 100000 and products < 8:
        return "Emerging Suppliers"
    
    else:
        return "Standard Suppliers"


def save_enhanced_metrics(all_metrics, cluster_profiles, spark):
    """Save comprehensive training metrics"""
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    
    # Find best models by silhouette
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
    
    # Save locally
    with open(f"{LOCAL_METRICS_PATH}supplier_enhanced_metrics.json", "w") as f:
        json.dump(metrics_data, f, indent=2)
    
    # Upload to MinIO
    metrics_df = spark.createDataFrame([metrics_data])
    metrics_df.coalesce(1).write.mode("overwrite").json(
        f"{MODEL_OUTPUT_PATH}supplier_enhanced_metrics.json"
    )
    
    print(f"✅ Saved enhanced metrics to MinIO")


def main():
    print("="*80)
    print("ENHANCED Supplier Performance Clustering - Training")
    print("="*80)

    spark = create_spark_session()

    # Load data
    df = load_and_validate_data(spark)
    if df is None:
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)
    if df is None:
        spark.stop()
        return

    # Validate clusterability
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
    print("Applying PCA for dimensionality reduction...")
    pca = PCA(k=8, inputCol="features_scaled", outputCol="features")
    pca_model = pca.fit(df)
    df = pca_model.transform(df)

    df.cache()

    # Train K-Means with stability
    k_values = [3, 4, 5]
    kmeans_models, kmeans_metrics = train_kmeans_with_stability(df, "features", k_values, n_runs=3)

    # Train HDBSCAN
    hdbscan_best, hdbscan_results = train_hdbscan_clustering(df, "features")

    # Combine metrics
    all_metrics = kmeans_metrics

    # Profile best model
    best_model = max(kmeans_models, key=lambda m: next(
        met["silhouette"] for met in kmeans_metrics if met["k"] == m["k"]
    ))
    best_k = best_model["k"]
    
    predictions = best_model["model"].transform(df)
    predictions = predictions.withColumn("supplier_id", col("supplier_id"))
    
    cluster_profiles = profile_clusters(df, predictions, best_k)

    # Save models and metrics
    print("\nSaving models...")
    scaler_model.write().overwrite().save(f"{MODEL_OUTPUT_PATH}supplier_scaler")
    pca_model.write().overwrite().save(f"{MODEL_OUTPUT_PATH}supplier_pca")
    best_model["model"].write().overwrite().save(f"{MODEL_OUTPUT_PATH}supplier_kmeans")
    
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
    
    print(f"\nCluster Personas Identified: {len(cluster_profiles)}")
    for profile in cluster_profiles:
        print(f"  - {profile['persona']}: {profile['count']} suppliers")
    
    print("="*80)

    spark.stop()


if __name__ == "__main__":
    main()