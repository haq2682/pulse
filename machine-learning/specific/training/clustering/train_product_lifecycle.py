"""Product Lifecycle Clustering - Training"""

import os
import json
from datetime import datetime
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, log1p, expr, greatest, least
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline, PipelineModel

BUCKET = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET}/transformed/"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
METRICS_OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/metrics/"
LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"

FEATURES = ["log_age", "log_sales_velocity", "log_revenue_velocity", "log_turnover"]
K_RANGE = [3, 4, 5, 6]
MIN_PRODUCTS = 50


def create_spark():
    return (
        SparkSession.builder.appName("ProductLifecycleTrain")
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "1000") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ) \
        .config("inferSchema", "true") \
        .config("mergeSchema", "true") \
        .getOrCreate()
    )


def load_data(spark):
    df = spark.read.parquet(f"{INPUT_PATH}agg_products.parquet")
    df = df.dropDuplicates(["product_id"])
    print(f"Loaded {df.count()} products")
    return df


def validate_columns(df, required_cols):
    for c in required_cols:
        if c not in df.columns:
            print(f"Missing: {c}")
            return False
        if df.filter(col(c).isNotNull()).count() == 0:
            print(f"All null: {c}")
            return False
    return True


def prepare_features(df):
    required = ["product_id", "days_since_launch", "total_units_sold", "total_revenue", "inventory_turnover_rate"]
    if not validate_columns(df, required):
        return None
    
    for c in required[1:]:
        df = df.withColumn(c, coalesce(col(c), lit(0.0)))
    
    df = df.filter((col("total_units_sold") > 0) & (col("days_since_launch") > 0) & (col("days_since_launch") <= 3650))
    
    count = df.count()
    print(f"After filter: {count}")
    if count < MIN_PRODUCTS:
        return None
    
    df = df.withColumn("log_age", log1p(col("days_since_launch")))
    df = df.withColumn("effective_age", greatest(col("days_since_launch"), lit(30.0)))
    df = df.withColumn("sales_velocity", col("total_units_sold") / col("effective_age"))
    df = df.withColumn("revenue_velocity", col("total_revenue") / col("effective_age"))
    df = df.withColumn("log_sales_velocity", log1p(col("sales_velocity")))
    df = df.withColumn("log_revenue_velocity", log1p(col("revenue_velocity")))
    df = df.withColumn("log_turnover", log1p(col("inventory_turnover_rate")))
    
    turnover_p99 = df.agg(expr("percentile_approx(inventory_turnover_rate, 0.99)")).collect()[0][0]
    turnover_cap = max(turnover_p99 * 1.5, 20.0)  # reasonable hard cap ~20 is very high already
    
    df = df.withColumn(
        "inventory_turnover_rate",
        when(col("inventory_turnover_rate") > turnover_cap, lit(turnover_cap))
         .otherwise(col("inventory_turnover_rate"))
    )
    
    # Recompute log_turnover after capping
    df = df.withColumn("log_turnover", log1p(col("inventory_turnover_rate")))
    return df


def compute_stats(df):
    stats = df.agg(
        expr("percentile_approx(days_since_launch, 0.25)").alias("age_p25"),
        expr("percentile_approx(days_since_launch, 0.50)").alias("age_p50"),
        expr("percentile_approx(days_since_launch, 0.75)").alias("age_p75"),
        expr("percentile_approx(sales_velocity, 0.25)").alias("vel_p25"),
        expr("percentile_approx(sales_velocity, 0.50)").alias("vel_p50"),
        expr("percentile_approx(sales_velocity, 0.75)").alias("vel_p75"),
        expr("percentile_approx(inventory_turnover_rate, 0.50)").alias("turnover_p50"),
        expr("percentile_approx(inventory_turnover_rate, 0.75)").alias("turnover_p75"),
    ).collect()[0]
    
    return {k: float(stats[k]) if stats[k] else 0 for k in 
            ["age_p25", "age_p50", "age_p75", "vel_p25", "vel_p50", "vel_p75", "turnover_p50", "turnover_p75"]}


def build_pipeline():
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features_raw", handleInvalid="skip")
    scaler = MinMaxScaler(inputCol="features_raw", outputCol="features_scaled")
    return Pipeline(stages=[assembler, scaler])


def train_and_evaluate(df_scaled, k):
    kmeans = KMeans(featuresCol="features_scaled", predictionCol="prediction", k=k, seed=42, maxIter=100)
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    
    evaluator = ClusteringEvaluator(featuresCol="features_scaled", predictionCol="prediction", 
                                     metricName="silhouette", distanceMeasure="squaredEuclidean")
    silhouette = evaluator.evaluate(predictions)
    
    counts = predictions.groupBy("prediction").count().collect()
    min_c, max_c = min(r["count"] for r in counts), max(r["count"] for r in counts)
    
    return model, predictions, silhouette, min_c/max_c


def profile_clusters(predictions, stats):
    from pyspark.sql.functions import avg as spark_avg, count as spark_count
    
    cluster_stats = predictions.groupBy("prediction").agg(
        spark_count("product_id").alias("count"),
        spark_avg("days_since_launch").alias("avg_age"),
        spark_avg("sales_velocity").alias("avg_velocity"),
        spark_avg("inventory_turnover_rate").alias("avg_turnover"),
    ).orderBy("prediction").collect()
    
    profiles = []
    for row in cluster_stats:
        age = float(row["avg_age"])
        vel  = float(row["avg_velocity"])
        turn = float(row["avg_turnover"])
        count = int(row["count"])
        
        # Tunable multipliers — adjust these based on business intuition
        GROWTH_VEL_THRESH  = stats["vel_p75"] * 1.35     # 35% above 75th percentile
        GROWTH_TURN_THRESH = stats["turnover_p50"] * 1.5
        DECLINE_VEL_THRESH = stats["vel_p25"] * 0.55     # well below 25th
        DECLINE_AGE_THRESH = stats["age_p50"] * 1.25     # older than median
        
        if age < stats["age_p25"]:
            if vel >= stats["vel_p50"]:
                stage = "Early Traction / Introduction"
            else:
                stage = "Pre-launch / Very Early"
        elif vel >= GROWTH_VEL_THRESH and turn >= GROWTH_TURN_THRESH:
            stage = "High Growth"
        elif vel < DECLINE_VEL_THRESH and age > DECLINE_AGE_THRESH:
            stage = "Decline"
        elif turn >= stats["turnover_p75"] * 1.6:
            stage = "High Turnover Growth"
        elif vel >= stats["vel_p50"] * 0.75 and vel <= stats["vel_p75"] * 1.3:
            stage = "Stable Maturity"
        else:
            stage = "Transition / Uncertain"
        
        profiles.append({
            "cluster_id": int(row["prediction"]),
            "count": count,
            "avg_age_days": age,
            "avg_velocity": vel,
            "avg_turnover": turn,
            "stage": stage
        })
    
    return profiles


def save_model_and_metrics(spark, preprocess_model, kmeans_model, profiles, stats, best_k, best_silhouette):
    full_stages = list(preprocess_model.stages) + [kmeans_model]
    full_pipeline = PipelineModel(stages=full_stages)
    full_pipeline.write().overwrite().save(f"{MODEL_OUTPUT_PATH}product_lifecycle_pipeline")
    
    metrics = {
        "training_date": datetime.now().isoformat(),
        "best_k": best_k,
        "silhouette_score": best_silhouette,
        "features": FEATURES,
        "stats": stats,
    }
    
    os.makedirs(LOCAL_METRICS_PATH, exist_ok=True)
    with open(f"{LOCAL_METRICS_PATH}metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    with open(f"{LOCAL_METRICS_PATH}profiles.json", "w") as f:
        json.dump(profiles, f, indent=2)
    
    try:
        metrics_df = spark.createDataFrame([metrics])
        metrics_df.coalesce(1).write.mode("overwrite").json(
            f"{METRICS_OUTPUT_PATH}metrics"
        )
        
        profiles_df = spark.createDataFrame(profiles)
        profiles_df.coalesce(1).write.mode("overwrite").json(
            f"{METRICS_OUTPUT_PATH}profiles"
        )
        print("Metrics & profiles saved to S3 (overwritten)")
    except Exception as e:
        print(f"S3 warning: {e}")


def main():
    spark = create_spark()
    
    df = load_data(spark)
    df = prepare_features(df)
    if df is None:
        spark.stop()
        return
    
    df.cache()
    stats = compute_stats(df)
    print(f"Stats: {stats}")
    
    preprocess_pipeline = build_pipeline()
    preprocess_model = preprocess_pipeline.fit(df)
    df_scaled = preprocess_model.transform(df)
    df_scaled.cache()
    
    MIN_CLUSTER_PCT = 0.03  # smallest cluster should be at least ~3% of total
    
    best_k, best_sil, best_model, best_pred = None, -1, None, None
    
    for k in K_RANGE:
        model, predictions, sil, balance = train_and_evaluate(df_scaled, k)
        print(f"k={k}: silhouette={sil:.4f}, balance={balance:.3f}")
        
        # Check minimum cluster size
        counts = predictions.groupBy("prediction").count().collect()
        total = sum(r["count"] for r in counts)
        min_cluster_size = min(r["count"] for r in counts)
        min_pct = min_cluster_size / total
        
        if min_pct < MIN_CLUSTER_PCT:
            print(f"  ⚠️  Warning k={k}: smallest cluster only {min_cluster_size} products ({min_pct:.1%})")
            
        if sil > best_sil:
            best_k, best_sil, best_model, best_pred = k, sil, model, predictions
    
    if best_pred is None:
        print("\n" + "="*60)
        print("ERROR: No valid clustering model found")
        print("All k values were rejected due to small clusters (< 3% of total)")
        print("Suggestions:")
        print("  • Lower MIN_CLUSTER_PCT (e.g. to 0.015 or 0.02)")
        print("  • Increase MIN_PRODUCTS or relax filtering")
        print("  • Try wider K_RANGE including smaller k (k=3)")
        print("  • Investigate data distribution (many similar products?)")
        print("="*60)
        spark.stop()
        return
    
    profiles = profile_clusters(best_pred, stats)
    print(f"\nBest: k={best_k}, silhouette={best_sil:.4f}")
    for p in profiles:
        print(f"  Cluster {p['cluster_id']}: {p['stage']} ({p['count']})")
    
    save_model_and_metrics(spark, preprocess_model, best_model, profiles, stats, best_k, best_sil)
    
    df.unpersist()
    df_scaled.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()