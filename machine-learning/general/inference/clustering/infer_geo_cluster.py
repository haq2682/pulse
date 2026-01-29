"""
Geographic Sales Clustering - Inference Script
Generates geographic market segments and performance analysis
"""

import os
import findspark

findspark.init()

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

# Environment configuration
BUCKET = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET}/transformed/"
MODEL_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"

# MANUAL SELECTION
SELECTED_MODEL_TYPE = "kmeans"  # Options: 'kmeans', 'gmm', 'bisecting_kmeans'

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


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder.appName("GeographicClusteringInference")
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


def load_data(spark):
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


def load_models(spark):
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


def generate_predictions(spark, df, model, scaler, pca, k):
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


def main():
    print("=" * 80)
    print("Geographic Sales Clustering - Inference")
    print(f"Model: {SELECTED_MODEL_TYPE.upper()}")
    print("=" * 80)

    spark = create_spark_session()

    # Load data
    df = load_data(spark)
    if df is None:
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)

    # Load models
    model, scaler, pca, k = load_models(spark)
    if model is None:
        spark.stop()
        return

    # Generate predictions
    predictions = generate_predictions(spark, df, model, scaler, pca, k)

    # Save
    save_predictions(predictions, f"{OUTPUT_PATH}geographic_clustering.parquet")

    print("\nInference completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()