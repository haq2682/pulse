"""
Product Affinity Clustering - Inference Script (IMPROVED)
Matches improved training with log transformations and PCA
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, concat_ws, collect_list, struct, to_json,
    sum as _sum, avg, log1p
)
from pyspark.ml.feature import VectorAssembler, StandardScalerModel, StringIndexerModel, PCAModel
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
        SparkSession.builder.appName("ProductAffinityInference")
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
    """Load product and affinity data"""
    try:
        products_path = f"{INPUT_PATH}agg_products.parquet"
        print(f"Loading products from: {products_path}")
        products_df = spark.read.parquet(products_path)

        affinity_path = f"{INPUT_PATH}agg_product_affinity.parquet"
        print(f"Loading affinity from: {affinity_path}")
        affinity_df = spark.read.parquet(affinity_path)

        # Aggregate affinity metrics (same as training)
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
        print(f"Loaded {df.count()} products")

        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features matching training preprocessing"""
    print("Preparing features...")

    # Original features
    original_features = [
        "sell_price", "avg_rating", "total_units_sold", "total_orders",
        "unique_customers", "profit_margin", "total_revenue",
        "avg_affinity_score", "total_co_occurrences", "avg_lift",
        "strong_affinity_count", "cross_category_ratio",
    ]

    for col_name in original_features:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Apply same filters as training
    df = df.filter(col("total_orders") > 0)
    df = df.filter((col("sell_price") >= 0) & (col("sell_price") <= 10000))
    df = df.filter((col("total_revenue") >= 0) & (col("total_revenue") <= 1000000))
    df = df.filter(col("category").isNotNull())

    # Apply log transformations (same as training)
    df = df.withColumn("log_sell_price", log1p(col("sell_price")))
    df = df.withColumn("log_total_units_sold", log1p(col("total_units_sold")))
    df = df.withColumn("log_total_orders", log1p(col("total_orders")))
    df = df.withColumn("log_total_revenue", log1p(col("total_revenue")))
    df = df.withColumn("log_total_co_occurrences", log1p(col("total_co_occurrences")))

    print(f"Prepared {df.count()} products for clustering")
    return df


def load_models(spark):
    """Load all required models"""
    try:
        # Load preprocessing models
        scaler = StandardScalerModel.load(f"{MODEL_PATH}product_affinity_scaler")
        pca = PCAModel.load(f"{MODEL_PATH}product_affinity_pca")
        category_indexer = StringIndexerModel.load(f"{MODEL_PATH}product_affinity_category_indexer")
        print("Loaded preprocessing models")

        # Load metrics
        metrics_df = spark.read.json(f"{MODEL_PATH}product_affinity_metrics.json")
        metrics_row = metrics_df.select("best_models").first()
        best_models = metrics_row["best_models"]

        selected_info = best_models[SELECTED_MODEL_TYPE]
        if selected_info is None:
            print(f"ERROR: {SELECTED_MODEL_TYPE} model not available")
            return None, None, None, None, None

        k = selected_info["k"]
        print(f"Selected: {SELECTED_MODEL_TYPE} with k={k}")

        # Load clustering model
        model_path = f"{MODEL_PATH}product_affinity_{SELECTED_MODEL_TYPE}"
        if SELECTED_MODEL_TYPE == "kmeans":
            model = KMeansModel.load(model_path)
        elif SELECTED_MODEL_TYPE == "gmm":
            model = GaussianMixtureModel.load(model_path)
        elif SELECTED_MODEL_TYPE == "bisecting_kmeans":
            model = BisectingKMeansModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {SELECTED_MODEL_TYPE}")

        return model, scaler, pca, category_indexer, k

    except Exception as e:
        print(f"ERROR: Failed to load models: {str(e)}")
        return None, None, None, None, None


def generate_predictions(spark, df, model, scaler, pca, category_indexer, k):
    """Apply model and generate predictions"""
    print("Generating predictions...")

    # Encode category
    df = category_indexer.transform(df)

    # Assemble features
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df = assembler.transform(df)

    # Scale and apply PCA
    df = scaler.transform(df)
    df = pca.transform(df)

    # Apply clustering model
    predictions = model.transform(df)

    # Compute cluster characteristics
    cluster_stats = (
        predictions.groupBy("prediction")
        .agg(
            avg("sell_price").alias("avg_price"),
            avg("avg_rating").alias("avg_rating"),
            avg("total_units_sold").alias("avg_sales"),
            collect_list("category").alias("categories"),
        )
        .collect()
    )

    # Assign cluster labels
    cluster_labels = {}
    for row in cluster_stats:
        cluster_id = row["prediction"]
        avg_price = row["avg_price"]
        avg_rating = row["avg_rating"]
        avg_sales = row["avg_sales"]

        if avg_price > 500 and avg_rating > 4.0:
            label = "Premium High-Rated"
        elif avg_sales > 500:
            label = "High Volume Sellers"
        elif avg_rating > 4.5:
            label = "Top Rated Products"
        elif avg_price < 50:
            label = "Budget-Friendly"
        else:
            label = f"Product Group {cluster_id}"

        cluster_labels[cluster_id] = label
        print(f"Cluster {cluster_id}: {label} (${avg_price:.2f}, {avg_rating:.2f}★)")

    # Add labels to predictions
    from functools import reduce
    mapping_expr = reduce(
        lambda acc, item: acc.when(col("prediction") == item[0], lit(item[1])),
        cluster_labels.items(),
        when(lit(False), lit(None)),
    )
    predictions = predictions.withColumn("cluster_label", mapping_expr.otherwise(lit("Unknown")))

    # Generate recommendations
    cluster_products = (
        predictions.groupBy("prediction")
        .agg(
            collect_list(
                struct(col("product_id"), col("product_name"), col("category"), col("avg_rating"))
            ).alias("products")
        )
    )

    predictions = predictions.join(cluster_products, on="prediction", how="left")
    predictions = predictions.withColumn("recommended_products", to_json(col("products")))
    predictions = predictions.withColumn("cross_sell_opportunities", col("recommended_products"))

    # Add metadata
    predictions = predictions.withColumn("cluster_date", lit(datetime.now()))
    predictions = predictions.withColumn(
        "clustering_id", concat_ws("_", col("product_id"), lit("current"))
    )
    predictions = predictions.withColumn("model_version", lit(f"{SELECTED_MODEL_TYPE}_k{k}"))
    predictions = predictions.withColumn("cluster_centroid_distance", lit(0.0))

    # Select output columns
    output_cols = [
        "clustering_id",
        "product_id",
        "cluster_date",
        col("prediction").alias("cluster_id"),
        "cluster_label",
        "cluster_centroid_distance",
        "recommended_products",
        "cross_sell_opportunities",
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
    print("Product Affinity Clustering - Inference (IMPROVED)")
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
    model, scaler, pca, category_indexer, k = load_models(spark)
    if model is None:
        spark.stop()
        return

    # Generate predictions
    predictions = generate_predictions(spark, df, model, scaler, pca, category_indexer, k)

    # Save
    save_predictions(predictions, f"{OUTPUT_PATH}product_affinity_clustering.parquet")

    print("\nInference completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()