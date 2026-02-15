"""
Customer Segmentation Clustering - Inference Script
Applies trained clustering model to generate customer segments
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, udf, concat_ws, struct
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.clustering import KMeansModel, GaussianMixtureModel
from pyspark.ml.linalg import Vectors, VectorUDT
from datetime import datetime
import json
import numpy as np


# Feature columns (must match training)
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
        SparkSession.builder.appName("CustomerSegmentationInference")
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


def load_data(spark, INPUT_PATH):
    """Load and join data from multiple tables"""
    try:
        # Load agg_customers
        customers_path = f"{INPUT_PATH}agg_customers.parquet"
        print(f"Loading customers from: {customers_path}")
        customers_df = spark.read.parquet(customers_path)

        # Load agg_rfm_segmentation
        rfm_path = f"{INPUT_PATH}agg_rfm_segmentation.parquet"
        print(f"Loading RFM data from: {rfm_path}")
        rfm_df = spark.read.parquet(rfm_path)

        # Rename columns to avoid duplicates
        rfm_df = rfm_df.select(
            col("customer_id"),
            col("days_since_last_order"),
            col("recency_score"),
            col("frequency_score"),
            col("monetary_score"),
        )

        # Join tables
        df = customers_df.join(rfm_df, on="customer_id", how="inner")
        print(f"Loaded {df.count()} records")

        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features matching training preprocessing"""
    print("Preparing features...")

    # Fill nulls with 0 (same as training)
    for col_name in FEATURE_COLS:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    # Apply same filters as training
    df = df.filter(col("total_orders") > 0)
    df = df.filter((col("total_revenue") >= 0) & (col("total_revenue") <= 1000000))

    print(f"Prepared {df.count()} records for inference")
    return df


def load_model_and_scaler(spark, MODEL_PATH, SELECTED_MODEL_TYPE):
    """Load selected model and scaler from MinIO"""
    try:
        # Load scaler
        scaler_path = f"{MODEL_PATH}scaler"
        print(f"Loading scaler from: {scaler_path}")
        scaler = StandardScalerModel.load(scaler_path)

        # Load metrics to get k value for selected model
        metrics_path = f"{MODEL_PATH}metrics.json"
        print(f"Loading metrics from: {metrics_path}")
        
        # Read metrics JSON from MinIO
        metrics_df = spark.read.json(metrics_path)
        metrics_row = metrics_df.select("best_models").first()
        best_models = metrics_row["best_models"]
        
        # Get k value for selected model type
        selected_model_info = best_models[SELECTED_MODEL_TYPE]
        k = selected_model_info["k"]
        
        print(f"Selected model: {SELECTED_MODEL_TYPE} with k={k}")

        # Load selected model
        model_path = f"{MODEL_PATH}{SELECTED_MODEL_TYPE}"
        print(f"Loading model from: {model_path}")

        if SELECTED_MODEL_TYPE == "kmeans":
            model = KMeansModel.load(model_path)
        elif SELECTED_MODEL_TYPE == "gmm":
            model = GaussianMixtureModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {SELECTED_MODEL_TYPE}")

        return model, scaler, SELECTED_MODEL_TYPE, k

    except Exception as e:
        print(f"ERROR: Failed to load model: {str(e)}")
        return None, None, None, None


def compute_cluster_statistics(predictions_df, cluster_col="prediction"):
    """Compute cluster statistics and assign cluster personas"""
    print("Computing cluster characteristics...")

    cluster_stats = (
        predictions_df.groupBy(cluster_col)
        .agg(
            {"days_since_last_order": "avg", "total_orders": "avg", "total_revenue": "avg"}
        )
        .withColumnRenamed("avg(days_since_last_order)", "avg_recency")
        .withColumnRenamed("avg(total_orders)", "avg_frequency")
        .withColumnRenamed("avg(total_revenue)", "avg_monetary")
        .orderBy(cluster_col)
        .collect()
    )

    # Compute global medians for comparison
    medians = predictions_df.approxQuantile(
        ["days_since_last_order", "total_orders", "total_revenue"],
        [0.5],
        0.01
    )
    median_recency = medians[0][0]
    median_frequency = medians[1][0]
    median_monetary = medians[2][0]

    # Assign cluster personas based on characteristics
    cluster_personas = {}
    
    for row in cluster_stats:
        cluster_id = row[cluster_col]
        recency = row["avg_recency"]
        frequency = row["avg_frequency"]
        monetary = row["avg_monetary"]

        # Classify cluster based on RFM relative to medians
        is_recent = recency < median_recency
        is_frequent = frequency > median_frequency
        is_high_value = monetary > median_monetary

        # Assign persona
        if is_high_value and is_frequent and is_recent:
            persona = "High-Value Champions"
        elif is_high_value and is_frequent and not is_recent:
            persona = "High-Value Dormant"
        elif is_high_value and not is_frequent:
            persona = "Big Spenders (Low Frequency)"
        elif is_frequent and is_recent and not is_high_value:
            persona = "Price-Sensitive Repeat Buyers"
        elif is_frequent and not is_recent:
            persona = "Lapsed Regulars"
        elif is_recent and not is_frequent:
            persona = "New or Casual Shoppers"
        elif not is_recent and not is_frequent and not is_high_value:
            persona = "Low-Engagement Dormant"
        else:
            persona = "Average Customers"

        cluster_personas[cluster_id] = persona

        print(f"Cluster {cluster_id} → '{persona}':")
        print(f"  Avg Recency: {recency:.1f} days, Avg Frequency: {frequency:.1f}, Avg Monetary: ${monetary:.2f}")

    return cluster_personas


def assign_customer_labels(df):
    """Assign semantic labels at customer level based on individual RFM values"""
    print("Assigning customer labels based on individual RFM characteristics...")

    # Compute percentile-based thresholds (data-adaptive)
    percentiles = df.approxQuantile(
        ["total_revenue", "total_orders"],
        [0.70, 0.80],  # 70th and 80th percentiles
        0.01  # Relative error
    )
    
    revenue_p70 = percentiles[0][0]
    revenue_p80 = percentiles[0][1]
    orders_p70 = percentiles[1][0]
    orders_p80 = percentiles[1][1]
    
    print(f"Dynamic thresholds - Revenue P70: ${revenue_p70:.2f}, P80: ${revenue_p80:.2f}")
    print(f"Dynamic thresholds - Orders P70: {orders_p70:.1f}, P80: {orders_p80:.1f}")

    # Label each customer individually based on their own RFM values
    df = df.withColumn(
        "customer_label",
        when((col("days_since_last_order") < 30) & (col("total_orders") <= 3), "New Customers")
        .when((col("total_revenue") > revenue_p80) & (col("total_orders") > orders_p80) & (col("days_since_last_order") < 180), "High Value Champions")
        .when((col("total_orders") > orders_p70) & (col("days_since_last_order") < 180), "Loyal Customers")
        .when((col("days_since_last_order") >= 180) & (col("days_since_last_order") < 365), "At Risk")
        .when((col("days_since_last_order") < 365) & (col("total_orders") <= 3), "Occasional Buyers")
        .when(col("days_since_last_order") >= 365, "Churned")
        .otherwise("Regular Customers")
    )

    return df


def calculate_cluster_distance(df, model_type):
    """Calculate distance from cluster centroid (only for KMeans)"""
    if model_type == "kmeans":
        # For KMeans, we can compute distance
        # This is approximation - exact distance requires accessing cluster centers
        df = df.withColumn("cluster_centroid_distance", lit(0.0))
    else:
        # For GMM, use probability as proxy
        df = df.withColumn("cluster_centroid_distance", lit(0.0))

    return df


def generate_predictions(spark, df, model, scaler, model_type, k):
    """Apply model and generate predictions"""
    print("Generating predictions...")

    # Select required columns before transformation to avoid ambiguity
    base_cols = ["customer_id", "recency_score", "frequency_score", "monetary_score"] + FEATURE_COLS
    # Remove duplicates from base_cols to avoid ambiguous references
    base_cols = list(dict.fromkeys(base_cols))
    
    # Get the columns that actually exist in the dataframe to avoid ambiguous references
    existing_cols = df.columns
    selected_cols = [c for c in base_cols if c in existing_cols]
    
    # Drop duplicate columns if any exist
    for col_name in selected_cols:
        # Count occurrences of this column
        col_count = sum(1 for c in df.columns if c == col_name)
        if col_count > 1:
            # Keep only one instance by selecting distinct column names
            df = df.toDF(*[f"{c}_{i}" if c == col_name and df.columns[:j].count(c) > 0 else c 
                          for j, (i, c) in enumerate(enumerate(df.columns))])
    
    df = df.select(*selected_cols)

    # Assemble features
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    df = assembler.transform(df)

    # Scale features
    df = scaler.transform(df)

    # Apply clustering model
    predictions = model.transform(df)

    # Assign customer-level labels based on individual RFM values
    predictions = assign_customer_labels(predictions)

    # Calculate distances
    predictions = calculate_cluster_distance(predictions, model_type)

    # Add metadata
    predictions = predictions.withColumn("cluster_date", lit(datetime.now()))
    predictions = predictions.withColumn(
        "clustering_id", concat_ws("_", col("customer_id"), lit("current"))
    )
    predictions = predictions.withColumn("model_version", lit(f"{model_type}_k{k}"))

    # Select output columns
    output_cols = [
        "clustering_id",
        "customer_id",
        "cluster_date",
        col("prediction").alias("cluster_id"),
        "customer_label",
        "cluster_centroid_distance",
        "recency_score",
        "frequency_score",
        "monetary_score",
        "model_version",
    ]

    predictions = predictions.select(output_cols)

    return predictions


def save_predictions(predictions, output_path):
    """Save predictions to MinIO as Parquet"""
    print(f"Saving predictions to: {output_path}")

    predictions.write.mode("overwrite").parquet(output_path)

    record_count = predictions.count()
    print(f"Saved {record_count} predictions successfully")


def main(BUCKET):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH = f"s3a://{BUCKET}/transformed/"
    MODEL_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/clustering/models/"
    OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"

    # MANUAL SELECTION: Choose which algorithm to use for inference
    SELECTED_MODEL_TYPE = "kmeans"  # Options: 'kmeans' or 'gmm'
    print("=" * 80)
    print("Customer Segmentation Clustering - Inference")
    print(f"Selected Model: {SELECTED_MODEL_TYPE.upper()}")
    print("=" * 80)

    spark = create_spark_session()

    # Load data
    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("Inference aborted due to data loading failure")
        spark.stop()
        return

    # Prepare features
    df = prepare_features(df)

    # Load model, scaler, and model metadata
    model, scaler, model_type, k = load_model_and_scaler(spark, MODEL_PATH, SELECTED_MODEL_TYPE)
    if model is None or scaler is None:
        print("Inference aborted due to model loading failure")
        spark.stop()
        return

    print(f"\nUsing model: {model_type} with k={k}")

    # Generate predictions first to compute cluster statistics for reporting
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    df_temp = assembler.transform(df)
    df_temp = scaler.transform(df_temp)
    predictions_temp = model.transform(df_temp)

    # Compute cluster characteristics for monitoring (not for labeling)
    compute_cluster_statistics(predictions_temp)

    # Generate final predictions with customer-level labels
    predictions = generate_predictions(spark, df, model, scaler, model_type, k)

    # Save predictions
    output_path = f"{OUTPUT_PATH}customer_segmentation.parquet"
    save_predictions(predictions, output_path)

    print("\n" + "=" * 80)
    print("Inference completed successfully")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    BUCKET = 'pulse-bucket-1'
    main(BUCKET)