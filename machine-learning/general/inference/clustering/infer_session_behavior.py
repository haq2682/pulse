"""
Session Behavior Clustering - ENHANCED Inference Script
Generates behavior personas with confidence scores and recommendations
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, log1p, concat_ws, struct, to_json, array, udf
)
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel, PCAModel, StringIndexer
from pyspark.ml.clustering import KMeansModel
from datetime import datetime

# Environment configuration
BUCKET = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET}/transformed/"
MODEL_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"

# Feature columns (must match training)
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
        SparkSession.builder.appName("SessionBehaviorInference")
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


def load_data(spark):
    """Load session data"""
    try:
        sessions_path = f"{INPUT_PATH}agg_customer_sessions.parquet"
        print(f"Loading sessions from: {sessions_path}")
        df = spark.read.parquet(sessions_path)
        print(f"Loaded {df.count()} sessions")
        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features matching training"""
    print("Preparing features...")

    numeric_cols = [
        "session_duration_minutes", "pages_viewed", "products_viewed",
        "items_added_to_cart", "conversion_flag", "cart_abandonment_flag",
        "pages_per_minute", "products_per_page", "cart_add_rate",
        "session_engagement_score", "cart_value"
    ]
    
    for col_name in numeric_cols:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    df = df.withColumn("device_type", coalesce(col("device_type"), lit("Unknown")))
    df = df.withColumn("referrer_source", coalesce(col("referrer_source"), lit("Unknown")))

    df = df.filter(
        (col("pages_viewed") > 0) | (col("session_duration_minutes") >= 0.5)
    )
    df = df.filter(col("session_duration_minutes") <= 1440)

    df = df.withColumn("log_cart_value", log1p(col("cart_value")))

    # Encode categorical features
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

    print(f"Prepared {df.count()} sessions for clustering")
    return df


def load_models_and_profiles(spark):
    """Load models and behavior profiles from training"""
    try:
        scaler = StandardScalerModel.load(f"{MODEL_PATH}session_behavior_scaler")
        pca = PCAModel.load(f"{MODEL_PATH}session_behavior_pca")
        model = KMeansModel.load(f"{MODEL_PATH}session_behavior_kmeans")
        print("✅ Loaded models")

        metrics_df = spark.read.json(f"{MODEL_PATH}session_behavior_metrics.json")
        metrics_row = metrics_df.first()

        if metrics_row:
            # Access as attributes if Row, fallback to dict if needed
            cluster_profiles = getattr(metrics_row, "cluster_profiles", None)
            if cluster_profiles is None and "cluster_profiles" in metrics_row:
                cluster_profiles = metrics_row["cluster_profiles"]
            production_readiness = getattr(metrics_row, "production_readiness", None)
            if production_readiness is None and "production_readiness" in metrics_row:
                production_readiness = metrics_row["production_readiness"]
        else:
            cluster_profiles = []
            production_readiness = {}

        print(f"	Loaded {len(cluster_profiles)} behavior profiles")
        best_silhouette = 0
        if production_readiness:
            best_silhouette = production_readiness["best_silhouette"] if "best_silhouette" in production_readiness else 0
        print(f"Best Silhouette: {best_silhouette:.4f}")

        return model, scaler, pca, cluster_profiles, production_readiness

    except Exception as e:
        print(f"ERROR: Failed to load models: {str(e)}")
        return None, None, None, None, None


def compute_confidence_scores(predictions_df, model):
    """Compute confidence scores based on distance to centroid"""
    print("Computing confidence scores...")
    
    centers = model.clusterCenters()
    
    def distance_to_center(features, prediction):
        if features is None:
            return 0.0
        center = centers[prediction]
        dist = float(sum((features[i] - center[i]) ** 2 for i in range(len(features))) ** 0.5)
        return dist
    
    distance_udf = udf(distance_to_center, DoubleType())
    predictions_df = predictions_df.withColumn(
        "distance_to_center",
        distance_udf(col("features"), col("prediction"))
    )
    
    max_dist_row = predictions_df.agg({"distance_to_center": "max"}).collect()[0]
    max_dist = max_dist_row[0] if max_dist_row[0] else 1.0
    
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        lit(1.0) - (col("distance_to_center") / lit(max_dist))
    )
    
    # Validation flags
    predictions_df = predictions_df.withColumn(
        "validation_flag",
        when(col("confidence_score") < 0.65, lit("Review Pattern"))
        .when(col("confidence_score") < 0.80, lit("Monitor"))
        .otherwise(lit("Confident"))
    )
    
    return predictions_df


def assign_behavior_personas(df, cluster_profiles):
    """Assign behavior personas from training profiles"""
    print("Assigning behavior personas...")
    
    from functools import reduce
    
    persona_expr = reduce(
        lambda acc, profile: acc.when(
            col("prediction") == int(profile["cluster_id"]),
            lit(profile["persona"])
        ),
        cluster_profiles,
        when(lit(False), lit(None)),
    )
    df = df.withColumn("behavior_type", persona_expr.otherwise(lit("Standard Sessions")))
    
    return df


def create_behavior_characteristics_json(df, cluster_profiles):
    """Create behavior characteristics JSON"""
    print("Creating behavior characteristics JSON...")
    
    from functools import reduce
    
    char_expr = reduce(
        lambda acc, profile: acc.when(
            col("prediction") == int(profile["cluster_id"]),
            to_json(struct(
                col("session_duration_minutes").alias("session_duration"),
                col("pages_viewed").alias("pages_viewed"),
                col("products_viewed").alias("products_viewed"),
                col("items_added_to_cart").alias("cart_adds"),
                col("conversion_flag").alias("converted"),
                col("confidence_score").alias("assignment_confidence"),
                lit(profile["avg_duration"]).alias("cluster_avg_duration"),
                lit(profile["avg_pages"]).alias("cluster_avg_pages"),
                lit(profile["conversion_rate"]).alias("cluster_conversion_rate"),
                lit(profile["persona"]).alias("behavior_persona"),
            ))
        ),
        cluster_profiles,
        when(lit(False), lit(None)),
    )
    
    df = df.withColumn("behavior_characteristics", char_expr.otherwise(lit("{}")))
    return df


def create_engagement_recommendations(df):
    """Create persona-specific engagement recommendations"""
    print("Creating engagement recommendations...")
    
    df = df.withColumn(
        "engagement_recommendations",
        when(
            col("behavior_type") == "Quick Buyers",
            to_json(array(
                lit("✅ Ideal customer behavior - maintain streamlined experience"),
                lit("Consider loyalty rewards for repeat quick purchases"),
                lit("Optimize checkout speed and mobile experience"),
                when(col("device_type") == "Mobile", lit("Excellent mobile conversion - continue mobile optimization")).otherwise(lit("")),
            ))
        )
        .when(
            col("behavior_type") == "Cart Abandoners",
            to_json(array(
                lit("🚨 HIGH PRIORITY: Implement cart recovery campaign"),
                lit("Send abandoned cart email within 1 hour"),
                when(col("cart_value") > 100, lit("High-value cart - offer incentive or free shipping")).otherwise(lit("")),
                lit("Review checkout friction points"),
                lit("Consider exit-intent popups with discount"),
                when(col("validation_flag") == "Review Pattern", lit("⚠️  Unusual abandonment pattern - investigate")).otherwise(lit("")),
            ))
        )
        .when(
            col("behavior_type") == "Researchers",
            to_json(array(
                lit("📚 High research intent - nurture with content"),
                lit("Send product comparison guides"),
                lit("Retarget with specific products viewed"),
                lit("Offer live chat support for questions"),
                when(col("products_viewed") > 15, lit("Very high interest - consider phone follow-up")).otherwise(lit("")),
            ))
        )
        .when(
            col("behavior_type") == "Engaged Shoppers",
            to_json(array(
                lit("Positive engagement signals"),
                when(col("items_added_to_cart") > 0, lit("Items in cart - send reminder if no conversion within 24h")).otherwise(lit("")),
                lit("Retarget with viewed products"),
                lit("Offer personalized recommendations"),
            ))
        )
        .when(
            col("behavior_type") == "Casual Browsers",
            to_json(array(
                lit("Low purchase intent - brand awareness opportunity"),
                lit("Retarget with broad category ads"),
                lit("Build email list for nurture campaigns"),
                when(col("session_duration_minutes") < 2, lit("Very brief visit - improve landing page relevance")).otherwise(lit("")),
            ))
        )
        .when(
            col("behavior_type") == "Window Shoppers",
            to_json(array(
                lit("Product interest without depth - reduce friction"),
                lit("Simplify product pages and CTAs"),
                lit("Highlight key benefits and social proof"),
                lit("Offer bundle deals or limited-time discounts"),
            ))
        )
        .when(
            col("behavior_type") == "Successful Converters",
            to_json(array(
                lit("✅ Successful conversion!"),
                lit("Send post-purchase follow-up"),
                lit("Request product review"),
                lit("Offer cross-sell/upsell opportunities"),
                lit("Build loyalty with rewards program"),
            ))
        )
        .otherwise(
            to_json(array(
                lit("Standard session pattern"),
                when(col("validation_flag") == "Review Pattern", lit("⚠️  Unusual behavior - may need investigation")).otherwise(lit("")),
            ))
        )
    )
    
    return df


def generate_predictions(spark, df, model, scaler, pca, cluster_profiles):
    """Generate predictions with full business context"""
    print("Generating predictions...")

    # Save original metrics
    original_cols = [
        "session_id", "customer_id", "session_duration_minutes",
        "pages_viewed", "products_viewed", "items_added_to_cart",
        "conversion_flag", "cart_abandonment_flag", "cart_value",
        "device_type", "referrer_source",
    ]
    df_original = df.select(original_cols)

    # Transform for clustering
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="features_raw")
    df_transformed = assembler.transform(df)
    df_transformed = scaler.transform(df_transformed)
    df_transformed = pca.transform(df_transformed)

    # Apply clustering
    predictions = model.transform(df_transformed)
    
    # Compute confidence scores
    predictions = compute_confidence_scores(predictions, model)

    # Join back original values, keep distance_to_center
    predictions = predictions.select(
        "session_id", "prediction", "features", "confidence_score", "validation_flag", "distance_to_center"
    )
    predictions = predictions.join(df_original, on="session_id", how="left")

    # Assign personas
    predictions = assign_behavior_personas(predictions, cluster_profiles)

    # Create characteristics and recommendations
    predictions = create_behavior_characteristics_json(predictions, cluster_profiles)
    predictions = create_engagement_recommendations(predictions)

    # Add metadata
    predictions = predictions.withColumn("cluster_date", lit(datetime.now()))
    predictions = predictions.withColumn(
        "clustering_id", concat_ws("_", col("session_id"), lit("current"))
    )
    predictions = predictions.withColumn("model_version", lit("enhanced_kmeans"))
    predictions = predictions.withColumn("cluster_centroid_distance", col("distance_to_center"))

    # Select output columns
    output_cols = [
        "clustering_id",
        "session_id",
        "cluster_date",
        col("prediction").alias("cluster_id"),
        "behavior_type",
        "cluster_centroid_distance",
        "confidence_score",
        "validation_flag",
        "behavior_characteristics",
        "engagement_recommendations",
        "model_version",
    ]

    return predictions.select(output_cols)


def save_predictions_with_summary(predictions, output_path):
    """Save predictions and print summary"""
    print(f"Saving predictions to: {output_path}")
    predictions.write.mode("overwrite").parquet(output_path)
    
    count = predictions.count()
    print(f"\n{'='*80}")
    print(f"PREDICTION SUMMARY")
    print(f"{'='*80}")
    print(f"Total sessions: {count}")
    
    print("\nBy Behavior Type:")
    predictions.groupBy("behavior_type").count().orderBy("count", ascending=False).show(truncate=False)
    
    print("By Validation Flag:")
    predictions.groupBy("validation_flag").count().orderBy("count", ascending=False).show()
    
    # Key metrics
    cart_abandoners = predictions.filter(col("behavior_type") == "Cart Abandoners").count()
    converters = predictions.filter(col("behavior_type").contains("Converter")).count()
    low_confidence = predictions.filter(col("confidence_score") < 0.70).count()
    
    print(f"\n📊 Key Metrics:")
    print(f"  Cart Abandoners (recovery opportunity): {cart_abandoners} ({cart_abandoners/count*100:.1f}%)")
    print(f"  Successful Converters: {converters} ({converters/count*100:.1f}%)")
    print(f"  Low Confidence Assignments: {low_confidence} ({low_confidence/count*100:.1f}%)")
    
    print(f"{'='*80}\n")


def main():
    print("="*80)
    print("ENHANCED Session Behavior Clustering - Inference")
    print("="*80)

    spark = create_spark_session()

    df = load_data(spark)
    if df is None:
        spark.stop()
        return

    df = prepare_features(df)

    model, scaler, pca, cluster_profiles, readiness = load_models_and_profiles(spark)
    if model is None:
        spark.stop()
        return

    predictions = generate_predictions(spark, df, model, scaler, pca, cluster_profiles)

    save_predictions_with_summary(predictions, f"{OUTPUT_PATH}session_behavior_clustering.parquet")

    print("✅ Enhanced inference completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()