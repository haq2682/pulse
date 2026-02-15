"""
Supplier Performance Clustering - ENHANCED Inference Script
With: Business personas, confidence scores, validation layer
"""

import os
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, log1p, concat_ws, struct, to_json, array, udf, sqrt
)
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel, PCAModel
from pyspark.ml.clustering import KMeansModel
from datetime import datetime

# Environment configuration
BUCKET = "pulse-bucket-1"


# Feature columns (must match training)
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
        SparkSession.builder.appName("SupplierClusteringEnhancedInference")
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


def load_data(spark, INPUT_PATH):
    """Load supplier data"""
    try:
        suppliers_path = f"{INPUT_PATH}agg_suppliers.parquet"
        inv_health_path = f"{INPUT_PATH}agg_supplier_inventory_health.parquet"
        
        suppliers_df = spark.read.parquet(suppliers_path)
        inv_health_df = spark.read.parquet(inv_health_path)

        inv_health_df = inv_health_df.select(
            col("supplier_id"),
            col("breach_rate"),
            col("supplier_inventory_health_score").alias("inv_health_score"),
        )

        df = suppliers_df.join(inv_health_df, on="supplier_id", how="left")
        print(f"Loaded {df.count()} suppliers")

        return df

    except Exception as e:
        print(f"ERROR: Failed to load data: {str(e)}")
        return None


def prepare_features(df):
    """Prepare features matching training"""
    print("Preparing features...")

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

    df = df.filter(col("total_products_supplied") > 0)
    df = df.filter((col("total_revenue_generated") >= 0) & (col("total_revenue_generated") <= 10000000))
    df = df.filter((col("stockout_rate") >= 0) & (col("stockout_rate") <= 100))

    df = df.withColumn("log_total_revenue_generated", log1p(col("total_revenue_generated")))
    df = df.withColumn("log_total_products_supplied", log1p(col("total_products_supplied")))
    df = df.withColumn("log_total_units_sold", log1p(col("total_units_sold")))
    df = df.withColumn("log_total_orders_fulfilled", log1p(col("total_orders_fulfilled")))

    print(f"Prepared {df.count()} suppliers for clustering")
    return df


def load_models_and_profiles(spark, MODEL_PATH):
    """Load models and business profiles from training"""
    try:
        scaler = StandardScalerModel.load(f"{MODEL_PATH}supplier_scaler")
        pca = PCAModel.load(f"{MODEL_PATH}supplier_pca")
        model = KMeansModel.load(f"{MODEL_PATH}supplier_kmeans")
        print("✅ Loaded models")

        # Load enhanced metrics with cluster profiles
        metrics_df = spark.read.json(f"{MODEL_PATH}supplier_enhanced_metrics.json")

        metrics_row = metrics_df.first()
        cluster_profiles = []
        production_readiness = {}
        if metrics_row:
            # Defensive: metrics_row may be Row or dict
            cluster_profiles = metrics_row["cluster_profiles"] if "cluster_profiles" in metrics_row else []
            production_readiness = metrics_row["production_readiness"] if "production_readiness" in metrics_row else {}
        print(f"✅ Loaded {len(cluster_profiles)} cluster profiles")
        print(f"Production Readiness:")
        if isinstance(production_readiness, dict):
            print(f"  Best Silhouette: {production_readiness.get('best_silhouette', 0):.4f}")
            print(f"  Stability Passed: {production_readiness.get('stability_passed', False)}")
        else:
            print("  Best Silhouette: 0.0000")
            print("  Stability Passed: False")
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
    
    # Normalize to 0-1 scale (confidence)
    max_dist_row = predictions_df.agg({"distance_to_center": "max"}).collect()[0]
    max_dist = max_dist_row[0] if max_dist_row[0] else 1.0
    
    predictions_df = predictions_df.withColumn(
        "confidence_score",
        lit(1.0) - (col("distance_to_center") / lit(max_dist))
    )
    
    # Business validation flags
    predictions_df = predictions_df.withColumn(
        "validation_flag",
        when(col("confidence_score") < 0.60, lit("High Risk - Manual Review Required"))
        .when(col("confidence_score") < 0.75, lit("Medium Risk - Monitor Closely"))
        .otherwise(lit("Confident Assignment"))
    )
    
    return predictions_df


def assign_personas_and_tiers(df, cluster_profiles):
    """Assign business personas and tier recommendations"""
    print("Assigning business personas and tiers...")
    
    # Create mapping from cluster_id to persona
    from functools import reduce
    
    persona_expr = reduce(
        lambda acc, profile: acc.when(
            col("prediction") == int(profile["cluster_id"]),
            lit(profile["persona"])
        ),
        cluster_profiles,
        when(lit(False), lit(None)),
    )
    df = df.withColumn("business_persona", persona_expr.otherwise(lit("Unclassified")))
    
    # Assign tiers based on persona
    df = df.withColumn(
        "performance_tier",
        when(col("business_persona").isin(["Strategic Partners", "High-Growth Suppliers"]), lit("Premium"))
        .when(col("business_persona").isin(["Reliable Performers", "Standard Suppliers"]), lit("Standard"))
        .when(col("business_persona").isin(["Risk Suppliers"]), lit("At Risk"))
        .otherwise(lit("Standard"))
    )
    
    # Add action urgency
    df = df.withColumn(
        "action_urgency",
        when(col("business_persona") == "Risk Suppliers", lit("Immediate"))
        .when((col("confidence_score") < 0.70) & (col("performance_tier") == "At Risk"), lit("Urgent"))
        .when(col("performance_tier") == "At Risk", lit("High"))
        .when(col("business_persona") == "Emerging Suppliers", lit("Monitor"))
        .otherwise(lit("Routine"))
    )
    
    return df


def create_enhanced_recommendations(df, cluster_profiles):
    """Create persona-specific recommendations"""
    print("Creating enhanced recommendations...")
    
    df = df.withColumn(
        "improvement_recommendations",
        when(
            col("business_persona") == "Risk Suppliers",
            to_json(array(
                lit("🚨 IMMEDIATE ACTION: Review contract terms"),
                when(col("stockout_rate") > 30, lit("Critical: Stockout rate exceeds 30% - supply chain intervention needed")).otherwise(lit("")),
                when(col("supplier_reliability_score") < 3.0, lit("Critical: Reliability below minimum threshold")).otherwise(lit("")),
                lit("Develop performance improvement plan with milestones"),
                lit("Evaluate alternative suppliers"),
                when(col("validation_flag").contains("Manual"), lit("⚠️  Near cluster boundary - verify classification")).otherwise(lit("")),
            ))
        )
        .when(
            col("business_persona") == "Strategic Partners",
            to_json(array(
                lit("✅ Maintain strategic relationship"),
                lit("Consider long-term partnership agreements"),
                lit("Explore collaborative innovation opportunities"),
                when(col("total_products_supplied") < 15, lit("Opportunity: Expand product portfolio")).otherwise(lit("")),
                lit("Ensure contract terms reflect premium status"),
            ))
        )
        .when(
            col("business_persona") == "High-Growth Suppliers",
            to_json(array(
                lit("📈 High-growth trajectory - increase engagement"),
                lit("Provide strategic support for scaling"),
                when(col("stockout_rate") > 15, lit("Target: Reduce stockout rate to <15% for premium tier")).otherwise(lit("")),
                lit("Consider strategic partnership elevation"),
            ))
        )
        .when(
            col("business_persona") == "Reliable Performers",
            to_json(array(
                lit("Maintain current performance levels"),
                when(col("avg_profit_margin") < 200, lit("Opportunity: Improve margins to 200+")).otherwise(lit("")),
                lit("Monitor for growth opportunities"),
            ))
        )
        .when(
            col("business_persona") == "Long-Tail Suppliers",
            to_json(array(
                lit("Evaluate strategic value vs. management cost"),
                when(col("supplier_rating") >= 4.0, lit("Consider volume increase opportunities")).otherwise(lit("")),
                lit("Optimize contract terms for efficiency"),
            ))
        )
        .when(
            col("business_persona") == "Emerging Suppliers",
            to_json(array(
                lit("Monitor development trajectory"),
                lit("Provide support for growth and improvement"),
                when(col("supplier_rating") < 3.5, lit("Focus: Quality improvement to reach 3.5+ rating")).otherwise(lit("")),
            ))
        )
        .otherwise(
            to_json(array(
                lit("Maintain standard supplier relationship"),
                when(col("validation_flag").contains("Monitor"), lit("Note: Moderate confidence - periodic review recommended")).otherwise(lit("")),
            ))
        )
    )
    
    return df


def create_performance_metrics_json(df, cluster_profiles):
    """Create comprehensive performance metrics"""
    print("Creating performance metrics JSON...")
    
    from functools import reduce
    
    # Build metrics from cluster profiles
    metrics_expr = reduce(
        lambda acc, profile: acc.when(
            col("prediction") == int(profile["cluster_id"]),
            to_json(struct(
                col("supplier_rating").alias("current_rating"),
                col("total_revenue_generated").alias("revenue"),
                col("avg_profit_margin").alias("profit_margin"),
                col("stockout_rate").alias("stockout_rate"),
                col("supplier_reliability_score").alias("reliability"),
                col("confidence_score").alias("assignment_confidence"),
                lit(profile["avg_rating"]).alias("cluster_avg_rating"),
                lit(profile["avg_margin"]).alias("cluster_avg_margin"),
                lit(profile["avg_stockout"]).alias("cluster_avg_stockout"),
                lit(profile["avg_reliability"]).alias("cluster_avg_reliability"),
                lit(profile["persona"]).alias("cluster_persona"),
                col("business_persona").alias("assigned_persona"),
            ))
        ),
        cluster_profiles,
        when(lit(False), lit(None)),
    )
    
    df = df.withColumn("performance_metrics", metrics_expr.otherwise(lit("{}")))
    return df


def generate_predictions(spark, df, model, scaler, pca, cluster_profiles):
    """Generate predictions with full business context"""
    print("Generating predictions...")

    # Save original metrics
    original_cols = [
        "supplier_id", "supplier_rating", "total_revenue_generated",
        "avg_profit_margin", "stockout_rate", "supplier_reliability_score",
        "avg_restock_lead_time", "supplier_performance_score",
        "total_products_supplied",
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
    
    # Join back original values
    predictions = predictions.select(
        "supplier_id", "prediction", "features", "confidence_score", "validation_flag"
    )
    predictions = predictions.join(df_original, on="supplier_id", how="left")

    # Assign personas and tiers
    predictions = assign_personas_and_tiers(predictions, cluster_profiles)

    # Create metrics and recommendations
    predictions = create_performance_metrics_json(predictions, cluster_profiles)
    predictions = create_enhanced_recommendations(predictions, cluster_profiles)

    # Add metadata
    predictions = predictions.withColumn("cluster_date", lit(datetime.now()))
    predictions = predictions.withColumn(
        "clustering_id", concat_ws("_", col("supplier_id"), lit("current"))
    )
    predictions = predictions.withColumn("model_version", lit("enhanced_kmeans"))

    # Select output columns
    output_cols = [
        "clustering_id",
        "supplier_id",
        "cluster_date",
        col("prediction").alias("cluster_id"),
        "business_persona",
        "performance_tier",
        "confidence_score",
        "validation_flag",
        "action_urgency",
        "performance_metrics",
        "improvement_recommendations",
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
    print(f"Total suppliers: {count}")
    
    print("\nBy Business Persona:")
    predictions.groupBy("business_persona").count().orderBy("count", ascending=False).show(truncate=False)
    
    print("By Performance Tier:")
    predictions.groupBy("performance_tier").count().orderBy("performance_tier").show()
    
    print("By Validation Flag:")
    predictions.groupBy("validation_flag").count().orderBy("count", ascending=False).show(truncate=False)
    
    print("By Action Urgency:")
    predictions.groupBy("action_urgency").count().orderBy("count", ascending=False).show()
    
    # Risk analysis
    risk_count = predictions.filter(col("action_urgency").isin(["Immediate", "Urgent"])).count()
    print(f"\n⚠️  High-Priority Actions Required: {risk_count} suppliers ({risk_count/count*100:.1f}%)")
    
    low_confidence = predictions.filter(col("confidence_score") < 0.70).count()
    print(f"⚠️  Low Confidence Assignments: {low_confidence} suppliers ({low_confidence/count*100:.1f}%)")
    
    print(f"{'='*80}\n")


def main(BUCKET):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH = f"s3a://{BUCKET}/transformed/"
    MODEL_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/clustering/models/"
    OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"
    print("="*80)
    print("ENHANCED Supplier Performance Clustering - Inference")
    print("="*80)

    spark = create_spark_session()

    df = load_data(spark, INPUT_PATH)
    if df is None:
        spark.stop()
        return

    df = prepare_features(df)

    model, scaler, pca, cluster_profiles, readiness = load_models_and_profiles(spark, MODEL_PATH)
    if model is None:
        spark.stop()
        return

    predictions = generate_predictions(spark, df, model, scaler, pca, cluster_profiles)

    save_predictions_with_summary(predictions, f"{OUTPUT_PATH}supplier_clustering.parquet")

    print("✅ Enhanced inference completed successfully!")
    spark.stop()


if __name__ == "__main__":
    BUCKET = "pulse-bucket-1"
    main(BUCKET)