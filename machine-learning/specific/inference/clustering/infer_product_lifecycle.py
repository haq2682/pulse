"""Product Lifecycle Clustering - Inference"""

import os
import sys

import json
from datetime import datetime
from pathlib import Path
# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session
from general.utils.plot_exporter import export_inference_outputs_plot
from specific.model_registry import resolve_best_model


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, log1p, greatest, concat_ws, to_json, struct, udf, expr
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import DenseVector, SparseVector


FEATURES = ["log_age", "log_sales_velocity", "log_revenue_velocity", "log_turnover"]
MODEL_CANDIDATES = ["product_lifecycle_pipeline"]


def create_spark():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "ProductLifecycleInfer",
        extra_configs={
            "spark.sql.shuffle.partitions": "8",
        },
    )


def load_data(spark, INPUT_PATH):
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
    
    df = df.withColumn("log_age", log1p(col("days_since_launch")))
    df = df.withColumn("effective_age", greatest(col("days_since_launch"), lit(30.0)))
    df = df.withColumn("sales_velocity", col("total_units_sold") / col("effective_age"))
    df = df.withColumn("revenue_velocity", col("total_revenue") / col("effective_age"))
    df = df.withColumn("log_sales_velocity", log1p(col("sales_velocity")))
    df = df.withColumn("log_revenue_velocity", log1p(col("revenue_velocity")))
    df = df.withColumn("log_turnover", log1p(col("inventory_turnover_rate")))
    
    print(f"Prepared {df.count()} products")
    return df


def load_model_and_profiles(MODEL_PATH, LOCAL_METRICS_PATH, pipeline_model_name="product_lifecycle_pipeline"):
    pipeline = PipelineModel.load(f"{MODEL_PATH}{pipeline_model_name}")
    
    profiles, stats = [], {}
    try:
        with open(f"{LOCAL_METRICS_PATH}profiles.json", "r") as f:
            profiles = json.load(f)
        with open(f"{LOCAL_METRICS_PATH}metrics.json", "r") as f:
            stats = json.load(f).get("stats", {})
    except:
        print("Could not load local metrics")
    
    return pipeline, profiles, stats


def assign_lifecycle(df, stats, profiles):
    if profiles:
        stage_map = {p["cluster_id"]: p["stage"] for p in profiles}
        
        def get_stage(cluster_id):
            return stage_map.get(int(cluster_id), "Maturity") if cluster_id is not None else "Maturity"
        
        stage_udf = udf(get_stage, StringType())
        df = df.withColumn("lifecycle_stage", stage_udf(col("prediction")))
    else:
        age_p25 = stats.get("age_p25", 365)
        age_p50 = stats.get("age_p50", 730)
        vel_p25 = stats.get("vel_p25", 0.01)
        vel_p75 = stats.get("vel_p75", 0.1)
        turn_p50 = stats.get("turnover_p50", 1.0)
        
        df = df.withColumn("lifecycle_stage",
            when((col("days_since_launch") < age_p25) & (col("sales_velocity") >= vel_p75), lit("Growth"))
            .when(col("days_since_launch") < age_p25, lit("Introduction"))
            .when((col("sales_velocity") >= vel_p75) & (col("inventory_turnover_rate") >= turn_p50), lit("Growth"))
            .when((col("sales_velocity") < vel_p25) & (col("days_since_launch") > age_p50), lit("Decline"))
            .otherwise(lit("Maturity"))
        )
    
    return df


def compute_confidence(df, pipeline):
    kmeans_model = pipeline.stages[-1]
    centers = [c.tolist() for c in kmeans_model.clusterCenters()]
    
    def calc_dist(features, pred):
        if features is None:
            return 999.0
        try:
            arr = features.toArray() if hasattr(features, 'toArray') else list(features)
            center = centers[int(pred)]
            return float(sum((arr[i] - center[i])**2 for i in range(len(arr)))**0.5)
        except:
            return 999.0
    
    dist_udf = udf(calc_dist, DoubleType())
    df = df.withColumn("distance", dist_udf(col("features_scaled"), col("prediction")))
    
    dist_stats = df.agg(expr("min(distance)").alias("min_d"), expr("max(distance)").alias("max_d")).collect()[0]
    min_d, max_d = float(dist_stats["min_d"] or 0), float(dist_stats["max_d"] or 1)
    range_d = max(max_d - min_d, 0.001)
    
    df = df.withColumn("confidence_score", 
        when(lit(1.0) - ((col("distance") - lit(min_d)) / lit(range_d)) > 1.0, lit(1.0))
        .when(lit(1.0) - ((col("distance") - lit(min_d)) / lit(range_d)) < 0.0, lit(0.0))
        .otherwise(lit(1.0) - ((col("distance") - lit(min_d)) / lit(range_d)))
    )
    
    return df


def add_recommendations(df):
    df = df.withColumn("strategic_recommendations",
        when(col("lifecycle_stage") == "Introduction",
            to_json(struct(lit("Build awareness").alias("primary"), lit("Invest in marketing").alias("secondary"))))
        .when(col("lifecycle_stage") == "Growth",
            to_json(struct(lit("Scale operations").alias("primary"), lit("Increase inventory").alias("secondary"))))
        .when(col("lifecycle_stage") == "Maturity",
            to_json(struct(lit("Optimize profitability").alias("primary"), lit("Reduce costs").alias("secondary"))))
        .when(col("lifecycle_stage") == "Decline",
            to_json(struct(lit("Consider discontinuation").alias("primary"), lit("Minimize costs").alias("secondary"))))
        .otherwise(to_json(struct(lit("Monitor trends").alias("primary"), lit("Analyze data").alias("secondary"))))
    )
    return df


def create_output(df):
    df = df.withColumn("clustering_id", concat_ws("_", col("product_id"), lit("v1")))
    df = df.withColumn("cluster_date", lit(datetime.now()))
    df = df.withColumn("model_version", lit("v1"))
    
    df = df.withColumn("stage_characteristics",
        to_json(struct(
            col("days_since_launch").alias("age_days"),
            col("sales_velocity"),
            col("revenue_velocity"),
            col("inventory_turnover_rate").alias("turnover"),
            col("confidence_score")
        ))
    )
    
    return df.select(
        "clustering_id", "product_id", "cluster_date",
        col("prediction").alias("cluster_id"),
        "lifecycle_stage",
        col("distance").alias("cluster_centroid_distance"),
        "stage_characteristics", "strategic_recommendations", "model_version"
    )


def save_and_summarize(df, output_path):
    df.write.mode("overwrite").parquet(output_path)
    
    total = df.count()
    print(f"\nTotal: {total}")
    print("\nLifecycle Distribution:")
    df.groupBy("lifecycle_stage").count().orderBy("count", ascending=False).show()
    print("\nCluster Distribution:")
    df.groupBy("cluster_id").count().orderBy("cluster_id").show()


def main(BUCKET, EXPORT_PLOTS=False):
    INPUT_PATH = f"s3a://{BUCKET}/transformed/"
    MODEL_PATH = f"s3a://{BUCKET}/machine-learning/clustering/models/"
    METRICS_PATH = f"s3a://{BUCKET}/machine-learning/clustering/metrics/"
    OUTPUT_PATH = f"s3a://{BUCKET}/machine-learning/clustering/predictions/"
    LOCAL_METRICS_PATH = "/tmp/clustering_metrics/"
    preferred_model = os.getenv("PRODUCT_LIFECYCLE_MODEL", "product_lifecycle_pipeline")
    spark = create_spark()

    selected_model, model_source, _ = resolve_best_model(
        spark,
        MODEL_PATH,
        MODEL_CANDIDATES,
        preferred_model=preferred_model,
    )
    print(f"Selected model: {selected_model} (source: {model_source})")
    
    df = load_data(spark, INPUT_PATH)
    df = prepare_features(df)
    if df is None:
        spark.stop()
        return
    
    pipeline, profiles, stats = load_model_and_profiles(
        MODEL_PATH,
        LOCAL_METRICS_PATH,
        pipeline_model_name=selected_model,
    )
    if pipeline is None:
        print("Failed to load model")
        spark.stop()
        return
    
    predictions = pipeline.transform(df)
    predictions = assign_lifecycle(predictions, stats, profiles)
    predictions = compute_confidence(predictions, pipeline)
    predictions = add_recommendations(predictions)

    export_inference_outputs_plot(
        model_name="product_lifecycle_pipeline",
        predictions_df=predictions,
        label_column="lifecycle_stage",
        numeric_columns=["confidence_score"],
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
        run_name="product_lifecycle_pipeline",
    )
    
    output = create_output(predictions)
    save_and_summarize(output, f"{OUTPUT_PATH}product_lifecycle_clustering.parquet")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET = "pulse-bucket-1"
    main(BUCKET, EXPORT_PLOTS=False)
