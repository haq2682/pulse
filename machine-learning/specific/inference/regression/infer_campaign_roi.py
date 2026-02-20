"""
Campaign ROI Prediction - Inference Script
Predicts expected revenue for campaigns, then computes ROI from the prediction.

Architecture:
- Loads ONE trained model: campaign_revenue (RandomForest)
- Loads 3 fitted StringIndexerModels from training (identical category mappings)
- No scaler — RF doesn't need it
- Predicts revenue
- Computes ROI externally: (predicted_revenue - spent) / spent * 100
"""

import os
import json
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel
from pyspark.ml.regression import RandomForestRegressionModel
from datetime import datetime
import uuid

# Load environment variables
load_dotenv()

# Feature set — must match training (no leakage features, no scaler)
NUMERIC_FEATURES = [
    "budget", "spent_amount", "budget_utilization", "remaining_budget",
    "impressions", "clicks", "conversions",
    "click_through_rate", "conversion_rate",
    "cost_per_click", "cost_per_conversion", "cost_per_impression",
    "days_active", "avg_daily_spend", "avg_daily_impressions",
    "avg_daily_clicks", "avg_daily_conversions",
    "engagement_efficiency", "reach_efficiency",
    "campaign_type_avg_roi", "campaign_type_avg_conversion_rate",
    "campaign_type_avg_ctr",
    "campaign_type_idx", "target_audience_idx", "campaign_status_idx",
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Campaign_ROI_Inference")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.2.6,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
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


def load_model_artifacts(model_base_path):
    """
    Load all artifacts saved during training:
    - RandomForest model
    - 3 fitted StringIndexerModels (category → index mappings from training)
    """
    try:
        revenue_model = RandomForestRegressionModel.load(
            f"{model_base_path}campaign_revenue"
        )
        ct_indexer = StringIndexerModel.load(
            f"{model_base_path}indexer_campaign_type"
        )
        aud_indexer = StringIndexerModel.load(
            f"{model_base_path}indexer_target_audience"
        )
        status_indexer = StringIndexerModel.load(
            f"{model_base_path}indexer_campaign_status"
        )
        print("✓ Revenue model loaded")
        print("✓ Campaign type indexer loaded")
        print("✓ Target audience indexer loaded")
        print("✓ Campaign status indexer loaded")
        return revenue_model, ct_indexer, aud_indexer, status_indexer
    except Exception as e:
        print(f"✗ Failed to load model artifacts: {str(e)}")
        return None, None, None, None


def validate_dataset(spark, path, name):
    """Check if dataset exists"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ {name} dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ {name} dataset validation failed: {str(e)}")
        return None, 0


def calculate_historical_campaign_performance(campaigns_df):
    """Calculate historical performance by campaign type"""
    print("Calculating historical campaign performance...")

    completed = campaigns_df.filter(
        (F.col("campaign_status") == "Completed")
        & F.col("revenue_generated").isNotNull()
        & (F.col("revenue_generated") > 0)
    )

    with_roi = completed.withColumn(
        "historical_roi",
        F.when(
            F.col("spent_amount") > 0,
            ((F.col("revenue_generated") - F.col("spent_amount")) / F.col("spent_amount")) * 100,
        ).otherwise(0),
    )

    stats = with_roi.groupBy("campaign_type").agg(
        F.avg("historical_roi").alias("campaign_type_avg_roi"),
        F.avg("conversion_rate").alias("campaign_type_avg_conversion_rate"),
        F.avg("click_through_rate").alias("campaign_type_avg_ctr"),
    )

    stats = stats.fillna({
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0,
    })

    print("✓ Historical performance calculated")
    return stats


def create_inference_features(campaigns_df, historical_stats_df):
    """Create same features as training (no leakage features)."""
    print("Creating inference features...")

    features = campaigns_df

    # Budget utilization
    features = (
        features
        .withColumn(
            "budget_utilization",
            F.when(F.col("budget") > 0, F.col("spent_amount") / F.col("budget")).otherwise(1.0),
        )
        .withColumn(
            "remaining_budget",
            F.greatest(F.lit(0), F.col("budget") - F.col("spent_amount")),
        )
    )

    # Cost metrics
    features = (
        features
        .withColumn(
            "cost_per_click",
            F.when(F.col("clicks") > 0, F.col("spent_amount") / F.col("clicks")).otherwise(0),
        )
        .withColumn(
            "cost_per_conversion",
            F.when(F.col("conversions") > 0, F.col("spent_amount") / F.col("conversions")).otherwise(0),
        )
        .withColumn(
            "cost_per_impression",
            F.when(F.col("impressions") > 0, F.col("spent_amount") / F.col("impressions")).otherwise(0),
        )
    )

    # Daily averages
    features = (
        features
        .withColumn(
            "avg_daily_spend",
            F.when(F.col("days_active") > 0, F.col("spent_amount") / F.col("days_active")).otherwise(0),
        )
        .withColumn(
            "avg_daily_impressions",
            F.when(F.col("days_active") > 0, F.col("impressions") / F.col("days_active")).otherwise(0),
        )
        .withColumn(
            "avg_daily_clicks",
            F.when(F.col("days_active") > 0, F.col("clicks") / F.col("days_active")).otherwise(0),
        )
        .withColumn(
            "avg_daily_conversions",
            F.when(F.col("days_active") > 0, F.col("conversions") / F.col("days_active")).otherwise(0),
        )
    )

    # Efficiency metrics (non-leaky)
    features = (
        features
        .withColumn(
            "engagement_efficiency",
            F.when(F.col("clicks") > 0, F.col("conversions") / F.col("clicks")).otherwise(0),
        )
        .withColumn(
            "reach_efficiency",
            F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(0),
        )
    )

    # Join historical stats
    features = features.join(historical_stats_df, "campaign_type", "left")

    # Fill nulls
    features = features.fillna({
        "budget": 0,
        "spent_amount": 0,
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "click_through_rate": 0,
        "conversion_rate": 0,
        "days_active": 1,
        "target_audience": "Unknown",
        "campaign_status": "Unknown",
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0,
    })

    feature_count = features.count()
    print(f"✓ Inference features created: {feature_count} campaigns")
    return features


def prepare_inference_data(df, ct_indexer, aud_indexer, status_indexer):
    """
    Apply the SAVED indexers from training, then assemble features.
    No scaler — RF doesn't need it, and this avoids train/inference skew.
    """
    # Apply saved indexer models (identical category→index mapping as training)
    df_idx = ct_indexer.transform(df)
    df_idx = aud_indexer.transform(df_idx)
    df_idx = status_indexer.transform(df_idx)

    existing_features = [f for f in NUMERIC_FEATURES if f in df_idx.columns]

    assembler = VectorAssembler(
        inputCols=existing_features, outputCol="features", handleInvalid="keep"
    )
    df_assembled = assembler.transform(df_idx)

    df_prepared = df_assembled.select(
        "campaign_id",
        "campaign_type",
        "budget",
        "spent_amount",
        "conversions",
        "click_through_rate",
        "conversion_rate",
        "features",
    )

    print("✓ Data prepared using saved indexers (no scaling)")
    return df_prepared


def generate_predictions(revenue_model, df):
    """
    Predict revenue, then compute ROI externally.
    This avoids the instability of directly predicting a ratio target.
    """
    # ── Predict revenue ──────────────────────────────────────────────────
    df_pred = revenue_model.transform(df).withColumnRenamed("prediction", "predicted_revenue")

    # Ensure non-negative revenue
    df_pred = df_pred.withColumn(
        "predicted_revenue",
        F.greatest(F.lit(0), F.col("predicted_revenue")),
    )

    # ── Compute ROI from predicted revenue (the key architectural choice) ─
    df_pred = df_pred.withColumn(
        "predicted_roi",
        F.when(
            F.col("spent_amount") > 0,
            ((F.col("predicted_revenue") - F.col("spent_amount")) / F.col("spent_amount")) * 100,
        ).otherwise(F.lit(0)),
    )

    # ── Prediction metadata ──────────────────────────────────────────────
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())

    # Predicted conversions based on current trajectory
    df_pred = df_pred.withColumn(
        "predicted_conversions",
        F.when(F.col("conversions") > 0, F.col("conversions")).otherwise(F.lit(0)).cast("integer"),
    )

    # Predicted CTR
    df_pred = df_pred.withColumn(
        "predicted_ctr",
        F.when(F.col("click_through_rate") > 0, F.col("click_through_rate")).otherwise(F.lit(0.02)),
    )

    # Confidence intervals (±20% of predicted revenue → convert to ROI)
    df_pred = (
        df_pred
        .withColumn("revenue_ci_lower", F.col("predicted_revenue") * 0.8)
        .withColumn("revenue_ci_upper", F.col("predicted_revenue") * 1.2)
        .withColumn(
            "roi_ci_lower",
            F.when(
                F.col("spent_amount") > 0,
                ((F.col("predicted_revenue") * 0.8 - F.col("spent_amount")) / F.col("spent_amount")) * 100,
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "roi_ci_upper",
            F.when(
                F.col("spent_amount") > 0,
                ((F.col("predicted_revenue") * 1.2 - F.col("spent_amount")) / F.col("spent_amount")) * 100,
            ).otherwise(F.lit(0)),
        )
    )

    # ── Optimization recommendations ─────────────────────────────────────
    def generate_recommendations(budget, spent, conversions, ctr, conv_rate, predicted_roi):
        recommendations = []
        budget_util = (spent / budget * 100) if budget and budget > 0 else 0

        if predicted_roi is not None and predicted_roi < 50:
            recommendations.append({
                "type": "low_roi_warning",
                "priority": "high",
                "message": f"Campaign shows low projected ROI ({predicted_roi:.1f}%). Consider adjusting targeting or creative.",
            })

        if ctr is not None and ctr < 0.01:
            recommendations.append({
                "type": "low_ctr",
                "priority": "medium",
                "message": "Low click-through rate. Consider improving ad copy or visuals.",
            })

        if conv_rate is not None and conv_rate < 0.02:
            recommendations.append({
                "type": "low_conversion",
                "priority": "medium",
                "message": "Low conversion rate. Review landing page and user experience.",
            })

        if budget_util > 80:
            recommendations.append({
                "type": "budget_depletion",
                "priority": "high",
                "message": f"Budget {budget_util:.0f}% utilized. Consider increasing budget if ROI is positive.",
            })

        if predicted_roi is not None and predicted_roi > 200:
            recommendations.append({
                "type": "high_performer",
                "priority": "low",
                "message": f"Excellent projected ROI ({predicted_roi:.1f}%). Consider scaling this campaign.",
            })

        return json.dumps(recommendations)

    recommendations_udf = F.udf(generate_recommendations, StringType())

    df_pred = df_pred.withColumn(
        "optimization_recommendations",
        recommendations_udf(
            F.col("budget"), F.col("spent_amount"), F.col("conversions"),
            F.col("click_through_rate"), F.col("conversion_rate"), F.col("predicted_roi"),
        ),
    )

    # Confidence score
    df_pred = df_pred.withColumn(
        "confidence_score",
        F.when(F.col("conversions") > 50, F.lit(0.95))
        .when(F.col("conversions") > 10, F.lit(0.85))
        .otherwise(F.lit(0.70)),
    )

    # ── Final output ─────────────────────────────────────────────────────
    output_df = df_pred.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("campaign_id"),
        current_timestamp.alias("prediction_date"),
        F.col("predicted_roi"),
        F.col("predicted_revenue"),
        F.col("predicted_conversions"),
        F.col("predicted_ctr"),
        F.col("roi_ci_lower").alias("confidence_interval_lower"),
        F.col("roi_ci_upper").alias("confidence_interval_upper"),
        F.col("revenue_ci_lower"),
        F.col("revenue_ci_upper"),
        F.col("optimization_recommendations"),
        F.col("confidence_score"),
        F.lit("random_forest_revenue_v2").alias("model_version"),
    )

    pred_count = output_df.count()
    print(f"✓ Generated {pred_count} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_sample_predictions(df, n=5):
    """Display sample predictions"""
    print("\n" + "=" * 80)
    print(f"Sample Campaign Predictions (first {n} campaigns)")
    print("=" * 80)

    sample = df.select(
        "campaign_id", "predicted_roi", "predicted_revenue",
        "predicted_conversions", "confidence_score",
    ).limit(n).collect()

    for row in sample:
        print(f"Campaign: {row['campaign_id']:<30}")
        print(f"  Predicted Revenue: ${row['predicted_revenue']:>10,.2f}")
        print(f"  Predicted ROI:     {row['predicted_roi']:>8.1f}%  (derived from revenue)")
        print(f"  Predicted Conversions: {row['predicted_conversions']:>6}")
        print(f"  Confidence: {row['confidence_score'] * 100:>6.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "=" * 80)
    print("Prediction Summary Statistics")
    print("=" * 80)

    stats = df.select(
        F.count("campaign_id").alias("total_campaigns"),
        F.avg("predicted_roi").alias("avg_predicted_roi"),
        F.avg("predicted_revenue").alias("avg_predicted_revenue"),
        F.sum("predicted_revenue").alias("total_predicted_revenue"),
        F.sum(F.when(F.col("predicted_roi") > 100, 1).otherwise(0)).alias("high_roi_campaigns"),
        F.sum(F.when(F.col("predicted_roi") < 50, 1).otherwise(0)).alias("low_roi_campaigns"),
        F.avg("confidence_score").alias("avg_confidence"),
    ).collect()[0]

    print(f"Total Campaigns:            {stats['total_campaigns']}")
    print(f"Avg Predicted Revenue:      ${stats['avg_predicted_revenue']:,.2f}")
    print(f"Total Predicted Revenue:    ${stats['total_predicted_revenue']:,.2f}")
    print(f"Avg Predicted ROI:          {stats['avg_predicted_roi']:.1f}%  (derived)")
    print(f"High ROI Campaigns (>100%): {stats['high_roi_campaigns']}")
    print(f"Low ROI Campaigns (<50%):   {stats['low_roi_campaigns']}")
    print(f"Average Confidence:         {stats['avg_confidence'] * 100:.1f}%")
    print("=" * 80)


def main(BUCKET_NAME):
    INPUT_CAMPAIGNS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_marketing_campaigns.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/campaign_roi/"
    MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/campaign_roi/"

    print("\n" + "=" * 80)
    print("Campaign ROI Prediction - Inference (Revenue → ROI)")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Load all artifacts ───────────────────────────────────────
    print("Step 1: Load Model Artifacts")
    print("-" * 80)
    revenue_model, ct_indexer, aud_indexer, status_indexer = load_model_artifacts(
        MODEL_BASE_PATH
    )

    if any(x is None for x in [revenue_model, ct_indexer, aud_indexer, status_indexer]):
        print("\n✗ Inference aborted: Model artifacts not found")
        spark.stop()
        return

    # ── Step 2: Load datasets ────────────────────────────────────────────
    print("\nStep 2: Load Datasets")
    print("-" * 80)

    campaigns_df, _ = validate_dataset(spark, INPUT_CAMPAIGNS_PATH, "Marketing Campaigns")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")

    if campaigns_df is None or orders_df is None:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return

    # ── Step 3: Historical performance ───────────────────────────────────
    print("\nStep 3: Calculate Historical Performance")
    print("-" * 80)
    historical_stats = calculate_historical_campaign_performance(campaigns_df)

    # ── Step 4: Feature engineering ──────────────────────────────────────
    print("\nStep 4: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(campaigns_df, historical_stats)

    # ── Step 5: Prepare data with SAVED indexers ─────────────────────────
    print("\nStep 5: Data Preparation (saved indexers, no scaling)")
    print("-" * 80)
    df_prepared = prepare_inference_data(
        df_features, ct_indexer, aud_indexer, status_indexer
    )

    # ── Step 6: Generate predictions ─────────────────────────────────────
    print("\nStep 6: Generate Predictions (revenue → ROI)")
    print("-" * 80)
    predictions_df = generate_predictions(revenue_model, df_prepared)

    display_sample_predictions(predictions_df)
    display_summary_statistics(predictions_df)

    # ── Step 7: Save predictions ─────────────────────────────────────────
    print("\nStep 7: Save Predictions")
    print("-" * 80)

    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)
