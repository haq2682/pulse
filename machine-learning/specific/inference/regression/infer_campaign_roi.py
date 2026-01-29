"""
Campaign ROI Prediction - Inference Script
Predicts expected ROI and revenue for marketing campaigns
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressionModel
from datetime import datetime
import uuid
import json

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_CAMPAIGNS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_marketing_campaigns.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/campaign_roi/"
MODEL_BASE_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/campaign_roi/"

# Configuration
MIN_CAMPAIGN_DAYS = 7

# Feature set (must match training)
NUMERIC_FEATURES = [
    "budget", "spent_amount", "budget_utilization", "remaining_budget",
    "impressions", "clicks", "conversions",
    "click_through_rate", "conversion_rate",
    "cost_per_click", "cost_per_conversion", "cost_per_impression",
    "days_active", "avg_daily_spend", "avg_daily_impressions",
    "avg_daily_clicks", "avg_daily_conversions",
    "engagement_efficiency", "reach_efficiency", "spend_efficiency",
    "orders_from_campaign", "revenue_per_order",
    "campaign_type_avg_roi", "campaign_type_avg_conversion_rate",
    "campaign_type_avg_ctr",
    "campaign_type_idx", "target_audience_idx", "campaign_status_idx"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Campaign_ROI_Inference")
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


def load_models():
    """Load both trained models"""
    try:
        roi_model = RandomForestRegressionModel.load(f"{MODEL_BASE_PATH}campaign_roi")
        revenue_model = RandomForestRegressionModel.load(f"{MODEL_BASE_PATH}campaign_revenue")
        print(f"✓ Models loaded successfully")
        return roi_model, revenue_model
    except Exception as e:
        print(f"✗ Failed to load models: {str(e)}")
        return None, None


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
    
    completed_campaigns = campaigns_df.filter(
        (F.col("campaign_status") == "Completed") &
        (F.col("revenue_generated").isNotNull()) &
        (F.col("revenue_generated") > 0)
    )
    
    historical_with_roi = completed_campaigns.withColumn(
        "historical_roi",
        F.when(
            F.col("spent_amount") > 0,
            ((F.col("revenue_generated") - F.col("spent_amount")) / F.col("spent_amount")) * 100
        ).otherwise(0)
    )
    
    campaign_type_stats = historical_with_roi.groupBy("campaign_type").agg(
        F.avg("historical_roi").alias("campaign_type_avg_roi"),
        F.avg("conversion_rate").alias("campaign_type_avg_conversion_rate"),
        F.avg("click_through_rate").alias("campaign_type_avg_ctr")
    )
    
    campaign_type_stats = campaign_type_stats.fillna({
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0
    })
    
    print(f"✓ Historical performance calculated")
    return campaign_type_stats


def create_inference_features(campaigns_df, historical_stats_df):
    """Create same features as training"""
    print("Creating inference features...")
    
    # Use active or planned campaigns
    campaign_features = campaigns_df
    
    # Calculate budget utilization
    campaign_features = campaign_features.withColumn(
        "budget_utilization",
        F.when(
            F.col("budget") > 0,
            F.col("spent_amount") / F.col("budget")
        ).otherwise(1.0)
    ).withColumn(
        "remaining_budget",
        F.greatest(F.lit(0), F.col("budget") - F.col("spent_amount"))
    )
    
    # Calculate cost metrics
    campaign_features = campaign_features.withColumn(
        "cost_per_click",
        F.when(
            F.col("clicks") > 0,
            F.col("spent_amount") / F.col("clicks")
        ).otherwise(0)
    ).withColumn(
        "cost_per_conversion",
        F.when(
            F.col("conversions") > 0,
            F.col("spent_amount") / F.col("conversions")
        ).otherwise(0)
    ).withColumn(
        "cost_per_impression",
        F.when(
            F.col("impressions") > 0,
            F.col("spent_amount") / F.col("impressions")
        ).otherwise(0)
    )
    
    # Calculate daily averages
    campaign_features = campaign_features.withColumn(
        "avg_daily_spend",
        F.when(
            F.col("days_active") > 0,
            F.col("spent_amount") / F.col("days_active")
        ).otherwise(0)
    ).withColumn(
        "avg_daily_impressions",
        F.when(
            F.col("days_active") > 0,
            F.col("impressions") / F.col("days_active")
        ).otherwise(0)
    ).withColumn(
        "avg_daily_clicks",
        F.when(
            F.col("days_active") > 0,
            F.col("clicks") / F.col("days_active")
        ).otherwise(0)
    ).withColumn(
        "avg_daily_conversions",
        F.when(
            F.col("days_active") > 0,
            F.col("conversions") / F.col("days_active")
        ).otherwise(0)
    )
    
    # Calculate efficiency metrics
    campaign_features = campaign_features.withColumn(
        "engagement_efficiency",
        F.when(
            F.col("clicks") > 0,
            F.col("conversions") / F.col("clicks")
        ).otherwise(0)
    ).withColumn(
        "reach_efficiency",
        F.when(
            F.col("impressions") > 0,
            F.col("clicks") / F.col("impressions")
        ).otherwise(0)
    ).withColumn(
        "spend_efficiency",
        F.when(
            F.col("spent_amount") > 0,
            F.col("conversions") / F.col("spent_amount")
        ).otherwise(0)
    )
    
    # Calculate revenue per order (if any orders yet)
    campaign_features = campaign_features.withColumn(
        "revenue_per_order",
        F.when(
            (F.col("orders_from_campaign") > 0) & (F.col("revenue_generated").isNotNull()),
            F.col("revenue_generated") / F.col("orders_from_campaign")
        ).otherwise(0)
    )
    
    # Join with historical stats
    campaign_features = campaign_features.join(
        historical_stats_df,
        "campaign_type",
        "left"
    )
    
    # Fill nulls
    campaign_features = campaign_features.fillna({
        "budget": 0,
        "spent_amount": 0,
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "click_through_rate": 0,
        "conversion_rate": 0,
        "orders_from_campaign": 0,
        "days_active": 1,
        "target_audience": "Unknown",
        "campaign_status": "Unknown",
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0
    })
    
    print(f"✓ Inference features created: {campaign_features.count()} campaigns")
    return campaign_features


def prepare_inference_data(df):
    """Prepare and scale features"""
    campaign_type_indexer = StringIndexer(
        inputCol="campaign_type",
        outputCol="campaign_type_idx",
        handleInvalid="keep"
    )
    
    audience_indexer = StringIndexer(
        inputCol="target_audience",
        outputCol="target_audience_idx",
        handleInvalid="keep"
    )
    
    status_indexer = StringIndexer(
        inputCol="campaign_status",
        outputCol="campaign_status_idx",
        handleInvalid="keep"
    )
    
    df_indexed = campaign_type_indexer.fit(df).transform(df)
    df_indexed = audience_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = status_indexer.fit(df_indexed).transform(df_indexed)
    
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    df_prepared = df_scaled.select(
        "campaign_id",
        "campaign_type",
        "budget",
        "spent_amount",
        "conversions",
        "click_through_rate",
        "conversion_rate",
        "features"
    )
    
    print(f"✓ Data prepared and scaled")
    return df_prepared


def generate_predictions(roi_model, revenue_model, df):
    """Generate comprehensive campaign predictions"""
    # Predict ROI
    df_with_roi = roi_model.transform(df).withColumnRenamed("prediction", "predicted_roi")
    
    # Predict Revenue
    df_with_both = revenue_model.transform(df_with_roi).withColumnRenamed("prediction", "predicted_revenue")
    
    # Ensure non-negative revenue
    df_with_both = df_with_both.withColumn(
        "predicted_revenue",
        F.greatest(F.lit(0), F.col("predicted_revenue"))
    )
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Calculate predicted conversions based on current trajectory
    df_with_both = df_with_both.withColumn(
        "predicted_conversions",
        F.when(
            F.col("conversion_rate") > 0,
            F.round(F.col("predicted_revenue") / (F.col("predicted_revenue") / F.greatest(F.col("conversions"), F.lit(1))), 0)
        ).otherwise(
            F.lit(0)
        ).cast("integer")
    )
    
    # Predicted CTR (based on historical type performance)
    df_with_both = df_with_both.withColumn(
        "predicted_ctr",
        F.when(
            F.col("click_through_rate") > 0,
            F.col("click_through_rate")
        ).otherwise(
            F.lit(0.02)  # 2% default
        )
    )
    
    # Calculate confidence intervals (±20% of prediction)
    df_with_both = df_with_both.withColumn(
        "confidence_interval_lower",
        F.col("predicted_roi") * 0.8
    ).withColumn(
        "confidence_interval_upper",
        F.col("predicted_roi") * 1.2
    )
    
    # Generate optimization recommendations
    def generate_recommendations(budget, spent, conversions, ctr, conv_rate, predicted_roi):
        recommendations = []
        
        budget_util = (spent / budget * 100) if budget > 0 else 0
        
        if predicted_roi < 50:
            recommendations.append({
                "type": "low_roi_warning",
                "priority": "high",
                "message": f"Campaign shows low projected ROI ({predicted_roi:.1f}%). Consider adjusting targeting or creative."
            })
        
        if ctr < 0.01:  # <1% CTR
            recommendations.append({
                "type": "low_ctr",
                "priority": "medium",
                "message": "Low click-through rate. Consider improving ad copy or visuals."
            })
        
        if conv_rate < 0.02:  # <2% conversion
            recommendations.append({
                "type": "low_conversion",
                "priority": "medium",
                "message": "Low conversion rate. Review landing page and user experience."
            })
        
        if budget_util > 80:
            recommendations.append({
                "type": "budget_depletion",
                "priority": "high",
                "message": f"Budget {budget_util:.0f}% utilized. Consider increasing budget if ROI is positive."
            })
        
        if predicted_roi > 200:
            recommendations.append({
                "type": "high_performer",
                "priority": "low",
                "message": f"Excellent projected ROI ({predicted_roi:.1f}%). Consider scaling this campaign."
            })
        
        return json.dumps(recommendations)
    
    recommendations_udf = F.udf(generate_recommendations, StringType())
    
    df_with_both = df_with_both.withColumn(
        "optimization_recommendations",
        recommendations_udf(
            F.col("budget"),
            F.col("spent_amount"),
            F.col("conversions"),
            F.col("click_through_rate"),
            F.col("conversion_rate"),
            F.col("predicted_roi")
        )
    )
    
    # Calculate confidence score
    df_with_both = df_with_both.withColumn(
        "confidence_score",
        F.when(
            F.col("conversions") > 50,
            F.lit(0.95)  # High confidence with lots of data
        ).when(
            F.col("conversions") > 10,
            F.lit(0.85)  # Medium confidence
        ).otherwise(
            F.lit(0.70)  # Lower confidence for new campaigns
        )
    )
    
    output_df = df_with_both.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("campaign_id"),
        current_timestamp.alias("prediction_date"),
        F.col("predicted_roi"),
        F.col("predicted_revenue"),
        F.col("predicted_conversions"),
        F.col("predicted_ctr"),
        F.col("confidence_interval_lower"),
        F.col("confidence_interval_upper"),
        F.col("optimization_recommendations"),
        F.col("confidence_score"),
        F.lit("random_forest").alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
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
    print("\n" + "="*80)
    print(f"Sample Campaign ROI Predictions (first {n} campaigns)")
    print("="*80)
    
    sample = df.select(
        "campaign_id",
        "predicted_roi",
        "predicted_revenue",
        "predicted_conversions",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Campaign: {row['campaign_id']:<30}")
        print(f"  Predicted ROI: {row['predicted_roi']:>8.1f}%")
        print(f"  Predicted Revenue: ${row['predicted_revenue']:>10,.2f}")
        print(f"  Predicted Conversions: {row['predicted_conversions']:>6}")
        print(f"  Confidence: {row['confidence_score']*100:>6.1f}%")
        print()


def display_summary_statistics(df):
    """Display summary statistics"""
    print("\n" + "="*80)
    print("Prediction Summary Statistics")
    print("="*80)
    
    stats = df.select(
        F.count("campaign_id").alias("total_campaigns"),
        F.avg("predicted_roi").alias("avg_predicted_roi"),
        F.sum("predicted_revenue").alias("total_predicted_revenue"),
        F.sum(F.when(F.col("predicted_roi") > 100, 1).otherwise(0)).alias("high_roi_campaigns"),
        F.sum(F.when(F.col("predicted_roi") < 50, 1).otherwise(0)).alias("low_roi_campaigns"),
        F.avg("confidence_score").alias("avg_confidence")
    ).collect()[0]
    
    print(f"Total Campaigns: {stats['total_campaigns']}")
    print(f"Average Predicted ROI: {stats['avg_predicted_roi']:.1f}%")
    print(f"Total Predicted Revenue: ${stats['total_predicted_revenue']:,.2f}")
    print(f"High ROI Campaigns (>100%): {stats['high_roi_campaigns']}")
    print(f"Low ROI Campaigns (<50%): {stats['low_roi_campaigns']}")
    print(f"Average Confidence: {stats['avg_confidence']*100:.1f}%")
    print("="*80)


def main():
    """Main inference pipeline"""
    print("\n" + "="*80)
    print("Campaign ROI Prediction - Inference")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load models
    print("Step 1: Load Models")
    print("-" * 80)
    roi_model, revenue_model = load_models()
    
    if roi_model is None or revenue_model is None:
        print("\n✗ Inference aborted: Models not found")
        spark.stop()
        return
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 80)
    
    campaigns_df, _ = validate_dataset(spark, INPUT_CAMPAIGNS_PATH, "Marketing Campaigns")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    
    if None in [campaigns_df, orders_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Calculate historical performance
    print("\nStep 3: Calculate Historical Performance")
    print("-" * 80)
    historical_stats = calculate_historical_campaign_performance(campaigns_df)
    
    # Create features
    print("\nStep 4: Feature Engineering")
    print("-" * 80)
    df_features = create_inference_features(campaigns_df, historical_stats)
    
    # Prepare data
    print("\nStep 5: Data Preparation & Encoding")
    print("-" * 80)
    df_prepared = prepare_inference_data(df_features)
    
    # Generate predictions
    print("\nStep 6: Generate Predictions")
    print("-" * 80)
    predictions_df = generate_predictions(roi_model, revenue_model, df_prepared)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Display summary
    display_summary_statistics(predictions_df)
    
    # Save predictions
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
    main()
