"""
Campaign ROI Prediction - Training Script
Predicts marketing campaign return on investment

Target Calculation:
- ROI = (revenue_generated - spent_amount) / spent_amount * 100
- Only use COMPLETED campaigns with actual revenue for training
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_CAMPAIGNS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_marketing_campaigns.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/campaign_roi/"
MIN_RECORDS_THRESHOLD = 100
MAX_NULL_PERCENTAGE = 95.0
MIN_CAMPAIGN_DAYS = 7  # Minimum days active for meaningful metrics

# Configuration
USE_CROSS_VALIDATION = False

# Required columns
REQUIRED_CAMPAIGN_COLUMNS = ["campaign_id", "campaign_type", "spent_amount"]

# Feature set
NUMERIC_FEATURES = [
    # Campaign budget & spend
    "budget",
    "spent_amount",
    "budget_utilization",  # spent / budget
    "remaining_budget",
    
    # Campaign performance metrics
    "impressions",
    "clicks",
    "conversions",
    "click_through_rate",
    "conversion_rate",
    "cost_per_click",
    "cost_per_conversion",
    "cost_per_impression",
    
    # Campaign characteristics
    "days_active",
    "avg_daily_spend",
    "avg_daily_impressions",
    "avg_daily_clicks",
    "avg_daily_conversions",
    
    # Efficiency metrics
    "engagement_efficiency",  # conversions / clicks
    "reach_efficiency",  # clicks / impressions
    "spend_efficiency",  # conversions / spent
    
    # Revenue metrics (if partially completed)
    "orders_from_campaign",
    "revenue_per_order",
    
    # Historical campaign type performance
    "campaign_type_avg_roi",
    "campaign_type_avg_conversion_rate",
    "campaign_type_avg_ctr",
    
    # Categorical (indexed)
    "campaign_type_idx",
    "target_audience_idx",
    "campaign_status_idx"
]

TARGET_COLUMN_ROI = "actual_roi"
TARGET_COLUMN_REVENUE = "actual_revenue"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Campaign_ROI_Training")
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


def validate_columns(df, required_columns, dataset_name):
    """Validate columns exist and are not entirely null"""
    print(f"\nValidating columns for {dataset_name}...")
    
    existing_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"✗ Missing columns in {dataset_name}: {', '.join(missing_columns)}")
        return False, missing_columns, []
    
    total_count = df.count()
    null_columns = []
    
    for col in required_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 100
        
        if null_pct > MAX_NULL_PERCENTAGE:
            print(f"✗ Column '{col}' is {null_pct:.1f}% null (threshold: {MAX_NULL_PERCENTAGE}%)")
            null_columns.append(col)
        elif null_pct > 50:
            print(f"⚠  Column '{col}' is {null_pct:.1f}% null (may affect accuracy)")
    
    if null_columns:
        return False, [], null_columns
    
    print(f"✓ All required columns validated for {dataset_name}")
    return True, [], []


def calculate_historical_campaign_performance(campaigns_df):
    """
    Calculate historical performance metrics by campaign type
    This provides baseline expectations for each campaign type
    """
    print("Calculating historical campaign type performance...")
    
    # Only use completed campaigns with revenue data
    completed_campaigns = campaigns_df.filter(
        (F.col("campaign_status") == "Completed") &
        (F.col("revenue_generated").isNotNull()) &
        (F.col("revenue_generated") > 0)
    )
    
    # Calculate actual ROI for historical campaigns
    historical_with_roi = completed_campaigns.withColumn(
        "historical_roi",
        F.when(
            F.col("spent_amount") > 0,
            ((F.col("revenue_generated") - F.col("spent_amount")) / F.col("spent_amount")) * 100
        ).otherwise(0)
    )
    
    # Aggregate by campaign type
    campaign_type_stats = historical_with_roi.groupBy("campaign_type").agg(
        F.avg("historical_roi").alias("campaign_type_avg_roi"),
        F.avg("conversion_rate").alias("campaign_type_avg_conversion_rate"),
        F.avg("click_through_rate").alias("campaign_type_avg_ctr"),
        F.count("campaign_id").alias("campaign_type_count")
    )
    
    campaign_type_stats = campaign_type_stats.fillna({
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0
    })
    
    print(f"✓ Historical performance calculated for {campaign_type_stats.count()} campaign types")
    return campaign_type_stats


def create_campaign_roi_features(campaigns_df, orders_df, historical_stats_df):
    """
    Create comprehensive campaign features with ROI target
    Only use campaigns with actual revenue data for training
    """
    print("Creating campaign ROI features...")
    
    # Filter to completed campaigns with revenue data
    # These are campaigns where we know the actual outcome
    completed_campaigns = campaigns_df.filter(
        (F.col("revenue_generated").isNotNull()) &
        (F.col("revenue_generated") > 0) &
        (F.col("spent_amount") > 0) &
        (F.col("days_active") >= MIN_CAMPAIGN_DAYS)  # Sufficient runtime
    )
    
    print(f"Total campaigns: {campaigns_df.count()}")
    print(f"Completed campaigns with revenue: {completed_campaigns.count()}")
    
    # Calculate target variables
    campaign_features = completed_campaigns.withColumn(
        TARGET_COLUMN_ROI,
        ((F.col("revenue_generated") - F.col("spent_amount")) / F.col("spent_amount")) * 100
    ).withColumn(
        TARGET_COLUMN_REVENUE,
        F.col("revenue_generated")
    )
    
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
    
    # Calculate revenue per order
    campaign_features = campaign_features.withColumn(
        "revenue_per_order",
        F.when(
            F.col("orders_from_campaign") > 0,
            F.col("revenue_generated") / F.col("orders_from_campaign")
        ).otherwise(0)
    )
    
    # Join with historical campaign type performance
    campaign_features = campaign_features.join(
        historical_stats_df,
        "campaign_type",
        "left"
    )
    
    # Fill nulls
    campaign_features = campaign_features.fillna({
        "budget": 0,
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "click_through_rate": 0,
        "conversion_rate": 0,
        "orders_from_campaign": 0,
        "target_audience": "Unknown",
        "campaign_status": "Unknown",
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0
    })
    
    print(f"✓ Campaign ROI features created: {campaign_features.count()} records")
    return campaign_features


def prepare_training_data(df, target_column):
    """Prepare data with encoding and scaling for specific target"""
    print(f"Preparing training data for target: {target_column}...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(target_column).isNotNull()) &
        (F.col("spent_amount") > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
        return None
    
    # Encode categorical features
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
    
    df_indexed = campaign_type_indexer.fit(df_valid).transform(df_valid)
    df_indexed = audience_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = status_indexer.fit(df_indexed).transform(df_indexed)
    
    # Filter features that exist
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    missing_features = [f for f in NUMERIC_FEATURES if f not in df_indexed.columns]
    
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} features for training")
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Select final columns
    df_prepared = df_scaled.select(
        "campaign_id",
        "features",
        F.col(target_column).alias("label")
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model, existing_features


def train_random_forest(train_df, test_df, target_name, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print(f"Training Random Forest for {target_name}")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=200,
        maxDepth=15,
        seed=42
    )
    
    if use_cv:
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [150, 200, 250]) \
            .addGrid(rf.maxDepth, [12, 15, 18]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=rf,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = rf.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions


def evaluate_model(predictions, model_name, target_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name} for {target_name}...")
    
    rmse_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    mape_df = predictions.filter(F.col("label") > 0).withColumn(
        "ape",
        F.abs((F.col("label") - F.col("prediction")) / F.col("label")) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0] if mape_df.count() > 0 else 0
    
    metrics = {
        "model": model_name,
        "target": target_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape
    }
    
    if "roi" in target_name.lower():
        print(f"  RMSE: {rmse:.2f}%")
        print(f"  MAE: {mae:.2f}%")
    else:
        print(f"  RMSE: ${rmse:,.2f}")
        print(f"  MAE: ${mae:,.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("Campaign ROI Prediction - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    campaigns_df, _ = validate_dataset(spark, INPUT_CAMPAIGNS_PATH, "Marketing Campaigns")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    
    if None in [campaigns_df, orders_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    campaign_valid, _, _ = validate_columns(campaigns_df, REQUIRED_CAMPAIGN_COLUMNS, "Campaigns")
    
    if not campaign_valid:
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Calculate historical performance
    print("\nStep 3: Calculate Historical Campaign Performance")
    print("-" * 60)
    historical_stats = calculate_historical_campaign_performance(campaigns_df)
    
    # Create features
    print("\nStep 4: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_campaign_roi_features(campaigns_df, orders_df, historical_stats)
    
    # Train model for ROI
    print("\n" + "="*60)
    print("TRAINING: Campaign ROI Model")
    print("="*60)
    
    result_roi = prepare_training_data(df_features, TARGET_COLUMN_ROI)
    
    if result_roi is None:
        print("\n✗ Training aborted: Insufficient data for ROI model")
        spark.stop()
        return
    
    df_prepared_roi, scaler_roi, feature_list = result_roi
    
    print(f"\n{'='*60}")
    print(f"Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    train_roi, test_roi = df_prepared_roi.randomSplit([0.8, 0.2], seed=42)
    print(f"\nTraining set: {train_roi.count()} records")
    print(f"Test set: {test_roi.count()} records")
    
    model_roi, pred_roi = train_random_forest(
        train_roi, test_roi, "campaign_roi", USE_CROSS_VALIDATION
    )
    metrics_roi = evaluate_model(pred_roi, "random_forest", "campaign_roi")
    save_model(model_roi, "campaign_roi")
    
    # Train model for Revenue
    print("\n" + "="*60)
    print("TRAINING: Campaign Revenue Model")
    print("="*60)
    
    result_revenue = prepare_training_data(df_features, TARGET_COLUMN_REVENUE)
    
    if result_revenue is None:
        print("\n✗ Training aborted: Insufficient data for revenue model")
        spark.stop()
        return
    
    df_prepared_revenue, scaler_revenue, _ = result_revenue
    
    train_revenue, test_revenue = df_prepared_revenue.randomSplit([0.8, 0.2], seed=42)
    print(f"\nTraining set: {train_revenue.count()} records")
    print(f"Test set: {test_revenue.count()} records")
    
    model_revenue, pred_revenue = train_random_forest(
        train_revenue, test_revenue, "campaign_revenue", USE_CROSS_VALIDATION
    )
    metrics_revenue = evaluate_model(pred_revenue, "random_forest", "campaign_revenue")
    save_model(model_revenue, "campaign_revenue")
    
    # Summary
    print("\n" + "="*60)
    print("Training Summary")
    print("="*60)
    print(f"\nCampaign ROI Model:")
    print(f"  R²: {metrics_roi['r2']:.4f}")
    print(f"  RMSE: {metrics_roi['rmse']:.2f}%")
    print(f"  MAE: {metrics_roi['mae']:.2f}%")
    
    print(f"\nCampaign Revenue Model:")
    print(f"  R²: {metrics_revenue['r2']:.4f}")
    print(f"  RMSE: ${metrics_revenue['rmse']:,.2f}")
    print(f"  MAE: ${metrics_revenue['mae']:,.2f}")
    
    print(f"\n✓ Training completed")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
