"""
Campaign ROI Prediction - Training Script
Predicts marketing campaign revenue, then computes ROI in inference.

Architecture Decision:
- We train a REVENUE model only (stable, naturally positive target)
- ROI is computed post-prediction in inference: (predicted_revenue - spent) / spent * 100
- Direct ROI prediction causes extreme instability due to ratio target + small denominators

Pipeline Notes:
- No StandardScaler — RandomForest is tree-based and invariant to monotonic
  feature transformations. Scaling only adds pipeline complexity and increases
  the inference failure surface.
- Fitted StringIndexers are persisted so inference uses identical category→index
  mappings. Re-fitting indexers at inference time is a silent production killer
  because category ordering may differ, causing the model to receive different
  feature vectors.
- Historical campaign_type_avg_* features include training rows (mild leakage).
  For production, compute these using only campaigns that started before each
  row's start_date (leave-one-out or time-based split). Acceptable for
  non-time-aware modeling.

Target:
- revenue_generated (from COMPLETED campaigns only)
"""

import os
import json
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()


# ── Feature set ──────────────────────────────────────────────────────────────
# REMOVED leakage features that encode revenue information:
#   - revenue_per_order     (directly derived from revenue)
#   - orders_from_campaign  (highly correlated with revenue)
#   - spend_efficiency      (conversions / spent — proxy for revenue)
# REMOVED StandardScaler — unnecessary for RandomForest (tree-based, split-invariant)
NUMERIC_FEATURES = [
    # Campaign budget & spend
    "budget",
    "spent_amount",
    "budget_utilization",       # spent / budget
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

    # Efficiency metrics (non-leaky)
    "engagement_efficiency",    # conversions / clicks
    "reach_efficiency",         # clicks / impressions

    # Historical campaign type performance
    # NOTE: mild leakage — see docstring above
    "campaign_type_avg_roi",
    "campaign_type_avg_conversion_rate",
    "campaign_type_avg_ctr",

    # Categorical (indexed)
    "campaign_type_idx",
    "target_audience_idx",
    "campaign_status_idx",
]

TARGET_COLUMN = "actual_revenue"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Campaign_Revenue_Training")
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


def validate_columns(df, required_columns, dataset_name, max_null_pct):
    """Validate columns exist and are not entirely null"""
    print(f"\nValidating columns for {dataset_name}...")

    existing_columns = set(df.columns)
    missing_columns = [c for c in required_columns if c not in existing_columns]

    if missing_columns:
        print(f"✗ Missing columns in {dataset_name}: {', '.join(missing_columns)}")
        return False, missing_columns, []

    total_count = df.count()
    null_columns = []

    for col in required_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 100

        if null_pct > max_null_pct:
            print(f"✗ Column '{col}' is {null_pct:.1f}% null (threshold: {max_null_pct}%)")
            null_columns.append(col)
        elif null_pct > 50:
            print(f"⚠  Column '{col}' is {null_pct:.1f}% null (may affect accuracy)")

    if null_columns:
        return False, [], null_columns

    print(f"✓ All required columns validated for {dataset_name}")
    return True, [], []


def calculate_historical_campaign_performance(campaigns_df):
    """
    Calculate historical performance metrics by campaign type.
    Provides baseline expectations for each campaign type.

    NOTE: This includes all completed campaigns, which means training rows
    contribute to their own campaign_type_avg_* features (mild leakage).
    For stricter separation, compute using only campaigns with start_date
    before each row's start_date.
    """
    print("Calculating historical campaign type performance...")

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
        F.count("campaign_id").alias("campaign_type_count"),
    )

    stats = stats.fillna({
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0,
    })

    print(f"✓ Historical performance calculated for {stats.count()} campaign types")
    return stats


def create_campaign_features(campaigns_df, historical_stats_df, min_campaign_days):
    """
    Create features with revenue as the target.
    Only completed campaigns with actual revenue are used for training.
    """
    print("Creating campaign revenue features...")

    completed = campaigns_df.filter(
        F.col("revenue_generated").isNotNull()
        & (F.col("revenue_generated") > 0)
        & (F.col("spent_amount") > 0)
        & (F.col("days_active") >= min_campaign_days)
    )

    total_count = campaigns_df.count()
    completed_count = completed.count()
    print(f"Total campaigns: {total_count}")
    print(f"Completed campaigns with revenue: {completed_count}")

    # ── Target ───────────────────────────────────────────────────────────
    features = completed.withColumn(TARGET_COLUMN, F.col("revenue_generated"))

    # ── Budget utilization ───────────────────────────────────────────────
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

    # ── Cost metrics ─────────────────────────────────────────────────────
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

    # ── Daily averages ───────────────────────────────────────────────────
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

    # ── Efficiency metrics (non-leaky) ───────────────────────────────────
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

    # ── Join historical stats ────────────────────────────────────────────
    features = features.join(historical_stats_df, "campaign_type", "left")

    # ── Fill nulls ───────────────────────────────────────────────────────
    features = features.fillna({
        "budget": 0,
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "click_through_rate": 0,
        "conversion_rate": 0,
        "target_audience": "Unknown",
        "campaign_status": "Unknown",
        "campaign_type_avg_roi": 0,
        "campaign_type_avg_conversion_rate": 0,
        "campaign_type_avg_ctr": 0,
    })

    feature_count = features.count()
    print(f"✓ Campaign revenue features created: {feature_count} records")
    return features


def prepare_training_data(df, min_records):
    """
    Encode categoricals, assemble features.
    Returns prepared df + fitted indexer models + feature list.

    No scaling — RandomForest is tree-based and invariant to monotonic
    feature transformations.
    """
    print(f"Preparing training data for target: {TARGET_COLUMN}...")

    df_valid = df.filter(F.col(TARGET_COLUMN).isNotNull() & (F.col("spent_amount") > 0))
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")

    if valid_count < min_records:
        print(f"✗ Insufficient data: {valid_count} < {min_records}")
        return None

    # ── Fit & transform categoricals ─────────────────────────────────────
    # Fitted models are returned so they can be persisted for inference.
    ct_indexer_model = StringIndexer(
        inputCol="campaign_type", outputCol="campaign_type_idx", handleInvalid="keep"
    ).fit(df_valid)
    df_idx = ct_indexer_model.transform(df_valid)

    aud_indexer_model = StringIndexer(
        inputCol="target_audience", outputCol="target_audience_idx", handleInvalid="keep"
    ).fit(df_idx)
    df_idx = aud_indexer_model.transform(df_idx)

    status_indexer_model = StringIndexer(
        inputCol="campaign_status", outputCol="campaign_status_idx", handleInvalid="keep"
    ).fit(df_idx)
    df_idx = status_indexer_model.transform(df_idx)

    # ── Feature selection ────────────────────────────────────────────────
    existing_features = [f for f in NUMERIC_FEATURES if f in df_idx.columns]
    missing_features = [f for f in NUMERIC_FEATURES if f not in df_idx.columns]
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    print(f"Using {len(existing_features)} features for training")

    # ── Assemble directly into "features" (no scaling step) ──────────────
    assembler = VectorAssembler(
        inputCols=existing_features, outputCol="features", handleInvalid="keep"
    )
    df_assembled = assembler.transform(df_idx)

    df_prepared = df_assembled.select(
        "campaign_id",
        "spent_amount",
        "features",
        F.col(TARGET_COLUMN).alias("label"),
    )

    prepared_count = df_prepared.count()
    print(f"✓ Data prepared: {prepared_count} records")

    # Report cardinalities so maxBins can be set dynamically
    ct_card = len(ct_indexer_model.labels)
    aud_card = len(aud_indexer_model.labels)
    status_card = len(status_indexer_model.labels)
    max_cardinality = max(ct_card, aud_card, status_card)
    print(f"  Indexer cardinalities: campaign_type={ct_card}, "
          f"target_audience={aud_card}, campaign_status={status_card}")
    print(f"  Max cardinality: {max_cardinality}")

    return (
        df_prepared,
        existing_features,
        ct_indexer_model,
        aud_indexer_model,
        status_indexer_model,
        max_cardinality,
    )


def train_random_forest(train_df, test_df, max_cardinality=32, use_cv=False):
    """Train Random Forest regressor for revenue prediction."""
    print("\n" + "=" * 60)
    print("Training Random Forest for campaign revenue")
    print("=" * 60)

    # maxBins must be >= number of categories in every categorical feature.
    # Default 32 is too small when StringIndexer produces high-cardinality
    # outputs (e.g. 518 audience segments). We set it dynamically.
    max_bins = max(32, max_cardinality + 1)
    print(f"  maxBins set to {max_bins} (max categorical cardinality: {max_cardinality})")

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=200,
        maxDepth=15,
        maxBins=max_bins,
        seed=42,
    )

    if use_cv:
        param_grid = (
            ParamGridBuilder()
            .addGrid(rf.numTrees, [150, 200, 250])
            .addGrid(rf.maxDepth, [12, 15, 18])
            .build()
        )
        evaluator = RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName="r2"
        )
        cv = CrossValidator(
            estimator=rf, estimatorParamMaps=param_grid,
            evaluator=evaluator, numFolds=3, seed=42,
        )
        model = cv.fit(train_df).bestModel
    else:
        model = rf.fit(train_df)

    predictions = model.transform(test_df)
    return model, predictions


def evaluate_model(predictions):
    """Evaluate the revenue model and also compute derived ROI metrics."""
    print("\nEvaluating revenue model...")

    rmse = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    ).evaluate(predictions)
    mae = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="mae"
    ).evaluate(predictions)
    r2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2"
    ).evaluate(predictions)

    mape_df = predictions.filter(F.col("label") > 0).withColumn(
        "ape", F.abs((F.col("label") - F.col("prediction")) / F.col("label")) * 100
    )
    mape_count = mape_df.count()
    mape = mape_df.agg(F.avg("ape")).collect()[0][0] if mape_count > 0 else 0

    # ── Derived ROI evaluation ───────────────────────────────────────────
    roi_df = predictions.filter(F.col("spent_amount") > 0).withColumn(
        "actual_roi",
        ((F.col("label") - F.col("spent_amount")) / F.col("spent_amount")) * 100,
    ).withColumn(
        "predicted_roi",
        ((F.col("prediction") - F.col("spent_amount")) / F.col("spent_amount")) * 100,
    )

    roi_count = roi_df.count()
    roi_mae = (
        roi_df.withColumn(
            "roi_error", F.abs(F.col("actual_roi") - F.col("predicted_roi"))
        ).agg(F.avg("roi_error")).collect()[0][0]
        if roi_count > 0
        else None
    )

    metrics = {
        "model": "random_forest",
        "target": "campaign_revenue",
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape,
        "derived_roi_mae": roi_mae,
    }

    print(f"  Revenue RMSE: ${rmse:,.2f}")
    print(f"  Revenue MAE:  ${mae:,.2f}")
    print(f"  R²:           {r2:.4f}")
    print(f"  Revenue MAPE: {mape:.2f}%")
    if roi_mae is not None:
        print(f"  Derived ROI MAE: {roi_mae:.2f}% (ROI computed from predicted revenue)")

    return metrics


def save_artifacts(
    model,
    feature_list,
    ct_indexer_model,
    aud_indexer_model,
    status_indexer_model,
    model_output_path,
):
    """
    Persist everything needed to reproduce inference identically:
    - RandomForest model
    - 3 fitted StringIndexerModel instances (category → index mappings)
    - Feature list (JSON)
    """
    model_path = f"{model_output_path}campaign_revenue"
    ct_path = f"{model_output_path}indexer_campaign_type"
    aud_path = f"{model_output_path}indexer_target_audience"
    status_path = f"{model_output_path}indexer_campaign_status"
    features_path = f"{model_output_path}feature_list.json"

    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")

    ct_indexer_model.write().overwrite().save(ct_path)
    print(f"✓ Campaign type indexer saved: {ct_path}")

    aud_indexer_model.write().overwrite().save(aud_path)
    print(f"✓ Target audience indexer saved: {aud_path}")

    status_indexer_model.write().overwrite().save(status_path)
    print(f"✓ Campaign status indexer saved: {status_path}")

    spark = SparkSession.getActiveSession()
    features_df = spark.createDataFrame(
        [(json.dumps({"features": feature_list}),)],
        ["json"]
    )

    features_df.write.mode("overwrite").text(features_path)
    print(f"✓ Feature list saved: {features_path}")


def main(BUCKET_NAME):
    INPUT_CAMPAIGNS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_marketing_campaigns.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/campaign_roi/"
    MIN_RECORDS_THRESHOLD = 100
    MAX_NULL_PERCENTAGE = 95.0
    MIN_CAMPAIGN_DAYS = 7

    USE_CROSS_VALIDATION = False

    REQUIRED_CAMPAIGN_COLUMNS = ["campaign_id", "campaign_type", "spent_amount"]

    print("\n" + "=" * 60)
    print("Campaign Revenue Training (ROI computed in inference)")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Load datasets ────────────────────────────────────────────
    print("Step 1: Load Datasets")
    print("-" * 60)

    campaigns_df, _ = validate_dataset(spark, INPUT_CAMPAIGNS_PATH, "Marketing Campaigns")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")

    if campaigns_df is None or orders_df is None:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return

    # ── Step 2: Column validation ────────────────────────────────────────
    print("\nStep 2: Column Validation")
    print("-" * 60)

    valid, _, _ = validate_columns(
        campaigns_df, REQUIRED_CAMPAIGN_COLUMNS, "Campaigns", MAX_NULL_PERCENTAGE
    )
    if not valid:
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return

    # ── Step 3: Historical performance ───────────────────────────────────
    print("\nStep 3: Calculate Historical Campaign Performance")
    print("-" * 60)
    historical_stats = calculate_historical_campaign_performance(campaigns_df)

    # ── Step 4: Feature engineering ──────────────────────────────────────
    print("\nStep 4: Feature Engineering")
    print("-" * 60)
    df_features = create_campaign_features(campaigns_df, historical_stats, MIN_CAMPAIGN_DAYS)

    # ── Step 5: Prepare & train ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("TRAINING: Campaign Revenue Model")
    print("=" * 60)

    result = prepare_training_data(df_features, MIN_RECORDS_THRESHOLD)
    if result is None:
        print("\n✗ Training aborted: Insufficient data")
        spark.stop()
        return

    df_prepared, feature_list, ct_model, aud_model, status_model, max_cardinality = result

    print(f"\n{'=' * 60}")
    print(f"Feature Set ({len(feature_list)} features):")
    print(f"{'=' * 60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")

    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)

    # Cache counts — avoid recomputing DAG on each .count() call
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"\nTraining set: {train_count} records")
    print(f"Test set:     {test_count} records")

    if train_count == 0 or test_count == 0:
        print("✗ Empty train/test split — aborting")
        spark.stop()
        return

    model, predictions = train_random_forest(
        train_df, test_df, max_cardinality, USE_CROSS_VALIDATION
    )
    metrics = evaluate_model(predictions)

    # ── Step 6: Save all artifacts ───────────────────────────────────────
    print("\nStep 6: Save Model Artifacts")
    print("-" * 60)
    save_artifacts(
        model, feature_list, ct_model, aud_model, status_model, MODEL_OUTPUT_PATH
    )

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Training Summary")
    print("=" * 60)
    print(f"  R²:           {metrics['r2']:.4f}")
    print(f"  Revenue RMSE: ${metrics['rmse']:,.2f}")
    print(f"  Revenue MAE:  ${metrics['mae']:,.2f}")
    print(f"  Revenue MAPE: {metrics['mape']:.2f}%")
    if metrics.get("derived_roi_mae") is not None:
        print(f"  Derived ROI MAE: {metrics['derived_roi_mae']:.2f}%")

    print(f"\n✓ Training completed")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)
