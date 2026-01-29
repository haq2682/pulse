import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, avg, max as spark_max, min as spark_min
)
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/customer_churn"
MIN_LABELED_RECORDS = 100

# Feature columns used for training
FEATURE_COLUMNS = [
    "days_since_last_purchase",
    "order_frequency",
    "customer_lifetime_value",
    "avg_days_between_orders",
    "total_orders",
    "total_revenue",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "days_since_last_login",
    "customer_tenure_days",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "avg_order_value",
    "cancellation_rate"
]

TARGET_COLUMN = "churn_risk"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("CustomerChurnTraining") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "1000") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ) \
        .config("inferSchema", "true") \
        .config("mergeSchema", "true") \
        .getOrCreate()


def load_data(spark, path):
    """Load data from MinIO"""
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records from {path}")
        return df
    except Exception as e:
        print(f"✗ Failed to load data from {path}: {e}")
        return None


def validate_dataset(df, required_columns):
    """
    Validate dataset structure and content
    Returns: (is_valid, message)
    """
    if df is None:
        return False, "Dataset is None"
    
    # Check required columns exist
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    
    # Check columns have at least some non-null values
    for col_name in required_columns:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            return False, f"Column {col_name} is entirely null"
    
    return True, "Validation passed"


def generate_churn_labels(df):
    """
    Generate churn_risk labels using business rules
    
    Business Rules:
    - High Risk: days_since_last_purchase > 60 OR (cart_abandonment_rate > 0.7 AND order_frequency < 2)
    - Low Risk: days_since_last_purchase <= 30 AND order_frequency >= 3 AND cart_abandonment_rate < 0.3
    - Medium Risk: Everything else
    """
    print("Generating churn_risk labels...")
    
    # Fill nulls in columns used for label generation to prevent null labels
    label_gen_cols = ["days_since_last_purchase", "cart_abandonment_rate", "order_frequency"]
    df_filled = df.fillna(0, subset=label_gen_cols)
    
    df_with_label = df_filled.withColumn(
        TARGET_COLUMN,
        when(
            (col("days_since_last_purchase") > 60) | 
            ((col("cart_abandonment_rate") > 0.7) & (col("order_frequency") < 2)),
            "High"
        ).when(
            (col("days_since_last_purchase") <= 30) & 
            (col("order_frequency") >= 3) & 
            (col("cart_abandonment_rate") < 0.3),
            "Low"
        ).otherwise("Medium")
    )
    
    # Show label distribution
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Label distribution:")
    label_dist.show()
    
    return df_with_label


def prepare_features(df, feature_cols):
    """
    Prepare features for ML training
    - Fill nulls with 0 (business decision: missing = no activity)
    - Filter out null target values
    - Assemble features into vector
    - Encode target labels
    """
    # Fill nulls with 0
    df_filled = df.fillna(0, subset=feature_cols)
    
    # Filter out records with null target (StringIndexer doesn't handle nulls)
    df_clean = df_filled.filter(col(TARGET_COLUMN).isNotNull())
    
    record_count_before = df_filled.count()
    record_count_after = df_clean.count()
    if record_count_before != record_count_after:
        print(f"⚠️  Filtered {record_count_before - record_count_after} records with null {TARGET_COLUMN}")
    
    # Assemble features into vector
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector = assembler.transform(df_clean)
    
    # Encode target labels (High=2, Medium=1, Low=0)
    indexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    indexer_model = indexer.fit(df_vector)
    df_indexed = indexer_model.transform(df_vector)
    
    print(f"✓ Prepared features: {len(feature_cols)} columns vectorized")
    return df_indexed, indexer_model


def train_logistic_regression(train_df):
    """Train Logistic Regression model"""
    print("\n[1/2] Training Logistic Regression...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.5)
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_random_forest(train_df):
    """Train Random Forest model"""
    print("\n[2/2] Training Random Forest...")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
    model = rf.fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


def evaluate_model(model, test_df, model_name):
    """
    Evaluate model and compute metrics
    Returns: dict of metrics
    """
    predictions = model.transform(test_df)
    
    # Multiclass metrics
    mc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    
    accuracy = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
    precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
    recall = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})
    f1 = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})
    
    metrics = {
        "model_name": model_name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1
    }
    
    print(f"\n{model_name} Metrics:")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    
    return metrics


def save_model(model, indexer_model, output_dir, model_name):
    """Save trained model and indexer to MinIO"""
    model_path = f"{output_dir}/{model_name}"
    indexer_path = f"{output_dir}/{model_name}_indexer"
    
    model.write().overwrite().save(model_path)
    indexer_model.write().overwrite().save(indexer_path)
    
    print(f"✓ Saved {model_name} to {model_path}")


def main():
    print("=" * 60)
    print("Customer Churn Prediction - Training Pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load data
    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("✗ Training stopped: Failed to load data")
        return
    
    # Generate labels if not present
    if TARGET_COLUMN not in df.columns:
        df = generate_churn_labels(df)
    
    # Validate dataset
    all_required_cols = FEATURE_COLUMNS + [TARGET_COLUMN]
    is_valid, message = validate_dataset(df, all_required_cols)
    if not is_valid:
        print(f"✗ Training stopped: {message}")
        return
    
    # Check minimum labeled records
    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_LABELED_RECORDS:
        print(f"✗ Training stopped: Insufficient labeled data ({labeled_count} < {MIN_LABELED_RECORDS})")
        return
    
    print(f"✓ Dataset validated: {labeled_count} labeled records")
    
    # Prepare features
    df_prepared, indexer_model = prepare_features(df, FEATURE_COLUMNS)
    
    # Split data (80/20 train/test)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split data: {train_df.count()} train, {test_df.count()} test")
    
    # Train multiple models (GBT excluded - only supports binary classification)
    models = [
        train_logistic_regression(train_df),
        train_random_forest(train_df)
    ]
    
    # Evaluate all models
    print("\n" + "=" * 60)
    print("Model Evaluation & Comparison")
    print("=" * 60)
    
    all_metrics = []
    for model, model_name in models:
        metrics = evaluate_model(model, test_df, model_name)
        all_metrics.append(metrics)
        
        # Save each model
        save_model(model, indexer_model, MODEL_OUTPUT_DIR, model_name)
    
    # Compare models
    print("\n" + "=" * 60)
    print("Model Comparison Summary")
    print("=" * 60)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | Acc: {m['accuracy']:.4f}")
    
    print("\n" + "=" * 60)
    print("✓ Training completed successfully")
    print("=" * 60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   1. Review model metrics above")
    print("   2. Select ONE model for inference based on F1-score or business needs")
    print(f"   3. Update inference script to load selected model")
    print("   4. Available models: LogisticRegression, RandomForest")
    print(f"   5. Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()