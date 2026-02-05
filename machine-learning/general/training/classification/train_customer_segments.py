import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.multi_bucket_loader import (
    load_data_from_all_buckets,
    validate_training_data,
    get_general_model_output_path,
    get_training_window,
    GENERAL_MODEL_BUCKET
)

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "customer_segments"
INPUT_RELATIVE_PATH = "transformed/agg_customers.parquet"
INPUT_RFM_PATH = "transformed/agg_rfm_segmentation.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("classification", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)

# Feature columns - ONLY raw behavioral features, NOT RFM scores (to prevent leakage)
NUMERICAL_FEATURES = [
    "days_since_last_order",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "avg_days_between_orders",
    "customer_lifetime_value",
    "cancellation_rate"
]

CATEGORICAL_FEATURES = [
    "preferred_device_type",
    "preferred_referrer_source"
]

TARGET_COLUMN = "customer_segment_label"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("CustomerSegmentTraining") \
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


def join_datasets(customers_df, rfm_df):
    """
    Join agg_customers and agg_rfm_segmentation on customer_id
    Only select unique columns from RFM table to avoid duplicates
    
    Both tables share: recency_score, frequency_score, monetary_score, 
    customer_segment_label, rfm_segment, rfm_overall_score, rfm_category, churn_risk
    
    Unique to RFM: days_since_last_order, total_orders_rfm, total_revenue_rfm,
    engagement_level, purchase_behavior, spending_pattern
    """
    if customers_df is None or rfm_df is None:
        return None
    
    # Select only unique columns from RFM table (avoid duplicates)
    rfm_unique_cols = ["customer_id", "days_since_last_order"]
    rfm_selected = rfm_df.select(*rfm_unique_cols)
    
    # Join on customer_id
    joined_df = customers_df.join(rfm_selected, on="customer_id", how="left")
    
    print(f"✓ Joined datasets: {joined_df.count()} records")
    return joined_df


def validate_dataset(df, required_columns):
    """Validate dataset structure and content"""
    if df is None:
        return False, "Dataset is None"
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    
    for col_name in required_columns:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            return False, f"Column {col_name} is entirely null"
    
    return True, "Validation passed"


def generate_rfm_segment_labels(df):
    """
    Generate customer_segment_label using RFM business rules
    
    Segments:
    - Champions: R=5, F=5, M=5
    - Loyal Customers: R>=3, F>=4, M>=4
    - Potential Loyalists: R>=3, F>=3, M>=2
    - At Risk: R<=2, F>=4, M>=4
    - Cant Lose Them: R<=2, F>=4, M>=3
    - Hibernating: R<=2, F<=2, M>=2
    - Lost: R<=2, F<=2, M<=2
    - New Customers: R>=4, F<=2, M<=2
    - Promising: R>=3, F<=2, M<=2
    - Need Attention: R<=3, F<=3, M<=3
    """
    print("Generating customer_segment_label using RFM rules...")
    
    # Fill nulls in RFM scores to prevent null labels
    rfm_cols = ["recency_score", "frequency_score", "monetary_score"]
    df_filled = df.fillna(0, subset=rfm_cols)
    
    df_with_label = df_filled.withColumn(
        TARGET_COLUMN,
        when(
            (col("recency_score") == 5) & (col("frequency_score") == 5) & (col("monetary_score") == 5),
            "Champions"
        ).when(
            (col("recency_score") >= 3) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
            "Loyal Customers"
        ).when(
            (col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 2),
            "Potential Loyalists"
        ).when(
            (col("recency_score") <= 2) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
            "At Risk"
        ).when(
            (col("recency_score") <= 2) & (col("frequency_score") >= 4) & (col("monetary_score") >= 3),
            "Cant Lose Them"
        ).when(
            (col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") >= 2),
            "Hibernating"
        ).when(
            (col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2),
            "Lost"
        ).when(
            (col("recency_score") >= 4) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2),
            "New Customers"
        ).when(
            (col("recency_score") >= 3) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2),
            "Promising"
        ).otherwise("Need Attention")
    )
    
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Segment distribution:")
    label_dist.show()
    
    return df_with_label


def prepare_features(train_df, test_df, numerical_features, categorical_features):
    """
    Prepare features with encoding and scaling - FIT ON TRAIN ONLY
    
    CRITICAL: To prevent data leakage:
    1. Fit all preprocessors (indexers, scaler, label indexer) on TRAINING data only
    2. Transform both train and test using fitted preprocessors
    
    Steps:
    - Fill nulls with 0 for numerical features
    - Fill nulls with 'Unknown' for categorical features
    - Index categorical features (fit on train)
    - Assemble and scale numerical features (fit scaler on train)
    - Combine all features
    - Encode target labels (fit on train)
    """
    # Fill nulls - both train and test
    train_filled = train_df.fillna(0, subset=numerical_features)
    train_filled = train_filled.fillna("Unknown", subset=categorical_features)
    
    test_filled = test_df.fillna(0, subset=numerical_features)
    test_filled = test_filled.fillna("Unknown", subset=categorical_features)
    
    # Filter null targets
    train_clean = train_filled.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_filled.filter(col(TARGET_COLUMN).isNotNull())
    
    record_count_before = train_filled.count()
    record_count_after = train_clean.count()
    if record_count_before != record_count_after:
        print(f"⚠️  Filtered {record_count_before - record_count_after} training records with null {TARGET_COLUMN}")
    
    # Index categorical features - FIT ON TRAIN ONLY
    categorical_indexed_cols = []
    categorical_indexers = []
    
    for cat_col in categorical_features:
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_indexed", handleInvalid="keep")
        indexer_model = indexer.fit(train_clean)  # FIT ON TRAIN ONLY
        train_clean = indexer_model.transform(train_clean)
        test_clean = indexer_model.transform(test_clean)  # TRANSFORM TEST
        categorical_indexed_cols.append(f"{cat_col}_indexed")
        categorical_indexers.append(indexer_model)
    
    # Assemble numerical features - both train and test
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features")
    train_clean = numerical_assembler.transform(train_clean)
    test_clean = numerical_assembler.transform(test_clean)
    
    # Scale numerical features - FIT SCALER ON TRAIN ONLY
    scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")
    scaler_model = scaler.fit(train_clean)  # FIT ON TRAIN ONLY
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)  # TRANSFORM TEST
    
    # Combine scaled numerical and indexed categorical features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Encode target labels - FIT ON TRAIN ONLY
    label_indexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    label_indexer_model = label_indexer.fit(train_vector)  # FIT ON TRAIN ONLY
    train_indexed = label_indexer_model.transform(train_vector)
    test_indexed = label_indexer_model.transform(test_vector)  # TRANSFORM TEST
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical")
    print(f"  - Numerical features scaled with StandardScaler (fit on train only)")
    print(f"  - Categorical features indexed (fit on train only)")
    print(f"  - RFM scores EXCLUDED to prevent data leakage")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model,
        "label_indexer": label_indexer_model
    }


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
    """Evaluate model and compute metrics"""
    predictions = model.transform(test_df)
    
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


def save_models(model, preprocessors, output_dir, model_name):
    """Save trained model and preprocessors to MinIO"""
    model_path = f"{output_dir}/{model_name}"
    
    model.write().overwrite().save(model_path)
    
    # Save preprocessors
    for i, indexer in enumerate(preprocessors["categorical_indexers"]):
        indexer_path = f"{output_dir}/{model_name}_cat_indexer_{i}"
        indexer.write().overwrite().save(indexer_path)
    
    scaler_path = f"{output_dir}/{model_name}_scaler"
    preprocessors["scaler"].write().overwrite().save(scaler_path)
    
    label_indexer_path = f"{output_dir}/{model_name}_label_indexer"
    preprocessors["label_indexer"].write().overwrite().save(label_indexer_path)
    
    print(f"✓ Saved {model_name} and preprocessors to {model_path}")


def main():
    print("=" * 60)
    print("Customer Segment Classification - Training Pipeline")
    print("=" * 60)
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load data from all buckets
    print("\nStep 1: Loading data from all MinIO buckets...")
    customers_df, customers_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=NUMERICAL_FEATURES,
        filter_nulls=True
    )
    
    if customers_df is None:
        print("⚠️  No customer data available. Skipping training.")
        spark.stop()
        return
    
    # Validate training data window
    is_valid, customers_df = validate_training_data(
        customers_df, customers_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        spark.stop()
        return
    
    # Load RFM data from all buckets
    rfm_required_cols = ["customer_id", "days_since_last_order"]
    rfm_df, _ = load_data_from_all_buckets(
        spark, INPUT_RFM_PATH, 
        required_columns=rfm_required_cols,
        filter_nulls=True
    )
    
    if rfm_df is None:
        print("⚠️  Training skipped: Failed to load RFM data")
        spark.stop()
        return
    
    # Join datasets
    df = join_datasets(customers_df, rfm_df)
    if df is None:
        print("⚠️  Training skipped: Failed to join datasets")
        spark.stop()
        return
    
    # Generate labels if not present
    if TARGET_COLUMN not in df.columns:
        df = generate_rfm_segment_labels(df)
    
    # Validate dataset
    all_required_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES + [TARGET_COLUMN]
    is_valid, message = validate_dataset(df, all_required_cols)
    if not is_valid:
        print(f"⚠️  Training skipped: {message}")
        spark.stop()
        return
    
    # Check minimum labeled records
    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_RECORDS:
        print(f"⚠️  Training skipped: Insufficient labeled data ({labeled_count} < {MIN_RECORDS})")
        spark.stop()
        return
    
    print(f"✓ Dataset validated: {labeled_count} labeled records")
    
    # CRITICAL: Split data FIRST, before any preprocessing
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split data: {train_df_raw.count()} train, {test_df_raw.count()} test (BEFORE preprocessing)")
    
    # Prepare features - fit preprocessors on TRAIN only, transform both
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES
    )
    print(f"✓ After preprocessing: {train_df.count()} train, {test_df.count()} test")
    
    # Train multiple models
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
        
        # Save each model with preprocessors
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)
    
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
    print("   3. Update inference script to load selected model")
    print("   4. Available models: LogisticRegression, RandomForest")
    print(f"   5. Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()