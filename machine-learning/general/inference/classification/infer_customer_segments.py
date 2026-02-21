import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel, StandardScalerModel
from pyspark.ml.classification import LogisticRegressionModel, RandomForestClassificationModel
import findspark

findspark.init()

# Feature columns (must match training) - NO RFM scores to prevent leakage
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


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("CustomerSegmentInference") \
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


def load_model_and_preprocessors(spark, model_dir, model_name):
    """Load trained model and all preprocessors from MinIO"""
    try:
        model_path = f"{model_dir}/{model_name}"
        
        # Load model
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {model_name}")
        
        # Load categorical indexers
        categorical_indexers = []
        for i in range(len(CATEGORICAL_FEATURES)):
            indexer_path = f"{model_dir}/{model_name}_cat_indexer_{i}"
            indexer = StringIndexerModel.load(indexer_path)
            categorical_indexers.append(indexer)
        
        # Load scaler
        scaler_path = f"{model_dir}/{model_name}_scaler"
        scaler = StandardScalerModel.load(scaler_path)
        
        # Load label indexer
        label_indexer_path = f"{model_dir}/{model_name}_label_indexer"
        label_indexer = StringIndexerModel.load(label_indexer_path)
        
        print(f"✓ Loaded model and preprocessors: {model_name}")
        return model, {
            "categorical_indexers": categorical_indexers,
            "scaler": scaler,
            "label_indexer": label_indexer
        }
    except Exception as e:
        print(f"✗ Failed to load model from {model_dir}: {e}")
        return None, None


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
            print(f"⚠️  Warning: Column {col_name} is entirely null")
    
    return True, "Validation passed"


def prepare_features(df, numerical_features, categorical_features, preprocessors):
    """
    Prepare features using saved preprocessors (must match training pipeline)
    - Fill nulls
    - Index categorical features using saved indexers
    - Assemble and scale numerical features using saved scaler
    - Combine all features
    """
    # Fill nulls (same as training)
    df_filled = df.fillna(0, subset=numerical_features)
    df_filled = df_filled.fillna("Unknown", subset=categorical_features)
    
    # Apply categorical indexers
    categorical_indexed_cols = []
    for i, cat_col in enumerate(categorical_features):
        indexer = preprocessors["categorical_indexers"][i]
        df_filled = indexer.transform(df_filled)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
    
    # Assemble numerical features
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features")
    df_filled = numerical_assembler.transform(df_filled)
    
    # Scale numerical features using saved scaler
    scaler = preprocessors["scaler"]
    df_filled = scaler.transform(df_filled)
    
    # Combine all features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    df_vector = final_assembler.transform(df_filled)
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical")
    return df_vector


def extract_feature_importance(model, model_name):
    """Extract feature importance from model"""
    if model_name == "RandomForest":
        importances = model.featureImportances.toArray()
        # First 9 are numerical (scaled), next 2 are categorical
        feature_names = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
        feature_importance = {
            feature_names[i]: float(importances[i]) if i < len(importances) else 0.0
            for i in range(len(feature_names))
        }
        return feature_importance
    return {}


def generate_predictions(spark, df, model, preprocessors, model_name, feature_importance, MODEL_VERSION):
    """Generate predictions and format output according to schema"""
    predictions = model.transform(df)
    
    # Convert prediction index back to label
    label_indexer = preprocessors["label_indexer"]
    labels = label_indexer.labels
    index_to_label = udf(lambda idx: labels[int(idx)], StringType())
    
    # Extract probability for predicted class
    extract_probability = udf(
        lambda prob, pred: float(prob[int(pred)]) if prob else 0.0,
        DoubleType()
    )
    
    # Calculate confidence score (max probability)
    calculate_confidence = udf(
        lambda prob: float(max(prob)) if prob else 0.0,
        DoubleType()
    )
    
    # Calculate RFM score (sum of R, F, M scores)
    calculate_rfm_score = udf(
        lambda r, f, m: float(r + f + m) if all(x is not None for x in [r, f, m]) else 0.0,
        DoubleType()
    )
    
    # Format predictions according to output schema
    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("customer_id"),
        current_timestamp().alias("prediction_date"),
        index_to_label(col("prediction")).alias("predicted_segment"),
        extract_probability(col("probability"), col("prediction")).alias("segment_probability"),
        calculate_rfm_score(
            col("recency_score"), 
            col("frequency_score"), 
            col("monetary_score")
        ).alias("rfm_score"),
        calculate_confidence(col("probability")).alias("confidence_score"),
        lit(MODEL_VERSION).alias("model_version")
    )
    
    # Generate unique prediction IDs
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    output_df = output_df.withColumn("prediction_id", generate_uuid())
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO as Parquet"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Saved predictions to {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {e}")
        return False


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH_CUSTOMERS = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    INPUT_PATH_RFM = f"s3a://{BUCKET_NAME}/transformed/agg_rfm_segmentation.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/customer_segment_predictions"
    MODEL_INPUT_DIR = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/classification/models/customer_segments"

    # ⚠️ MANUAL INTERVENTION REQUIRED: Select model to use for inference
    # Available options: "LogisticRegression", "RandomForest"
    SELECTED_MODEL = "RandomForest"  # <-- CHANGE THIS BASED ON TRAINING RESULTS

    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print("=" * 60)
    print("Customer Segment Classification - Inference Pipeline")
    print("=" * 60)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load model and preprocessors
    model, preprocessors = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None or preprocessors is None:
        print("✗ Inference stopped: Failed to load model")
        return
    
    # Load both tables
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)
    rfm_df = load_data(spark, INPUT_PATH_RFM)
    
    if customers_df is None or rfm_df is None:
        print("✗ Inference stopped: Failed to load data")
        return
    
    # Join datasets
    df = join_datasets(customers_df, rfm_df)
    if df is None:
        print("✗ Inference stopped: Failed to join datasets")
        return
    
    # Validate dataset
    required_cols = ["customer_id"] + NUMERICAL_FEATURES + CATEGORICAL_FEATURES
    is_valid, message = validate_dataset(df, required_cols)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return
    
    print(f"✓ Dataset validated")
    
    # Prepare features
    df_prepared = prepare_features(df, NUMERICAL_FEATURES, CATEGORICAL_FEATURES, preprocessors)
    
    # Extract feature importance
    feature_importance = extract_feature_importance(model, SELECTED_MODEL)
    if feature_importance:
        print(f"✓ Extracted feature importance (top 3):")
        for feat, imp in sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:3]:
            print(f"  - {feat}: {imp:.4f}")
    
    # Generate predictions
    predictions_df = generate_predictions(
        spark, df_prepared, model, preprocessors, SELECTED_MODEL, feature_importance, MODEL_VERSION
    )
    
    # Show sample predictions
    print("\nSample predictions:")
    predictions_df.select(
        "customer_id", "predicted_segment", "segment_probability", "rfm_score", "confidence_score"
    ).show(5, truncate=False)
    
    # Save predictions
    success = save_predictions(predictions_df, OUTPUT_PATH)
    
    if success:
        print("\n" + "=" * 60)
        print("✓ Inference completed successfully")
        print("=" * 60)
    else:
        print("\n✗ Inference failed")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME= "pulse-bucket-1"
    main(BUCKET_NAME)