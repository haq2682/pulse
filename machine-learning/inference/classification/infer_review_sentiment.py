import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp, concat_ws, length, regexp_replace, lower, trim, when
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDFModel, StringIndexerModel, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegressionModel, NaiveBayesModel, RandomForestClassificationModel,
    MultilayerPerceptronClassificationModel
)
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_reviews.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/review_sentiment_predictions"
MODEL_INPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/review_sentiment"

# ⚠️ MANUAL INTERVENTION REQUIRED: Select model to use for inference
# Available options: "LogisticRegression", "NaiveBayes", "RandomForest", "MultilayerPerceptron"
SELECTED_MODEL = "NaiveBayes"  # <-- CHANGE THIS BASED ON TRAINING RESULTS

MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("ReviewSentimentInference") \
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


def load_model_and_preprocessors(spark, model_dir, model_name):
    """Load trained model and all preprocessors from MinIO"""
    try:
        model_path = f"{model_dir}/{model_name}"
        
        # Load model
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "NaiveBayes":
            model = NaiveBayesModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        elif model_name == "MultilayerPerceptron":
            model = MultilayerPerceptronClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model type: {model_name}")
        
        # Load IDF model
        idf_path = f"{model_dir}/{model_name}_idf"
        idf_model = IDFModel.load(idf_path)
        
        # Load label indexer
        label_indexer_path = f"{model_dir}/{model_name}_label_indexer"
        label_indexer = StringIndexerModel.load(label_indexer_path)
        
        print(f"✓ Loaded model and preprocessors: {model_name}")
        return model, {
            "idf_model": idf_model,
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


def preprocess_text(df):
    """Preprocess text features (same as training)"""
    print("Preprocessing text features...")
    
    # Fill nulls
    df = df.fillna("", subset=["review_title", "review_desc"])
    
    # Combine title and description
    df = df.withColumn("full_text", concat_ws(" ", col("review_title"), col("review_desc")))
    
    # Clean text
    df = df.withColumn("full_text", lower(col("full_text")))
    df = df.withColumn("full_text", regexp_replace(col("full_text"), "[^a-zA-Z0-9\\s]", " "))
    df = df.withColumn("full_text", regexp_replace(col("full_text"), "\\s+", " "))
    df = df.withColumn("full_text", trim(col("full_text")))
    
    # Add text length
    df = df.withColumn("text_length", length(col("full_text")))
    
    # Filter empty reviews
    df = df.filter(col("full_text") != "")
    
    print(f"✓ Preprocessed text: {df.count()} reviews with non-empty text")
    return df


def prepare_features(df, preprocessors):
    """
    Prepare features using saved preprocessors (must match training pipeline)
    """
    # Tokenize
    tokenizer = Tokenizer(inputCol="full_text", outputCol="words")
    df = tokenizer.transform(df)
    
    # Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)
    
    # HashingTF
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000)
    df = hashingTF.transform(df)
    
    # IDF using saved model
    idf_model = preprocessors["idf_model"]
    df = idf_model.transform(df)
    
    # Combine features
    assembler = VectorAssembler(
        inputCols=["tfidf_features", "text_length"],
        outputCol="features"
    )
    df = assembler.transform(df)
    
    print(f"✓ Prepared features: TF-IDF (1000 features) + text_length")
    return df


def extract_key_phrases(df):
    """Extract key phrases from filtered_words (top 3 words as simple approach)"""
    extract_phrases = udf(
        lambda words: str(words[:3] if words and len(words) > 0 else []),
        StringType()
    )
    
    df = df.withColumn("key_phrases", extract_phrases(col("filtered_words")))
    return df


def generate_predictions(spark, df, model, preprocessors, model_name):
    """Generate predictions and format output according to schema"""
    predictions = model.transform(df)
    
    # Convert prediction index back to label
    label_indexer = preprocessors["label_indexer"]
    labels = label_indexer.labels
    index_to_label = udf(lambda idx: labels[int(idx)], StringType())
    
    # Map sentiment to score (-1 to 1)
    sentiment_to_score = udf(
        lambda sentiment: 1.0 if sentiment == "Positive" else (-1.0 if sentiment == "Negative" else 0.0),
        DoubleType()
    )
    
    # Calculate confidence score (max probability)
    calculate_confidence = udf(
        lambda prob: float(max(prob)) if prob else 0.0,
        DoubleType()
    )
    
    # Extract key phrases
    df_with_phrases = extract_key_phrases(predictions)
    
    # Format predictions according to output schema
    output_df = df_with_phrases.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("review_id"),
        col("product_id"),
        current_timestamp().alias("prediction_date"),
        index_to_label(col("prediction")).alias("predicted_sentiment"),
        sentiment_to_score(index_to_label(col("prediction"))).alias("sentiment_score"),
        calculate_confidence(col("probability")).alias("confidence_score"),
        col("key_phrases"),
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


def main():
    print("=" * 60)
    print("Review Sentiment Classification - Inference Pipeline")
    print("=" * 60)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load model and preprocessors
    model, preprocessors = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None or preprocessors is None:
        print("✗ Inference stopped: Failed to load model")
        return
    
    # Load data
    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("✗ Inference stopped: Failed to load data")
        return
    
    # Validate dataset
    required_cols = ["review_id", "product_id", "review_title", "review_desc"]
    is_valid, message = validate_dataset(df, required_cols)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return
    
    print(f"✓ Dataset validated")
    
    # Preprocess text
    df = preprocess_text(df)
    
    # Prepare features
    df_prepared = prepare_features(df, preprocessors)
    
    # Generate predictions
    predictions_df = generate_predictions(spark, df_prepared, model, preprocessors, SELECTED_MODEL)
    
    # Show sample predictions
    print("\nSample predictions:")
    predictions_df.select(
        "review_id", "predicted_sentiment", "sentiment_score", "confidence_score"
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
    main()
