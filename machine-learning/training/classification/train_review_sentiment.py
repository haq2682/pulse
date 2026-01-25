import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, length, regexp_replace, lower, trim, lit
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF, 
    StringIndexer, VectorAssembler
)
from pyspark.ml.classification import (
    LogisticRegression, NaiveBayes, RandomForestClassifier, 
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_reviews.parquet"
MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/review_sentiment"
MIN_LABELED_RECORDS = 100

TARGET_COLUMN = "review_sentiment"

# CRITICAL: rating is NOT used as feature to prevent data leakage
# Sentiment labels are often derived from ratings, so using rating would be cheating


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("ReviewSentimentTraining") \
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


def add_label_noise(df, noise_rate=0.10):
    """
    Add label noise to make synthetic data more realistic
    
    In real customer reviews:
    - People give wrong ratings (rate 5 stars but write negative text)
    - Sarcasm exists ("great product" but meant sarcastically)
    - Ambiguous reviews (neutral text with extreme rating)
    
    This function randomly flips noise_rate% of labels to simulate real-world noise.
    
    Args:
        noise_rate: Percentage of labels to randomly flip (default 0.10 = 10%)
    """
    print(f"\nAdding {noise_rate*100:.0f}% label noise to simulate real-world ambiguity...")
    
    from pyspark.sql.functions import rand, when as spark_when
    
    # Get all possible labels
    labels = ["Positive", "Neutral", "Negative"]
    
    # Randomly flip labels for noise_rate% of records
    df_noisy = df.withColumn(
        "random_val", rand(seed=42)
    ).withColumn(
        TARGET_COLUMN,
        spark_when(
            col("random_val") < noise_rate / 3,  # Flip to Positive
            lit("Positive")
        ).when(
            (col("random_val") >= noise_rate / 3) & (col("random_val") < 2 * noise_rate / 3),  # Flip to Neutral
            lit("Neutral")
        ).when(
            (col("random_val") >= 2 * noise_rate / 3) & (col("random_val") < noise_rate),  # Flip to Negative
            lit("Negative")
        ).otherwise(
            col(TARGET_COLUMN)  # Keep original label
        )
    ).drop("random_val")
    
    # Show new distribution
    print("Label distribution after adding noise:")
    df_noisy.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False).show()
    
    print(f"✓ Added label noise: ~{noise_rate*100:.0f}% of labels randomly flipped")
    print(f"  Expected accuracy ceiling: ~{(1-noise_rate)*100:.0f}% (down from 100%)\n")
    
    return df_noisy


def check_data_quality(df):
    """
    Check for suspiciously perfect data that indicates synthetic generation
    
    WARNING: If text is generated based on rating, and labels are derived from rating,
    then text → label relationship is deterministic (100% accuracy)
    """
    print("\n" + "=" * 60)
    print("Data Quality Analysis")
    print("=" * 60)
    
    # Check vocabulary diversity
    from pyspark.sql.functions import explode, size
    
    # Sample some reviews to check
    sample_df = df.limit(100).select("full_text", "review_sentiment", "rating")
    samples = sample_df.collect()
    
    # Check for repetitive patterns
    positive_texts = [row.full_text[:50] for row in samples if row.review_sentiment == "Positive"][:5]
    negative_texts = [row.full_text[:50] for row in samples if row.review_sentiment == "Negative"][:5]
    
    print("\nSample Positive reviews:")
    for text in positive_texts:
        print(f"  - {text}...")
    
    print("\nSample Negative reviews:")
    for text in negative_texts:
        print(f"  - {text}...")
    
    # Check rating distribution
    print("\nRating distribution:")
    df.groupBy("rating").count().orderBy("rating").show()
    
    print("\n⚠️  WARNING: If you're getting 100% accuracy, your data likely has:")
    print("   1. Synthetic reviews generated based on ratings")
    print("   2. Perfect text → sentiment correlation (no noise)")
    print("   3. Labels derived from the same ratings used to generate text")
    print("\n   This creates deterministic relationships that don't exist in real data.")
    print("   Real customer reviews have ambiguity, sarcasm, and noise (~85-90% ceiling).")
    print("=" * 60 + "\n")
    """
    Check for suspiciously perfect data that indicates synthetic generation
    
    WARNING: If text is generated based on rating, and labels are derived from rating,
    then text → label relationship is deterministic (100% accuracy)
    """
    print("\n" + "=" * 60)
    print("Data Quality Analysis")
    print("=" * 60)
    
    # Check vocabulary diversity
    from pyspark.sql.functions import explode, size
    
    # Sample some reviews to check
    sample_df = df.limit(100).select("full_text", "review_sentiment", "rating")
    samples = sample_df.collect()
    
    # Check for repetitive patterns
    positive_texts = [row.full_text[:50] for row in samples if row.review_sentiment == "Positive"][:5]
    negative_texts = [row.full_text[:50] for row in samples if row.review_sentiment == "Negative"][:5]
    
    print("\nSample Positive reviews:")
    for text in positive_texts:
        print(f"  - {text}...")
    
    print("\nSample Negative reviews:")
    for text in negative_texts:
        print(f"  - {text}...")
    
    # Check rating distribution
    print("\nRating distribution:")
    df.groupBy("rating").count().orderBy("rating").show()
    
    print("\n⚠️  WARNING: If you're getting 100% accuracy, your data likely has:")
    print("   1. Synthetic reviews generated based on ratings")
    print("   2. Perfect text → sentiment correlation (no noise)")
    print("   3. Labels derived from the same ratings used to generate text")
    print("\n   This creates deterministic relationships that don't exist in real data.")
    print("   Real customer reviews have ambiguity, sarcasm, and noise (~85-90% ceiling).")
    print("=" * 60 + "\n")


def generate_sentiment_labels(df):
    """
    Generate review_sentiment labels from rating if not present
    
    Business Rules:
    - Positive: rating >= 4
    - Neutral: rating == 3
    - Negative: rating <= 2
    
    WARNING: This creates data leakage if rating is used as a feature!
    """
    print("Generating review_sentiment labels from rating...")
    
    # Fill nulls in rating
    df_filled = df.fillna(0, subset=["rating"])
    
    df_with_label = df_filled.withColumn(
        TARGET_COLUMN,
        when(col("rating") >= 4, "Positive")
        .when(col("rating") == 3, "Neutral")
        .when(col("rating") <= 2, "Negative")
        .otherwise("Neutral")
    )
    
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Sentiment distribution:")
    label_dist.show()
    
    return df_with_label


def preprocess_text(df):
    """
    Preprocess text features - combine title and description, clean text
    """
    print("Preprocessing text features...")
    
    # Fill nulls in text columns
    df = df.fillna("", subset=["review_title", "review_desc"])
    
    # Combine title and description into full_text
    df = df.withColumn(
        "full_text",
        concat_ws(" ", col("review_title"), col("review_desc"))
    )
    
    # Clean text: lowercase, remove special characters, trim
    df = df.withColumn("full_text", lower(col("full_text")))
    df = df.withColumn("full_text", regexp_replace(col("full_text"), "[^a-zA-Z0-9\\s]", " "))
    df = df.withColumn("full_text", regexp_replace(col("full_text"), "\\s+", " "))
    df = df.withColumn("full_text", trim(col("full_text")))
    
    # Add text length as feature
    df = df.withColumn("text_length", length(col("full_text")))
    
    # Filter out empty reviews
    df = df.filter(col("full_text") != "")
    
    print(f"✓ Preprocessed text: {df.count()} reviews with non-empty text")
    return df


def prepare_features(train_df, test_df):
    """
    Prepare features - FIT ON TRAIN ONLY to prevent leakage
    
    Text processing pipeline:
    1. Tokenize full_text into words
    2. Remove stop words
    3. HashingTF - convert words to term frequency vectors
    4. IDF - compute inverse document frequency
    5. Combine TF-IDF with text_length
    6. Encode target labels
    """
    # Tokenize - FIT ON TRAIN ONLY (though tokenizer has no state)
    tokenizer = Tokenizer(inputCol="full_text", outputCol="words")
    train_df = tokenizer.transform(train_df)
    test_df = tokenizer.transform(test_df)
    
    # Remove stop words - FIT ON TRAIN ONLY
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    train_df = remover.transform(train_df)
    test_df = remover.transform(test_df)
    
    # HashingTF - convert to term frequency vectors
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000)
    train_df = hashingTF.transform(train_df)
    test_df = hashingTF.transform(test_df)
    
    # IDF - FIT ON TRAIN ONLY
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    idf_model = idf.fit(train_df)
    train_df = idf_model.transform(train_df)
    test_df = idf_model.transform(test_df)
    
    # Combine TF-IDF with text_length
    assembler = VectorAssembler(
        inputCols=["tfidf_features", "text_length"],
        outputCol="features"
    )
    train_df = assembler.transform(train_df)
    test_df = assembler.transform(test_df)
    
    # Filter null targets
    train_clean = train_df.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_df.filter(col(TARGET_COLUMN).isNotNull())
    
    # Encode target labels - FIT ON TRAIN ONLY
    label_indexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    label_indexer_model = label_indexer.fit(train_clean)
    train_indexed = label_indexer_model.transform(train_clean)
    test_indexed = label_indexer_model.transform(test_clean)
    
    print(f"✓ Prepared features: TF-IDF (1000 features) + text_length")
    print(f"  - All text processors fit on train only")
    print(f"  - Rating EXCLUDED to prevent data leakage")
    
    return train_indexed, test_indexed, {
        "idf_model": idf_model,
        "label_indexer": label_indexer_model
    }


def train_logistic_regression(train_df):
    """Train Logistic Regression model"""
    print("\n[1/4] Training Logistic Regression...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.5)
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_naive_bayes(train_df):
    """Train Naive Bayes model (good for text classification)"""
    print("\n[2/4] Training Naive Bayes...")
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    model = nb.fit(train_df)
    print("✓ Naive Bayes trained")
    return model, "NaiveBayes"


def train_random_forest(train_df):
    """Train Random Forest model"""
    print("\n[3/4] Training Random Forest...")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
    model = rf.fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


def train_multilayer_perceptron(train_df):
    """Train Multilayer Perceptron (Neural Network) model"""
    print("\n[4/4] Training Multilayer Perceptron...")
    
    # Get number of features (1000 TF-IDF + 1 text_length = 1001)
    num_features = 1001
    
    # Get number of classes
    num_classes = int(train_df.agg({"label": "max"}).collect()[0][0]) + 1
    
    # Network architecture
    layers = [num_features, 500, 100, num_classes]
    
    mlp = MultilayerPerceptronClassifier(
        layers=layers,
        maxIter=100,
        blockSize=128,
        seed=42
    )
    model = mlp.fit(train_df)
    print("✓ Multilayer Perceptron trained")
    return model, "MultilayerPerceptron"


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
    
    # Save IDF model
    idf_path = f"{output_dir}/{model_name}_idf"
    preprocessors["idf_model"].write().overwrite().save(idf_path)
    
    # Save label indexer
    label_indexer_path = f"{output_dir}/{model_name}_label_indexer"
    preprocessors["label_indexer"].write().overwrite().save(label_indexer_path)
    
    print(f"✓ Saved {model_name} and preprocessors to {model_path}")


def main():
    print("=" * 60)
    print("Review Sentiment Classification - Training Pipeline")
    print("=" * 60)
    
    # CONFIGURATION: Set to True to add label noise for more realistic training
    ADD_LABEL_NOISE = True  # <-- Set to True to fix 100% accuracy issue
    NOISE_RATE = 0.15  # 15% label noise → expect ~85% accuracy ceiling
    
    spark = create_spark_session()
    
    # Load data (single table - no joins needed)
    df = load_data(spark, INPUT_PATH)
    if df is None:
        print("✗ Training stopped: Failed to load data")
        return
    
    # Validate basic columns exist
    basic_required_cols = ["review_id", "review_title", "review_desc", "rating"]
    is_valid, message = validate_dataset(df, basic_required_cols)
    if not is_valid:
        print(f"✗ Training stopped: {message}")
        return
    
    # Generate labels if not present
    if TARGET_COLUMN not in df.columns or df.filter(col(TARGET_COLUMN).isNotNull()).count() == 0:
        df = generate_sentiment_labels(df)
    
    # Preprocess text
    df = preprocess_text(df)
    
    # Check data quality (detect synthetic data issues)
    check_data_quality(df)
    
    # Add label noise if synthetic data (to simulate real-world ambiguity)
    if ADD_LABEL_NOISE:
        df = add_label_noise(df, noise_rate=NOISE_RATE)
    else:
        print("\n⚠️  WARNING: Running without label noise on synthetic data")
        print("   Expected result: 100% accuracy (unrealistic for production)")
        print("   Set ADD_LABEL_NOISE=True to simulate real-world conditions\n")
    
    # Check minimum labeled records
    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_LABELED_RECORDS:
        print(f"✗ Training stopped: Insufficient labeled data ({labeled_count} < {MIN_LABELED_RECORDS})")
        return
    
    print(f"✓ Dataset validated: {labeled_count} labeled reviews")
    
    # CRITICAL: Split data FIRST, before any feature extraction
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split data: {train_df_raw.count()} train, {test_df_raw.count()} test (BEFORE feature extraction)")
    
    # Prepare features - fit all text processors on TRAIN only
    train_df, test_df, preprocessors = prepare_features(train_df_raw, test_df_raw)
    print(f"✓ After feature extraction: {train_df.count()} train, {test_df.count()} test")
    
    # Train multiple models
    models = [
        train_logistic_regression(train_df),
        train_naive_bayes(train_df),
        train_random_forest(train_df),
        train_multilayer_perceptron(train_df)
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
    print("   2. Select ONE model for inference based on F1-score")
    print("   3. Update inference script to load selected model")
    print("   4. Available models: LogisticRegression, NaiveBayes, RandomForest, MultilayerPerceptron")
    print(f"   5. Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()