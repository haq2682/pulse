import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import (
    RandomForestClassifier, DecisionTreeClassifier,
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH_PAYMENTS = f"s3a://{BUCKET_NAME}/transformed/agg_payments.parquet"
INPUT_PATH_ORDERS = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_PATH_CUSTOMERS = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/payment_success"
MIN_LABELED_RECORDS = 100

# Features - NO LEAKAGE: exclude anything known only after payment completion
NUMERICAL_FEATURES = [
    "total_amount",
    "processing_fee",
    "subtotal",
    "tax_amount",
    "shipping_cost",
    "total_discount",
    "total_quantity",
    "unique_products_ordered"
]

CATEGORICAL_FEATURES = [
    "payment_method",
    "payment_provider",
    "country",
    "currency"
]

TARGET_COLUMN = "payment_status"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("PaymentSuccessTraining") \
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


def join_datasets(payments_df, orders_df, customers_df):
    """
    Join agg_payments -> agg_orders -> agg_customers
    Select only necessary columns to avoid duplicates
    
    Join strategy:
    1. payments LEFT JOIN orders ON order_id
    2. result LEFT JOIN customers ON customer_id
    """
    if payments_df is None or orders_df is None or customers_df is None:
        return None
    
    # Select unique columns from orders (avoid duplicate order_id)
    orders_cols = [
        "order_id",
        "customer_id",
        "total_amount",
        "subtotal",
        "tax_amount",
        "shipping_cost",
        "total_discount",
        "currency",
        "total_quantity",
        "unique_products_ordered"
    ]
    orders_selected = orders_df.select(*orders_cols)
    
    # Select unique columns from customers (avoid duplicate customer_id)
    customers_cols = ["customer_id", "country"]
    customers_selected = customers_df.select(*customers_cols)
    
    # Join payments with orders
    joined_df = payments_df.join(orders_selected, on="order_id", how="left")
    
    # Join result with customers
    joined_df = joined_df.join(customers_selected, on="customer_id", how="left")
    
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


def clean_target_labels(df):
    """
    Clean and standardize payment_status labels
    Map variations to standard labels: 'Completed', 'Failed', 'Pending'
    """
    print("Cleaning payment_status labels...")
    
    df_cleaned = df.withColumn(
        TARGET_COLUMN,
        when(col(TARGET_COLUMN).isin("Completed", "Success", "Successful"), "Completed")
        .when(col(TARGET_COLUMN).isin("Failed", "Failure", "Declined"), "Failed")
        .when(col(TARGET_COLUMN).isin("Pending", "Processing"), "Pending")
        .otherwise(col(TARGET_COLUMN))
    )
    
    label_dist = df_cleaned.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Label distribution:")
    label_dist.show()
    
    return df_cleaned


def prepare_features(train_df, test_df, numerical_features, categorical_features):
    """
    Prepare features - FIT ON TRAIN ONLY to prevent leakage
    
    Steps:
    - Fill nulls with 0 for numerical, 'Unknown' for categorical
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
        indexer_model = indexer.fit(train_clean)
        train_clean = indexer_model.transform(train_clean)
        test_clean = indexer_model.transform(test_clean)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
        categorical_indexers.append(indexer_model)
    
    # Assemble numerical features
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features")
    train_clean = numerical_assembler.transform(train_clean)
    test_clean = numerical_assembler.transform(test_clean)
    
    # Scale numerical features - FIT ON TRAIN ONLY (necessary due to different scales)
    scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")
    scaler_model = scaler.fit(train_clean)
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)
    
    # Combine all features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Encode target labels - FIT ON TRAIN ONLY
    label_indexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    label_indexer_model = label_indexer.fit(train_vector)
    train_indexed = label_indexer_model.transform(train_vector)
    test_indexed = label_indexer_model.transform(test_vector)
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical")
    print(f"  - Numerical features scaled with StandardScaler (fit on train only)")
    print(f"  - Categorical features indexed (fit on train only)")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model,
        "label_indexer": label_indexer_model
    }


def train_random_forest_optimized(train_df):
    """Train an optimized Random Forest"""

    print("\n[1/3] Training Optimized Random Forest (FAST MODE)...")

    # Cache training data for performance
    train_df.cache()
    train_df.count()  # materialize cache

    rf = RandomForestClassifier(
        numTrees=180,                 # Strong but not excessive
        maxDepth=14,                  # Good balance: accuracy vs speed
        minInstancesPerNode=2,        # Prevent overfitting
        featureSubsetStrategy="sqrt", # Best general-purpose setting
        subsamplingRate=0.8,          # Speed + generalization
        seed=42,
        maxBins=128                   # Reduce memory overhead
    )

    model = rf.fit(train_df)

    print("✓ Optimized Random Forest trained (fast mode)")
    return model, "RandomForestOptimized"


def train_decision_tree(train_df):
    """Train Decision Tree model"""
    print("\n[2/3] Training Decision Tree...")
    dt = DecisionTreeClassifier(maxDepth=10, seed=42)
    model = dt.fit(train_df)
    print("✓ Decision Tree trained")
    return model, "DecisionTree"


def train_multilayer_perceptron(train_df):
    """Train Multilayer Perceptron (Neural Network) model"""
    print("\n[3/3] Training Multilayer Perceptron (Neural Network)...")
    
    # Get number of features from training data
    num_features = len(train_df.select("features").first()[0])
    
    # Get number of classes from label indexer
    num_classes = int(train_df.agg({"label": "max"}).collect()[0][0]) + 1
    
    # Define network architecture: input -> hidden layers -> output
    # Example: 12 features -> 24 neurons -> 12 neurons -> 3 classes
    layers = [num_features, num_features * 2, num_features, num_classes]
    
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
    print("Payment Success Prediction - Training Pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load all three tables
    payments_df = load_data(spark, INPUT_PATH_PAYMENTS)
    orders_df = load_data(spark, INPUT_PATH_ORDERS)
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)
    
    if payments_df is None or orders_df is None or customers_df is None:
        print("✗ Training stopped: Failed to load data")
        return
    
    # Join datasets
    df = join_datasets(payments_df, orders_df, customers_df)
    if df is None:
        print("✗ Training stopped: Failed to join datasets")
        return
    
    # Clean target labels
    df = clean_target_labels(df)
    
    # Validate dataset
    all_required_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES + [TARGET_COLUMN]
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
    
    # CRITICAL: Split data FIRST, before any preprocessing
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split data: {train_df_raw.count()} train, {test_df_raw.count()} test (BEFORE preprocessing)")
    
    # Prepare features - fit preprocessors on TRAIN only
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES
    )
    print(f"✓ After preprocessing: {train_df.count()} train, {test_df.count()} test")
    
    # Train multiple models
    models = [
        train_random_forest_optimized(train_df),
        train_decision_tree(train_df),
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
    print("   2. Select ONE model for inference based on F1-score or business needs")
    print("   3. Update inference script to load selected model")
    print("   4. Available models: RandomForest,")
    print("                        DecisionTree, MultilayerPerceptron")
    print(f"   5. Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()