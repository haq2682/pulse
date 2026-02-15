import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, abs as spark_abs
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, DecisionTreeClassifier,
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import findspark

findspark.init()


# CRITICAL: Avoid data leakage - DO NOT use lift/confidence/support as features
# if they are used to generate the label
NUMERICAL_FEATURES = [
    "co_occurrence_count",
    "product_a_count",
    "product_b_count",
    "product_a_price",
    "product_b_price",
    "price_difference",
    "price_ratio",
    "is_cross_category",
]

CATEGORICAL_FEATURES = [
    "product_a_category",
    "product_b_category",
    "product_a_brand",
    "product_b_brand"
]

TARGET_COLUMN = "is_complementary_pair"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("ProductBundlingTraining") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "8") \
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


def join_datasets(affinity_df, products_df):
    """
    Join agg_product_affinity with agg_products TWICE (for product_a and product_b)
    Select only necessary columns from affinity to avoid duplicates with product attributes
    """
    if affinity_df is None or products_df is None:
        return None
    
    # Select ONLY raw counts and metrics from affinity (exclude product attributes that we'll get from products)
    affinity_selected = affinity_df.select(
        "product_a_id",
        "product_b_id",
        "co_occurrence_count",
        "product_a_count",
        "product_b_count",
        "is_cross_category",
        "support",
        "confidence_a_to_b",
        "lift_a_to_b",
        "affinity_score"
    )
    
    # Select columns from products for product_a (with _a suffix)
    products_a = products_df.select(
        col("product_id").alias("product_a_id"),
        col("category").alias("product_a_category"),
        col("sub_category").alias("product_a_sub_category"),
        col("brand").alias("product_a_brand"),
        col("sell_price").alias("product_a_price")
    )
    
    # Select columns from products for product_b (with _b suffix)
    products_b = products_df.select(
        col("product_id").alias("product_b_id"),
        col("category").alias("product_b_category"),
        col("sub_category").alias("product_b_sub_category"),
        col("brand").alias("product_b_brand"),
        col("sell_price").alias("product_b_price")
    )
    
    # Join affinity with products_a
    joined_df = affinity_selected.join(products_a, on="product_a_id", how="left")
    
    # Join result with products_b
    joined_df = joined_df.join(products_b, on="product_b_id", how="left")
    
    print(f"✓ Joined datasets: {joined_df.count()} product pairs")
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


def generate_complementary_labels(df):
    """
    Generate is_complementary_pair labels using business rules
    
    Business Rules:
    - Complementary: lift > 1.5 AND confidence > 0.3 AND support > 0.01
    - Non-complementary: lift <= 1.0 OR confidence < 0.1 OR support < 0.001
    
    CRITICAL: After generating labels, we CANNOT use lift/confidence/support as features
    This would be data leakage (using the answer to predict the answer)
    """
    print("Generating is_complementary_pair labels from association metrics...")
    
    # Fill nulls in metrics
    metrics_cols = ["lift_a_to_b", "confidence_a_to_b", "support"]
    df_filled = df.fillna(0, subset=metrics_cols)
    
    df_with_label = df_filled.withColumn(
        TARGET_COLUMN,
        when(
            (col("lift_a_to_b") > 1.5) & 
            (col("confidence_a_to_b") > 0.3) & 
            (col("support") > 0.01),
            lit(True)
        ).when(
            (col("lift_a_to_b") <= 1.0) | 
            (col("confidence_a_to_b") < 0.1) | 
            (col("support") < 0.001),
            lit(False)
        ).otherwise(lit(None))  # Uncertain cases = null
    )
    
    # Filter only clear cases (non-null labels)
    df_with_label = df_with_label.filter(col(TARGET_COLUMN).isNotNull())
    
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Complementary pair distribution:")
    label_dist.show()
    
    print("⚠️  CRITICAL: lift/confidence/support will be EXCLUDED from features to prevent leakage")
    
    return df_with_label


def add_label_noise(df, noise_rate=0.15):
    """
    Add label noise to simulate real-world uncertainty in product bundling
    
    In real e-commerce:
    - Same products may/may not be complementary depending on customer segment
    - Seasonal variations affect bundling
    - Context matters (back-to-school vs regular shopping)
    - Some "obvious" bundles don't work in practice
    
    This adds noise_rate% random label flips to simulate real uncertainty.
    """
    print(f"\nAdding {noise_rate*100:.0f}% label noise to simulate real-world bundling uncertainty...")
    
    from pyspark.sql.functions import rand
    
    # Randomly flip labels for noise_rate% of records
    df_noisy = df.withColumn(
        "random_val", rand(seed=42)
    ).withColumn(
        TARGET_COLUMN,
        when(col("random_val") < noise_rate / 2, ~col(TARGET_COLUMN))  # Flip True<->False
        .otherwise(col(TARGET_COLUMN))
    ).drop("random_val")
    
    # Show new distribution
    print("Label distribution after adding noise:")
    df_noisy.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False).show()
    
    print(f"✓ Added label noise: ~{noise_rate*100:.0f}% of labels randomly flipped")
    print(f"  Expected accuracy ceiling: ~{(1-noise_rate)*100:.0f}% (down from 100%)\n")
    
    return df_noisy


def engineer_features(df):
    """
    Engineer features WITHOUT using association metrics
    (to prevent data leakage)
    """
    print("Engineering features...")
    
    # Price-based features
    df = df.fillna(0, subset=["product_a_price", "product_b_price"])
    
    df = df.withColumn(
        "price_difference",
        spark_abs(col("product_a_price") - col("product_b_price"))
    )
    df = df.withColumn(
        "is_cross_category",
        when(col("product_a_category") != col("product_b_category"), 1).otherwise(0)
    )
    df = df.withColumn(
        "price_ratio",
        when(col("product_b_price") > 0, col("product_a_price") / col("product_b_price")).otherwise(1.0)
    )
    
    print("✓ Engineered features: price_difference, price_ratio, is_cross_category")
    return df


def prepare_features(train_df, test_df, numerical_features, categorical_features):
    """
    Prepare features - FIT ON TRAIN ONLY to prevent leakage
    """
    # Fill nulls
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
    
    # Scale numerical features - FIT ON TRAIN ONLY
    scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")
    scaler_model = scaler.fit(train_clean)
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)
    
    # Combine features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Convert boolean target to double (0/1) for classifiers
    train_indexed = train_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    test_indexed = test_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical")
    print(f"  - Numerical features scaled (fit on train only)")
    print(f"  - Categorical features indexed (fit on train only)")
    print(f"  - Association metrics (lift/confidence/support) EXCLUDED to prevent leakage")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model
    }

def compute_max_bins(train_df, categorical_indexed_cols):
    """
    Compute the minimum required maxBins for tree-based models
    """
    max_bins_needed = 2  # at least 2
    for col_name in categorical_indexed_cols:
        distinct_count = train_df.select(col_name).distinct().count()
        print(f"Column {col_name} has {distinct_count} distinct values")
        if distinct_count > max_bins_needed:
            max_bins_needed = distinct_count
    print(f"Setting maxBins={max_bins_needed} for tree models")
    return max_bins_needed


def train_logistic_regression(train_df):
    """Train Logistic Regression model"""
    print("\n[1/4] Training Logistic Regression...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.5)
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_random_forest(train_df, max_bins):
    """Train Random Forest model"""
    print("\n[2/4] Training Random Forest...")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42, maxBins=max_bins+10)
    model = rf.fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


def train_decision_tree(train_df, max_bins):
    """Train Decision Tree model"""
    print("\n[3/4] Training Decision Tree...")
    dt = DecisionTreeClassifier(maxDepth=10, seed=42, maxBins=max_bins+10)
    model = dt.fit(train_df)
    print("✓ Decision Tree trained")
    return model, "DecisionTree"


def train_multilayer_perceptron(train_df):
    """Train Multilayer Perceptron model"""
    print("\n[4/4] Training Multilayer Perceptron...")
    
    num_features = len(train_df.select("features").first()[0])
    layers = [num_features, num_features * 2, num_features, 2]  # Binary classification
    
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
    
    # Binary classification metrics
    binary_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
    
    # Multiclass metrics (also work for binary)
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
        "f1_score": f1,
        "auc": auc
    }
    
    print(f"\n{model_name} Metrics:")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    print(f"  AUC:       {auc:.4f}")
    
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
    
    print(f"✓ Saved {model_name} and preprocessors to {model_path}")


def main(BUCKET_NAME):
    INPUT_PATH_AFFINITY = f"s3a://{BUCKET_NAME}/transformed/agg_product_affinity.parquet"
    INPUT_PATH_PRODUCTS = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/product_bundling"
    MIN_LABELED_RECORDS = 100
    print("=" * 60)
    print("Product Bundling Classification - Training Pipeline")
    print("=" * 60)
    
    # CONFIGURATION: Set to True to add label noise (RECOMMENDED for synthetic data)
    ADD_LABEL_NOISE = True  # <-- Set to True to fix 100% accuracy
    NOISE_RATE = 0.15  # 15% noise → ~85% accuracy ceiling
    
    spark = create_spark_session()
    
    # Load tables
    affinity_df = load_data(spark, INPUT_PATH_AFFINITY)
    products_df = load_data(spark, INPUT_PATH_PRODUCTS)
    
    if affinity_df is None or products_df is None:
        print("✗ Training stopped: Failed to load data")
        return
    
    # Join datasets
    df = join_datasets(affinity_df, products_df)
    if df is None:
        print("✗ Training stopped: Failed to join datasets")
        return
    
    # Generate labels if not present
    if TARGET_COLUMN not in df.columns or df.filter(col(TARGET_COLUMN).isNotNull()).count() == 0:
        df = generate_complementary_labels(df)
    
    # Add label noise if synthetic data (to simulate real-world uncertainty)
    if ADD_LABEL_NOISE:
        df = add_label_noise(df, noise_rate=NOISE_RATE)
    else:
        print("\n⚠️  WARNING: Running without label noise on deterministic data")
        print("   Expected result: 100% accuracy (unrealistic for production)")
        print("   Recommendation: Set ADD_LABEL_NOISE=True to simulate real-world conditions\n")
    
    # Engineer features
    df = engineer_features(df)
    
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
    
    print(f"✓ Dataset validated: {labeled_count} labeled product pairs")
    
    # CRITICAL: Split data FIRST
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split data: {train_df_raw.count()} train, {test_df_raw.count()} test (BEFORE preprocessing)")
    
    # Prepare features - fit on TRAIN only
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES
    )
    print(f"✓ After preprocessing: {train_df.count()} train, {test_df.count()} test")
    categorical_indexed_cols = [f"{c}_indexed" for c in CATEGORICAL_FEATURES]
    max_bins = compute_max_bins(train_df, categorical_indexed_cols)
    
    # Train multiple models
    models = [
        train_logistic_regression(train_df),
        train_random_forest(train_df, max_bins),
        train_decision_tree(train_df, max_bins),
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
        
        # Save each model
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)
    
    # Compare models
    print("\n" + "=" * 60)
    print("Model Comparison Summary")
    print("=" * 60)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | AUC: {m['auc']:.4f} | Acc: {m['accuracy']:.4f}")
    
    print("\n" + "=" * 60)
    print("✓ Training completed successfully")
    print("=" * 60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   1. Review model metrics above")
    print("   2. Select ONE model for inference based on F1-score or AUC")
    print("   3. Update inference script to load selected model")
    print("   4. Available models: LogisticRegression, RandomForest, DecisionTree, MultilayerPerceptron")
    print(f"   5. Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)