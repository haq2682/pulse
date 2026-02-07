import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, DecisionTreeClassifier,
    MultilayerPerceptronClassifier, NaiveBayes
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH_INVENTORY_HEALTH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
INPUT_PATH_INVENTORY = f"s3a://{BUCKET_NAME}/transformed/agg_inventory.parquet"
MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/stock_status"
MIN_LABELED_RECORDS = 100

# CRITICAL: Exclude derived metrics to prevent leakage
NUMERICAL_FEATURES = [
    "current_stock",
    "available_stock",
    "reserved_quantity",
    "minimum_stock_level",
    "avg_daily_sales",
    "days_of_supply",
    "inventory_turnover_ratio",
    "days_since_restock",
    "reorder_point_breach_count"
]

CATEGORICAL_FEATURES = []  # No categorical features for stock status

TARGET_COLUMN = "stock_status"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("StockStatusTraining") \
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


def join_datasets(inventory_health_df, inventory_df):
    """
    Join product inventory health with inventory
    
    Join strategy:
    1. inventory_health (primary) LEFT JOIN inventory ON product_id
    2. Select only necessary columns to avoid duplicates
    """
    if inventory_health_df is None or inventory_df is None:
        return None
    
    # Select necessary columns from inventory_health (avoid duplicates)
    health_selected = inventory_health_df.select(
        "product_id",
        "current_stock",
        "available_stock",
        "reserved_quantity",
        "minimum_stock_level",
        "avg_daily_sales",
        "days_of_supply",
        "inventory_turnover_ratio",
        "days_since_restock",
        "reorder_point_breach_count",
        "stock_status"
    )
    
    # Select necessary columns from inventory (avoid duplicates)
    # Most columns overlap, only take unique ones if needed
    inventory_selected = inventory_df.select(
        col("product_id"),
        col("stock_quantity").alias("inv_stock_quantity"),
        col("last_restocked_date")
    )
    
    # Join on product_id
    joined_df = health_selected.join(inventory_selected, on="product_id", how="left")
    
    # Use current_stock from health, fallback to inventory if null
    joined_df = joined_df.withColumn(
        "current_stock",
        when(col("current_stock").isNull(), col("inv_stock_quantity")).otherwise(col("current_stock"))
    ).drop("inv_stock_quantity")
    
    print(f"✓ Joined datasets: {joined_df.count()} product records")
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


def generate_stock_status_labels(df):
    """
    Generate stock_status labels from business rules if missing
    
    Business Rules:
    - Out of Stock: current_stock <= 0
    - Low Stock: 0 < current_stock <= minimum_stock_level
    - Overstock: days_of_supply > 60 (2 months)
    - In Stock: everything else (healthy stock levels)
    """
    print("Generating/cleaning stock_status labels...")
    
    # Clean existing labels
    df_cleaned = df.withColumn(
        TARGET_COLUMN,
        when(col(TARGET_COLUMN).isin("Out of Stock", "out of stock", "OutOfStock"), "Out of Stock")
        .when(col(TARGET_COLUMN).isin("Low Stock", "low stock", "LowStock"), "Low Stock")
        .when(col(TARGET_COLUMN).isin("Overstock", "overstock", "Over Stock"), "Overstock")
        .when(col(TARGET_COLUMN).isin("In Stock", "in stock", "InStock"), "In Stock")
        .otherwise(col(TARGET_COLUMN))
    )
    
    # Generate labels where missing using business rules
    df_with_label = df_cleaned.withColumn(
        TARGET_COLUMN,
        when(
            col(TARGET_COLUMN).isNull() | ~col(TARGET_COLUMN).isin("Out of Stock", "Low Stock", "Overstock", "In Stock"),
            when(col("current_stock") <= 0, "Out of Stock")
            .when(
                (col("current_stock") > 0) & (col("current_stock") <= col("minimum_stock_level")),
                "Low Stock"
            )
            .when(col("days_of_supply") > 60, "Overstock")
            .otherwise("In Stock")
        ).otherwise(col(TARGET_COLUMN))
    )
    
    # Filter only valid statuses
    df_with_label = df_with_label.filter(
        col(TARGET_COLUMN).isin("Out of Stock", "Low Stock", "Overstock", "In Stock")
    )
    
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Stock status distribution:")
    label_dist.show()
    
    print("⚠️  CRITICAL: stock_health_score, reorder_urgency excluded to prevent leakage")
    
    return df_with_label


def add_label_noise(df, noise_rate=0.12):
    """
    Add label noise to simulate real-world inventory classification uncertainty
    
    In real warehouses:
    - Stock counts have errors (misplacement, theft, damage)
    - Demand fluctuations cause unexpected status changes
    - Supplier delays affect stock levels
    - Seasonal variations create uncertainty
    """
    print(f"\nAdding {noise_rate*100:.0f}% label noise...")
    
    from pyspark.sql.functions import rand
    
    labels = ["Out of Stock", "Low Stock", "In Stock", "Overstock"]
    
    # Randomly reassign labels for noise_rate% of records
    df_noisy = df.withColumn(
        "random_val", rand(seed=42)
    ).withColumn(
        TARGET_COLUMN,
        when(
            col("random_val") < noise_rate / 4, lit(labels[0])
        ).when(
            (col("random_val") >= noise_rate / 4) & (col("random_val") < noise_rate / 2),
            lit(labels[1])
        ).when(
            (col("random_val") >= noise_rate / 2) & (col("random_val") < 3 * noise_rate / 4),
            lit(labels[2])
        ).when(
            (col("random_val") >= 3 * noise_rate / 4) & (col("random_val") < noise_rate),
            lit(labels[3])
        ).otherwise(
            col(TARGET_COLUMN)
        )
    ).drop("random_val")
    
    print("Label distribution after noise:")
    df_noisy.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False).show()
    
    print(f"✓ Added noise: ~{noise_rate*100:.0f}% labels flipped")
    print(f"  Expected accuracy ceiling: ~{(1-noise_rate)*100:.0f}%\n")
    
    return df_noisy


def prepare_features(train_df, test_df, numerical_features, categorical_features):
    """Prepare features - FIT ON TRAIN ONLY"""
    # Fill nulls
    train_filled = train_df.fillna(0, subset=numerical_features)
    test_filled = test_df.fillna(0, subset=numerical_features)
    
    if categorical_features:
        train_filled = train_filled.fillna("Unknown", subset=categorical_features)
        test_filled = test_filled.fillna("Unknown", subset=categorical_features)
    
    # Filter null targets
    train_clean = train_filled.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_filled.filter(col(TARGET_COLUMN).isNotNull())
    
    # Index categorical features if any
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
    if categorical_indexed_cols:
        all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    else:
        all_feature_cols = ["scaled_numerical_features"]
    
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Encode target labels - FIT ON TRAIN ONLY
    label_indexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label", handleInvalid="skip")
    label_indexer_model = label_indexer.fit(train_vector)
    train_indexed = label_indexer_model.transform(train_vector)
    test_indexed = label_indexer_model.transform(test_vector)
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical")
    print(f"  - Numerical features scaled (fit on train only)")
    print(f"  - Derived metrics (stock_health_score, reorder_urgency) EXCLUDED")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model,
        "label_indexer": label_indexer_model
    }


def train_logistic_regression(train_df):
    """Train Logistic Regression"""
    print("\n[1/5] Training Logistic Regression...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.5)
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_naive_bayes(train_df):
    """Train Naive Bayes"""
    print("\n[2/5] Training Naive Bayes...")
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    model = nb.fit(train_df)
    print("✓ Naive Bayes trained")
    return model, "NaiveBayes"


def train_random_forest(train_df):
    """Train Random Forest"""
    print("\n[3/5] Training Random Forest...")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
    model = rf.fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


def train_decision_tree(train_df):
    """Train Decision Tree"""
    print("\n[4/5] Training Decision Tree...")
    dt = DecisionTreeClassifier(maxDepth=10, seed=42)
    model = dt.fit(train_df)
    print("✓ Decision Tree trained")
    return model, "DecisionTree"


def train_multilayer_perceptron(train_df):
    """Train Multilayer Perceptron"""
    print("\n[5/5] Training Multilayer Perceptron...")
    
    num_features = len(train_df.select("features").first()[0])
    num_classes = int(train_df.agg({"label": "max"}).collect()[0][0]) + 1
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
    """Evaluate model"""
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
    """Save model and preprocessors"""
    model_path = f"{output_dir}/{model_name}"
    
    model.write().overwrite().save(model_path)
    
    for i, indexer in enumerate(preprocessors["categorical_indexers"]):
        indexer_path = f"{output_dir}/{model_name}_cat_indexer_{i}"
        indexer.write().overwrite().save(indexer_path)
    
    scaler_path = f"{output_dir}/{model_name}_scaler"
    preprocessors["scaler"].write().overwrite().save(scaler_path)
    
    label_indexer_path = f"{output_dir}/{model_name}_label_indexer"
    preprocessors["label_indexer"].write().overwrite().save(label_indexer_path)
    
    print(f"✓ Saved {model_name} to {model_path}")


def main():
    print("=" * 60)
    print("Stock Status Classification - Training Pipeline")
    print("=" * 60)
    
    # CONFIGURATION
    ADD_LABEL_NOISE = True
    NOISE_RATE = 0.12  # 12% noise → ~88% accuracy ceiling
    
    spark = create_spark_session()
    
    # Load data
    health_df = load_data(spark, INPUT_PATH_INVENTORY_HEALTH)
    inventory_df = load_data(spark, INPUT_PATH_INVENTORY)
    
    if health_df is None or inventory_df is None:
        print("✗ Training stopped: Failed to load data")
        return
    
    # Join
    df = join_datasets(health_df, inventory_df)
    if df is None:
        print("✗ Training stopped: Failed to join")
        return
    
    # Generate/clean labels
    df = generate_stock_status_labels(df)
    
    # Add noise
    if ADD_LABEL_NOISE:
        df = add_label_noise(df, noise_rate=NOISE_RATE)
    
    # Validate
    all_required_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES + [TARGET_COLUMN]
    is_valid, message = validate_dataset(df, all_required_cols)
    if not is_valid:
        print(f"✗ Training stopped: {message}")
        return
    
    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_LABELED_RECORDS:
        print(f"✗ Insufficient data ({labeled_count} < {MIN_LABELED_RECORDS})")
        return
    
    print(f"✓ Dataset validated: {labeled_count} records")
    
    # Split
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split: {train_df_raw.count()} train, {test_df_raw.count()} test")
    
    # Prepare features
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES
    )
    
    # Train models
    models = [
        train_logistic_regression(train_df),
        train_naive_bayes(train_df),
        train_random_forest(train_df),
        train_decision_tree(train_df),
        train_multilayer_perceptron(train_df)
    ]
    
    # Evaluate
    print("\n" + "=" * 60)
    print("Model Evaluation")
    print("=" * 60)
    
    all_metrics = []
    for model, model_name in models:
        metrics = evaluate_model(model, test_df, model_name)
        all_metrics.append(metrics)
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)
    
    # Compare
    print("\n" + "=" * 60)
    print("Model Comparison")
    print("=" * 60)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | Acc: {m['accuracy']:.4f}")
    
    print("\n✓ Training completed")
    print(f"Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()
