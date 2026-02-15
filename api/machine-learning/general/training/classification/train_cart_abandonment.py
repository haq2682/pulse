import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, hour, dayofweek, 
    avg, count, sum as spark_sum, max as spark_max,
    datediff, current_date, log as spark_log,
    expr, rand
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, Bucketizer
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, 
    GBTClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
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
MODEL_NAME = "cart_abandonment"
INPUT_RELATIVE_PATH = "transformed/agg_cart_abandonment_analysis.parquet"
INPUT_SESSIONS_PATH = "transformed/agg_customer_sessions.parquet"
INPUT_ORDERS_PATH = "transformed/agg_orders.parquet"
INPUT_CUSTOMERS_PATH = "transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("classification", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)

# Base features (CRITICAL: Exclude time_in_cart_hours to prevent leakage)
NUMERICAL_FEATURES = [
    "cart_total_value",
    "cart_items_count",
    "cart_avg_item_price",
    "session_duration_minutes",
    "pages_viewed",
    "products_viewed",
    "pages_per_minute",
    "items_added_to_cart"
]

CATEGORICAL_FEATURES = [
    "device_used",
    "referrer_source"
]

TARGET_COLUMN = "will_abandon"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("CartAbandonmentTraining_Improved") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "1000") \
        .config("spark.dynamicAllocation.initialExecutors", "1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
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
        print(f"⚠ Failed to load data from {path}: {e}")
        return None


def generate_abandonment_labels(df):
    """
    FIXED: Generate will_abandon labels WITHOUT time-based condition
    
    Convert to binary:
    - Abandoned → True (will abandon)
    - Converted → False (will not abandon)  
    - Active → NULL (exclude - outcome unknown)
    """
    print("\n" + "="*60)
    print("Label Generation (FIXED - No Time Condition)")
    print("="*60)
    
    # Show original distribution
    print("\nOriginal cart_status distribution:")
    df.groupBy("cart_status").count().orderBy("count", ascending=False).show()
    
    # Create labels WITHOUT time condition
    df_with_label = df.withColumn(
        TARGET_COLUMN,
        when(col("cart_status") == "Abandoned", lit(True))
        .when(col("cart_status") == "Converted", lit(False))
        .otherwise(lit(None))   
    )
    
    # Filter only certain outcomes
    df_labeled = df_with_label.filter(col(TARGET_COLUMN).isNotNull())
    
    # Check class distribution
    label_dist = df_labeled.groupBy(TARGET_COLUMN).count().orderBy(TARGET_COLUMN)
    print("\nLabel distribution:")
    label_dist.show()
    
    # Calculate imbalance ratio
    label_counts = label_dist.collect()
    if len(label_counts) == 2:
        abandoned_count = [r['count'] for r in label_counts if r[TARGET_COLUMN] == True][0]
        converted_count = [r['count'] for r in label_counts if r[TARGET_COLUMN] == False][0]
        imbalance_ratio = max(abandoned_count, converted_count) / min(abandoned_count, converted_count)
        print(f"\n⚠️  Class imbalance ratio: {imbalance_ratio:.2f}:1")
        
        if imbalance_ratio > 3:
            print(f"⚠️  SEVERE IMBALANCE DETECTED - Will apply SMOTE-like sampling")
    
    return df_labeled


def engineer_features(df):
    """
    Enhanced feature engineering to improve predictive power
    """
    print("\n" + "="*60)
    print("Feature Engineering")
    print("="*60)
    
    # 1. Engagement ratios
    df = df.withColumn(
        "engagement_rate",
        col("products_viewed") / (col("pages_viewed") + 1)  # +1 to avoid division by zero
    )
    
    df = df.withColumn(
        "cart_fill_rate", 
        col("items_added_to_cart") / (col("products_viewed") + 1)
    )
    
    df = df.withColumn(
        "avg_time_per_page",
        col("session_duration_minutes") / (col("pages_viewed") + 1)
    )
    
    # 2. Cart value features
    df = df.withColumn(
        "log_cart_value",
        spark_log(col("cart_total_value") + 1)
    )
    
    df = df.withColumn(
        "high_value_cart",
        when(col("cart_total_value") > 200, 1.0).otherwise(0.0)
    )
    
    # 3. Price psychology features
    df = df.withColumn(
        "price_point_sensitivity",
        when(col("cart_avg_item_price") < 20, lit("low"))
        .when(col("cart_avg_item_price") < 100, lit("medium"))
        .otherwise(lit("high"))
    )
    
    # 4. Behavioral indicators
    df = df.withColumn(
        "browsing_intensity",
        col("pages_per_minute") * col("session_duration_minutes")
    )
    
    df = df.withColumn(
        "cart_commitment_score",
        (col("cart_items_count") * col("cart_total_value")) / (col("session_duration_minutes") + 1)
    )
    
    # 5. Cart complexity
    df = df.withColumn(
        "single_item_cart",
        when(col("cart_items_count") == 1, 1.0).otherwise(0.0)
    )
    
    df = df.withColumn(
        "large_cart",
        when(col("cart_items_count") >= 5, 1.0).otherwise(0.0)
    )
    
    print("✓ Created engagement features")
    print("✓ Created value-based features")
    print("✓ Created behavioral indicators")
    
    # Update feature lists
    new_numerical = [
        "engagement_rate",
        "cart_fill_rate", 
        "avg_time_per_page",
        "log_cart_value",
        "high_value_cart",
        "browsing_intensity",
        "cart_commitment_score",
        "single_item_cart",
        "large_cart"
    ]
    
    new_categorical = [
        "price_point_sensitivity"
    ]
    
    return df, new_numerical, new_categorical


def add_customer_history_features(df, customers_df=None, orders_df=None):
    """
    Add customer history features if data available - FIXED for correct schema
    """
    if customers_df is None and orders_df is None:
        print("\n⚠️  Customer history data not available - skipping customer features")
        return df, [], []
    
    print("\n" + "="*60)
    print("Customer History Features")
    print("="*60)
    
    # Priority 1: Use agg_customers table if available (pre-aggregated)
    if customers_df is not None:
        print("Using agg_customers table for customer features...")
        
        # Select relevant columns from customers table based on actual schema
        customer_features = customers_df.select(
            "customer_id",
            "total_orders",
            "avg_order_value",
            "last_order_date",
            "customer_lifetime_value",
            "order_recency_days",
            "customer_segment_label",
            "is_repeat_customer",
            "cart_abandonment_rate",
            "session_conversion_rate"
        )
        
        # Join with main dataframe
        df = df.join(customer_features, on="customer_id", how="left")
        
        # Create is_returning_customer from is_repeat_customer
        df = df.withColumn(
            "is_returning_customer_calc",
            when(col("is_repeat_customer") == 1, 1.0).otherwise(0.0)
        )
        
        print("✓ Added customer features from agg_customers table")
        
        return df, [
            "total_orders",
            "avg_order_value", 
            "customer_lifetime_value",
            "order_recency_days",
            "is_returning_customer_calc",
            "cart_abandonment_rate",
            "session_conversion_rate"
        ], ["customer_segment_label"]
    
    # Priority 2: Compute from agg_orders if customers table not available
    elif orders_df is not None:
        print("Computing customer features from agg_orders table...")
        
        # Use correct column names from schema:
        # - total_amount (not order_total)
        # - order_placed_at (not order_date)
        customer_stats = orders_df.groupBy("customer_id").agg(
            count("*").alias("total_orders_calc"),
            avg("total_amount").alias("avg_order_value_calc"),
            spark_max("order_placed_at").alias("last_order_date_calc"),
            spark_sum("total_amount").alias("lifetime_value_calc")
        )
        
        df = df.join(customer_stats, on="customer_id", how="left")
        
        # Days since last order
        df = df.withColumn(
            "days_since_last_order_calc",
            datediff(current_date(), col("last_order_date_calc"))
        )
        
        # Customer value segments
        df = df.withColumn(
            "customer_segment_calc",
            when(col("total_orders_calc").isNull() | (col("total_orders_calc") == 0), lit("new"))
            .when(col("total_orders_calc") <= 2, lit("occasional"))
            .when(col("total_orders_calc") <= 5, lit("regular"))
            .otherwise(lit("loyal"))
        )
        
        df = df.withColumn(
            "is_returning_customer_calc",
            when(col("total_orders_calc") > 0, 1.0).otherwise(0.0)
        )
        
        print("✓ Added purchase history features computed from orders")
        
        return df, [
            "total_orders_calc", 
            "avg_order_value_calc", 
            "days_since_last_order_calc",
            "is_returning_customer_calc",
            "lifetime_value_calc"
        ], ["customer_segment_calc"]
    
    return df, [], []


def balance_dataset(df, method="undersample", oversample_ratio=0.5):
    """
    Balance the dataset to improve model performance
    
    Methods:
    - undersample: Reduce majority class
    - oversample: Increase minority class
    - combined: Mix of both (SMOTE-like)
    """
    print("\n" + "="*60)
    print(f"Dataset Balancing ({method})")
    print("="*60)
    
    # Get class counts
    class_counts = df.groupBy(TARGET_COLUMN).count().collect()
    abandoned = [r['count'] for r in class_counts if r[TARGET_COLUMN] == True][0]
    converted = [r['count'] for r in class_counts if r[TARGET_COLUMN] == False][0]
    
    minority_class = True if abandoned < converted else False
    majority_class = not minority_class
    minority_count = min(abandoned, converted)
    majority_count = max(abandoned, converted)
    
    print(f"Before balancing:")
    print(f"  Minority class ({minority_class}): {minority_count}")
    print(f"  Majority class ({majority_class}): {majority_count}")
    print(f"  Ratio: {majority_count/minority_count:.2f}:1")
    
    if method == "undersample":
        # Keep all minority, sample majority
        df_minority = df.filter(col(TARGET_COLUMN) == minority_class)
        df_majority = df.filter(col(TARGET_COLUMN) == majority_class)
        
        # Sample majority to match minority
        sample_fraction = minority_count / majority_count
        df_majority_sampled = df_majority.sample(False, sample_fraction, seed=42)
        
        df_balanced = df_minority.union(df_majority_sampled)
        
    elif method == "oversample":
        # Keep all majority, oversample minority
        df_minority = df.filter(col(TARGET_COLUMN) == minority_class)
        df_majority = df.filter(col(TARGET_COLUMN) == majority_class)
        
        # Oversample minority (with replacement)
        target_minority = int(majority_count * oversample_ratio)
        oversample_ratio_calc = target_minority / minority_count
        
        df_minority_oversampled = df_minority.sample(True, oversample_ratio_calc, seed=42)
        df_balanced = df_majority.union(df_minority_oversampled)
        
    elif method == "combined":
        # SMOTE-like: oversample minority + undersample majority
        df_minority = df.filter(col(TARGET_COLUMN) == minority_class)
        df_majority = df.filter(col(TARGET_COLUMN) == majority_class)
        
        # Target 60-40 split
        target_count = int((minority_count + majority_count) * 0.6)
        
        # Oversample minority to 40% of target
        minority_target = int(target_count * 0.4)
        minority_ratio = minority_target / minority_count
        df_minority_over = df_minority.sample(True, minority_ratio, seed=42)
        
        # Undersample majority to 60% of target  
        majority_target = int(target_count * 0.6)
        majority_ratio = majority_target / majority_count
        df_majority_under = df_majority.sample(False, majority_ratio, seed=42)
        
        df_balanced = df_minority_over.union(df_majority_under)
    
    else:
        print(f"⚠️  Unknown method '{method}', returning original dataset")
        return df
    
    # Check new distribution
    new_dist = df_balanced.groupBy(TARGET_COLUMN).count().collect()
    new_abandoned = [r['count'] for r in new_dist if r[TARGET_COLUMN] == True][0]
    new_converted = [r['count'] for r in new_dist if r[TARGET_COLUMN] == False][0]
    
    print(f"\nAfter balancing:")
    print(f"  Abandoned: {new_abandoned}")
    print(f"  Converted: {new_converted}")
    print(f"  Ratio: {max(new_abandoned, new_converted)/min(new_abandoned, new_converted):.2f}:1")
    print(f"  Total records: {df_balanced.count()}")
    
    return df_balanced


def prepare_features(train_df, test_df, numerical_features, categorical_features):
    """Prepare features - FIT ON TRAIN ONLY"""
    print("\n" + "="*60)
    print("Feature Preparation")
    print("="*60)
    
    # Fill nulls
    train_filled = train_df.fillna(0, subset=numerical_features)
    train_filled = train_filled.fillna("Unknown", subset=categorical_features)
    
    test_filled = test_df.fillna(0, subset=numerical_features)
    test_filled = test_filled.fillna("Unknown", subset=categorical_features)
    
    # Filter null targets
    train_clean = train_filled.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_filled.filter(col(TARGET_COLUMN).isNotNull())
    
    print(f"Train records: {train_clean.count()}")
    print(f"Test records: {test_clean.count()}")
    
    # Index categorical features - FIT ON TRAIN ONLY
    categorical_indexed_cols = []
    categorical_indexers = []
    
    for cat_col in categorical_features:
        indexer = StringIndexer(
            inputCol=cat_col, 
            outputCol=f"{cat_col}_indexed", 
            handleInvalid="keep"
        )
        indexer_model = indexer.fit(train_clean)
        train_clean = indexer_model.transform(train_clean)
        test_clean = indexer_model.transform(test_clean)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
        categorical_indexers.append(indexer_model)
    
    # Assemble numerical features
    numerical_assembler = VectorAssembler(
        inputCols=numerical_features, 
        outputCol="numerical_features",
        handleInvalid="skip"
    )
    train_clean = numerical_assembler.transform(train_clean)
    test_clean = numerical_assembler.transform(test_clean)
    
    # Scale numerical features - FIT ON TRAIN ONLY
    scaler = StandardScaler(
        inputCol="numerical_features", 
        outputCol="scaled_numerical_features",
        withMean=True,
        withStd=True
    )
    scaler_model = scaler.fit(train_clean)
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)
    
    # Combine features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(
        inputCols=all_feature_cols, 
        outputCol="features",
        handleInvalid="skip"
    )
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Convert boolean target to double (0/1)
    train_indexed = train_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    test_indexed = test_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    
    print(f"✓ Prepared {len(numerical_features)} numerical + {len(categorical_features)} categorical features")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model
    }


def train_with_cv(train_df, model_type="RandomForest"):
    """
    Train model with cross-validation and hyperparameter tuning
    """
    print(f"\n" + "="*60)
    print(f"Training {model_type.upper()} with Cross-Validation")
    print("="*60)
    
    if model_type == "LogisticRegression":
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=100
        )
        paramGrid = ParamGridBuilder() \
            .addGrid(classifier.regParam, [0.001, 0.01, 0.1]) \
            .addGrid(classifier.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
            
    elif model_type == "RandomForest":
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            seed=42
        )
        paramGrid = ParamGridBuilder() \
            .addGrid(classifier.numTrees, [100, 200, 300]) \
            .addGrid(classifier.maxDepth, [8, 12, 16]) \
            .addGrid(classifier.minInstancesPerNode, [10, 20, 30]) \
            .build()
            
    elif model_type == "GBT":
        classifier = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            seed=42
        )
        paramGrid = ParamGridBuilder() \
            .addGrid(classifier.maxIter, [50, 100, 150]) \
            .addGrid(classifier.maxDepth, [4, 6, 8]) \
            .addGrid(classifier.stepSize, [0.05, 0.1, 0.15]) \
            .build()
    
    # Cross-validator
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    
    cv = CrossValidator(
        estimator=classifier,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )
    
    # Fit
    print(f"Training with {len(paramGrid)} parameter combinations...")
    cv_model = cv.fit(train_df)
    
    print(f"✓ Best model found with AUC: {max(cv_model.avgMetrics):.4f}")
    
    return cv_model.bestModel, model_type.upper()


def detailed_evaluation(model, test_df, model_name):
    """
    Detailed model evaluation with multiple metrics
    """
    print(f"\n" + "="*60)
    print(f"{model_name} - Detailed Evaluation")
    print("="*60)
    
    predictions = model.transform(test_df)
    
    # Binary metrics
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="label", 
        rawPredictionCol="rawPrediction"
    )
    auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
    auprc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderPR"})
    
    # Multiclass metrics
    mc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    
    accuracy = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
    precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
    recall = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})
    f1 = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})
    
    # Per-class metrics
    print("\nConfusion Matrix Analysis:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    
    # Class-specific metrics
    for class_label in [0.0, 1.0]:
        class_name = "Converted" if class_label == 0.0 else "Abandoned"
        tp = predictions.filter((col("label") == class_label) & (col("prediction") == class_label)).count()
        fp = predictions.filter((col("label") != class_label) & (col("prediction") == class_label)).count()
        fn = predictions.filter((col("label") == class_label) & (col("prediction") != class_label)).count()
        tn = predictions.filter((col("label") != class_label) & (col("prediction") != class_label)).count()
        
        class_precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        class_recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        class_f1 = 2 * (class_precision * class_recall) / (class_precision + class_recall) if (class_precision + class_recall) > 0 else 0
        
        print(f"\n{class_name} (Label={class_label}):")
        print(f"  Precision: {class_precision:.4f}")
        print(f"  Recall:    {class_recall:.4f}")
        print(f"  F1-Score:  {class_f1:.4f}")
    
    print(f"\n{'='*60}")
    print(f"Overall Metrics:")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  F1-Score:  {f1:.4f}")
    print(f"  AUC-ROC:   {auc:.4f} {'🎯' if auc >= 0.85 else '⚠️' if auc >= 0.70 else '❌'}")
    print(f"  AUC-PR:    {auprc:.4f}")
    
    metrics = {
        "model_name": model_name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "auc_roc": auc,
        "auc_pr": auprc
    }
    
    return metrics, predictions


def save_models(model, preprocessors, output_dir, model_name):
    """Save model and preprocessors"""
    model_path = f"{output_dir}/{model_name}"
    
    try:
        model.write().overwrite().save(model_path)
        
        for i, indexer in enumerate(preprocessors["categorical_indexers"]):
            indexer_path = f"{output_dir}/{model_name}_indexer_{i}"
            indexer.write().overwrite().save(indexer_path)
        
        scaler_path = f"{output_dir}/{model_name}_scaler"
        preprocessors["scaler"].write().overwrite().save(scaler_path)
        
        print(f"✓ Saved {model_name} to {model_path}")
    except Exception as e:
        print(f"⚠️  Failed to save {model_name}: {e}")


def main():
    print("=" * 80)
    print("Cart Abandonment Risk - IMPROVED Training Pipeline")
    print("=" * 80)
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print("=" * 80)
    
    # ========== CONFIGURATION ==========
    BALANCE_METHOD = "combined"  # Options: "undersample", "oversample", "combined", None
    USE_CV = False  # Use cross-validation for hyperparameter tuning
    MODELS_TO_TRAIN = ["LogisticRegression", "RandomForest", "GBT"]  # Train subset for faster iteration
    
    spark = create_spark_session()
    
    # ========== LOAD DATA FROM ALL BUCKETS ==========
    print("\nStep 1: Loading data from all MinIO buckets...")
    cart_df, cart_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=["cart_id", "cart_status", "customer_id", "session_id"],
        filter_nulls=False
    )
    
    if cart_df is None:
        print("⚠️  No cart data available. Skipping training.")
        spark.stop()
        return
    
    # Validate training data window
    is_valid, cart_df = validate_training_data(
        cart_df, cart_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient cart data.")
        spark.stop()
        return
    
    # Load sessions data
    sessions_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_SESSIONS_PATH,
        required_columns=["session_id"],
        filter_nulls=False
    )
    customers_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_CUSTOMERS_PATH,
        required_columns=["customer_id"],
        filter_nulls=False
    )
    orders_df, _ = load_data_from_all_buckets(
        spark,
        INPUT_ORDERS_PATH,
        required_columns=["order_id", "customer_id"],
        filter_nulls=False
    )
    
    if sessions_df is None:
        print("⚠️  Training skipped: Failed to load sessions data")
        spark.stop()
        return
    
    # ========== JOIN DATASETS ==========
    # Select necessary columns from cart
    cart_selected = cart_df.select(
        "cart_id", "cart_status", "customer_id",
        "cart_items_count", "cart_total_value", "cart_avg_item_price",
        "device_used", "session_id"
    )
    
    # Select necessary columns from sessions
    sessions_selected = sessions_df.select(
        col("session_id"),
        col("session_duration_minutes"),
        col("pages_viewed"),
        col("products_viewed"),
        col("pages_per_minute"),
        col("referrer_source"),
        col("items_added_to_cart")
    )
    
    # Join
    df = cart_selected.join(sessions_selected, on="session_id", how="left")
    print(f"✓ Joined datasets: {df.count()} records")
    
    # ========== GENERATE LABELS (FIXED) ==========
    df = generate_abandonment_labels(df)
    
    # ========== FEATURE ENGINEERING ==========
    df, new_num_features, new_cat_features = engineer_features(df)
    
    # Add customer history if available
    df, cust_num_features, cust_cat_features = add_customer_history_features(
        df, customers_df, orders_df
    )
    
    # Combine all features
    all_numerical = NUMERICAL_FEATURES + new_num_features + cust_num_features
    all_categorical = CATEGORICAL_FEATURES + new_cat_features + cust_cat_features
    
    print(f"\nTotal features: {len(all_numerical)} numerical + {len(all_categorical)} categorical")
    
    # ========== BALANCE DATASET ==========
    if BALANCE_METHOD:
        df = balance_dataset(df, method=BALANCE_METHOD)
    
    # ========== SPLIT DATA ==========
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"\n✓ Split: {train_df_raw.count()} train, {test_df_raw.count()} test")
    
    # ========== PREPARE FEATURES ==========
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, all_numerical, all_categorical
    )
    train_df = train_df.cache()
    test_df = test_df.cache()
    
    # ========== TRAIN MODELS ==========
    all_metrics = []
    
    for model_type in MODELS_TO_TRAIN:
        if USE_CV:
            model, model_name = train_with_cv(train_df, model_type)
        else:
            # Train without CV (faster but less optimal)
            if model_type == "LogisticRegression":
                model = LogisticRegression(maxIter=100, regParam=0.01).fit(train_df)
            elif model_type == "RandomForest":
                model = RandomForestClassifier(numTrees=200, maxDepth=12).fit(train_df)
            elif model_type == "GBT":
                model = GBTClassifier(maxIter=100, maxDepth=6).fit(train_df)
            model_name = model_type
        
        # Evaluate
        metrics, predictions = detailed_evaluation(model, test_df, model_name)
        all_metrics.append(metrics)
        
        # Save
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)
    
    # ========== FINAL COMPARISON ==========
    print("\n" + "=" * 80)
    print("FINAL MODEL COMPARISON")
    print("=" * 80)
    for m in sorted(all_metrics, key=lambda x: x["auc_roc"], reverse=True):
        status = "🎯 EXCELLENT" if m["auc_roc"] >= 0.85 else "✓ Good" if m["auc_roc"] >= 0.70 else "⚠️  Needs Work"
        print(f"{m['model_name']:25s} | AUC: {m['auc_roc']:.4f} | F1: {m['f1_score']:.4f} | {status}")
    
    print("\n✓ Training completed")
    print(f"Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()