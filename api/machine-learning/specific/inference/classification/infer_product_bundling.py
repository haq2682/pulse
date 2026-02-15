import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp, when, abs as spark_abs
from pyspark.sql.types import StringType, DoubleType, BooleanType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel, StandardScalerModel
from pyspark.ml.classification import (
    LogisticRegressionModel, RandomForestClassificationModel,
    DecisionTreeClassificationModel, MultilayerPerceptronClassificationModel
)
import findspark

findspark.init()


# Feature columns (must match training EXACTLY)
NUMERICAL_FEATURES = [
    "co_occurrence_count",
    "product_a_count",
    "product_b_count",
    "product_a_price",
    "product_b_price",
    "price_difference",
    "price_ratio",
    "is_cross_category"  # ← CRITICAL: Must match training (numerical not categorical)
]

CATEGORICAL_FEATURES = [
    "product_a_category",
    "product_b_category"
    # NOTE: brands handled via FeatureHasher, not StringIndexer
]


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return SparkSession.builder \
        .appName("ProductBundlingInference") \
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
    Join affinity pairs with product attributes (same as training)
    Select only necessary columns from affinity to avoid duplicates
    """
    if affinity_df is None or products_df is None:
        return None
    
    # Select ONLY raw counts and metrics from affinity (exclude product attributes)
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
    
    # Select columns for product_a
    products_a = products_df.select(
        col("product_id").alias("product_a_id"),
        col("category").alias("product_a_category"),
        col("sub_category").alias("product_a_sub_category"),
        col("brand").alias("product_a_brand"),
        col("sell_price").alias("product_a_price")
    )
    
    # Select columns for product_b
    products_b = products_df.select(
        col("product_id").alias("product_b_id"),
        col("category").alias("product_b_category"),
        col("sub_category").alias("product_b_sub_category"),
        col("brand").alias("product_b_brand"),
        col("sell_price").alias("product_b_price")
    )
    
    # Join
    joined_df = affinity_selected.join(products_a, on="product_a_id", how="left")
    joined_df = joined_df.join(products_b, on="product_b_id", how="left")
    
    print(f"✓ Joined datasets: {joined_df.count()} product pairs")
    return joined_df


def engineer_features(df):
    """Engineer derived features (same as training)"""
    print("Engineering features...")
    
    df = df.fillna(0, subset=["product_a_price", "product_b_price"])
    
    # Cast is_cross_category to int (same as training)
    df = df.withColumn(
        "is_cross_category",
        col("is_cross_category").cast("int")
    )
    
    df = df.withColumn("price_difference", spark_abs(col("product_a_price") - col("product_b_price")))
    df = df.withColumn(
        "price_ratio",
        when(col("product_b_price") > 0, col("product_a_price") / col("product_b_price")).otherwise(1.0)
    )
    
    print(f"✓ Engineered features")
    return df


def load_model_and_preprocessors(spark, model_dir, model_name):
    """Load trained model and all preprocessors from MinIO"""
    try:
        model_path = f"{model_dir}/{model_name}"
        
        # Load model
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        elif model_name == "DecisionTree":
            model = DecisionTreeClassificationModel.load(model_path)
        elif model_name == "MultilayerPerceptron":
            model = MultilayerPerceptronClassificationModel.load(model_path)
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
        
        print(f"✓ Loaded model and preprocessors: {model_name}")
        return model, {
            "categorical_indexers": categorical_indexers,
            "scaler": scaler
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
    """Prepare features using saved preprocessors (must match training pipeline)"""
    from pyspark.ml.feature import FeatureHasher
    
    # Fill nulls (same as training)
    df_filled = df.fillna(0, subset=numerical_features)
    df_filled = df_filled.fillna("Unknown", subset=categorical_features)
    
    # Hash brands (same as training) - NOT using StringIndexer for brands
    hasher = FeatureHasher(
        inputCols=["product_a_brand", "product_b_brand"],
        outputCol="brand_hash",
        numFeatures=128
    )
    df_filled = hasher.transform(df_filled)
    
    # Apply categorical indexers (only for product categories, not brands)
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
    
    # Combine all features (same order as training)
    all_feature_cols = ["scaled_numerical_features", "brand_hash"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    df_vector = final_assembler.transform(df_filled)
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(categorical_features)} categorical + brand_hash")
    return df_vector


def calculate_association_metrics(df, total_orders=None):
    """
    Calculate association metrics (support, confidence, lift) from raw counts
    These are calculated at inference time, not used as training features
    """
    if total_orders is None:
        total_orders_val = df.agg({"product_a_count": "max"}).collect()[0][0]
        if total_orders_val is None or total_orders_val == 0:
            total_orders_val = 1000
    else:
        total_orders_val = total_orders
    
    # Support: P(A ∩ B)
    df = df.withColumn("support", col("co_occurrence_count") / lit(total_orders_val))
    
    # Confidence A→B: P(B|A)
    df = df.withColumn(
        "confidence",
        when(col("product_a_count") > 0, col("co_occurrence_count") / col("product_a_count")).otherwise(0.0)
    )
    
    # Lift: P(B|A) / P(B)
    df = df.withColumn(
        "lift",
        when(
            (col("product_b_count") > 0) & (col("confidence") > 0),
            col("confidence") / (col("product_b_count") / lit(total_orders_val))
        ).otherwise(1.0)
    )
    
    return df


def determine_bundle_category(df):
    """Determine bundle category based on product attributes and affinity"""
    bundle_category_udf = udf(
        lambda is_cross, same_cat, lift_val: 
            "Accessory Bundle" if is_cross and lift_val > 2.0
            else ("Essential Bundle" if (same_cat == "Yes" or same_cat == True) and lift_val > 1.5
            else ("Complete Set" if is_cross and lift_val > 1.2
            else "Value Pack")),
        StringType()
    )
    
    # Create same_category column for bundle category logic
    df = df.withColumn(
        "same_category",
        when(col("product_a_category") == col("product_b_category"), "Yes").otherwise("No")
    )
    
    df = df.withColumn(
        "bundle_category",
        bundle_category_udf(col("is_cross_category"), col("same_category"), col("lift"))
    )
    
    return df


def generate_predictions(spark, df, model, preprocessors, model_name, MODEL_VERSION):
    """Generate predictions and format output according to schema"""
    predictions = model.transform(df)
    
    # Convert prediction (0/1) to boolean
    to_boolean = udf(lambda pred: bool(pred > 0.5), BooleanType())
    
    # Extract probability for positive class
    extract_probability = udf(
        lambda prob: float(prob[1]) if prob and len(prob) > 1 else 0.0,
        DoubleType()
    )
    
    # Calculate confidence score
    calculate_confidence = udf(
        lambda prob: float(max(prob)) if prob and len(prob) > 0 else 0.0,
        DoubleType()
    )
    
    # Calculate association metrics
    predictions = calculate_association_metrics(predictions)
    
    # Determine bundle category
    predictions = determine_bundle_category(predictions)
    
    # Calculate expected bundle revenue
    predictions = predictions.withColumn(
        "expected_bundle_revenue",
        col("product_a_price") + col("product_b_price")
    )
    
    # Recommend bundle discount based on lift
    predictions = predictions.withColumn(
        "recommended_bundle_discount",
        when(col("lift") > 2.5, 0.20)
        .when(col("lift") > 2.0, 0.15)
        .when(col("lift") > 1.5, 0.10)
        .otherwise(0.05) * col("expected_bundle_revenue")
    )
    
    # Use affinity_score from database if available, otherwise use probability
    predictions = predictions.withColumn(
        "affinity_score_final",
        when(col("affinity_score").isNotNull(), col("affinity_score"))
        .otherwise(extract_probability(col("probability")))
    )
    
    # Format predictions according to output schema
    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("product_a_id").alias("product_id_a"),
        col("product_b_id").alias("product_id_b"),
        current_timestamp().alias("prediction_date"),
        to_boolean(col("prediction")).alias("is_complementary"),
        col("bundle_category"),
        col("affinity_score_final").alias("affinity_score"),
        col("support"),
        col("confidence"),
        col("lift"),
        col("expected_bundle_revenue"),
        col("recommended_bundle_discount"),
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
    INPUT_PATH_AFFINITY = f"s3a://{BUCKET_NAME}/transformed/agg_product_affinity.parquet"
    INPUT_PATH_PRODUCTS = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/product_bundling_predictions"
    MODEL_INPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/product_bundling"

    # ⚠️ MANUAL INTERVENTION REQUIRED: Select model to use for inference
    # Available options: "LogisticRegression", "RandomForest", "DecisionTree", "MultilayerPerceptron"
    SELECTED_MODEL = "RandomForest"  # <-- CHANGE THIS BASED ON TRAINING RESULTS

    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print("=" * 60)
    print("Product Bundling Classification - Inference Pipeline")
    print("=" * 60)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load model and preprocessors
    model, preprocessors = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None or preprocessors is None:
        print("✗ Inference stopped: Failed to load model")
        return
    
    # Load tables
    affinity_df = load_data(spark, INPUT_PATH_AFFINITY)
    products_df = load_data(spark, INPUT_PATH_PRODUCTS)
    
    if affinity_df is None or products_df is None:
        print("✗ Inference stopped: Failed to load data")
        return
    
    # Join datasets
    df = join_datasets(affinity_df, products_df)
    if df is None:
        print("✗ Inference stopped: Failed to join datasets")
        return
    
    # Engineer features
    df = engineer_features(df)
    
    # Validate dataset
    required_cols = ["product_a_id", "product_b_id"] + NUMERICAL_FEATURES + CATEGORICAL_FEATURES
    is_valid, message = validate_dataset(df, required_cols)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return
    
    print(f"✓ Dataset validated")
    
    # Prepare features
    df_prepared = prepare_features(df, NUMERICAL_FEATURES, CATEGORICAL_FEATURES, preprocessors)
    
    # Generate predictions
    predictions_df = generate_predictions(spark, df_prepared, model, preprocessors, SELECTED_MODEL, MODEL_VERSION)
    
    # Show sample predictions
    print("\nSample predictions:")
    predictions_df.select(
        "product_id_a", "product_id_b", "is_complementary", "bundle_category",
        "affinity_score", "lift", "confidence_score"
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
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)