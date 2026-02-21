import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, current_timestamp, when
from pyspark.sql.types import StringType, DoubleType, BooleanType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel, StandardScalerModel
from pyspark.ml.classification import (
    LogisticRegressionModel, NaiveBayesModel, RandomForestClassificationModel,
    DecisionTreeClassificationModel, MultilayerPerceptronClassificationModel
)
import findspark

findspark.init()

# Feature columns (must match training)
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

CATEGORICAL_FEATURES = []


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("StockStatusInference") \
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
        print(f"✗ Failed to load: {e}")
        return None


def join_datasets(health_df, inventory_df):
    """Join inventory health with inventory (same as training)"""
    if health_df is None or inventory_df is None:
        return None
    
    health_selected = health_df.select(
        "product_id",
        "current_stock",
        "available_stock",
        "reserved_quantity",
        "minimum_stock_level",
        "avg_daily_sales",
        "days_of_supply",
        "inventory_turnover_ratio",
        "days_since_restock",
        "reorder_point_breach_count"
    )
    
    inventory_selected = inventory_df.select(
        col("product_id"),
        col("stock_quantity").alias("inv_stock_quantity"),
        col("last_restocked_date")
    )
    
    joined_df = health_selected.join(inventory_selected, on="product_id", how="left")
    
    joined_df = joined_df.withColumn(
        "current_stock",
        when(col("current_stock").isNull(), col("inv_stock_quantity")).otherwise(col("current_stock"))
    ).drop("inv_stock_quantity")
    
    print(f"✓ Joined: {joined_df.count()} records")
    return joined_df


def load_model_and_preprocessors(spark, model_dir, model_name):
    """Load model and preprocessors"""
    try:
        model_path = f"{model_dir}/{model_name}"
        
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "NaiveBayes":
            model = NaiveBayesModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        elif model_name == "DecisionTree":
            model = DecisionTreeClassificationModel.load(model_path)
        elif model_name == "MultilayerPerceptron":
            model = MultilayerPerceptronClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model: {model_name}")
        
        categorical_indexers = []
        for i in range(len(CATEGORICAL_FEATURES)):
            indexer_path = f"{model_dir}/{model_name}_cat_indexer_{i}"
            indexer = StringIndexerModel.load(indexer_path)
            categorical_indexers.append(indexer)
        
        scaler_path = f"{model_dir}/{model_name}_scaler"
        scaler = StandardScalerModel.load(scaler_path)
        
        label_indexer_path = f"{model_dir}/{model_name}_label_indexer"
        label_indexer = StringIndexerModel.load(label_indexer_path)
        
        print(f"✓ Loaded model: {model_name}")
        return model, {
            "categorical_indexers": categorical_indexers,
            "scaler": scaler,
            "label_indexer": label_indexer
        }
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return None, None


def validate_dataset(df, required_columns):
    """Validate dataset"""
    if df is None:
        return False, "Dataset is None"
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    
    return True, "Validation passed"


def prepare_features(df, numerical_features, categorical_features, preprocessors):
    """Prepare features (same as training)"""
    df_filled = df.fillna(0, subset=numerical_features)
    
    if categorical_features:
        df_filled = df_filled.fillna("Unknown", subset=categorical_features)
    
    categorical_indexed_cols = []
    for i, cat_col in enumerate(categorical_features):
        indexer = preprocessors["categorical_indexers"][i]
        df_filled = indexer.transform(df_filled)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
    
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features")
    df_filled = numerical_assembler.transform(df_filled)
    
    scaler = preprocessors["scaler"]
    df_filled = scaler.transform(df_filled)
    
    if categorical_indexed_cols:
        all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    else:
        all_feature_cols = ["scaled_numerical_features"]
    
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    df_vector = final_assembler.transform(df_filled)
    
    print(f"✓ Prepared features")
    return df_vector


def generate_predictions(spark, df, model, preprocessors, model_name, MODEL_VERSION):
    """Generate predictions"""
    predictions = model.transform(df)
    
    # Convert prediction index back to label
    label_indexer = preprocessors["label_indexer"]
    labels = label_indexer.labels
    index_to_label = udf(lambda idx: labels[int(idx)], StringType())
    
    # Calculate confidence score
    calculate_confidence = udf(
        lambda prob: float(max(prob)) if prob else 0.0,
        DoubleType()
    )
    
    # Calculate days until stockout
    calculate_days_until_stockout = udf(
        lambda stock, sales: float(stock / sales) if sales and sales > 0 else 999.0,
        DoubleType()
    )
    
    # Determine reorder recommendation
    recommend_reorder = udf(
        lambda status: status in ["Out of Stock", "Low Stock"],
        BooleanType()
    )
    
    # Format output
    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("product_id"),
        current_timestamp().alias("prediction_date"),
        index_to_label(col("prediction")).alias("predicted_status"),
        calculate_days_until_stockout(
            col("current_stock"),
            col("avg_daily_sales")
        ).alias("days_until_stockout"),
        recommend_reorder(index_to_label(col("prediction"))).alias("reorder_recommendation"),
        calculate_confidence(col("probability")).alias("confidence_score"),
        lit(MODEL_VERSION).alias("model_version")
    )
    
    # Generate UUIDs
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    output_df = output_df.withColumn("prediction_id", generate_uuid())
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Saved to {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save: {e}")
        return False


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH_INVENTORY_HEALTH = f"s3a://{BUCKET_NAME}/transformed/agg_product_inventory_health.parquet"
    INPUT_PATH_INVENTORY = f"s3a://{BUCKET_NAME}/transformed/agg_inventory.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/stock_status_predictions"
    MODEL_INPUT_DIR = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/classification/models/stock_status"

    # ⚠️ MANUAL INTERVENTION: Select model
    SELECTED_MODEL = "RandomForest"  # <-- CHANGE BASED ON TRAINING RESULTS

    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print("=" * 60)
    print("Stock Status Classification - Inference Pipeline")
    print("=" * 60)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    # Load model
    model, preprocessors = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None:
        print("✗ Inference stopped: Failed to load model")
        return
    
    # Load data
    health_df = load_data(spark, INPUT_PATH_INVENTORY_HEALTH)
    inventory_df = load_data(spark, INPUT_PATH_INVENTORY)
    
    if health_df is None or inventory_df is None:
        print("✗ Inference stopped: Failed to load data")
        return
    
    # Join
    df = join_datasets(health_df, inventory_df)
    if df is None:
        print("✗ Inference stopped: Failed to join")
        return
    
    # Validate
    required_cols = ["product_id"] + NUMERICAL_FEATURES + CATEGORICAL_FEATURES
    is_valid, message = validate_dataset(df, required_cols)
    if not is_valid:
        print(f"✗ Inference stopped: {message}")
        return
    
    # Prepare features
    df_prepared = prepare_features(df, NUMERICAL_FEATURES, CATEGORICAL_FEATURES, preprocessors)
    
    # Generate predictions
    predictions_df = generate_predictions(spark, df_prepared, model, preprocessors, SELECTED_MODEL, MODEL_VERSION)
    
    # Show sample
    print("\nSample predictions:")
    predictions_df.select(
        "product_id", "predicted_status", "days_until_stockout",
        "reorder_recommendation", "confidence_score"
    ).show(5, truncate=False)
    
    # Save
    success = save_predictions(predictions_df, OUTPUT_PATH)
    
    if success:
        print("\n✓ Inference completed successfully")
    else:
        print("\n✗ Inference failed")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME='pulse-bucket-1'
    main(BUCKET_NAME)
