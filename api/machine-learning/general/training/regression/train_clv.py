"""
Customer Lifetime Value (CLV) Prediction - Training Script
Trains multiple regression models to predict customer lifetime value
"""

import os
import sys
import findspark
from dotenv import load_dotenv

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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime

# Load environment variables
load_dotenv()

# Configuration - General models output to pulse-bucket-1
MODEL_NAME = "clv"
INPUT_RELATIVE_PATH = "transformed/agg_customers.parquet"
MODEL_OUTPUT_DIR = get_general_model_output_path("regression", MODEL_NAME)

# Training record window (min, max records for training)
MIN_RECORDS, MAX_RECORDS = get_training_window(MODEL_NAME)

# Feature columns (avoiding leakage - no total_revenue or customer_lifetime_value)
FEATURE_COLUMNS = [
    "total_orders",
    "avg_order_value",
    "customer_tenure_days",
    "avg_days_between_orders",
    "order_frequency",
    "total_discount_received",
    "session_conversion_rate",
    "cart_abandonment_rate",
    "recency_score",
    "frequency_score",
    "monetary_score"
]

TARGET_COLUMN = "customer_lifetime_value"


def create_spark_session():
    """Initialize Spark session with MinIO configuration"""
    return (
        SparkSession.builder
        .appName("CLV_Model_Training")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "1000")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("inferSchema", "true")
        .config("mergeSchema", "true")
        .getOrCreate()
    )


def validate_dataset(spark, path):
    """Check if dataset exists and is readable"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ Dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ Dataset validation failed: {str(e)}")
        return None, 0


def validate_columns(df, required_columns):
    """Check if required columns exist and have non-null values"""
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"✗ Missing columns: {missing_columns}")
        return False
    
    # Check for non-null values in each column
    for col in required_columns:
        non_null_count = df.filter(F.col(col).isNotNull()).count()
        total_count = df.count()
        null_percentage = ((total_count - non_null_count) / total_count) * 100
        
        print(f"  {col}: {non_null_count}/{total_count} non-null ({null_percentage:.1f}% null)")
        
        if non_null_count == 0:
            print(f"✗ Column '{col}' is entirely null")
            return False
    
    print("✓ All required columns validated")
    return True


def prepare_training_data(df):
    """
    Prepare data for training:
    1. Filter records with valid target values
    2. Fill missing feature values with 0 (conservative approach)
    3. Create feature vector
    """
    # Filter records where target is not null and > 0
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) & 
        (F.col(TARGET_COLUMN) > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid CLV: {valid_count}")
    
    if valid_count < MIN_RECORDS:
        print(f"✗ Insufficient training data: {valid_count} < {MIN_RECORDS}")
        return None
    
    # Fill missing feature values with 0
    df_filled = df_valid.fillna(0, subset=FEATURE_COLUMNS)
    
    # Assemble features into vector
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep"
    )
    
    df_prepared = assembler.transform(df_filled).select("features", TARGET_COLUMN)
    
    print(f"✓ Data prepared: {df_prepared.count()} records ready for training")
    return df_prepared


def train_linear_regression(train_df, test_df):
    """Train Linear Regression model"""
    print("\n" + "="*60)
    print("Training Linear Regression Model")
    print("="*60)
    
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )
    
    model = lr.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "linear_regression"


def train_random_forest(train_df, test_df):
    """Train Random Forest Regressor"""
    print("\n" + "="*60)
    print("Training Random Forest Regressor")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    model = rf.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "random_forest"


def train_gbt(train_df, test_df):
    """Train Gradient Boosted Trees Regressor"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees Regressor")
    print("="*60)
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=50,
        maxDepth=5,
        seed=42
    )
    
    model = gbt.fit(train_df)
    predictions = model.transform(test_df)
    
    return model, predictions, "gbt"


def evaluate_model(predictions, model_name):
    """Evaluate regression model using multiple metrics"""
    print(f"\nEvaluating {model_name}...")
    
    # RMSE
    rmse_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = rmse_evaluator.evaluate(predictions)
    
    # MAE
    mae_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="mae"
    )
    mae = mae_evaluator.evaluate(predictions)
    
    # R2
    r2_evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN,
        predictionCol="prediction",
        metricName="r2"
    )
    r2 = r2_evaluator.evaluate(predictions)
    
    # MAPE (custom calculation)
    mape_df = predictions.withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]
    
    metrics = {
        "model": model_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape
    }
    
    print(f"  RMSE: {rmse:.2f}")
    print(f"  MAE: {mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save trained model to MinIO"""
    model_path = f"{MODEL_OUTPUT_DIR}/{model_name}"
    
    # Overwrite existing model
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("CLV Prediction Model Training - General Model")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Training window: {MIN_RECORDS} - {MAX_RECORDS} records")
    print(f"Model output: {MODEL_OUTPUT_DIR}")
    print("="*60 + "\n")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Load data from all buckets
    print("Step 1: Loading data from all MinIO buckets...")
    print("-" * 60)
    all_required_columns = FEATURE_COLUMNS + [TARGET_COLUMN]
    df, record_count = load_data_from_all_buckets(
        spark,
        INPUT_RELATIVE_PATH,
        required_columns=all_required_columns,
        filter_nulls=True
    )
    
    if df is None:
        print("⚠️  No data available. Skipping training.")
        spark.stop()
        return
    
    # Step 2: Validate training data window
    print("\nStep 2: Validate Training Data Window")
    print("-" * 60)
    is_valid, df = validate_training_data(
        df, record_count, MIN_RECORDS, MAX_RECORDS, MODEL_NAME
    )
    
    if not is_valid:
        print("⚠️  Training skipped due to insufficient data.")
        spark.stop()
        return
    
    # Step 3: Validate columns
    print("\nStep 3: Column Validation")
    print("-" * 60)
    
    if not validate_columns(df, all_required_columns):
        print("⚠️  Training skipped due to required columns missing or invalid")
        spark.stop()
        return
    
    # Step 4: Prepare training data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    df_prepared = prepare_training_data(df)
    
    if df_prepared is None:
        print("⚠️  Training skipped due to insufficient training data")
        spark.stop()
        return
    
    # Step 4: Split data
    print("\nStep 4: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"Training set: {train_count} records")
    print(f"Test set: {test_count} records")
    
    # Step 5: Train models
    print("\nStep 5: Model Training")
    print("-" * 60)
    
    models_results = []
    
    # Train Linear Regression
    lr_model, lr_predictions, lr_name = train_linear_regression(train_df, test_df)
    lr_metrics = evaluate_model(lr_predictions, lr_name)
    save_model(lr_model, lr_name)
    models_results.append(lr_metrics)
    
    # Train Random Forest
    rf_model, rf_predictions, rf_name = train_random_forest(train_df, test_df)
    rf_metrics = evaluate_model(rf_predictions, rf_name)
    save_model(rf_model, rf_name)
    models_results.append(rf_metrics)
    
    # Train GBT
    gbt_model, gbt_predictions, gbt_name = train_gbt(train_df, test_df)
    gbt_metrics = evaluate_model(gbt_predictions, gbt_name)
    save_model(gbt_model, gbt_name)
    models_results.append(gbt_metrics)
    
    # Step 6: Model Comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<12} {'MAE':<12} {'R²':<10} {'MAPE':<10}")
    print("-" * 60)
    
    for metrics in models_results:
        print(
            f"{metrics['model']:<25} "
            f"{metrics['rmse']:<12.2f} "
            f"{metrics['mae']:<12.2f} "
            f"{metrics['r2']:<10.4f} "
            f"{metrics['mape']:<10.2f}%"
        )
    
    # Find best model by R²
    best_model = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best_model['model']} (R² = {best_model['r2']:.4f})")
    print("="*60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Review model metrics above and select the best model for inference.")
    print(f"   Update the MODEL_NAME variable in predict_clv.py")
    print(f"   Available models: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed successfully\n")
    
    spark.stop()


if __name__ == "__main__":
    main()