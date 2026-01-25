"""
Average Order Value (AOV) Prediction - Training Script
Predicts next order value for customers using historical patterns
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = "pulse-bucket-1"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/aov_prediction/"
MIN_RECORDS_THRESHOLD = 100

# Configuration
USE_CROSS_VALIDATION = False

# Feature columns (NO avg_order_value or customer_lifetime_value to prevent leakage)
FEATURE_COLUMNS = [
    # Customer metrics
    "total_orders",
    "customer_tenure_days",
    "avg_items_per_order",
    "avg_days_between_orders",
    "days_since_last_purchase",
    
    # Behavioral metrics
    "session_conversion_rate",
    "cart_abandonment_rate",
    "cancellation_rate",
    "avg_discount_per_order",
    
    # RFM scores
    "recency_score",
    "frequency_score",
    "monetary_score",
    
    # Order lag features (historical AOV)
    "aov_lag_1",
    "aov_lag_2",
    "aov_lag_3",
    "aov_rolling_3",
    "aov_trend",
    
    # Categorical (will be encoded)
    "customer_segment_label_idx",
    "preferred_payment_method_idx",
    "preferred_device_type_idx"
]

TARGET_COLUMN = "next_order_value"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("AOV_Prediction_Training")
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


def validate_dataset(spark, path, name):
    """Check if dataset exists"""
    try:
        df = spark.read.parquet(path)
        record_count = df.count()
        print(f"✓ {name} dataset found: {record_count} records")
        return df, record_count
    except Exception as e:
        print(f"✗ {name} dataset validation failed: {str(e)}")
        return None, 0


def create_customer_order_features(customers_df, orders_df):
    """
    Create features by joining customers with their order history
    Target: Predict NEXT order value using HISTORICAL patterns
    """
    print("Creating customer-order features...")
    
    # Filter delivered orders only
    orders_filtered = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    
    # Create window for each customer ordered by order date
    customer_window = Window.partitionBy("customer_id").orderBy("order_placed_at")
    
    # Add order sequence number and lag features for each customer
    orders_with_lags = orders_filtered.withColumn(
        "order_seq",
        F.row_number().over(customer_window)
    ).withColumn(
        "aov_lag_1",
        F.lag("total_amount", 1).over(customer_window)
    ).withColumn(
        "aov_lag_2",
        F.lag("total_amount", 2).over(customer_window)
    ).withColumn(
        "aov_lag_3",
        F.lag("total_amount", 3).over(customer_window)
    )
    
    # Calculate rolling average and trend
    window_rolling = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-3, -1)
    
    orders_with_lags = orders_with_lags.withColumn(
        "aov_rolling_3",
        F.avg("total_amount").over(window_rolling)
    ).withColumn(
        "aov_trend",
        F.when(
            (F.col("aov_lag_1").isNotNull()) & (F.col("aov_lag_2").isNotNull()) & (F.col("aov_lag_2") > 0),
            (F.col("aov_lag_1") - F.col("aov_lag_2")) / F.col("aov_lag_2")
        ).otherwise(0)
    )
    
    # Target: Current order's value (we predict this using previous orders)
    orders_with_lags = orders_with_lags.withColumn(
        "next_order_value",
        F.col("total_amount")
    )
    
    # Join with customer data (snapshot at order time)
    customer_order_data = orders_with_lags.join(
        customers_df.select(
            "customer_id",
            "total_orders",
            "customer_tenure_days",
            "avg_items_per_order",
            "avg_days_between_orders",
            "days_since_last_purchase",
            "session_conversion_rate",
            "cart_abandonment_rate",
            "cancellation_rate",
            "avg_discount_per_order",
            "recency_score",
            "frequency_score",
            "monetary_score",
            "customer_segment_label",
            "preferred_payment_method",
            "preferred_device_type"
        ),
        "customer_id",
        "left"
    )
    
    # Filter: Only keep orders where we have at least 1 previous order (for lag features)
    # This ensures we're predicting based on history
    customer_order_data = customer_order_data.filter(F.col("order_seq") > 1)
    
    # Fill nulls in lag features with 0
    customer_order_data = customer_order_data.fillna({
        "aov_lag_1": 0,
        "aov_lag_2": 0,
        "aov_lag_3": 0,
        "aov_rolling_3": 0,
        "aov_trend": 0
    })
    
    # Fill nulls in customer metrics
    customer_order_data = customer_order_data.fillna({
        "session_conversion_rate": 0,
        "cart_abandonment_rate": 0,
        "cancellation_rate": 0,
        "avg_discount_per_order": 0,
        "avg_days_between_orders": 0,
        "days_since_last_purchase": 0,
        "avg_items_per_order": 0,
        "recency_score": 0,
        "frequency_score": 0,
        "monetary_score": 0
    })
    
    # Fill nulls in categorical features
    customer_order_data = customer_order_data.fillna({
        "customer_segment_label": "Unknown",
        "preferred_payment_method": "Unknown",
        "preferred_device_type": "Unknown"
    })
    
    print(f"✓ Customer-order features created: {customer_order_data.count()} records")
    return customer_order_data


def prepare_training_data(df):
    """
    Prepare data with encoding and scaling
    """
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0)
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
        return None
    
    # Encode categorical features
    indexer_segment = StringIndexer(
        inputCol="customer_segment_label",
        outputCol="customer_segment_label_idx",
        handleInvalid="keep"
    )
    
    indexer_payment = StringIndexer(
        inputCol="preferred_payment_method",
        outputCol="preferred_payment_method_idx",
        handleInvalid="keep"
    )
    
    indexer_device = StringIndexer(
        inputCol="preferred_device_type",
        outputCol="preferred_device_type_idx",
        handleInvalid="keep"
    )
    
    # Apply indexers
    df_indexed = indexer_segment.fit(df_valid).transform(df_valid)
    df_indexed = indexer_payment.fit(df_indexed).transform(df_indexed)
    df_indexed = indexer_device.fit(df_indexed).transform(df_indexed)
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features_unscaled",
        handleInvalid="keep"
    )
    
    df_assembled = assembler.transform(df_indexed)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Select final columns
    df_prepared = df_scaled.select(
        "customer_id",
        "features",
        TARGET_COLUMN
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model


def train_linear_regression(train_df, test_df, use_cv=False):
    """Train Linear Regression"""
    print("\n" + "="*60)
    print("Training Linear Regression")
    print("="*60)
    
    lr = LinearRegression(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.001, 0.01, 0.1]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = lr.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "linear_regression"


def train_random_forest(train_df, test_df, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print("Training Random Forest")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        numTrees=150,
        maxDepth=12,
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [100, 150, 200]) \
            .addGrid(rf.maxDepth, [10, 12, 15]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=rf,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = rf.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "random_forest"


def train_gbt(train_df, test_df, use_cv=False):
    """Train GBT"""
    print("\n" + "="*60)
    print("Training Gradient Boosted Trees")
    print("="*60)
    
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COLUMN,
        maxIter=100,
        maxDepth=6,
        seed=42
    )
    
    if use_cv:
        print("Using CrossValidator...")
        param_grid = ParamGridBuilder() \
            .addGrid(gbt.maxIter, [50, 100, 150]) \
            .addGrid(gbt.maxDepth, [5, 6, 7]) \
            .build()
        
        evaluator = RegressionEvaluator(
            labelCol=TARGET_COLUMN,
            predictionCol="prediction",
            metricName="r2"
        )
        
        cv = CrossValidator(
            estimator=gbt,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        
        model = cv.fit(train_df).bestModel
    else:
        model = gbt.fit(train_df)
    
    predictions = model.transform(test_df)
    return model, predictions, "gbt"


def evaluate_model(predictions, model_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name}...")
    
    rmse_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="r2")
    
    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)
    
    mape_df = predictions.withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0]
    
    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    
    print(f"  RMSE: ${rmse:.2f}")
    print(f"  MAE: ${mae:.2f}")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main():
    """Main training pipeline"""
    print("\n" + "="*60)
    print("AOV Prediction Model Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    
    if None in [customers_df, orders_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Create features
    print("\nStep 2: Feature Engineering")
    print("-" * 60)
    df_features = create_customer_order_features(customers_df, orders_df)
    
    # Prepare data
    print("\nStep 3: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features)
    
    if result is None:
        print("\n✗ Training aborted: Insufficient data")
        spark.stop()
        return
    
    df_prepared, scaler = result
    
    # Split data
    print("\nStep 4: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train models
    print("\nStep 5: Model Training")
    print("-" * 60)
    
    models_results = []
    
    lr_model, lr_pred, lr_name = train_linear_regression(train_df, test_df, USE_CROSS_VALIDATION)
    lr_metrics = evaluate_model(lr_pred, lr_name)
    save_model(lr_model, lr_name)
    models_results.append(lr_metrics)
    
    rf_model, rf_pred, rf_name = train_random_forest(train_df, test_df, USE_CROSS_VALIDATION)
    rf_metrics = evaluate_model(rf_pred, rf_name)
    save_model(rf_model, rf_name)
    models_results.append(rf_metrics)
    
    gbt_model, gbt_pred, gbt_name = train_gbt(train_df, test_df, USE_CROSS_VALIDATION)
    gbt_metrics = evaluate_model(gbt_pred, gbt_name)
    save_model(gbt_model, gbt_name)
    models_results.append(gbt_metrics)
    
    # Model comparison
    print("\n" + "="*60)
    print("Model Comparison Summary")
    print("="*60)
    print(f"{'Model':<25} {'RMSE':<15} {'MAE':<15} {'R²':<10} {'MAPE':<10}")
    print("-" * 60)
    
    for m in models_results:
        print(f"{m['model']:<25} ${m['rmse']:<14.2f} ${m['mae']:<14.2f} {m['r2']:<10.4f} {m['mape']:<10.2f}%")
    
    best = max(models_results, key=lambda x: x['r2'])
    print("\n" + "="*60)
    print(f"Best Model: {best['model']} (R² = {best['r2']:.4f})")
    print("="*60)
    print("\n⚠️  MANUAL INTERVENTION REQUIRED:")
    print("   Update MODEL_NAME in predict_aov.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
