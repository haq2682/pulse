"""
Session Conversion Value Prediction - Training Script
Predicts expected order value if a session converts to a purchase

Target Calculation:
- For converted sessions: target = actual order.total_amount
- Model learns: session behavior + customer history → conversion value
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
INPUT_SESSIONS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customer_sessions.parquet"
INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/session_conversion_value/"
MIN_RECORDS_THRESHOLD = 100
MAX_NULL_PERCENTAGE = 95.0

# Configuration
USE_CROSS_VALIDATION = False

# Required columns
REQUIRED_SESSION_COLUMNS = ["session_id", "pages_viewed", "items_added_to_cart"]
REQUIRED_ORDER_COLUMNS = ["order_id", "total_amount"]

# Feature set
NUMERIC_FEATURES = [
    # Session engagement metrics
    "pages_viewed",
    "products_viewed",
    "session_duration_minutes",
    "pages_per_minute",
    "products_per_page",
    "session_engagement_score",
    
    # Cart behavior
    "items_added_to_cart",
    "cart_value",
    "cart_add_rate",
    "avg_cart_item_value",
    "browse_to_cart_ratio",
    
    # Customer historical behavior
    "customer_total_orders",
    "customer_lifetime_value",
    "customer_avg_order_value",
    "customer_recency_days",
    "customer_session_conversion_rate",
    "customer_cart_abandonment_rate",
    "is_new_customer",
    "is_repeat_customer",
    
    # Customer RFM
    "rfm_overall_score",
    "recency_score",
    "frequency_score",
    "monetary_score",
    
    # Time-based features
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_business_hours",
    
    # Categorical (indexed)
    "device_type_idx",
    "referrer_source_idx",
    "customer_segment_idx"
]

TARGET_COLUMN = "conversion_value"


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("Session_Conversion_Value_Training")
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


def validate_columns(df, required_columns, dataset_name):
    """Validate columns exist and are not entirely null"""
    print(f"\nValidating columns for {dataset_name}...")
    
    existing_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"✗ Missing columns in {dataset_name}: {', '.join(missing_columns)}")
        return False, missing_columns, []
    
    total_count = df.count()
    null_columns = []
    
    for col in required_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 100
        
        if null_pct > MAX_NULL_PERCENTAGE:
            print(f"✗ Column '{col}' is {null_pct:.1f}% null (threshold: {MAX_NULL_PERCENTAGE}%)")
            null_columns.append(col)
        elif null_pct > 50:
            print(f"⚠  Column '{col}' is {null_pct:.1f}% null (may affect accuracy)")
    
    if null_columns:
        return False, [], null_columns
    
    print(f"✓ All required columns validated for {dataset_name}")
    return True, [], []


def create_session_conversion_features(sessions_df, customers_df, orders_df):
    """
    Create comprehensive session features with conversion value target
    Only use CONVERTED sessions for training
    """
    print("Creating session conversion features...")
    
    # Filter to converted sessions only (where we have actual order values)
    # The 'converted' column is 1 for sessions that led to an order
    converted_sessions = sessions_df.filter(F.col("converted") == 1)
    
    print(f"Total sessions: {sessions_df.count()}")
    print(f"Converted sessions: {converted_sessions.count()}")
    
    # Extract orders that came from sessions
    # Link through customer_id and timestamp proximity
    # Assume order placed within session timeframe or shortly after
    session_orders = converted_sessions.alias("s").join(
        orders_df.alias("o"),
        (F.col("s.customer_id") == F.col("o.customer_id")) &
        (F.col("o.order_placed_at") >= F.col("s.session_start")) &
        (F.col("o.order_placed_at") <= F.date_add(F.col("s.session_end"), 1)),  # Within 1 day after session
        "inner"
    ).select(
        F.col("s.session_id"),
        F.col("s.customer_id"),
        F.col("s.session_start"),
        F.col("s.pages_viewed"),
        F.col("s.products_viewed"),
        F.col("s.session_duration_minutes"),
        F.col("s.items_added_to_cart"),
        F.col("s.cart_value"),
        F.col("s.device_type"),
        F.col("s.referrer_source"),
        F.col("s.pages_per_minute"),
        F.col("s.products_per_page"),
        F.col("s.cart_add_rate"),
        F.col("s.avg_cart_item_value"),
        F.col("s.session_engagement_score"),
        F.col("o.total_amount").alias(TARGET_COLUMN)  # This is our target!
    )
    
    # Handle duplicates (session might match multiple orders - take first/max)
    window_spec = Window.partitionBy("session_id").orderBy(F.desc("conversion_value"))
    session_orders = session_orders.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    ).filter(
        F.col("row_num") == 1
    ).drop("row_num")
    
    print(f"Sessions with order values: {session_orders.count()}")
    
    # Join with customer historical data
    session_features = session_orders.join(
        customers_df.select(
            "customer_id",
            F.col("total_orders").alias("customer_total_orders"),
            F.col("customer_lifetime_value"),
            F.col("avg_order_value").alias("customer_avg_order_value"),
            F.col("order_recency_days").alias("customer_recency_days"),
            F.col("session_conversion_rate").alias("customer_session_conversion_rate"),
            F.col("cart_abandonment_rate").alias("customer_cart_abandonment_rate"),
            F.col("is_repeat_customer"),
            F.col("rfm_overall_score"),
            F.col("recency_score"),
            F.col("frequency_score"),
            F.col("monetary_score"),
            F.col("customer_segment")
        ),
        "customer_id",
        "left"  # Left join - keep sessions even if no customer history
    )
    
    print(f"After customer join: {session_features.count()}")
    
    # Fill nulls for customer features (new customers)
    session_features = session_features.fillna({
        "customer_total_orders": 0,
        "customer_lifetime_value": 0,
        "customer_avg_order_value": 0,
        "customer_recency_days": 999,
        "customer_session_conversion_rate": 0,
        "customer_cart_abandonment_rate": 0,
        "is_repeat_customer": 0,
        "rfm_overall_score": 0,
        "recency_score": 0,
        "frequency_score": 0,
        "monetary_score": 0,
        "customer_segment": "Unknown"
    })
    
    # Calculate is_new_customer
    session_features = session_features.withColumn(
        "is_new_customer",
        F.when(F.col("customer_total_orders") <= 1, 1.0).otherwise(0.0)
    )
    
    # Calculate browse to cart ratio
    session_features = session_features.withColumn(
        "browse_to_cart_ratio",
        F.when(
            F.col("products_viewed") > 0,
            F.col("items_added_to_cart") / F.col("products_viewed")
        ).otherwise(0)
    )
    
    # Extract time-based features
    session_features = session_features.withColumn(
        "hour_of_day",
        F.hour(F.col("session_start"))
    ).withColumn(
        "day_of_week",
        F.dayofweek(F.col("session_start"))
    ).withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("session_start")).isin([1, 7]), 1.0).otherwise(0.0)
    ).withColumn(
        "is_business_hours",
        F.when(
            (F.hour(F.col("session_start")) >= 9) &
            (F.hour(F.col("session_start")) <= 17),
            1.0
        ).otherwise(0.0)
    )
    
    # Fill nulls in session features
    session_features = session_features.fillna({
        "pages_viewed": 1,
        "products_viewed": 0,
        "session_duration_minutes": 1,
        "items_added_to_cart": 0,
        "cart_value": 0,
        "pages_per_minute": 0,
        "products_per_page": 0,
        "cart_add_rate": 0,
        "avg_cart_item_value": 0,
        "session_engagement_score": 0,
        "device_type": "Unknown",
        "referrer_source": "Unknown"
    })
    
    print(f"✓ Session conversion features created: {session_features.count()} records")
    return session_features


def prepare_training_data(df):
    """Prepare data with encoding and scaling"""
    print("Preparing training data...")
    
    # Filter valid records
    df_valid = df.filter(
        (F.col(TARGET_COLUMN).isNotNull()) &
        (F.col(TARGET_COLUMN) > 0)  # Only positive conversion values
    )
    
    valid_count = df_valid.count()
    print(f"Records with valid target: {valid_count}")
    
    if valid_count < MIN_RECORDS_THRESHOLD:
        print(f"✗ Insufficient data: {valid_count} < {MIN_RECORDS_THRESHOLD}")
        return None
    
    # Encode categorical features
    device_indexer = StringIndexer(
        inputCol="device_type",
        outputCol="device_type_idx",
        handleInvalid="keep"
    )
    
    referrer_indexer = StringIndexer(
        inputCol="referrer_source",
        outputCol="referrer_source_idx",
        handleInvalid="keep"
    )
    
    segment_indexer = StringIndexer(
        inputCol="customer_segment",
        outputCol="customer_segment_idx",
        handleInvalid="keep"
    )
    
    df_indexed = device_indexer.fit(df_valid).transform(df_valid)
    df_indexed = referrer_indexer.fit(df_indexed).transform(df_indexed)
    df_indexed = segment_indexer.fit(df_indexed).transform(df_indexed)
    
    # Filter features that exist
    existing_features = [f for f in NUMERIC_FEATURES if f in df_indexed.columns]
    missing_features = [f for f in NUMERIC_FEATURES if f not in df_indexed.columns]
    
    if missing_features:
        print(f"⚠  Skipping missing features: {', '.join(missing_features)}")
    
    print(f"Using {len(existing_features)} features for training")
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=existing_features,
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
        "session_id",
        "customer_id",
        "features",
        TARGET_COLUMN
    )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared, scaler_model, existing_features


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
        numTrees=200,
        maxDepth=15,
        seed=42
    )
    
    if use_cv:
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [150, 200, 250]) \
            .addGrid(rf.maxDepth, [12, 15, 18]) \
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
        maxIter=150,
        maxDepth=8,
        seed=42
    )
    
    if use_cv:
        param_grid = ParamGridBuilder() \
            .addGrid(gbt.maxIter, [100, 150, 200]) \
            .addGrid(gbt.maxDepth, [6, 8, 10]) \
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
    
    mape_df = predictions.filter(F.col(TARGET_COLUMN) > 0).withColumn(
        "ape",
        F.abs((F.col(TARGET_COLUMN) - F.col("prediction")) / F.col(TARGET_COLUMN)) * 100
    )
    mape = mape_df.agg(F.avg("ape")).collect()[0][0] if mape_df.count() > 0 else 0
    
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
    print("Session Conversion Value Prediction - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    sessions_df, _ = validate_dataset(spark, INPUT_SESSIONS_PATH, "Customer Sessions")
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    
    if None in [sessions_df, customers_df, orders_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    session_valid, _, _ = validate_columns(sessions_df, REQUIRED_SESSION_COLUMNS, "Sessions")
    order_valid, _, _ = validate_columns(orders_df, REQUIRED_ORDER_COLUMNS, "Orders")
    
    if not (session_valid and order_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_session_conversion_features(sessions_df, customers_df, orders_df)
    
    # Prepare data
    print("\nStep 4: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features)
    
    if result is None:
        print("\n✗ Training aborted: Insufficient data")
        spark.stop()
        return
    
    df_prepared, scaler, feature_list = result
    
    print(f"\n{'='*60}")
    print(f"Final Feature Set ({len(feature_list)} features):")
    print(f"{'='*60}")
    for i, feat in enumerate(feature_list, 1):
        print(f"{i:2d}. {feat}")
    
    # Split data
    print("\nStep 5: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train models
    print("\nStep 6: Model Training")
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
    print("   Update MODEL_NAME in predict_session_conversion.py")
    print(f"   Available: {', '.join([m['model'] for m in models_results])}")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
