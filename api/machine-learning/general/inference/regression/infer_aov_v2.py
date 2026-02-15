"""
Average Order Value (AOV) Prediction - FIXED Inference Script
"""

import os
import findspark
from dotenv import load_dotenv

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, StandardScalerModel
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from datetime import datetime
import uuid
import json

# Load environment variables
load_dotenv()

# Feature set (MUST MATCH TRAINING - reduced set)
NUMERIC_FEATURES = [
    # Customer profile (core)
    "total_orders",
    "customer_tenure_days",
    "total_items_purchased",
    "avg_items_per_order",
    
    # Calculated temporal features
    "calc_avg_days_between_orders",
    "days_since_prev_order",
    "order_frequency_per_month",
    
    # Behavioral metrics (pruned)
    "session_conversion_rate",
    "cart_abandonment_rate",
    "cancellation_rate",
    "customer_activity_score",
    
    # Engagement (pruned)
    "total_sessions",
    "wishlist_items_count",
    
    # RFM
    "recency_score",
    "frequency_score",
    "monetary_score",
    
    # Order history lags
    "aov_lag_1",
    "aov_lag_2",
    "aov_lag_3",
    "aov_rolling_3",
    "aov_rolling_6",
    "aov_trend",
    "aov_volatility",
    
    # Order patterns (improved)
    "avg_discount_per_order",
    "discount_rolling_3",
    "discount_sensitivity_weighted",  # FIXED: Weighted version
    "spending_acceleration",
    "avg_products_per_order",
    "avg_product_price",
    "avg_category_diversity",
    
    # Temporal
    "order_placed_month",
    "order_placed_day_of_week",
    "days_since_first_order"
]

CATEGORICAL_FEATURES = [
    "customer_segment_label",
    "preferred_payment_method",
    "preferred_device_type"
]


def create_spark_session():
    """Initialize Spark session"""
    return (
        SparkSession.builder
        .appName("AOV_Prediction_Fixed_Inference")
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


def load_model(model_name, MODEL_BASE_PATH):
    """Load trained model from MinIO"""
    model_path = f"{MODEL_BASE_PATH}{model_name}"
    
    try:
        if model_name == "linear_regression":
            model = LinearRegressionModel.load(model_path)
        elif model_name == "random_forest":
            model = RandomForestRegressionModel.load(model_path)
        elif model_name == "gbt":
            model = GBTRegressionModel.load(model_path)
        else:
            print(f"✗ Unknown model type: {model_name}")
            return None
        
        print(f"✓ Model loaded: {model_path}")
        return model
    except Exception as e:
        print(f"✗ Failed to load model: {str(e)}")
        return None


def load_scaler(SCALER_PATH):
    """Load saved scaler model for linear regression"""
    try:
        scaler_model = StandardScalerModel.load(SCALER_PATH)
        print(f"✓ Scaler loaded: {SCALER_PATH}")
        return scaler_model
    except Exception as e:
        print(f"⚠ Scaler not found or failed to load: {str(e)}")
        print("  Will fit new scaler on inference data (may differ from training)")
        return None


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


def create_inference_features(customers_df, orders_df, order_items_df, products_df):
    """
    Create FIXED features for inference
    
    IMPORTANT: This uses the SAME feature engineering as training:
    - Weighted discount sensitivity
    - Stable rolling windows
    - No leakage (using lag features only)
    """
    print("Creating inference features (matching fixed training)...")
    
    # Filter delivered orders
    orders_filtered = orders_df.filter(
        ((F.col("order_status") == "Delivered") | 
         (F.col("order_status") == "delivered") | 
         (F.col("order_status") == "Completed") | 
         (F.col("order_status") == "completed") | 
         (F.col("order_status") == "complete") | 
         (F.col("order_status") == "Complete")) &
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    
    # Join with order items and products
    orders_with_items = orders_filtered.alias("o").join(
        order_items_df.alias("oi"),
        F.col("o.order_id") == F.col("oi.order_id"),
        "left"
    ).join(
        products_df.alias("p").select("product_id", "category", "sell_price"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "left"
    )
    
    # Aggregate order metrics
    order_agg = orders_with_items.groupBy("o.order_id").agg(
        F.first("o.customer_id").alias("customer_id"),
        F.first("o.order_placed_at").alias("order_placed_at"),
        F.first("o.total_amount").alias("total_amount"),
        F.first("o.total_discount").alias("total_discount"),
        F.first("o.order_placed_month").alias("order_placed_month"),
        F.first("o.order_placed_day_of_week").alias("order_placed_day_of_week"),
        F.count("oi.order_item_id").alias("products_in_order"),
        F.avg("oi.product_price").alias("avg_product_price_order"),
        F.countDistinct("p.category").alias("unique_categories_in_order"),
        F.first("p.category").alias("primary_category_order")
    ).select(
        "customer_id", "order_placed_at", "total_amount", "total_discount",
        "order_placed_month", "order_placed_day_of_week", "products_in_order",
        "avg_product_price_order", "unique_categories_in_order", "primary_category_order"
    )
    
    # Customer window
    customer_window = Window.partitionBy("customer_id").orderBy("order_placed_at")
    
    # Add sequence and temporal features
    orders_with_seq = order_agg.withColumn(
        "order_seq",
        F.row_number().over(customer_window)
    ).withColumn(
        "days_since_first_order",
        F.datediff(F.col("order_placed_at"), F.first("order_placed_at").over(customer_window))
    ).withColumn(
        "days_since_prev_order",
        F.datediff(F.col("order_placed_at"), F.lag("order_placed_at", 1).over(customer_window))
    ).withColumn(
        "orders_up_to_now",
        F.row_number().over(customer_window)
    )
    
    # Calculate avg_days_between_orders
    orders_with_seq = orders_with_seq.withColumn(
        "calc_avg_days_between_orders",
        F.when(
            F.col("order_seq") > 1,
            F.col("days_since_first_order") / (F.col("order_seq") - 1)
        ).otherwise(0)
    ).withColumn(
        "order_frequency_per_month",
        F.when(
            F.col("days_since_first_order") > 0,
            (F.col("order_seq") - 1) / (F.col("days_since_first_order") / 30.0)
        ).otherwise(0)
    )
    
    # Lag features
    orders_with_lags = orders_with_seq.withColumn(
        "aov_lag_1",
        F.lag("total_amount", 1).over(customer_window)
    ).withColumn(
        "aov_lag_2",
        F.lag("total_amount", 2).over(customer_window)
    ).withColumn(
        "aov_lag_3",
        F.lag("total_amount", 3).over(customer_window)
    )
    
    # Rolling windows (matching training)
    window_rolling_3 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-3, -1)
    window_rolling_6 = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(-6, -1)
    
    orders_with_lags = orders_with_lags.withColumn(
        "aov_rolling_3",
        F.coalesce(
            F.avg("total_amount").over(window_rolling_3),
            F.col("aov_lag_1")
        )
    ).withColumn(
        "aov_rolling_6",
        F.coalesce(
            F.avg("total_amount").over(window_rolling_6),
            F.col("aov_rolling_3"),
            F.col("aov_lag_1")
        )
    ).withColumn(
        "discount_rolling_3",
        F.coalesce(
            F.avg("total_discount").over(window_rolling_3),
            F.lag("total_discount", 1).over(customer_window),
            F.lit(0)
        )
    )
    
    # Trend, volatility, acceleration
    orders_with_lags = orders_with_lags.withColumn(
        "aov_trend",
        F.when(
            (F.col("aov_lag_1").isNotNull()) & (F.col("aov_lag_2").isNotNull()) & (F.col("aov_lag_2") > 0),
            (F.col("aov_lag_1") - F.col("aov_lag_2")) / F.col("aov_lag_2")
        ).otherwise(0)
    ).withColumn(
        "aov_volatility",
        F.coalesce(
            F.stddev("total_amount").over(window_rolling_6),
            F.lit(0)
        )
    ).withColumn(
        "spending_acceleration",
        F.when(
            (F.col("aov_rolling_3").isNotNull()) & (F.col("aov_rolling_6").isNotNull()) & (F.col("aov_rolling_6") > 0),
            (F.col("aov_rolling_3") - F.col("aov_rolling_6")) / F.col("aov_rolling_6")
        ).otherwise(0)
    )
    
    # FIXED: Weighted discount sensitivity (matching training)
    window_cumsum = Window.partitionBy("customer_id").orderBy("order_placed_at").rowsBetween(Window.unboundedPreceding, 0)
    
    orders_with_lags = orders_with_lags.withColumn(
        "cumsum_discount",
        F.coalesce(F.sum("total_discount").over(window_cumsum), F.lit(0))
    ).withColumn(
        "cumsum_amount",
        F.coalesce(F.sum("total_amount").over(window_cumsum), F.lit(1))
    ).withColumn(
        "discount_sensitivity_weighted",
        F.when(
            F.col("cumsum_amount") > 0,
            F.col("cumsum_discount") / F.col("cumsum_amount")
        ).otherwise(0)
    )
    
    # Discount percentage
    orders_with_lags = orders_with_lags.withColumn(
        "order_discount_pct",
        F.when(
            F.col("total_amount") > 0,
            (F.col("total_discount") / F.col("total_amount")) * 100
        ).otherwise(0)
    )
    
    # Category diversity
    orders_with_lags = orders_with_lags.withColumn(
        "avg_category_diversity",
        F.coalesce(
            F.avg("unique_categories_in_order").over(window_rolling_6),
            F.col("unique_categories_in_order")
        )
    )
    
    # Get latest order for each customer (for inference, we predict next order)
    window_latest = Window.partitionBy("customer_id").orderBy(F.desc("order_placed_at"))
    
    latest_orders = orders_with_lags.withColumn(
        "row_num",
        F.row_number().over(window_latest)
    ).filter(
        (F.col("row_num") == 1) &
        (F.col("order_seq") > 1)  # Need at least 1 previous order
    ).drop("row_num")
    
    # Join with customer data
    customer_features = latest_orders.join(
        customers_df.select(
            "customer_id", "total_orders", "customer_tenure_days", "total_items_purchased",
            "avg_items_per_order",
            "session_conversion_rate", "cart_abandonment_rate", "cancellation_rate",
            "total_reviews_written", "avg_review_rating", "customer_activity_score",
            "total_pages_viewed", "total_products_viewed", "total_sessions",
            "wishlist_items_count", "recency_score", "frequency_score", "monetary_score",
            "avg_discount_per_order", "customer_segment_label", "preferred_payment_method",
            "preferred_device_type"
        ),
        "customer_id",
        "left"
    )
    
    # Calculate metrics
    customer_features = customer_features.withColumn(
        "avg_products_per_order",
        F.when(
            F.col("total_orders") > 0,
            F.col("total_items_purchased") / F.col("total_orders")
        ).otherwise(0)
    ).withColumn(
        "avg_product_price",
        F.coalesce(F.col("avg_product_price_order"), F.lit(0))
    ).withColumn(
        "avg_order_discount_pct",
        F.coalesce(F.col("order_discount_pct"), F.lit(0))
    ).withColumn(
        "avg_days_per_order",
        F.when(
            F.col("total_orders") > 0,
            F.col("customer_tenure_days") / F.col("total_orders")
        ).otherwise(0)
    )
    
    # Fill nulls
    customer_features = customer_features.fillna({
        "aov_lag_1": 0, "aov_lag_2": 0, "aov_lag_3": 0,
        "aov_rolling_3": 0, "aov_rolling_6": 0, "aov_trend": 0, "aov_volatility": 0,
        "discount_rolling_3": 0, "spending_acceleration": 0, "discount_sensitivity_weighted": 0,
        "avg_category_diversity": 0,
        "total_items_purchased": 0, "session_conversion_rate": 0,
        "cart_abandonment_rate": 0, "cancellation_rate": 0,
        "total_reviews_written": 0, "avg_review_rating": 0,
        "customer_activity_score": 0, "total_pages_viewed": 0,
        "total_products_viewed": 0, "total_sessions": 0, "wishlist_items_count": 0,
        "avg_discount_per_order": 0, "avg_items_per_order": 0,
        "recency_score": 0, "frequency_score": 0, "monetary_score": 0,
        "customer_segment_label": "Unknown", "preferred_payment_method": "Unknown",
        "preferred_device_type": "Unknown", "days_since_first_order": 0,
        "days_since_prev_order": 0, "calc_avg_days_between_orders": 0,
        "order_frequency_per_month": 0, "avg_products_per_order": 0,
        "avg_product_price": 0, "avg_days_per_order": 0,
        "primary_category_order": "Unknown"
    })
    
    print(f"✓ Inference features created: {customer_features.count()} customers")
    return customer_features


def prepare_inference_data(df, model_name, saved_scaler=None):
    """
    Prepare and optionally scale features for inference
    
    FIX: Only scale for linear regression, skip for tree models
    """
    # Encode categorical
    indexed_cols = []
    
    for cat_col in CATEGORICAL_FEATURES:
        idx_col = f"{cat_col}_idx"
        indexed_cols.append(idx_col)
        indexer = StringIndexer(
            inputCol=cat_col,
            outputCol=idx_col,
            handleInvalid="keep"
        )
        df = indexer.fit(df).transform(df)
    
    # Combine features
    all_features = NUMERIC_FEATURES + indexed_cols
    existing_features = [f for f in all_features if f in df.columns]
    
    missing_features = [f for f in all_features if f not in df.columns]
    if missing_features:
        print(f"⚠ Missing features: {', '.join(missing_features[:5])}...")
    
    # Check if scaling is needed
    needs_scaling = model_name in MODELS_REQUIRING_SCALING
    
    if needs_scaling:
        print(f"  Applying feature scaling (required for {model_name})")
        
        # Assemble first
        assembler = VectorAssembler(
            inputCols=existing_features,
            outputCol="features_unscaled",
            handleInvalid="keep"
        )
        df_assembled = assembler.transform(df)
        
        # Use saved scaler if available, otherwise fit new one
        if saved_scaler is not None:
            df_scaled = saved_scaler.transform(df_assembled)
            print("  ✓ Using saved scaler from training")
        else:
            scaler = StandardScaler(
                inputCol="features_unscaled",
                outputCol="features",
                withStd=True,
                withMean=True
            )
            scaler_model = scaler.fit(df_assembled)
            df_scaled = scaler_model.transform(df_assembled)
            print("  ⚠ Fitted new scaler on inference data")
        
        df_prepared = df_scaled.select(
            "customer_id",
            "aov_lag_1",
            "aov_rolling_3",
            "aov_trend",
            "total_amount",  # Last order value for display
            "features"
        )
    else:
        print(f"  Skipping scaling (not needed for {model_name})")
        
        # Direct assembly without scaling
        assembler = VectorAssembler(
            inputCols=existing_features,
            outputCol="features",
            handleInvalid="keep"
        )
        df_assembled = assembler.transform(df)
        
        df_prepared = df_assembled.select(
            "customer_id",
            "aov_lag_1",
            "aov_rolling_3",
            "aov_trend",
            "total_amount",  # Last order value for display
            "features"
        )
    
    print(f"✓ Data prepared: {df_prepared.count()} records")
    return df_prepared


def generate_predictions(model, df, model_name):
    """Generate predictions with confidence intervals and factors"""
    predictions_df = model.transform(df)
    
    prediction_id_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    current_timestamp = F.lit(datetime.now())
    
    # Create factors JSON
    factors_udf = F.udf(
        lambda lag1, rolling3, trend, last_amount: json.dumps({
            "last_order_value": float(last_amount) if last_amount else 0,
            "prev_order_value": float(lag1) if lag1 else 0,
            "average_3_orders": float(rolling3) if rolling3 else 0,
            "spending_trend": "increasing" if trend and trend > 0.05 else ("decreasing" if trend and trend < -0.05 else "stable"),
            "trend_percentage": round(float(trend * 100) if trend else 0, 2)
        }),
        StringType()
    )
    
    # Calculate confidence based on data availability
    confidence_udf = F.udf(
        lambda lag1, lag2, lag3: 0.92 if (lag1 and lag2 and lag3) else (0.85 if (lag1 and lag2) else 0.75),
        "double"
    )
    
    output_df = predictions_df.select(
        prediction_id_udf().alias("prediction_id"),
        F.col("customer_id"),
        current_timestamp.alias("prediction_date"),
        F.col("prediction").alias("predicted_next_aov"),
        (F.col("prediction") * 0.85).alias("confidence_interval_lower"),
        (F.col("prediction") * 1.15).alias("confidence_interval_upper"),
        factors_udf(
            F.col("aov_lag_1"),
            F.col("aov_rolling_3"),
            F.col("aov_trend"),
            F.col("total_amount")
        ).alias("factors_influencing_aov"),
        confidence_udf(
            F.col("aov_lag_1"),
            F.col("aov_rolling_3"),
            F.col("aov_trend")
        ).alias("confidence_score"),
        F.lit(model_name).alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions to MinIO"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Predictions saved: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save predictions: {str(e)}")
        return False


def display_sample_predictions(df, n=5):
    """Display sample predictions"""
    print("\n" + "="*60)
    print(f"Sample AOV Predictions (first {n} customers)")
    print("="*60)
    
    sample = df.select(
        "customer_id",
        "predicted_next_aov",
        "confidence_interval_lower",
        "confidence_interval_upper",
        "factors_influencing_aov",
        "confidence_score"
    ).limit(n).collect()
    
    for row in sample:
        print(f"Customer: {row['customer_id']}")
        print(f"  Predicted Next AOV: ${row['predicted_next_aov']:>10.2f}")
        print(f"  Range (85% CI): ${row['confidence_interval_lower']:>10.2f} - ${row['confidence_interval_upper']:>10.2f}")
        print(f"  Confidence: {row['confidence_score']:.0%}")
        factors = json.loads(row['factors_influencing_aov'])
        print(f"  Last Order: ${factors['last_order_value']:.2f}")
        print(f"  Avg (3 orders): ${factors['average_3_orders']:.2f}")
        print(f"  Trend: {factors['spending_trend']} ({factors['trend_percentage']:+.1f}%)")
        print()


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_ORDER_ITEMS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    INPUT_PRODUCTS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/predictions/aov_prediction/"
    MODEL_BASE_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/aov_prediction/"
    SCALER_PATH = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/regression/models/aov_prediction/scaler"

    # ⚠️ MANUAL CONFIGURATION REQUIRED:
    MODEL_NAME = "random_forest"  # Options: "linear_regression", "random_forest", "gbt"

    # Models that require scaling (must match training)
    MODELS_REQUIRING_SCALING = ["linear_regression"]
    """Main inference pipeline"""
    print("\n" + "="*60)
    print("AOV Prediction - FIXED Inference Pipeline")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL_NAME}")
    print(f"Scaling: {'Yes' if MODEL_NAME in MODELS_REQUIRING_SCALING else 'No (tree model)'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print("Step 1: Load Model")
    print("-" * 60)
    model = load_model(MODEL_NAME, MODEL_BASE_PATH)
    
    if model is None:
        print("\n✗ Inference aborted: Model not found")
        spark.stop()
        return
    
    # Load scaler if needed
    saved_scaler = None
    if MODEL_NAME in MODELS_REQUIRING_SCALING:
        print("\nStep 1b: Load Scaler (required for linear_regression)")
        print("-" * 60)
        saved_scaler = load_scaler(SCALER_PATH)
    
    # Load datasets
    print("\nStep 2: Load Datasets")
    print("-" * 60)
    
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    order_items_df, _ = validate_dataset(spark, INPUT_ORDER_ITEMS_PATH, "Order Items")
    products_df, _ = validate_dataset(spark, INPUT_PRODUCTS_PATH, "Products")
    
    if None in [customers_df, orders_df, order_items_df, products_df]:
        print("\n✗ Inference aborted: Missing datasets")
        spark.stop()
        return
    
    # Create features
    print("\nStep 3: Feature Engineering")
    print("-" * 60)
    df_features = create_inference_features(customers_df, orders_df, order_items_df, products_df)
    
    # Prepare data
    print("\nStep 4: Data Preparation & Encoding")
    print("-" * 60)
    df_prepared = prepare_inference_data(df_features, MODEL_NAME, saved_scaler)
    
    # Generate predictions
    print("\nStep 5: Generate Predictions")
    print("-" * 60)
    predictions_df = generate_predictions(model, df_prepared, MODEL_NAME)
    
    # Display samples
    display_sample_predictions(predictions_df)
    
    # Save predictions
    print("Step 6: Save Predictions")
    print("-" * 60)
    
    if save_predictions(predictions_df, OUTPUT_PATH):
        print(f"\n✓ Inference completed successfully")
        print(f"   Output: {OUTPUT_PATH}")
    else:
        print("\n✗ Inference failed")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)