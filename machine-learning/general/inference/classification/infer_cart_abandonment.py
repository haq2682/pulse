import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, udf, current_timestamp, when,
    log as spark_log, datediff, current_date,
    avg, count, sum as spark_sum, max as spark_max
)
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel, StandardScalerModel
from pyspark.ml.classification import (
    LogisticRegressionModel, RandomForestClassificationModel, GBTClassificationModel
)
import findspark

findspark.init()

# Base features (MUST match training - no time_in_cart_hours)
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


def create_spark_session():
    return SparkSession.builder \
        .appName("CartAbandonmentInference") \
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
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records from {path}")
        return df
    except Exception as e:
        print(f"⚠ Failed to load {path}: {e}")
        return None


def engineer_features(df):
    df = df.withColumn("engagement_rate", col("products_viewed") / (col("pages_viewed") + 1))
    df = df.withColumn("cart_fill_rate", col("items_added_to_cart") / (col("products_viewed") + 1))
    df = df.withColumn("avg_time_per_page", col("session_duration_minutes") / (col("pages_viewed") + 1))
    df = df.withColumn("log_cart_value", spark_log(col("cart_total_value") + 1))
    df = df.withColumn("high_value_cart", when(col("cart_total_value") > 200, 1.0).otherwise(0.0))
    df = df.withColumn(
        "price_point_sensitivity",
        when(col("cart_avg_item_price") < 20, lit("low"))
        .when(col("cart_avg_item_price") < 100, lit("medium"))
        .otherwise(lit("high"))
    )
    df = df.withColumn("browsing_intensity", col("pages_per_minute") * col("session_duration_minutes"))
    df = df.withColumn("cart_commitment_score", (col("cart_items_count") * col("cart_total_value")) / (col("session_duration_minutes") + 1))
    df = df.withColumn("single_item_cart", when(col("cart_items_count") == 1, 1.0).otherwise(0.0))
    df = df.withColumn("large_cart", when(col("cart_items_count") >= 5, 1.0).otherwise(0.0))
    
    new_numerical = [
        "engagement_rate", "cart_fill_rate", "avg_time_per_page", "log_cart_value",
        "high_value_cart", "browsing_intensity", "cart_commitment_score",
        "single_item_cart", "large_cart"
    ]
    new_categorical = ["price_point_sensitivity"]
    
    return df, new_numerical, new_categorical


def add_customer_history_features(df, customers_df=None, orders_df=None):
    if customers_df is None and orders_df is None:
        return df, [], []
    
    if customers_df is not None:
        customer_features = customers_df.select(
            "customer_id", "total_orders", "avg_order_value", "last_order_date",
            "customer_lifetime_value", "order_recency_days", "customer_segment_label",
            "is_repeat_customer", "cart_abandonment_rate", "session_conversion_rate"
        )
        df = df.join(customer_features, on="customer_id", how="left")
        df = df.withColumn("is_returning_customer_calc", when(col("is_repeat_customer") == 1, 1.0).otherwise(0.0))
        
        return df, [
            "total_orders", "avg_order_value", "customer_lifetime_value",
            "order_recency_days", "is_returning_customer_calc",
            "cart_abandonment_rate", "session_conversion_rate"
        ], ["customer_segment_label"]
    
    elif orders_df is not None:
        customer_stats = orders_df.groupBy("customer_id").agg(
            count("*").alias("total_orders_calc"),
            avg("total_amount").alias("avg_order_value_calc"),
            spark_max("order_placed_at").alias("last_order_date_calc"),
            spark_sum("total_amount").alias("lifetime_value_calc")
        )
        df = df.join(customer_stats, on="customer_id", how="left")
        df = df.withColumn("days_since_last_order_calc", datediff(current_date(), col("last_order_date_calc")))
        df = df.withColumn(
            "customer_segment_calc",
            when(col("total_orders_calc").isNull() | (col("total_orders_calc") == 0), lit("new"))
            .when(col("total_orders_calc") <= 2, lit("occasional"))
            .when(col("total_orders_calc") <= 5, lit("regular"))
            .otherwise(lit("loyal"))
        )
        df = df.withColumn("is_returning_customer_calc", when(col("total_orders_calc") > 0, 1.0).otherwise(0.0))
        
        return df, [
            "total_orders_calc", "avg_order_value_calc", "days_since_last_order_calc",
            "is_returning_customer_calc", "lifetime_value_calc"
        ], ["customer_segment_calc"]
    
    return df, [], []


def load_model_and_preprocessors(spark, model_dir, model_name):
    try:
        model_path = f"{model_dir}/{model_name}"
        
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
        elif model_name == "RandomForest":
            model = RandomForestClassificationModel.load(model_path)
        elif model_name == "GBT":
            model = GBTClassificationModel.load(model_path)
        else:
            raise ValueError(f"Unknown model: {model_name}")
        
        print(f"✓ Loaded model: {model_name}")
        return model
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return None


def prepare_features(df, numerical_features, categorical_features, MODEL_INPUT_DIR, SELECTED_MODEL):
    df_filled = df.fillna(0, subset=numerical_features)
    df_filled = df_filled.fillna("Unknown", subset=categorical_features)
    
    categorical_indexed_cols = []
    for i, cat_col in enumerate(categorical_features):
        indexer_path = f"{MODEL_INPUT_DIR}/{SELECTED_MODEL}_indexer_{i}"
        indexer = StringIndexerModel.load(indexer_path)
        df_filled = indexer.transform(df_filled)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
    
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features", handleInvalid="skip")
    df_filled = numerical_assembler.transform(df_filled)
    
    scaler_path = f"{MODEL_INPUT_DIR}/{SELECTED_MODEL}_scaler"
    scaler = StandardScalerModel.load(scaler_path)
    df_filled = scaler.transform(df_filled)
    
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features", handleInvalid="skip")
    df_vector = final_assembler.transform(df_filled)
    
    print(f"✓ Prepared features")
    return df_vector


def generate_predictions(spark, df, model, MODEL_VERSION):
    predictions = model.transform(df)
    
    map_to_output_status = udf(lambda pred: "Will Abandon" if pred > 0.5 else "Will Convert", StringType())
    extract_abandonment_prob = udf(lambda prob: float(prob[1]) if prob and len(prob) > 1 else 0.0, DoubleType())
    calculate_confidence = udf(lambda prob: float(max(prob)) if prob else 0.0, DoubleType())
    calculate_risk_score = udf(lambda prob: float(prob * 100) if prob else 0.0, DoubleType())
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    
    output_df = predictions.select(
        generate_uuid().alias("prediction_id"),
        col("cart_id"),
        col("customer_id"),
        current_timestamp().alias("prediction_date"),
        map_to_output_status(col("prediction")).alias("predicted_status"),
        extract_abandonment_prob(col("probability")).alias("abandonment_probability"),
        calculate_risk_score(extract_abandonment_prob(col("probability"))).alias("abandonment_risk_score"),
        calculate_confidence(col("probability")).alias("confidence_score"),
        lit(MODEL_VERSION).alias("model_version")
    )
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def main(BUCKET_NAME):
    GENERAL_BUCKET_NAME = "pulse-bucket-1"
    INPUT_PATH_CART = f"s3a://{BUCKET_NAME}/transformed/agg_cart_abandonment_analysis.parquet"
    INPUT_PATH_SESSIONS = f"s3a://{BUCKET_NAME}/transformed/agg_customer_sessions.parquet"
    INPUT_PATH_CUSTOMERS = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    INPUT_PATH_ORDERS = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/predictions/cart_abandonment_predictions"
    MODEL_INPUT_DIR = f"s3a://{GENERAL_BUCKET_NAME}/machine-learning/classification/models/cart_abandonment"

    SELECTED_MODEL = "RandomForest"
    MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"
    print("=" * 60)
    print("Cart Abandonment Risk - Inference Pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    
    model = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None:
        return
    
    cart_df = load_data(spark, INPUT_PATH_CART)
    sessions_df = load_data(spark, INPUT_PATH_SESSIONS)
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)
    orders_df = load_data(spark, INPUT_PATH_ORDERS)
    
    if cart_df is None or sessions_df is None:
        return
    
    cart_selected = cart_df.select(
        "cart_id", "customer_id", "cart_items_count", "cart_total_value",
        "cart_avg_item_price", "device_used", "session_id"
    )
    
    sessions_selected = sessions_df.select(
        "session_id", "session_duration_minutes", "pages_viewed",
        "products_viewed", "pages_per_minute", "referrer_source", "items_added_to_cart"
    )
    
    df = cart_selected.join(sessions_selected, on="session_id", how="left")
    
    df, new_num_features, new_cat_features = engineer_features(df)
    df, cust_num_features, cust_cat_features = add_customer_history_features(df, customers_df, orders_df)
    
    all_numerical = NUMERICAL_FEATURES + new_num_features + cust_num_features
    all_categorical = CATEGORICAL_FEATURES + new_cat_features + cust_cat_features
    
    df_prepared = prepare_features(df, all_numerical, all_categorical, MODEL_INPUT_DIR, SELECTED_MODEL)
    predictions_df = generate_predictions(spark, df_prepared, model, MODEL_VERSION)
    
    print("\nSample predictions:")
    predictions_df.select("cart_id", "predicted_status", "abandonment_probability", "abandonment_risk_score").show(5, truncate=False)
    
    predictions_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"✓ Saved to {OUTPUT_PATH}")
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME)