import os
import uuid
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, udf, current_timestamp, when, dayofweek, hour, month,
    rand, count, sum as spark_sum, avg, max as spark_max, datediff, current_date
)
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType, BooleanType
from pyspark.ml.feature import VectorAssembler, StringIndexerModel, StandardScalerModel
from pyspark.ml.classification import (
    LogisticRegressionModel, RandomForestClassificationModel,
    DecisionTreeClassificationModel, MultilayerPerceptronClassificationModel
)
import findspark

findspark.init()

# Configuration
BUCKET_NAME = "pulse-bucket-1"
INPUT_PATH_ORDERS = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
INPUT_PATH_ORDER_ITEMS = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
INPUT_PATH_PRODUCTS = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
INPUT_PATH_INVENTORY = f"s3a://{BUCKET_NAME}/transformed/agg_inventory.parquet"
INPUT_PATH_SUPPLIERS = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
INPUT_PATH_CUSTOMERS = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/classification/fulfillment_risk_predictions"
MODEL_INPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/models/fulfillment_risk"

# ⚠️ MANUAL INTERVENTION: Select model
SELECTED_MODEL = "RandomForest"  # <-- CHANGE BASED ON TRAINING RESULTS

MODEL_VERSION = f"{SELECTED_MODEL}_v1.0"

# Feature columns (must match training)
NUMERICAL_FEATURES = [
    "total_quantity", "unique_products_ordered", "total_amount",
    "products_in_stock_count", "products_low_stock_count", "avg_product_availability",
    "total_reserved_quantity", "primary_supplier_reliability", "avg_supplier_lead_time",
    "supplier_stockout_rate", "shipping_distance_km", "shipping_complexity_score",
    "customer_past_delivery_issues", "avg_fulfillment_time_for_category", "warehouse_current_load",
    "order_placed_day_of_week", "order_placed_hour", "weather_risk_score"
]

CATEGORICAL_FEATURES = ["order_size_category", "season"]

BOOLEAN_FEATURES = [
    "has_custom_items", "multiple_suppliers_required", "destination_remote_flag",
    "is_holiday_period", "is_peak_shopping_season", "logistics_disruption_flag"
]


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("FulfillmentRiskInference") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "0") \
        .config("spark.dynamicAllocation.maxExecutors", "10") \
        .config("spark.dynamicAllocation.initialExecutors", "2") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("inferSchema", "true") \
        .config("mergeSchema", "true") \
        .getOrCreate()


def load_data(spark, path):
    """Load data from MinIO"""
    try:
        df = spark.read.parquet(path)
        print(f"✓ Loaded {df.count()} records")
        return df
    except Exception as e:
        print(f"✗ Failed to load: {e}")
        return None


def join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df):
    """Same join logic as training"""
    print("\n📊 Joining tables...")
    
    # Aggregate order items
    order_agg = order_items_df.groupBy("order_id").agg(
        count("product_id").alias("unique_products_ordered"),
        spark_sum("quantity").alias("total_quantity"),
        count("product_id").alias("item_count")
    )
    
    orders_selected = orders_df.select(
        "order_id", "customer_id", "order_placed_at", "order_placed_day_of_week",
        "total_amount", "order_size_category", "season"
    )
    
    df = orders_selected.join(order_agg, on="order_id", how="left")
    
    # Product aggregates
    order_products = order_items_df.join(
        products_df.select("product_id", "category", "supplier_id", "current_stock"),
        on="product_id", how="left"
    )
    
    product_agg = order_products.groupBy("order_id").agg(
        count(when(col("current_stock") > 0, 1)).alias("products_in_stock_count"),
        count(when((col("current_stock") > 0) & (col("current_stock") <= 10), 1)).alias("products_low_stock_count"),
        avg(when(col("current_stock") > 0, 1).otherwise(0)).alias("avg_product_availability"),
        count("supplier_id").alias("supplier_count")
    )
    
    df = df.join(product_agg, on="order_id", how="left")
    
    # Inventory aggregates
    order_inventory = order_items_df.join(
        inventory_df.select("product_id", "reserved_quantity"),
        on="product_id", how="left"
    )
    
    inventory_agg = order_inventory.groupBy("order_id").agg(
        spark_sum("reserved_quantity").alias("total_reserved_quantity")
    )
    
    df = df.join(inventory_agg, on="order_id", how="left")
    
    # Supplier aggregates
    order_suppliers = order_items_df.join(
        products_df.select("product_id", "supplier_id"), on="product_id", how="left"
    ).select("order_id", "supplier_id").distinct()
    
    supplier_info = order_suppliers.join(
        suppliers_df.select(
            "supplier_id",
            col("supplier_reliability_score").alias("supplier_reliability"),
            col("avg_restock_lead_time").alias("supplier_lead_time"),
            col("stockout_rate").alias("supplier_stockout_rate")
        ),
        on="supplier_id", how="left"
    )
    
    supplier_agg = supplier_info.groupBy("order_id").agg(
        spark_max("supplier_reliability").alias("primary_supplier_reliability"),
        avg("supplier_lead_time").alias("avg_supplier_lead_time"),
        avg("supplier_stockout_rate").alias("supplier_stockout_rate"),
        count("supplier_id").alias("distinct_suppliers")
    )
    
    df = df.join(supplier_agg, on="order_id", how="left")
    
    # Customer info
    customer_info = customers_df.select(
        "customer_id",
        col("total_cancelled_orders").alias("customer_past_delivery_issues")
    )
    
    df = df.join(customer_info, on="customer_id", how="left")
    
    print(f"✓ Joined: {df.count()} orders")
    return df


def generate_simulated_features(df):
    """Generate simulated external features (same as training)"""
    print("🔧 Generating simulated features...")
    
    df = df.withColumn("shipping_distance_km", (rand(seed=42) * 1950 + 50))
    df = df.withColumn(
        "shipping_complexity_score",
        when(col("total_quantity") > 10, rand(seed=43) * 3 + 7)
        .when(col("total_quantity") > 5, rand(seed=43) * 3 + 4)
        .otherwise(rand(seed=43) * 4)
    )
    df = df.withColumn("destination_remote_flag", (rand(seed=44) < 0.1).cast("int"))
    df = df.withColumn("weather_risk_score", rand(seed=45) * 10)
    df = df.withColumn("logistics_disruption_flag", (rand(seed=46) < 0.05).cast("int"))
    df = df.withColumn("warehouse_current_load", rand(seed=47) * 100)
    df = df.withColumn("avg_fulfillment_time_for_category", rand(seed=48) * 4 + 3)
    df = df.withColumn("has_custom_items", lit(0))
    df = df.withColumn("multiple_suppliers_required", (col("distinct_suppliers") > 1).cast("int"))
    df = df.withColumn("order_placed_hour", hour(col("order_placed_at")))
    df = df.withColumn("is_holiday_period", when(month(col("order_placed_at")).isin(11, 12), 1).otherwise(0))
    df = df.withColumn("is_peak_shopping_season", when(month(col("order_placed_at")).isin(11, 12, 6, 7), 1).otherwise(0))
    
    print("✓ Generated features")
    return df


def load_model_and_preprocessors(spark, model_dir, model_name):
    """Load model and preprocessors"""
    try:
        model_path = f"{model_dir}/{model_name}"
        
        if model_name == "LogisticRegression":
            model = LogisticRegressionModel.load(model_path)
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
        
        print(f"✓ Loaded model: {model_name}")
        return model, {"categorical_indexers": categorical_indexers, "scaler": scaler}
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return None, None


def prepare_features(df, preprocessors):
    """Prepare features (same as training)"""
    df_filled = df.fillna(0, subset=NUMERICAL_FEATURES + BOOLEAN_FEATURES)
    df_filled = df_filled.fillna("Unknown", subset=CATEGORICAL_FEATURES)
    
    categorical_indexed_cols = []
    for i, cat_col in enumerate(CATEGORICAL_FEATURES):
        indexer = preprocessors["categorical_indexers"][i]
        df_filled = indexer.transform(df_filled)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
    
    all_numerical = NUMERICAL_FEATURES + BOOLEAN_FEATURES
    numerical_assembler = VectorAssembler(inputCols=all_numerical, outputCol="numerical_features")
    df_filled = numerical_assembler.transform(df_filled)
    
    scaler = preprocessors["scaler"]
    df_filled = scaler.transform(df_filled)
    
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    df_vector = final_assembler.transform(df_filled)
    
    print("✓ Prepared features")
    return df_vector


def generate_predictions(spark, df, model, model_name):
    """Generate comprehensive predictions"""
    print("🔮 Generating predictions...")
    
    predictions = model.transform(df)
    
    # Risk class mapping
    risk_labels = {0: "Low Risk", 1: "Medium Risk", 2: "High Risk", 3: "Critical Risk"}
    map_risk_label = udf(lambda p: risk_labels.get(int(p), "Unknown"), StringType())
    
    # Extract probabilities
    extract_prob = udf(lambda prob, idx: float(prob[idx]) if prob and len(prob) > idx else 0.0, DoubleType())
    
    # Calculate specific risks
    calc_delay_prob = udf(lambda prob: float(prob[1] + prob[2] + prob[3]) if prob else 0.5, DoubleType())
    calc_failure_prob = udf(lambda prob: float(prob[3]) if prob else 0.1, DoubleType())
    calc_partial_prob = udf(lambda prob: float(prob[2] * 0.3 + prob[3] * 0.5) if prob else 0.1, DoubleType())
    
    # Calculate timing predictions
    calc_ship_date = udf(
        lambda placed_at, risk: (datetime.fromisoformat(str(placed_at)) + timedelta(days=[1, 2, 3, 5][int(risk)])).date().isoformat() if placed_at else None,
        StringType()
    )
    
    calc_delivery_date = udf(
        lambda placed_at, risk: (datetime.fromisoformat(str(placed_at)) + timedelta(days=[5, 8, 12, 20][int(risk)])).date().isoformat() if placed_at else None,
        StringType()
    )
    
    calc_delay_days = udf(lambda risk: [0, 3, 7, 15][int(risk)], IntegerType())
    calc_confidence = udf(lambda prob: float(max(prob)) if prob else 0.5, DoubleType())
    
    # Risk factors
    get_primary_factor = udf(
        lambda risk, stock, supplier, weather: 
            "Supplier Reliability" if supplier and supplier < 0.7 else
            "Stock Availability" if stock and stock < 0.5 else
            "Weather Conditions" if weather and weather > 7 else
            "Shipping Distance" if risk > 1 else "Normal Operations",
        StringType()
    )
    
    get_secondary_factor = udf(
        lambda risk, load, peak: 
            "Peak Season Load" if peak else
            "Warehouse Capacity" if load and load > 80 else
            "Multiple Suppliers" if risk > 2 else "Standard Processing",
        StringType()
    )
    
    # Recommendations
    get_recommendation = udf(
        lambda risk: 
            "Emergency expedited shipping + supplier escalation" if risk == 3 else
            "Expedited shipping recommended" if risk == 2 else
            "Monitor closely, prepare backup plan" if risk == 1 else
            "Standard processing",
        StringType()
    )
    
    # Financial impact
    calc_delay_cost = udf(lambda risk, amount: float(amount * [0.0, 0.05, 0.10, 0.20][int(risk)]) if amount else 0.0, DoubleType())
    calc_expedite_cost = udf(lambda risk, amount: float(amount * [0.0, 0.10, 0.15, 0.25][int(risk)]) if amount else 0.0, DoubleType())
    calc_compensation = udf(lambda risk, amount: float(amount * [0.0, 0.05, 0.10, 0.25][int(risk)]) if amount else 0.0, DoubleType())
    
    # Format output
    output_df = predictions.select(
        lit(None).cast(StringType()).alias("prediction_id"),
        col("order_id"),
        col("customer_id"),
        current_timestamp().alias("prediction_timestamp"),
        col("prediction").cast(IntegerType()).alias("predicted_risk_class"),
        map_risk_label(col("prediction")).alias("predicted_risk_label"),
        extract_prob(col("probability"), lit(0)).alias("prob_low_risk"),
        extract_prob(col("probability"), lit(1)).alias("prob_medium_risk"),
        extract_prob(col("probability"), lit(2)).alias("prob_high_risk"),
        extract_prob(col("probability"), lit(3)).alias("prob_critical_risk"),
        calc_delay_prob(col("probability")).alias("delay_probability"),
        calc_failure_prob(col("probability")).alias("failure_probability"),
        calc_partial_prob(col("probability")).alias("partial_fulfillment_probability"),
        calc_ship_date(col("order_placed_at"), col("prediction")).alias("predicted_ship_date"),
        calc_delivery_date(col("order_placed_at"), col("prediction")).alias("predicted_delivery_date"),
        calc_delay_days(col("prediction")).alias("expected_delay_days"),
        calc_confidence(col("probability")).alias("delivery_window_confidence"),
        get_primary_factor(
            col("prediction"), col("avg_product_availability"),
            col("primary_supplier_reliability"), col("weather_risk_score")
        ).alias("primary_risk_factor"),
        get_secondary_factor(
            col("prediction"), col("warehouse_current_load"), col("is_peak_shopping_season")
        ).alias("secondary_risk_factor"),
        lit("{}").alias("risk_factor_breakdown"),
        get_recommendation(col("prediction")).alias("recommended_action"),
        (col("primary_supplier_reliability") > 0.8).alias("alternative_supplier_available"),
        (col("prediction") >= 2).alias("expedited_shipping_recommended"),
        (col("prediction") >= 1).alias("customer_communication_recommended"),
        calc_delay_cost(col("prediction"), col("total_amount")).alias("potential_delay_cost"),
        calc_expedite_cost(col("prediction"), col("total_amount")).alias("expedited_shipping_cost"),
        calc_compensation(col("prediction"), col("total_amount")).alias("customer_compensation_estimate"),
        calc_confidence(col("probability")).alias("prediction_confidence"),
        lit("{}").alias("feature_importance"),
        lit(MODEL_VERSION).alias("model_version"),
        current_timestamp().alias("created_at"),
        lit(f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}").alias("execution_batch_id")
    )
    
    # Generate UUIDs
    generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
    output_df = output_df.withColumn("prediction_id", generate_uuid())
    
    print(f"✓ Generated {output_df.count()} predictions")
    return output_df


def save_predictions(df, output_path):
    """Save predictions"""
    try:
        df.write.mode("overwrite").parquet(output_path)
        print(f"✓ Saved to {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to save: {e}")
        return False


def main():
    print("=" * 70)
    print("Order Fulfillment Risk - Inference Pipeline")
    print("=" * 70)
    print(f"Using model: {SELECTED_MODEL}")
    print("=" * 70)
    
    spark = create_spark_session()
    
    # Load model
    model, preprocessors = load_model_and_preprocessors(spark, MODEL_INPUT_DIR, SELECTED_MODEL)
    if model is None:
        print("✗ Inference stopped")
        return
    
    # Load data
    print("\n📦 Loading tables...")
    orders_df = load_data(spark, INPUT_PATH_ORDERS)
    order_items_df = load_data(spark, INPUT_PATH_ORDER_ITEMS)
    products_df = load_data(spark, INPUT_PATH_PRODUCTS)
    inventory_df = load_data(spark, INPUT_PATH_INVENTORY)
    suppliers_df = load_data(spark, INPUT_PATH_SUPPLIERS)
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)
    
    if None in [orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df]:
        print("✗ Inference stopped")
        return
    
    # Join tables
    df = join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df)
    
    # Generate features
    df = generate_simulated_features(df)
    
    # Prepare features
    df_prepared = prepare_features(df, preprocessors)
    
    # Generate predictions
    predictions_df = generate_predictions(spark, df_prepared, model, SELECTED_MODEL)
    
    # Show sample
    print("\nSample predictions:")
    predictions_df.select(
        "order_id", "predicted_risk_label", "delay_probability",
        "expected_delay_days", "recommended_action"
    ).show(5, truncate=False)
    
    # Save
    success = save_predictions(predictions_df, OUTPUT_PATH)
    
    if success:
        print("\n✓ Inference completed successfully")
    else:
        print("\n✗ Inference failed")
    
    spark.stop()


if __name__ == "__main__":
    main()
