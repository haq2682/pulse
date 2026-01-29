import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg, max as spark_max,
    dayofweek, hour, month, rand, datediff, current_date
)
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, OneHotEncoder
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, DecisionTreeClassifier,
    MultilayerPerceptronClassifier
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
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
MODEL_OUTPUT_DIR = f"s3a://{BUCKET_NAME}/machine-learning/models/fulfillment_risk"
MIN_LABELED_RECORDS = 100

# CRITICAL: Exclude features known only AFTER fulfillment
NUMERICAL_FEATURES = [
    "total_quantity",
    "unique_products_ordered",
    "total_amount",
    "products_in_stock_count",
    "products_low_stock_count",
    "avg_product_availability",
    "total_reserved_quantity",
    "primary_supplier_reliability",
    "avg_supplier_lead_time",
    "supplier_stockout_rate",
    "shipping_distance_km",
    "shipping_complexity_score",
    "customer_past_delivery_issues",
    "avg_fulfillment_time_for_category",
    "warehouse_current_load",
    "order_placed_day_of_week",
    "order_placed_hour",
    "weather_risk_score"
]

CATEGORICAL_FEATURES = [
    "order_size_category",
    "season"
]

BOOLEAN_FEATURES = [
    "has_custom_items",
    "multiple_suppliers_required",
    "destination_remote_flag",
    "is_holiday_period",
    "is_peak_shopping_season",
    "logistics_disruption_flag"
]

TARGET_COLUMN = "fulfillment_risk_class"  # 0=Low, 1=Medium, 2=High, 3=Critical


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("FulfillmentRiskTraining") \
        .master(os.getenv("SPARK_SERVER", "local[*]")) \
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        ) \
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
        print(f"✓ Loaded {df.count()} records from {path.split('/')[-1]}")
        return df
    except Exception as e:
        print(f"✗ Failed to load {path.split('/')[-1]}: {e}")
        return None


def join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df):
    """
    Complex 5-table join with feature engineering
    
    Join strategy:
    1. orders (base)
    2. Aggregate order_items per order → products_count, quantities
    3. Join products → category, supplier info
    4. Join inventory → stock status
    5. Join suppliers → reliability metrics
    6. Join customers → historical performance
    """
    print("\n📊 Starting multi-table join...")
    
    # Step 1: Aggregate order items per order
    order_agg = order_items_df.groupBy("order_id").agg(
        count("product_id").alias("unique_products_ordered"),
        spark_sum("quantity").alias("total_quantity"),
        count("product_id").alias("item_count")
    )
    
    # Step 2: Join orders with aggregated order items
    orders_selected = orders_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_placed_at",
        "order_placed_day_of_week",
        "total_amount",
        "order_size_category",
        "season",
        "order_shipped_at",  # For label generation only
        "order_delivered_at",  # For label generation only
        "delivery_days_diff"  # For label generation only
    )
    
    df = orders_selected.join(order_agg, on="order_id", how="left")
    
    # Step 3: Get product-level aggregates per order
    order_products = order_items_df.join(
        products_df.select("product_id", "category", "supplier_id", "current_stock"),
        on="product_id",
        how="left"
    )
    
    # Aggregate product info per order
    product_agg = order_products.groupBy("order_id").agg(
        count(when(col("current_stock") > 0, 1)).alias("products_in_stock_count"),
        count(when((col("current_stock") > 0) & (col("current_stock") <= 10), 1)).alias("products_low_stock_count"),
        avg(when(col("current_stock") > 0, 1).otherwise(0)).alias("avg_product_availability"),
        count("supplier_id").alias("supplier_count")
    )
    
    df = df.join(product_agg, on="order_id", how="left")
    
    # Step 4: Get inventory aggregates per order
    order_inventory = order_items_df.join(
        inventory_df.select("product_id", "reserved_quantity", "stock_status"),
        on="product_id",
        how="left"
    )
    
    inventory_agg = order_inventory.groupBy("order_id").agg(
        spark_sum("reserved_quantity").alias("total_reserved_quantity")
    )
    
    df = df.join(inventory_agg, on="order_id", how="left")
    
    # Step 5: Get primary supplier info per order
    # Get first supplier per order
    order_suppliers = order_items_df \
        .join(products_df.select("product_id", "supplier_id"), on="product_id", how="left") \
        .select("order_id", "supplier_id") \
        .distinct()
    
    # Join with supplier details
    supplier_info = order_suppliers.join(
        suppliers_df.select(
            "supplier_id",
            col("supplier_reliability_score").alias("supplier_reliability"),
            col("avg_restock_lead_time").alias("supplier_lead_time"),
            col("stockout_rate").alias("supplier_stockout_rate")
        ),
        on="supplier_id",
        how="left"
    )
    
    # Get primary (first) supplier per order
    supplier_agg = supplier_info.groupBy("order_id").agg(
        spark_max("supplier_reliability").alias("primary_supplier_reliability"),
        avg("supplier_lead_time").alias("avg_supplier_lead_time"),
        avg("supplier_stockout_rate").alias("supplier_stockout_rate"),
        count("supplier_id").alias("distinct_suppliers")
    )
    
    df = df.join(supplier_agg, on="order_id", how="left")
    
    # Step 6: Get customer historical performance
    customer_info = customers_df.select(
        "customer_id",
        col("total_cancelled_orders").alias("customer_past_delivery_issues")
    )
    
    df = df.join(customer_info, on="customer_id", how="left")
    
    print(f"✓ Joined all tables: {df.count()} orders with features")
    return df


def generate_simulated_features(df):
    """
    Generate simulated external features
    
    NOTE: In production, these would come from external APIs:
    - shipping_distance_km: from shipping address + warehouse location
    - weather_risk_score: from weather API
    - logistics_disruption_flag: from logistics tracking
    """
    print("\n🔧 Generating simulated external features...")
    
    # Simulate shipping distance (50-2000 km)
    df = df.withColumn("shipping_distance_km", (rand(seed=42) * 1950 + 50))
    
    # Simulate shipping complexity based on order size
    df = df.withColumn(
        "shipping_complexity_score",
        when(col("total_quantity") > 10, rand(seed=43) * 3 + 7)  # 7-10
        .when(col("total_quantity") > 5, rand(seed=43) * 3 + 4)  # 4-7
        .otherwise(rand(seed=43) * 4)  # 0-4
    )
    
    # Simulate remote destination (10% of orders)
    df = df.withColumn("destination_remote_flag", (rand(seed=44) < 0.1).cast("int"))
    
    # Simulate weather risk (0-10 scale)
    df = df.withColumn("weather_risk_score", rand(seed=45) * 10)
    
    # Simulate logistics disruption (5% of orders)
    df = df.withColumn("logistics_disruption_flag", (rand(seed=46) < 0.05).cast("int"))
    
    # Simulate warehouse load (0-100%)
    df = df.withColumn("warehouse_current_load", rand(seed=47) * 100)
    
    # Avg fulfillment time for category (3-7 days)
    df = df.withColumn("avg_fulfillment_time_for_category", rand(seed=48) * 4 + 3)
    
    # Custom items flag (assume False for all - no field in schema)
    df = df.withColumn("has_custom_items", lit(0))
    
    # Multiple suppliers required
    df = df.withColumn("multiple_suppliers_required", (col("distinct_suppliers") > 1).cast("int"))
    
    # Extract temporal features
    df = df.withColumn("order_placed_hour", hour(col("order_placed_at")))
    
    # Holiday period (Nov-Dec)
    df = df.withColumn(
        "is_holiday_period",
        when(month(col("order_placed_at")).isin(11, 12), 1).otherwise(0)
    )
    
    # Peak shopping season (Nov-Dec, Jun-Jul)
    df = df.withColumn(
        "is_peak_shopping_season",
        when(month(col("order_placed_at")).isin(11, 12, 6, 7), 1).otherwise(0)
    )
    
    print("✓ Generated simulated features")
    return df


def generate_risk_labels(df):
    """
    Generate fulfillment risk labels from order outcomes
    
    Risk Classes:
    - 0 (Low): Delivered on time (delivery_days <= 5)
    - 1 (Medium): Slight delay (5 < delivery_days <= 8)
    - 2 (High): Significant delay (8 < delivery_days <= 15) OR long pending
    - 3 (Critical): Major delay (delivery_days > 15) OR cancelled
    """
    print("\n🏷️  Generating fulfillment risk labels...")
    
    # Generate risk based on order status and delivery performance
    df_with_label = df.withColumn(
        TARGET_COLUMN,
        # Critical: Cancelled orders or very long delays
        when(col("order_status").isin("Cancelled", "cancelled", "Failed"), lit(3))
        .when(col("delivery_days_diff") > 15, lit(3))
        # High: Significant delays or long pending
        .when(col("delivery_days_diff") > 8, lit(2))
        .when(
            (col("order_status").isin("Pending", "pending", "Processing")) &
            (datediff(current_date(), col("order_placed_at")) > 10),
            lit(2)
        )
        # Medium: Slight delays
        .when(col("delivery_days_diff") > 5, lit(1))
        # Low: On-time delivery
        .when(col("delivery_days_diff").isNotNull(), lit(0))
        # Default for pending orders (assume low risk if recent)
        .when(col("order_status").isin("Pending", "pending", "Processing"), lit(0))
        .otherwise(lit(0))
    )
    
    # Filter valid labels
    df_with_label = df_with_label.filter(col(TARGET_COLUMN).isNotNull())
    
    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy("count", ascending=False)
    print("Risk class distribution:")
    label_dist.show()
    
    print("⚠️  CRITICAL: Excluded shipped/delivered dates from features (used for labels only)")
    
    # Now drop the leakage columns
    df_final = df_with_label.drop(
        "order_shipped_at",
        "order_delivered_at",
        "delivery_days_diff",
        "distinct_suppliers",
        "supplier_count",
        "item_count"
    )
    
    return df_final


def add_label_noise(df, noise_rate=0.13):
    """Add label noise to simulate fulfillment uncertainty"""
    print(f"\n🔀 Adding {noise_rate*100:.0f}% label noise...")
    
    labels = [0, 1, 2, 3]
    
    df_noisy = df.withColumn(
        "random_val", rand(seed=42)
    ).withColumn(
        TARGET_COLUMN,
        when(col("random_val") < noise_rate / 4, lit(labels[0]))
        .when((col("random_val") >= noise_rate / 4) & (col("random_val") < noise_rate / 2), lit(labels[1]))
        .when((col("random_val") >= noise_rate / 2) & (col("random_val") < 3 * noise_rate / 4), lit(labels[2]))
        .when((col("random_val") >= 3 * noise_rate / 4) & (col("random_val") < noise_rate), lit(labels[3]))
        .otherwise(col(TARGET_COLUMN))
    ).drop("random_val")
    
    print("Label distribution after noise:")
    df_noisy.groupBy(TARGET_COLUMN).count().orderBy(TARGET_COLUMN).show()
    
    print(f"✓ Added noise: ~{noise_rate*100:.0f}% labels flipped")
    print(f"  Expected accuracy ceiling: ~{(1-noise_rate)*100:.0f}%\n")
    
    return df_noisy


def prepare_features(train_df, test_df, numerical_features, categorical_features, boolean_features):
    """Prepare features - FIT ON TRAIN ONLY"""
    # Fill nulls
    train_filled = train_df.fillna(0, subset=numerical_features + boolean_features)
    train_filled = train_filled.fillna("Unknown", subset=categorical_features)
    
    test_filled = test_df.fillna(0, subset=numerical_features + boolean_features)
    test_filled = test_filled.fillna("Unknown", subset=categorical_features)
    
    # Filter null targets
    train_clean = train_filled.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_filled.filter(col(TARGET_COLUMN).isNotNull())
    
    # Index categorical features
    categorical_indexed_cols = []
    categorical_indexers = []
    
    for cat_col in categorical_features:
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_indexed", handleInvalid="keep")
        indexer_model = indexer.fit(train_clean)
        train_clean = indexer_model.transform(train_clean)
        test_clean = indexer_model.transform(test_clean)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
        categorical_indexers.append(indexer_model)
    
    # Assemble numerical + boolean features
    all_numerical = numerical_features + boolean_features
    numerical_assembler = VectorAssembler(inputCols=all_numerical, outputCol="numerical_features")
    train_clean = numerical_assembler.transform(train_clean)
    test_clean = numerical_assembler.transform(test_clean)
    
    # Scale numerical features
    scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")
    scaler_model = scaler.fit(train_clean)
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)
    
    # Combine all features
    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)
    
    # Encode target labels
    train_indexed = train_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    test_indexed = test_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    
    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(boolean_features)} boolean + {len(categorical_features)} categorical")
    
    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model
    }


def train_logistic_regression(train_df):
    """Train Logistic Regression"""
    print("\n[1/4] Training Logistic Regression...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.5)
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_random_forest(train_df):
    """Train Random Forest"""
    print("\n[2/4] Training Random Forest...")
    rf = RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
    model = rf.fit(train_df)
    print("✓ Random Forest trained")
    return model, "RandomForest"


def train_decision_tree(train_df):
    """Train Decision Tree"""
    print("\n[3/4] Training Decision Tree...")
    dt = DecisionTreeClassifier(maxDepth=10, seed=42)
    model = dt.fit(train_df)
    print("✓ Decision Tree trained")
    return model, "DecisionTree"


def train_multilayer_perceptron(train_df):
    """Train Multilayer Perceptron"""
    print("\n[4/4] Training Multilayer Perceptron...")
    
    num_features = len(train_df.select("features").first()[0])
    num_classes = 4  # Low, Medium, High, Critical
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
    
    print(f"✓ Saved {model_name}")


def main():
    print("=" * 70)
    print("Order Fulfillment Risk Classification - Training Pipeline")
    print("=" * 70)
    
    # CONFIGURATION
    ADD_LABEL_NOISE = True
    NOISE_RATE = 0.13  # 13% noise → ~87% accuracy ceiling
    
    spark = create_spark_session()
    
    # Load all tables
    print("\n📦 Loading tables...")
    orders_df = load_data(spark, INPUT_PATH_ORDERS)
    order_items_df = load_data(spark, INPUT_PATH_ORDER_ITEMS)
    products_df = load_data(spark, INPUT_PATH_PRODUCTS)
    inventory_df = load_data(spark, INPUT_PATH_INVENTORY)
    suppliers_df = load_data(spark, INPUT_PATH_SUPPLIERS)
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)
    
    if None in [orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df]:
        print("✗ Training stopped: Failed to load all tables")
        return
    
    # Join all tables
    df = join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df)
    
    # Generate simulated external features
    df = generate_simulated_features(df)
    
    # Generate risk labels
    df = generate_risk_labels(df)
    
    # Add noise
    if ADD_LABEL_NOISE:
        df = add_label_noise(df, noise_rate=NOISE_RATE)
    
    # Check minimum records
    labeled_count = df.filter(col(TARGET_COLUMN).isNotNull()).count()
    if labeled_count < MIN_LABELED_RECORDS:
        print(f"✗ Insufficient data ({labeled_count} < {MIN_LABELED_RECORDS})")
        return
    
    print(f"✓ Dataset ready: {labeled_count} orders")
    
    # Split
    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    print(f"✓ Split: {train_df_raw.count()} train, {test_df_raw.count()} test")
    
    # Prepare features
    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES, BOOLEAN_FEATURES
    )
    
    # Train models
    models = [
        train_logistic_regression(train_df),
        train_random_forest(train_df),
        train_decision_tree(train_df),
        train_multilayer_perceptron(train_df)
    ]
    
    # Evaluate
    print("\n" + "=" * 70)
    print("Model Evaluation")
    print("=" * 70)
    
    all_metrics = []
    for model, model_name in models:
        metrics = evaluate_model(model, test_df, model_name)
        all_metrics.append(metrics)
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)
    
    # Compare
    print("\n" + "=" * 70)
    print("Model Comparison")
    print("=" * 70)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | Acc: {m['accuracy']:.4f}")
    
    print("\n✓ Training completed")
    print(f"Models saved to: {MODEL_OUTPUT_DIR}")
    
    spark.stop()


if __name__ == "__main__":
    main()
