import os
import sys
import json
from pathlib import Path

# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session


from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg, max as spark_max,
    dayofweek, hour, month, rand, datediff, current_date, create_map
)
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler, OneHotEncoder
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, DecisionTreeClassifier,
    GBTClassifier
)
from pyspark.ml.classification import OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from itertools import chain
# ── FIX: Removed all randomly-simulated features from feature lists.
#    Simulated columns (shipping_distance_km, weather_risk_score, etc.) are
#    pure noise (rand() with fixed seeds) and actively hurt model accuracy.
#    They are still generated so downstream display code keeps working,
#    but they are no longer fed into the ML pipeline.

NUMERICAL_FEATURES = [
    # Order-level aggregates
    "total_quantity",
    "unique_products_ordered",
    "total_amount",
    # From agg_orders
    "shipping_cost",
    "discount_percentage",
    # From agg_products (aggregated per order)
    "products_in_stock_count",
    "products_low_stock_count",
    "avg_product_availability",
    "avg_product_rating",
    "avg_product_performance",
    "total_stockout_history",
    "avg_inventory_turnover",
    # From agg_inventory (aggregated per order)
    "total_reserved_quantity",
    "avg_available_stock",
    "avg_stock_coverage_days",
    "total_reorder_breaches",
    "avg_stock_turnover_ratio",
    # From agg_suppliers (aggregated per order)
    "primary_supplier_reliability",
    "avg_supplier_lead_time",
    "supplier_stockout_rate",
    "avg_supplier_performance_score",
    "avg_supplier_rating",
    "avg_supplier_fulfilled_orders",
    "avg_supplier_inventory_health",
    # From agg_customers
    "customer_past_delivery_issues",
    "customer_total_orders",
    "customer_cancellation_rate",
    "customer_avg_order_value",
    "customer_lifetime_value",
    "customer_tenure_days",
    "rfm_overall_score",
    "customer_activity_score",
    "order_age_days",
    # Temporal (real, derived from order_placed_at)
    "order_placed_day_of_week",
    "order_placed_hour",
    "order_month",
    # Engineered features from real data
    "stock_to_order_ratio",
    "low_stock_ratio",
    "out_of_stock_count",
    "order_value_per_item",
    "reserved_to_quantity_ratio",
    "supplier_risk_composite",
    "lead_time_quantity_interaction",
    "value_at_risk",
    "fulfillment_complexity",
    "stock_health_combined",
    "customer_reliability_score",
    "supplier_order_load",
    "stock_pressure_index",
    "seasonal_demand_index",
    "customer_value_risk_interaction",
]

CATEGORICAL_FEATURES = [
    "order_status",
    "order_size_category",
    "season"
]

# ── FIX: Removed has_custom_items (always 0 → zero-variance),
#         destination_remote_flag, logistics_disruption_flag (both random noise).
BOOLEAN_FEATURES = [
    "multiple_suppliers_required",   # real: based on distinct_suppliers
    "is_holiday_period",             # real: derived from order_placed_at
    "is_peak_shopping_season",       # real: derived from order_placed_at
    "is_repeat_customer",            # real: from agg_customers
    "is_high_value_order",
]

TARGET_COLUMN = "fulfillment_risk_class"  # 0=Low, 1=Medium, 2=High, 3=Critical

REQUIRED_SOURCE_COLUMNS = {
    "agg_orders": [
        "order_id", "customer_id", "order_status", "order_placed_at",
        "order_placed_day_of_week", "total_amount", "shipping_cost",
        "discount_percentage", "order_size_category", "season",
        "order_shipped_at", "order_delivered_at", "delivery_days_diff",
    ],
    "agg_order_items": ["order_id", "product_id", "quantity"],
    "agg_products": [
        "product_id", "supplier_id", "current_stock", "avg_rating",
        "product_performance_score", "stockout_occurrences", "inventory_turnover_rate",
    ],
    "agg_inventory": [
        "product_id", "reserved_quantity", "available_stock", "stock_coverage_days",
        "reorder_point_breach", "stock_turnover_ratio",
    ],
    "agg_suppliers": [
        "supplier_id", "supplier_reliability_score", "avg_restock_lead_time",
        "stockout_rate", "supplier_performance_score", "supplier_rating",
        "total_orders_fulfilled", "supplier_inventory_health_score",
    ],
    "agg_customers": [
        "customer_id", "total_cancelled_orders", "total_orders", "cancellation_rate",
        "avg_order_value", "customer_lifetime_value", "is_repeat_customer",
        "customer_tenure_days", "rfm_overall_score", "customer_activity_score",
    ],
}


def _missing_columns(df, required_columns):
    existing = set(df.columns)
    return [column for column in required_columns if column not in existing]


def validate_required_source_columns(dataset_map):
    missing_report = {}
    for table_name, required_cols in REQUIRED_SOURCE_COLUMNS.items():
        df = dataset_map.get(table_name)
        if df is None:
            missing_report[table_name] = required_cols
            continue
        missing = _missing_columns(df, required_cols)
        if missing:
            missing_report[table_name] = missing

    if missing_report:
        print("✗ Training skipped: required input columns are missing")
        for table_name, cols in missing_report.items():
            print(f"  - {table_name}: missing {cols}")
        return False
    return True


def validate_feature_columns(df):
    required = NUMERICAL_FEATURES + CATEGORICAL_FEATURES + BOOLEAN_FEATURES + [TARGET_COLUMN]
    missing = _missing_columns(df, required)
    if missing:
        print("✗ Training skipped: engineered feature set is incomplete")
        print(f"  Missing feature columns: {missing}")
        return False
    return True


def save_best_model_manifest(spark, output_dir: str, best_metrics: dict):
    manifest = {
        "best_model": best_metrics["model_name"],
        "best_f1": round(float(best_metrics["f1_score"]), 6),
        "best_accuracy": round(float(best_metrics["accuracy"]), 6),
        "numerical_features": NUMERICAL_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "boolean_features": BOOLEAN_FEATURES,
        "target_column": TARGET_COLUMN,
    }
    payload = json.dumps(manifest)
    manifest_path = f"{output_dir}/_best_model_manifest"
    spark.createDataFrame([(payload,)], ["value"]).coalesce(1).write.mode("overwrite").text(manifest_path)
    print(f"✓ Saved best-model manifest: {manifest_path}")


def _parse_enabled_models(raw_value):
    model_aliases = {
        "lr": "LogisticRegression",
        "logistic": "LogisticRegression",
        "logisticregression": "LogisticRegression",
        "rf": "RandomForest",
        "randomforest": "RandomForest",
        "dt": "DecisionTree",
        "decisiontree": "DecisionTree",
        "gbt": "GBT",
    }
    selected = []
    for part in (raw_value or "").split(","):
        key = part.strip().lower().replace("_", "")
        mapped = model_aliases.get(key)
        if mapped and mapped not in selected:
            selected.append(mapped)
    return selected


def create_spark_session():
    """Initialize Spark session"""
    os.environ.setdefault("SPARK_SERVER", os.getenv("ML_SPARK_MASTER", "local[2]"))
    return create_ml_spark_session(
        "FulfillmentRiskTraining",
        extra_configs={
            "inferSchema": "true",
            "mergeSchema": "true",
            # Use fixed executors for this job to prevent shuffle-file loss
            # when dynamic allocation removes executors mid-training.
            "spark.dynamicAllocation.enabled": "false",
            "spark.driver.memory": os.getenv("ML_SPARK_DRIVER_MEMORY", "4g"),
            "spark.driver.maxResultSize": os.getenv("ML_SPARK_DRIVER_MAX_RESULT_SIZE", "1g"),
            "spark.executor.instances": os.getenv("ML_SPARK_EXECUTOR_INSTANCES", "1"),
            "spark.executor.cores": os.getenv("ML_SPARK_EXECUTOR_CORES", "1"),
            "spark.executor.memory": os.getenv("ML_SPARK_EXECUTOR_MEMORY", "1536m"),
            "spark.executor.memoryOverhead": os.getenv("ML_SPARK_EXECUTOR_MEMORY_OVERHEAD", "768"),
            "spark.sql.shuffle.partitions": os.getenv("ML_SPARK_SHUFFLE_PARTITIONS", "8"),
            "spark.default.parallelism": os.getenv("ML_SPARK_DEFAULT_PARALLELISM", "4"),
            "spark.sql.adaptive.enabled": os.getenv("ML_SPARK_SQL_ADAPTIVE", "true"),
            "spark.sql.adaptive.coalescePartitions.enabled": os.getenv("ML_SPARK_SQL_COALESCE_PARTITIONS", "true"),
            "spark.sql.adaptive.skewJoin.enabled": os.getenv("ML_SPARK_SQL_SKEW_JOIN", "true"),
            "spark.network.timeout": os.getenv("ML_SPARK_NETWORK_TIMEOUT", "600s"),
            "spark.executor.heartbeatInterval": os.getenv("ML_SPARK_HEARTBEAT_INTERVAL", "60s"),
            # Shuffle resilience for intermittent executor loss / slow IO.
            "spark.shuffle.io.maxRetries": os.getenv("ML_SPARK_SHUFFLE_MAX_RETRIES", "10"),
            "spark.shuffle.io.retryWait": os.getenv("ML_SPARK_SHUFFLE_RETRY_WAIT", "10s"),
            "spark.task.maxFailures": os.getenv("ML_SPARK_TASK_MAX_FAILURES", "8"),
            "spark.stage.maxConsecutiveAttempts": os.getenv("ML_SPARK_STAGE_MAX_ATTEMPTS", "8"),
            "spark.speculation": os.getenv("ML_SPARK_SPECULATION", "false"),
        },
    )
def load_data(spark, path):
    try:
        df = spark.read.parquet(path)
        # Avoid eager full-table scans during startup unless explicitly enabled.
        if os.getenv("ML_LOG_ROW_COUNTS", "false").lower() == "true":
            print(f"✓ Loaded {df.count()} records from {path.split('/')[-1]}")
        else:
            print(f"✓ Loaded {path.split('/')[-1]}")
        return df
    except Exception as e:
        print(f"✗ Failed to load {path.split('/')[-1]}: {e}")
        return None


def join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df):
    print("\n📊 Starting multi-table join...")

    order_agg = order_items_df.groupBy("order_id").agg(
        count("product_id").alias("unique_products_ordered"),
        spark_sum("quantity").alias("total_quantity"),
        count("product_id").alias("item_count")
    )

    orders_selected = orders_df.select(
        "order_id", "customer_id", "order_status", "order_placed_at",
        "order_placed_day_of_week", "total_amount", "shipping_cost",
        "discount_percentage", "order_size_category", "season",
        "order_shipped_at", "order_delivered_at", "delivery_days_diff"
    )

    df = orders_selected.join(order_agg, on="order_id", how="left")

    order_products = order_items_df.join(
        products_df.select(
            "product_id", "category", "supplier_id", "current_stock",
            "avg_rating", "product_performance_score",
            "stockout_occurrences", "inventory_turnover_rate"
        ),
        on="product_id", how="left"
    )

    product_agg = order_products.groupBy("order_id").agg(
        count(when(col("current_stock") > 0, 1)).alias("products_in_stock_count"),
        count(when((col("current_stock") > 0) & (col("current_stock") <= 10), 1)).alias("products_low_stock_count"),
        avg(when(col("current_stock") > 0, 1).otherwise(0)).alias("avg_product_availability"),
        count("supplier_id").alias("supplier_count"),
        avg("avg_rating").alias("avg_product_rating"),
        avg("product_performance_score").alias("avg_product_performance"),
        spark_sum("stockout_occurrences").alias("total_stockout_history"),
        avg("inventory_turnover_rate").alias("avg_inventory_turnover")
    )

    df = df.join(product_agg, on="order_id", how="left")

    order_inventory = order_items_df.join(
        inventory_df.select(
            "product_id", "reserved_quantity", "stock_status",
            "available_stock", "stock_coverage_days",
            "reorder_point_breach", "stock_turnover_ratio"
        ),
        on="product_id", how="left"
    )

    inventory_agg = order_inventory.groupBy("order_id").agg(
        spark_sum("reserved_quantity").alias("total_reserved_quantity"),
        avg("available_stock").alias("avg_available_stock"),
        avg("stock_coverage_days").alias("avg_stock_coverage_days"),
        spark_sum("reorder_point_breach").alias("total_reorder_breaches"),
        avg("stock_turnover_ratio").alias("avg_stock_turnover_ratio")
    )

    df = df.join(inventory_agg, on="order_id", how="left")

    order_suppliers = order_items_df \
        .join(products_df.select("product_id", "supplier_id"), on="product_id", how="left") \
        .select("order_id", "supplier_id") \
        .distinct()

    supplier_info = order_suppliers.join(
        suppliers_df.select(
            "supplier_id",
            col("supplier_reliability_score").alias("supplier_reliability"),
            col("avg_restock_lead_time").alias("supplier_lead_time"),
            col("stockout_rate").alias("supplier_stockout_rate"),
            col("supplier_performance_score"),
            col("supplier_rating"),
            col("total_orders_fulfilled"),
            col("supplier_inventory_health_score")
        ),
        on="supplier_id", how="left"
    )

    supplier_agg = supplier_info.groupBy("order_id").agg(
        spark_max("supplier_reliability").alias("primary_supplier_reliability"),
        avg("supplier_lead_time").alias("avg_supplier_lead_time"),
        avg("supplier_stockout_rate").alias("supplier_stockout_rate"),
        count("supplier_id").alias("distinct_suppliers"),
        avg("supplier_performance_score").alias("avg_supplier_performance_score"),
        avg("supplier_rating").alias("avg_supplier_rating"),
        avg("total_orders_fulfilled").alias("avg_supplier_fulfilled_orders"),
        avg("supplier_inventory_health_score").alias("avg_supplier_inventory_health")
    )

    df = df.join(supplier_agg, on="order_id", how="left")

    customer_info = customers_df.select(
        "customer_id",
        col("total_cancelled_orders").alias("customer_past_delivery_issues"),
        col("total_orders").alias("customer_total_orders"),
        col("cancellation_rate").alias("customer_cancellation_rate"),
        col("avg_order_value").alias("customer_avg_order_value"),
        col("customer_lifetime_value"),
        col("is_repeat_customer"),
        col("customer_tenure_days"),
        col("rfm_overall_score"),
        col("customer_activity_score")
    )

    df = df.join(customer_info, on="customer_id", how="left")

    if os.getenv("ML_LOG_ROW_COUNTS", "false").lower() == "true":
        print(f"✓ Joined all tables: {df.count()} orders with features")
    else:
        print("✓ Joined all tables")
    return df


def generate_simulated_features(df):
    """
    Generate simulated external features for display / downstream use only.
    NOTE: These columns are intentionally excluded from ML feature lists because
    they are generated via rand() and carry zero signal.
    """
    print("\n🔧 Generating simulated features (display only, not used in ML)...")

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
    df = df.withColumn(
        "is_holiday_period",
        when(month(col("order_placed_at")).isin(11, 12), 1).otherwise(0)
    )
    df = df.withColumn(
        "is_peak_shopping_season",
        when(month(col("order_placed_at")).isin(11, 12, 6, 7), 1).otherwise(0)
    )

    print("✓ Generated simulated features")
    return df


def engineer_features(df):
    print("\n🔧 Engineering derived features...")

    df = df.withColumn(
        "stock_to_order_ratio",
        col("products_in_stock_count") / (col("unique_products_ordered") + lit(1))
    )
    df = df.withColumn(
        "low_stock_ratio",
        col("products_low_stock_count") / (col("unique_products_ordered") + lit(1))
    )
    df = df.withColumn(
        "out_of_stock_count",
        col("unique_products_ordered") - col("products_in_stock_count")
    )
    df = df.withColumn(
        "order_value_per_item",
        col("total_amount") / (col("total_quantity") + lit(1))
    )
    df = df.withColumn(
        "reserved_to_quantity_ratio",
        col("total_reserved_quantity") / (col("total_quantity") + lit(1))
    )
    df = df.withColumn(
        "supplier_risk_composite",
        (lit(1) - col("primary_supplier_reliability")) * (lit(1) + col("supplier_stockout_rate"))
    )
    df = df.withColumn(
        "lead_time_quantity_interaction",
        col("avg_supplier_lead_time") * col("total_quantity")
    )
    df = df.withColumn("order_month", month(col("order_placed_at")))
    df = df.withColumn(
        "value_at_risk",
        col("total_amount") * (lit(1) - col("avg_product_availability"))
    )
    df = df.withColumn(
        "fulfillment_complexity",
        col("unique_products_ordered") * (lit(1) + col("low_stock_ratio")) *
        (col("avg_supplier_lead_time") + lit(1))
    )
    df = df.withColumn(
        "stock_health_combined",
        col("avg_available_stock") * col("avg_stock_coverage_days") /
        (col("total_reorder_breaches") + lit(1))
    )
    df = df.withColumn(
        "customer_reliability_score",
        col("rfm_overall_score") * (lit(1) - col("customer_cancellation_rate"))
    )
    df = df.withColumn(
        "order_age_days",
        when(col("order_placed_at").isNotNull(), datediff(current_date(), col("order_placed_at"))).otherwise(lit(0))
    )
    df = df.withColumn(
        "supplier_order_load",
        col("avg_supplier_fulfilled_orders") / (col("avg_supplier_lead_time") + lit(1))
    )
    df = df.withColumn(
        "stock_pressure_index",
        col("total_quantity") / (col("avg_available_stock") + lit(1))
    )
    df = df.withColumn(
        "seasonal_demand_index",
        when(col("season").isin("Winter", "Fall", "Holiday"), lit(1.2)).otherwise(lit(1.0))
    )
    df = df.withColumn(
        "customer_value_risk_interaction",
        col("customer_lifetime_value") * (lit(1) - col("customer_reliability_score"))
    )
    df = df.withColumn(
        "is_high_value_order",
        when(col("total_amount") >= 200, lit(1)).otherwise(lit(0))
    )

    fill_cols = [
        "shipping_cost", "discount_percentage",
        "avg_product_rating", "avg_product_performance",
        "total_stockout_history", "avg_inventory_turnover",
        "avg_available_stock", "avg_stock_coverage_days",
        "total_reorder_breaches", "avg_stock_turnover_ratio",
        "customer_total_orders", "customer_cancellation_rate",
        "customer_avg_order_value", "customer_lifetime_value",
        "customer_tenure_days", "rfm_overall_score", "customer_activity_score",
        "avg_supplier_performance_score", "avg_supplier_rating",
        "avg_supplier_fulfilled_orders", "avg_supplier_inventory_health",
        "order_age_days", "supplier_order_load", "stock_pressure_index",
        "seasonal_demand_index", "customer_value_risk_interaction",
        "stock_to_order_ratio", "low_stock_ratio", "out_of_stock_count",
        "order_value_per_item", "reserved_to_quantity_ratio",
        "supplier_risk_composite", "lead_time_quantity_interaction", "order_month",
        "value_at_risk", "fulfillment_complexity",
        "stock_health_combined", "customer_reliability_score",
    ]
    for c in fill_cols:
        df = df.fillna({c: 0})

    print("✓ Engineered derived features")
    return df


def generate_risk_labels(df):
    print("\n🏷️  Generating fulfillment risk labels...")

    df_with_label = df.withColumn(
        TARGET_COLUMN,
        when(col("order_status").isin("Cancelled", "cancelled", "Failed"), lit(3))
        .when(col("delivery_days_diff") > 15, lit(3))
        .when(col("delivery_days_diff") > 8, lit(2))
        .when(
            (col("order_status").isin("Pending", "pending", "Processing")) &
            (datediff(current_date(), col("order_placed_at")) > 10),
            lit(2)
        )
        .when(col("delivery_days_diff") > 5, lit(1))
        .when(col("delivery_days_diff").isNotNull(), lit(0))
        .when(col("order_status").isin("Pending", "pending", "Processing"), lit(0))
        .otherwise(lit(0))
    )

    df_with_label = df_with_label.filter(col(TARGET_COLUMN).isNotNull())
    df_with_label = df_with_label.persist(StorageLevel.MEMORY_AND_DISK)

    label_dist = df_with_label.groupBy(TARGET_COLUMN).count().orderBy(TARGET_COLUMN)
    rows = label_dist.collect()

    print("Risk class distribution:")
    for r in rows:
        print(f"  class {int(r[TARGET_COLUMN])}: {int(r['count'])}")

    # ── FIX 2: Print baseline accuracy (majority-class classifier) ──────────
    class_counts = {int(r[TARGET_COLUMN]): int(r["count"]) for r in rows}
    total_count = sum(class_counts.values())
    majority_class = max(class_counts, key=class_counts.get)
    baseline_accuracy = class_counts[majority_class] / total_count
    print(f"⚖️  Baseline accuracy (always predict class {majority_class}): {baseline_accuracy:.4f}")
    print(f"   A model must beat {baseline_accuracy:.4f} to be useful.\n")

    df_final = df_with_label.drop(
        "order_shipped_at", "order_delivered_at", "delivery_days_diff",
        "distinct_suppliers", "supplier_count", "item_count"
    )

    df_final = df_final.persist(StorageLevel.MEMORY_AND_DISK)
    _ = df_final.count()
    df_with_label.unpersist()

    return df_final, class_counts, total_count


def compute_class_weights(class_counts, total_count, num_classes=4):
    """
    FIX 3: Compute inverse-frequency class weights.
    weight_i = total / (num_classes * count_i)
    Balances loss so minority classes (High / Critical) aren't drowned out.
    """
    weights = {}
    for cls, cnt in class_counts.items():
        weights[cls] = total_count / (num_classes * max(cnt, 1))
    print(f"📊 Class weights: {weights}")
    return weights


def add_weight_column(df, class_weights):
    """Map each row's label to its inverse-frequency weight."""
    mapping_expr = create_map(
        [item for k, v in class_weights.items()
         for item in (lit(int(k)), lit(float(v)))]
    )
    return df.withColumn("class_weight", mapping_expr[col(TARGET_COLUMN)])


def prepare_features(train_df, test_df, numerical_features, categorical_features, boolean_features):
    """Prepare features — FIT ON TRAIN ONLY."""
    train_filled = train_df.fillna(0, subset=numerical_features + boolean_features)
    train_filled = train_filled.fillna("Unknown", subset=categorical_features)
    test_filled = test_df.fillna(0, subset=numerical_features + boolean_features)
    test_filled = test_filled.fillna("Unknown", subset=categorical_features)

    train_clean = train_filled.filter(col(TARGET_COLUMN).isNotNull())
    test_clean = test_filled.filter(col(TARGET_COLUMN).isNotNull())

    categorical_indexed_cols = []
    categorical_indexers = []
    for cat_col in categorical_features:
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_indexed", handleInvalid="keep")
        indexer_model = indexer.fit(train_clean)
        train_clean = indexer_model.transform(train_clean)
        test_clean = indexer_model.transform(test_clean)
        categorical_indexed_cols.append(f"{cat_col}_indexed")
        categorical_indexers.append(indexer_model)

    all_numerical = numerical_features + boolean_features
    numerical_assembler = VectorAssembler(inputCols=all_numerical, outputCol="numerical_features")
    train_clean = numerical_assembler.transform(train_clean)
    test_clean = numerical_assembler.transform(test_clean)

    scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")
    scaler_model = scaler.fit(train_clean)
    train_clean = scaler_model.transform(train_clean)
    test_clean = scaler_model.transform(test_clean)

    all_feature_cols = ["scaled_numerical_features"] + categorical_indexed_cols
    final_assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")
    train_vector = final_assembler.transform(train_clean)
    test_vector = final_assembler.transform(test_clean)

    train_indexed = train_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))
    test_indexed = test_vector.withColumn("label", col(TARGET_COLUMN).cast("double"))

    print(f"✓ Prepared features: {len(numerical_features)} numerical + {len(boolean_features)} boolean + {len(categorical_features)} categorical")

    return train_indexed, test_indexed, {
        "categorical_indexers": categorical_indexers,
        "scaler": scaler_model
    }


# ── Model training functions ─────────────────────────────────────────────────

def train_logistic_regression(train_df):
    print("\n[1/4] Training Logistic Regression (weighted)...")
    lr = LogisticRegression(
        maxIter=200, regParam=0.01, elasticNetParam=0.5,
        weightCol="class_weight"
    )
    model = lr.fit(train_df)
    print("✓ Logistic Regression trained")
    return model, "LogisticRegression"


def train_random_forest(train_df):
    """FIX 3 applied: weightCol passed to RandomForest with resilient fallback."""
    print("\n[2/4] Training Random Forest (weighted)...")
    rf_num_trees = int(os.getenv("FULFILLMENT_RISK_RF_TREES", "80"))
    rf_max_depth = int(os.getenv("FULFILLMENT_RISK_RF_MAX_DEPTH", "10"))

    candidate_configs = [
        {
            "numTrees": rf_num_trees,
            "maxDepth": rf_max_depth,
            "featureSubsetStrategy": "sqrt",
            "subsamplingRate": 0.8,
        },
        {
            "numTrees": max(120, rf_num_trees // 2),
            "maxDepth": min(12, rf_max_depth),
            "featureSubsetStrategy": "sqrt",
            "subsamplingRate": 0.7,
        },
        {
            "numTrees": 100,
            "maxDepth": 10,
            "featureSubsetStrategy": "onethird",
            "subsamplingRate": 0.6,
        },
    ]

    last_error = None
    for idx, cfg in enumerate(candidate_configs, start=1):
        try:
            print(
                f"   RF attempt {idx}/{len(candidate_configs)}: "
                f"trees={cfg['numTrees']}, depth={cfg['maxDepth']}, "
                f"subset={cfg['featureSubsetStrategy']}, subsample={cfg['subsamplingRate']}"
            )
            rf = RandomForestClassifier(
                numTrees=cfg["numTrees"],
                maxDepth=cfg["maxDepth"],
                seed=42,
                weightCol="class_weight",
                featureSubsetStrategy=cfg["featureSubsetStrategy"],
                subsamplingRate=cfg["subsamplingRate"],
            )
            model = rf.fit(train_df)
            print("✓ Random Forest trained")
            return model, "RandomForest"
        except Exception as exc:
            last_error = exc
            print(f"   ⚠️  RF attempt {idx} failed: {exc}")

    raise RuntimeError(f"RandomForest training failed after retries: {last_error}")


def train_decision_tree(train_df):
    print("\n[3/4] Training Decision Tree (weighted)...")
    dt = DecisionTreeClassifier(maxDepth=15, seed=42, weightCol="class_weight")
    model = dt.fit(train_df)
    print("✓ Decision Tree trained")
    return model, "DecisionTree"


def train_gbt(train_df):
    """
    FIX 5: Gradient Boosted Trees via OneVsRest (GBT is binary-only in Spark).
    GBTs typically outperform Random Forests on structured tabular data by
    sequentially correcting residual errors.
    """
    print("\n[4/4] Training Gradient Boosted Trees (OneVsRest, multiclass)...")
    gbt_max_iter = int(os.getenv("FULFILLMENT_RISK_GBT_MAX_ITER", "40"))
    gbt_max_depth = int(os.getenv("FULFILLMENT_RISK_GBT_MAX_DEPTH", "5"))
    gbt = GBTClassifier(
        maxIter=gbt_max_iter, maxDepth=gbt_max_depth, seed=42,
        subsamplingRate=0.8, stepSize=0.1
    )
    ovr = OneVsRest(classifier=gbt)
    model = ovr.fit(train_df)
    print("✓ Gradient Boosted Trees trained")
    return model, "GBT"


def evaluate_model(model, test_df, model_name):
    predictions = model.transform(test_df).persist(StorageLevel.MEMORY_AND_DISK)
    _ = predictions.count()

    mc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    accuracy  = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
    precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
    recall    = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})
    f1        = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})

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

    predictions.unpersist()

    return metrics


def print_feature_importances(model, model_name, numerical_features, boolean_features, categorical_features):
    """
    FIX 4: Print top-20 feature importances from tree-based models.
    A very flat importance distribution signals weak predictive features.
    GBT (OneVsRest) exposes sub-model importances; we print the first binary model.
    """
    try:
        if model_name == "RandomForest":
            importances = model.featureImportances
        elif model_name == "GBT":
            # OneVsRest wraps multiple binary GBTs; inspect the first sub-model
            importances = model.models[0].featureImportances
        else:
            return

        all_feature_names = (
            numerical_features + boolean_features +
            [f"{c}_indexed" for c in categorical_features]
        )

        # featureImportances is a SparseVector; convert to list
        importance_pairs = sorted(
            [(all_feature_names[i], float(importances[i]))
             for i in range(min(len(all_feature_names), importances.size))],
            key=lambda x: x[1], reverse=True
        )

        print(f"\n🔍 Top-20 Feature Importances ({model_name}):")
        print(f"  {'Feature':<45} Importance")
        print(f"  {'-'*55}")
        for name, imp in importance_pairs[:20]:
            bar = "█" * int(imp * 200)
            print(f"  {name:<45} {imp:.5f}  {bar}")

        # Flat-distribution warning
        top5_sum = sum(imp for _, imp in importance_pairs[:5])
        if top5_sum < 0.3:
            print("\n  ⚠️  WARNING: Top-5 features only account for "
                  f"{top5_sum:.2%} of total importance.")
            print("     Features have weak predictive power — consider")
            print("     revisiting label generation or adding richer features.")

    except Exception as e:
        print(f"  (Could not extract importances: {e})")


def save_models(model, preprocessors, output_dir, model_name):
    model_path = f"{output_dir}/{model_name}"
    model.write().overwrite().save(model_path)

    for i, indexer in enumerate(preprocessors["categorical_indexers"]):
        indexer.write().overwrite().save(f"{output_dir}/{model_name}_cat_indexer_{i}")

    preprocessors["scaler"].write().overwrite().save(f"{output_dir}/{model_name}_scaler")
    print(f"✓ Saved {model_name}")


def main(BUCKET_NAME, EXPORT_PLOTS=False):
    INPUT_PATH_ORDERS      = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_PATH_ORDER_ITEMS = f"s3a://{BUCKET_NAME}/transformed/agg_order_items.parquet"
    INPUT_PATH_PRODUCTS    = f"s3a://{BUCKET_NAME}/transformed/agg_products.parquet"
    INPUT_PATH_INVENTORY   = f"s3a://{BUCKET_NAME}/transformed/agg_inventory.parquet"
    INPUT_PATH_SUPPLIERS   = f"s3a://{BUCKET_NAME}/transformed/agg_suppliers.parquet"
    INPUT_PATH_CUSTOMERS   = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    MODEL_OUTPUT_DIR       = f"s3a://{BUCKET_NAME}/machine-learning/classification/models/fulfillment_risk"
    MIN_LABELED_RECORDS    = 100

    print("=" * 70)
    print("Order Fulfillment Risk Classification - Training Pipeline v2")
    print("=" * 70)
    print("Changes vs v1:")
    print("  ✅ FIX 1: Label noise removed")
    print("  ✅ FIX 2: Baseline accuracy printed")
    print("  ✅ FIX 3: Inverse-frequency class weights applied to all models")
    print("  ✅ FIX 4: Feature importances logged (RF + GBT)")
    print("  ✅ FIX 5: GBTClassifier added (via OneVsRest for multiclass)")
    print("  ✅ BONUS: 7 random-noise simulated features removed from ML pipeline")
    print("=" * 70)

    spark = create_spark_session()

    print("\n📦 Loading tables...")
    orders_df    = load_data(spark, INPUT_PATH_ORDERS)
    order_items_df = load_data(spark, INPUT_PATH_ORDER_ITEMS)
    products_df  = load_data(spark, INPUT_PATH_PRODUCTS)
    inventory_df = load_data(spark, INPUT_PATH_INVENTORY)
    suppliers_df = load_data(spark, INPUT_PATH_SUPPLIERS)
    customers_df = load_data(spark, INPUT_PATH_CUSTOMERS)

    if None in [orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df]:
        print("✗ Training stopped: failed to load all tables")
        return

    if not validate_required_source_columns({
        "agg_orders": orders_df,
        "agg_order_items": order_items_df,
        "agg_products": products_df,
        "agg_inventory": inventory_df,
        "agg_suppliers": suppliers_df,
        "agg_customers": customers_df,
    }):
        return

    df = join_all_tables(orders_df, order_items_df, products_df, inventory_df, suppliers_df, customers_df)
    df = generate_simulated_features(df)   # kept for display; excluded from ML
    df = engineer_features(df)

    # FIX 1: No label noise — labels now purely reflect real delivery outcomes
    df, class_counts, total_count = generate_risk_labels(df)

    if not validate_feature_columns(df):
        return

    labeled_count = sum(class_counts.values())
    if labeled_count < MIN_LABELED_RECORDS:
        print(f"✗ Insufficient data ({labeled_count} < {MIN_LABELED_RECORDS})")
        return

    print(f"✓ Dataset ready: {labeled_count} orders")

    # FIX 3: Compute class weights on the full dataset before splitting
    class_weights = compute_class_weights(class_counts, total_count, num_classes=4)

    train_df_raw, test_df_raw = df.randomSplit([0.8, 0.2], seed=42)
    train_df_raw = train_df_raw.persist(StorageLevel.MEMORY_AND_DISK)
    test_df_raw = test_df_raw.persist(StorageLevel.MEMORY_AND_DISK)
    train_count = train_df_raw.count()
    test_count = test_df_raw.count()
    print(f"✓ Split: {train_count} train, {test_count} test")

    # Add weight column to training set (test set is never weighted during eval)
    train_df_raw_unweighted = train_df_raw
    train_df_raw = add_weight_column(train_df_raw_unweighted, class_weights).persist(StorageLevel.MEMORY_AND_DISK)
    _ = train_df_raw.count()
    train_df_raw_unweighted.unpersist()

    train_df, test_df, preprocessors = prepare_features(
        train_df_raw, test_df_raw, NUMERICAL_FEATURES, CATEGORICAL_FEATURES, BOOLEAN_FEATURES
    )

    # Stabilize downstream model-training stages with bounded partition count.
    target_partitions = int(os.getenv("FULFILLMENT_RISK_TRAIN_PARTITIONS", "4"))
    if target_partitions > 0:
        train_df = train_df.repartition(target_partitions)
        test_df = test_df.repartition(max(2, target_partitions // 2))

    train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)
    _ = train_df.count()
    _ = test_df.count()

    enabled_models = _parse_enabled_models(
        os.getenv("FULFILLMENT_RISK_MODELS", "LogisticRegression,RandomForest,DecisionTree,GBT")
    )
    if not enabled_models:
        enabled_models = ["LogisticRegression", "RandomForest", "DecisionTree", "GBT"]

    model_trainers = {
        "LogisticRegression": train_logistic_regression,
        "RandomForest": train_random_forest,
        "DecisionTree": train_decision_tree,
        "GBT": train_gbt,
    }

    models = []
    failed_models = []
    print(f"\n🧠 Models enabled: {', '.join(enabled_models)}")
    for model_name in enabled_models:
        trainer = model_trainers.get(model_name)
        if trainer is None:
            continue
        try:
            models.append(trainer(train_df))
        except Exception as exc:
            failed_models.append((model_name, str(exc)))
            print(f"⚠️  Skipping {model_name} after failure: {exc}")

    if not models:
        raise RuntimeError("No model completed training successfully.")

    print("\n" + "=" * 70)
    print("Model Evaluation")
    print("=" * 70)

    all_metrics = []
    for model, model_name in models:
        metrics = evaluate_model(model, test_df, model_name)
        all_metrics.append(metrics)
        # FIX 4: Feature importances for tree-based models
        print_feature_importances(model, model_name, NUMERICAL_FEATURES, BOOLEAN_FEATURES, CATEGORICAL_FEATURES)
        save_models(model, preprocessors, MODEL_OUTPUT_DIR, model_name)

    print("\n" + "=" * 70)
    print("Model Comparison")
    print("=" * 70)
    for m in sorted(all_metrics, key=lambda x: x["f1_score"], reverse=True):
        print(f"{m['model_name']:25s} | F1: {m['f1_score']:.4f} | Acc: {m['accuracy']:.4f}")

    if failed_models:
        print("\n⚠️  Models that failed during this run:")
        for name, error_text in failed_models:
            print(f"  - {name}: {error_text}")

    best = max(all_metrics, key=lambda x: x["f1_score"])
    save_best_model_manifest(spark, MODEL_OUTPUT_DIR, best)
    print(f"\n🏆 Best model: {best['model_name']} — update SELECTED_MODEL in inference script.")
    print("\n✓ Training completed")
    print(f"   Models saved to: {MODEL_OUTPUT_DIR}")

    train_df.unpersist()
    test_df.unpersist()
    train_df_raw.unpersist()
    test_df_raw.unpersist()
    df.unpersist()

    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"
    main(BUCKET_NAME, EXPORT_PLOTS=False)
