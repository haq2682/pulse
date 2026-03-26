"""
Delivery Time Prediction - Training Script
Predicts order delivery time in days

Target Calculation:
- Use ONLY delivered orders with actual delivery dates
- Target = delivery_days_diff (already calculated in schema)
- delivery_days = (order_delivered_at - order_placed_at) in days
"""

import os
import sys

import findspark
from dotenv import load_dotenv
from pathlib import Path
# Import spark_utils FIRST to set up JARs before pyspark imports
_ML_ROOT_VAR = next((p for p in Path(__file__).resolve().parents if p.name == "machine-learning"), None)
if _ML_ROOT_VAR and str(_ML_ROOT_VAR) not in sys.path:
    sys.path.insert(0, str(_ML_ROOT_VAR))

from spark_utils import create_ml_spark_session
from general.utils.plot_exporter import export_training_metrics_plot


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import (
    RandomForestRegressor,
    GBTRegressor,
    DecisionTreeRegressor,
    LinearRegression,
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from datetime import datetime

# Load environment variables
load_dotenv()


REQUIRED_ORDER_COLUMNS = ["order_id", "customer_id", "order_status", "order_placed_at", "delivery_days_diff"]
REQUIRED_CUSTOMER_COLUMNS = ["customer_id", "country", "state_province", "city"]

# Feature set
NUMERIC_FEATURES = [
    # Order characteristics
    "total_quantity",
    "total_amount",
    "shipping_cost",
    "subtotal",
    "tax_amount",
    "total_discount",
    "discount_percentage",
    "total_product_price",
    "unique_products_ordered",
    "avg_product_price",
    
    # Temporal features
    "order_placed_day_of_week",
    "order_placed_month",
    "order_placed_quarter",
    "order_placed_week_of_year",
    "order_placed_day_of_month",
    "is_weekend",
    "is_month_end",
    "is_holiday_season",
    
    # Location-based historical performance
    "city_avg_delivery_days",
    "city_delivery_std",
    "state_avg_delivery_days",
    "country_avg_delivery_days",
    "location_delivery_consistency",  # 1/std
    
    # Shipping cost tier performance
    "shipping_tier_avg_delivery",
    "shipping_cost_to_amount_ratio",
    "order_value_per_item",
    "log_total_amount",
    
    # Customer history
    "customer_total_orders",
    "customer_avg_delivery_days",
    "customer_tenure_days",
    "customer_lifetime_value",
    "customer_recency_days",
    "rfm_overall_score",
    "customer_monetary_per_order",
    "is_repeat_customer",
    
    # Order complexity
    "order_complexity_score",  # quantity * unique_products
    "order_size_tier",  # Small/Medium/Large
    
    # Distance indicators (proxy)
    "is_major_city",
    "is_capital_city",
    "location_shipping_interaction",
    
    # Categorical (indexed)
    "country_idx",
    "state_idx",
    "city_idx",
    "season_idx"
]

TARGET_COLUMN = "delivery_days_diff"


def ensure_columns(df, defaults: dict):
    """Ensure columns exist before fillna/feature assembly."""
    out = df
    for col_name, default_value in defaults.items():
        if col_name not in out.columns:
            out = out.withColumn(col_name, F.lit(default_value))
    return out


def create_spark_session():
    """Initialize Spark session"""
    return create_ml_spark_session(
        "Delivery_Time_Training",
        extra_configs={
                    "spark.driver.memory": os.getenv("ML_SPARK_DRIVER_MEMORY", "4g"),
                    "spark.driver.maxResultSize": os.getenv("ML_SPARK_DRIVER_MAX_RESULT_SIZE", "1g"),
                    "spark.executor.instances": os.getenv("ML_SPARK_EXECUTOR_INSTANCES", "1"),
                    "spark.executor.cores": os.getenv("ML_SPARK_EXECUTOR_CORES", "1"),
                    "spark.executor.memory": os.getenv("ML_SPARK_EXECUTOR_MEMORY", "1536m"),
                    "spark.executor.memoryOverhead": os.getenv("ML_SPARK_EXECUTOR_MEMORY_OVERHEAD", "768"),
                    "spark.sql.shuffle.partitions": os.getenv("ML_SPARK_SHUFFLE_PARTITIONS", "4"),
                    "spark.default.parallelism": os.getenv("ML_SPARK_DEFAULT_PARALLELISM", "4"),
                    "spark.sql.adaptive.enabled": os.getenv("ML_SPARK_SQL_ADAPTIVE", "true"),
                    "spark.sql.adaptive.coalescePartitions.enabled": os.getenv("ML_SPARK_SQL_COALESCE_PARTITIONS", "true"),
                    "spark.sql.adaptive.skewJoin.enabled": os.getenv("ML_SPARK_SQL_SKEW_JOIN", "true"),
                    "spark.network.timeout": os.getenv("ML_SPARK_NETWORK_TIMEOUT", "600s"),
                    "spark.executor.heartbeatInterval": os.getenv("ML_SPARK_HEARTBEAT_INTERVAL", "60s"),
                    "spark.shuffle.io.maxRetries": os.getenv("ML_SPARK_SHUFFLE_MAX_RETRIES", "10"),
                    "spark.shuffle.io.retryWait": os.getenv("ML_SPARK_SHUFFLE_RETRY_WAIT", "10s"),
                    "spark.task.maxFailures": os.getenv("ML_SPARK_TASK_MAX_FAILURES", "8"),
                    "spark.stage.maxConsecutiveAttempts": os.getenv("ML_SPARK_STAGE_MAX_ATTEMPTS", "8"),
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.kryoserializer.buffer.max": os.getenv("ML_SPARK_KRYO_BUFFER_MAX", "256m"),
                    "inferSchema": "true",
                    "mergeSchema": "true"
                },
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


def validate_columns(df, required_columns: list, dataset_name: str,
                     max_null_pct: float = 95.0) -> bool:
    """
    OPTIMISED: single .agg() pass to count nulls for all required columns at
    once instead of one query per column (was N+1 full Spark jobs, now 1).
    """
    print(f"\nValidating columns for {dataset_name}...")
 
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        print(f"  ✗ Missing columns: {', '.join(missing)}")
        return False
 
    # --- single aggregation over all columns ---
    agg_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in required_columns]
    agg_exprs.insert(0, F.count("*").alias("__total__"))
 
    counts = df.agg(*agg_exprs).collect()[0].asDict()
    total  = counts["__total__"]
    if total == 0:
        print(f"  ✗ {dataset_name} is empty")
        return False
 
    failed = []
    for col in required_columns:
        pct = counts[col] / total * 100
        if pct > max_null_pct:
            print(f"  ✗ '{col}' is {pct:.1f}% null (threshold {max_null_pct}%)")
            failed.append(col)
        elif pct > 50:
            print(f"  ⚠  '{col}' is {pct:.1f}% null")
 
    if failed:
        return False
 
    print(f"  ✓ All required columns validated")
    return True


def calculate_location_delivery_statistics(orders_df, customers_df):
    """
    Calculate historical delivery performance by location
    Uses PAST delivered orders to establish location baselines
    """
    print("Calculating location-based delivery statistics...")
    
    # Join orders with customer locations - only DELIVERED orders
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    ).join(
        customers_df.select("customer_id", "country", "state_province", "city"),
        "customer_id",
        "inner"
    )
    
    print(f"Delivered orders with locations: {delivered_orders.count()}")
    
    # City-level statistics
    city_stats = delivered_orders.groupBy("country", "state_province", "city").agg(
        F.avg("delivery_days_diff").alias("city_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("city_delivery_std"),
        F.count("order_id").alias("city_order_count")
    )
    
    # State-level statistics
    state_stats = delivered_orders.groupBy("country", "state_province").agg(
        F.avg("delivery_days_diff").alias("state_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("state_delivery_std"),
        F.count("order_id").alias("state_order_count")
    )
    
    # Country-level statistics
    country_stats = delivered_orders.groupBy("country").agg(
        F.avg("delivery_days_diff").alias("country_avg_delivery_days"),
        F.stddev("delivery_days_diff").alias("country_delivery_std"),
        F.count("order_id").alias("country_order_count")
    )
    
    # Fill nulls
    city_stats = city_stats.fillna({"city_delivery_std": 0})
    state_stats = state_stats.fillna({"state_delivery_std": 0})
    country_stats = country_stats.fillna({"country_delivery_std": 0})
    
    print(f"✓ Location statistics calculated")
    print(f"  Cities: {city_stats.count()}")
    print(f"  States: {state_stats.count()}")
    print(f"  Countries: {country_stats.count()}")
    
    return city_stats, state_stats, country_stats


def calculate_shipping_tier_statistics(orders_df):
    """Calculate delivery performance by shipping cost tier"""
    print("Calculating shipping tier statistics...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0) &
        (F.col("shipping_cost").isNotNull())
    )
    
    # Create shipping cost tiers
    delivered_with_tiers = delivered_orders.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    tier_stats = delivered_with_tiers.groupBy("shipping_tier").agg(
        F.avg("delivery_days_diff").alias("shipping_tier_avg_delivery"),
        F.count("order_id").alias("tier_order_count")
    )
    
    print(f"✓ Shipping tier statistics calculated")
    return tier_stats


def calculate_customer_delivery_history(orders_df):
    """Calculate historical delivery performance per customer"""
    print("Calculating customer delivery history...")
    
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    )
    
    customer_stats = delivered_orders.groupBy("customer_id").agg(
        F.avg("delivery_days_diff").alias("customer_avg_delivery_days"),
        F.count("order_id").alias("customer_order_count")
    )
    
    print(f"✓ Customer delivery history calculated")
    return customer_stats


def create_delivery_time_features(orders_df, customers_df, city_stats_df, state_stats_df, 
                                  country_stats_df, tier_stats_df, customer_stats_df):
    """
    Create comprehensive delivery time prediction features
    Only use features available at order placement time
    """
    print("Creating delivery time features...")
    
    # Start with delivered orders only for training
    delivered_orders = orders_df.filter(
        (F.col("order_status") == "Delivered") &
        (F.col("delivery_days_diff").isNotNull()) &
        (F.col("delivery_days_diff") > 0)
    )
    
    print(f"Delivered orders for training: {delivered_orders.count()}")
    
    # Join with customer locations
    order_features = delivered_orders.join(
        customers_df.select("customer_id", "country", "state_province", "city", "customer_segment"),
        "customer_id",
        "inner"
    )
    
    # Join with location statistics
    order_features = order_features.join(
        city_stats_df,
        ["country", "state_province", "city"],
        "left"
    ).join(
        state_stats_df.select("country", "state_province", "state_avg_delivery_days"),
        ["country", "state_province"],
        "left"
    ).join(
        country_stats_df.select("country", "country_avg_delivery_days"),
        "country",
        "left"
    )
    
    # Calculate shipping tier for this order
    order_features = order_features.withColumn(
        "shipping_tier",
        F.when(F.col("shipping_cost") < 5, "economy")
         .when(F.col("shipping_cost") < 15, "standard")
         .when(F.col("shipping_cost") < 30, "express")
         .otherwise("premium")
    )
    
    # Join with shipping tier statistics
    order_features = order_features.join(
        tier_stats_df,
        "shipping_tier",
        "left"
    )
    
    # Join with customer history
    order_features = order_features.join(
        customer_stats_df,
        "customer_id",
        "left"
    )

    optional_defaults = {
        "subtotal": 0.0,
        "tax_amount": 0.0,
        "total_discount": 0.0,
        "discount_percentage": 0.0,
        "total_product_price": 0.0,
        "order_placed_week_of_year": 1,
        "customer_tenure_days": 0,
        "customer_lifetime_value": 0.0,
        "rfm_overall_score": 0.0,
        "total_orders": 0,
        "total_revenue": 0.0,
        "days_since_last_purchase": 30,
        "order_recency_days": 30,
    }
    order_features = ensure_columns(order_features, optional_defaults)

    if "days_since_last_purchase" not in order_features.columns:
        if "last_order_date" in order_features.columns and "order_placed_at" in order_features.columns:
            order_features = order_features.withColumn(
                "days_since_last_purchase",
                F.greatest(
                    F.datediff(F.to_date("order_placed_at"), F.to_date("last_order_date")),
                    F.lit(0),
                ),
            )
        elif "order_recency_days" in order_features.columns:
            order_features = order_features.withColumn(
                "days_since_last_purchase",
                F.coalesce(F.col("order_recency_days"), F.lit(30)),
            )
        else:
            order_features = order_features.withColumn("days_since_last_purchase", F.lit(30))
    
    # Create temporal features
    order_features = order_features.withColumn(
        "is_weekend",
        F.when(F.col("order_placed_day_of_week").isin([6, 7]), 1).otherwise(0)
    ).withColumn(
        "is_month_end",
        F.when(F.col("order_placed_day_of_month") >= 25, 1).otherwise(0)
    ).withColumn(
        "is_holiday_season",
        F.when(F.col("order_placed_month").isin([11, 12]), 1).otherwise(0)
    )
    
    # Location consistency metric
    order_features = order_features.withColumn(
        "location_delivery_consistency",
        F.when(
            F.col("city_delivery_std") > 0,
            1.0 / F.col("city_delivery_std")
        ).otherwise(1.0)
    )
    
    # Shipping cost ratio
    order_features = order_features.withColumn(
        "shipping_cost_to_amount_ratio",
        F.when(
            F.col("total_amount") > 0,
            F.col("shipping_cost") / F.col("total_amount")
        ).otherwise(0)
    )

    order_features = order_features.withColumn(
        "order_value_per_item",
        F.when(
            F.col("total_quantity") > 0,
            F.col("total_amount") / F.col("total_quantity")
        ).otherwise(0.0)
    ).withColumn(
        "log_total_amount",
        F.log1p(F.greatest(F.coalesce(F.col("total_amount"), F.lit(0.0)), F.lit(0.0)))
    )
    
    # Customer indicators
    order_features = order_features.withColumn(
        "is_repeat_customer",
        F.when(F.col("customer_order_count") > 1, 1).otherwise(0)
    ).withColumn(
        "customer_total_orders",
        F.coalesce(F.col("customer_order_count"), F.lit(1))
    ).withColumn(
        "customer_recency_days",
        F.coalesce(F.col("days_since_last_purchase"), F.col("order_recency_days"), F.lit(30))
    ).withColumn(
        "customer_monetary_per_order",
        F.when(
            F.coalesce(F.col("total_orders"), F.lit(0)) > 0,
            F.coalesce(F.col("total_revenue"), F.lit(0.0)) / F.col("total_orders")
        ).otherwise(0.0)
    )
    
    # Order complexity
    order_features = order_features.withColumn(
        "order_complexity_score",
        F.col("total_quantity") * F.coalesce(F.col("unique_products_ordered"), F.lit(1))
    ).withColumn(
        "avg_product_price",
        F.when(
            F.col("total_quantity") > 0,
            F.col("total_amount") / F.col("total_quantity")
        ).otherwise(0)
    )
    
    # Order size tier
    order_features = order_features.withColumn(
        "order_size_tier",
        F.when(F.col("total_amount") < 50, 1)
         .when(F.col("total_amount") < 150, 2)
         .when(F.col("total_amount") < 300, 3)
         .otherwise(4)
    )
    
    # Major city indicators (simplified - would need actual city list)
    # Using order volume as proxy
    order_features = order_features.withColumn(
        "is_major_city",
        F.when(F.col("city_order_count") > 100, 1).otherwise(0)
    ).withColumn(
        "is_capital_city",
        F.when(F.col("city_order_count") > 200, 1).otherwise(0)
    ).withColumn(
        "location_shipping_interaction",
        F.coalesce(F.col("country_avg_delivery_days"), F.lit(7.0)) *
        F.coalesce(F.col("shipping_cost_to_amount_ratio"), F.lit(0.0))
    )

    # Fill nulls with reasonable defaults
    order_features = order_features.fillna({
        "total_quantity": 1,
        "total_amount": 50,
        "shipping_cost": 5,
        "unique_products_ordered": 1,
        "order_placed_day_of_week": 3,
        "order_placed_month": 6,
        "order_placed_quarter": 2,
        "order_placed_day_of_month": 15,
        "city_avg_delivery_days": 0,
        "city_delivery_std": 0,
        "state_avg_delivery_days": 0,
        "country_avg_delivery_days": 7,  # Global average
        "shipping_tier_avg_delivery": 7,
        "customer_avg_delivery_days": 0,
        "season": "Summer",
        "customer_tenure_days": 0,
        "customer_lifetime_value": 0,
        "rfm_overall_score": 0,
        "order_placed_week_of_year": 1,
        "subtotal": 0,
        "tax_amount": 0,
        "total_discount": 0,
        "discount_percentage": 0,
        "total_product_price": 0,
    })
    
    print(f"✓ Delivery time features created: {order_features.count()} orders")
    return order_features


def prepare_training_data(df, min_records: int = 100):
    """
    Encode categoricals, assemble feature vector, scale.
    OPTIMISED: removed two .count() calls; record check is deferred to after
    the .persist() so Spark only triggers one action for the count.
    """
    print("Preparing training data...")
 
    df_valid = df.filter(
        F.col(TARGET_COLUMN).isNotNull() &
        (F.col(TARGET_COLUMN) > 0) &
        (F.col(TARGET_COLUMN) < 100)
    )
 
    # Encode categoricals
    for in_col, out_col in [
        ("country", "country_idx"), ("state_province", "state_idx"),
        ("city", "city_idx"), ("season", "season_idx"),
    ]:
        indexer = StringIndexer(inputCol=in_col, outputCol=out_col, handleInvalid="keep")
        df_valid = indexer.fit(df_valid).transform(df_valid)
 
    existing_features = [f for f in NUMERIC_FEATURES if f in df_valid.columns]
    missing_features  = [f for f in NUMERIC_FEATURES if f not in df_valid.columns]
    if missing_features:
        print(f"  ⚠  Skipping missing features: {', '.join(missing_features)}")
    print(f"  Using {len(existing_features)} features")
 
    assembler = VectorAssembler(
        inputCols=existing_features,
        outputCol="features_unscaled",
        handleInvalid="skip",
    )
    df_assembled = assembler.transform(df_valid)
 
    scaler       = StandardScaler(inputCol="features_unscaled", outputCol="features",
                                  withStd=True, withMean=False)
    scaler_model = scaler.fit(df_assembled)
    df_scaled    = scaler_model.transform(df_assembled)
 
    df_prepared = df_scaled.select(
        "order_id",
        "features",
        F.col(TARGET_COLUMN).alias("label"),
    )
 
    # OPTIMISED: persist here so the count() below is the ONLY action that
    # triggers materialisation, and the subsequent split reuses cached data.
    df_prepared.persist(StorageLevel.MEMORY_AND_DISK)
    valid_count = df_prepared.count()   # single action
    print(f"  Records for training: {valid_count}")
 
    if valid_count < min_records:
        df_prepared.unpersist()
        print(f"  ✗ Insufficient data: {valid_count} < {min_records}")
        return None
 
    print("  ✓ Data prepared")
    return df_prepared, scaler_model, existing_features


def train_random_forest(train_df, test_df, use_cv=False):
    """Train Random Forest"""
    print("\n" + "="*60)
    print("Training Random Forest")
    print("="*60)
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
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
            labelCol="label",
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


def train_models_and_select_best(train_df, test_df):
    """Train multiple regressors and select the best by R², then RMSE."""
    candidates = [
        (
            "random_forest",
            RandomForestRegressor(
                featuresCol="features",
                labelCol="label",
                numTrees=140,
                maxDepth=12,
                minInstancesPerNode=10,
                seed=42,
            ),
        ),
        (
            "gbt_regressor",
            GBTRegressor(
                featuresCol="features",
                labelCol="label",
                maxIter=70,
                maxDepth=5,
                stepSize=0.1,
                seed=42,
            ),
        ),
        (
            "decision_tree",
            DecisionTreeRegressor(
                featuresCol="features",
                labelCol="label",
                maxDepth=12,
                minInstancesPerNode=40,
                seed=42,
            ),
        ),
        (
            "linear_regression",
            LinearRegression(
                featuresCol="features",
                labelCol="label",
                regParam=0.05,
                elasticNetParam=0.2,
                maxIter=60,
            ),
        ),
    ]

    results = []
    for model_name, estimator in candidates:
        print("\n" + "=" * 60)
        print(f"Training {model_name}")
        print("=" * 60)

        model = estimator.fit(train_df)
        predictions = model.transform(test_df)
        metrics = evaluate_model(predictions, model_name)
        try:
            predictions.unpersist()
        except Exception:
            pass
        results.append({"model_name": model_name, "model": model, "metrics": metrics})

    results.sort(key=lambda item: (-item["metrics"]["r2"], item["metrics"]["rmse"]))

    print("\n" + "=" * 60)
    print("Model Leaderboard")
    print("=" * 60)
    for rank, item in enumerate(results, 1):
        m = item["metrics"]
        print(
            f"{rank}. {item['model_name']:<18} "
            f"R²={m['r2']:.4f} RMSE={m['rmse']:.3f} MAE={m['mae']:.3f} MAPE={m['mape']:.2f}%"
        )

    best = results[0]
    return best["model"], best["model_name"], best["metrics"], results


def evaluate_model(predictions, model_name):
    """Evaluate model"""
    print(f"\nEvaluating {model_name}...")

    clean = (
        predictions
        .select(F.col("label").cast("double"), F.col("prediction").cast("double"))
        .filter(F.col("label").isNotNull() & F.col("prediction").isNotNull())
        .filter(~F.isnan("label") & ~F.isnan("prediction"))
        .withColumn("err", F.col("prediction") - F.col("label"))
        .withColumn("abs_err", F.abs(F.col("err")))
        .withColumn("sq_err", F.col("err") * F.col("err"))
        .withColumn(
            "ape",
            F.when(F.col("label") > 0, (F.col("abs_err") / F.col("label")) * 100.0),
        )
    )

    stats = clean.agg(
        F.count("*").alias("n"),
        F.sum("sq_err").alias("sse"),
        F.avg("sq_err").alias("mse"),
        F.avg("abs_err").alias("mae"),
        F.avg("ape").alias("mape"),
        F.sum("label").alias("sum_y"),
        F.sum(F.col("label") * F.col("label")).alias("sum_y_sq"),
    ).collect()[0]

    n = int(stats["n"] or 0)
    if n == 0:
        raise RuntimeError(f"No valid prediction rows to evaluate for model '{model_name}'")

    sse = float(stats["sse"] or 0.0)
    mse = float(stats["mse"] or 0.0)
    mae = float(stats["mae"] or 0.0)
    mape = float(stats["mape"] or 0.0)
    sum_y = float(stats["sum_y"] or 0.0)
    sum_y_sq = float(stats["sum_y_sq"] or 0.0)
    mean_y = sum_y / n
    sst = max(sum_y_sq - n * mean_y * mean_y, 0.0)
    r2 = 1.0 - (sse / sst) if sst > 0 else 0.0
    rmse = mse ** 0.5

    metrics = {"model": model_name, "rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    
    print(f"  RMSE: {rmse:.2f} days")
    print(f"  MAE: {mae:.2f} days")
    print(f"  R²: {r2:.4f}")
    print(f"  MAPE: {mape:.2f}%")
    
    return metrics


def save_model(model, model_name, MODEL_OUTPUT_PATH):
    """Save model to MinIO"""
    model_path = f"{MODEL_OUTPUT_PATH}{model_name}"
    model.write().overwrite().save(model_path)
    print(f"✓ Model saved: {model_path}")


def main(BUCKET_NAME, EXPORT_PLOTS=False):
    INPUT_ORDERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_orders.parquet"
    INPUT_CUSTOMERS_PATH = f"s3a://{BUCKET_NAME}/transformed/agg_customers.parquet"
    MODEL_OUTPUT_PATH = f"s3a://{BUCKET_NAME}/machine-learning/regression/models/delivery_time/"
    MIN_RECORDS_THRESHOLD = 100
    MAX_NULL_PERCENTAGE = 95.0

    # Configuration
    USE_CROSS_VALIDATION = False

    """Main training pipeline"""
    print("\n" + "="*60)
    print("Delivery Time Prediction - Training")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Cross-Validation: {'ENABLED' if USE_CROSS_VALIDATION else 'DISABLED'}\n")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load datasets
    print("Step 1: Load Datasets")
    print("-" * 60)
    
    orders_df, _ = validate_dataset(spark, INPUT_ORDERS_PATH, "Orders")
    customers_df, _ = validate_dataset(spark, INPUT_CUSTOMERS_PATH, "Customers")
    
    if None in [orders_df, customers_df]:
        print("\n✗ Training aborted: Missing datasets")
        spark.stop()
        return
    
    # Validate columns
    print("\nStep 2: Column Validation")
    print("-" * 60)
    
    orders_valid = validate_columns(orders_df, REQUIRED_ORDER_COLUMNS, "Orders", MAX_NULL_PERCENTAGE)
    customers_valid = validate_columns(customers_df, REQUIRED_CUSTOMER_COLUMNS, "Customers", MAX_NULL_PERCENTAGE)
    
    if not (orders_valid and customers_valid):
        print("\n✗ Training aborted: Required columns missing or entirely null")
        spark.stop()
        return
    
    # Calculate location statistics
    print("\nStep 3: Calculate Location Delivery Statistics")
    print("-" * 60)
    city_stats, state_stats, country_stats = calculate_location_delivery_statistics(orders_df, customers_df)
    
    # Calculate shipping tier statistics
    print("\nStep 4: Calculate Shipping Tier Statistics")
    print("-" * 60)
    tier_stats = calculate_shipping_tier_statistics(orders_df)
    
    # Calculate customer history
    print("\nStep 5: Calculate Customer Delivery History")
    print("-" * 60)
    customer_stats = calculate_customer_delivery_history(orders_df)
    
    # Create features
    print("\nStep 6: Feature Engineering with Target Calculation")
    print("-" * 60)
    df_features = create_delivery_time_features(
        orders_df, customers_df, city_stats, state_stats, 
        country_stats, tier_stats, customer_stats
    )
    
    # Prepare data
    print("\nStep 7: Data Preparation")
    print("-" * 60)
    result = prepare_training_data(df_features, MIN_RECORDS_THRESHOLD)
    
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
    print("\nStep 8: Train/Test Split")
    print("-" * 60)
    train_df, test_df = df_prepared.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
    test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)
    print(f"Training set: {train_df.count()} records")
    print(f"Test set: {test_df.count()} records")
    
    # Train model
    print("\nStep 9: Model Training")
    print("-" * 60)

    best_model, best_model_name, best_metrics, all_model_results = train_models_and_select_best(train_df, test_df)

    plot_metrics = [{**item["metrics"], "model": item["model_name"]} for item in all_model_results]
    export_training_metrics_plot(
        model_name="delivery_time",
        metrics=plot_metrics,
        export_plots=EXPORT_PLOTS,
        script_name=Path(__file__).stem,
    )

    print("\nStep 10: Persist Models")
    print("-" * 60)
    for item in all_model_results:
        save_model(item["model"], item["model_name"], MODEL_OUTPUT_PATH)
    save_model(best_model, "best_model", MODEL_OUTPUT_PATH)
    
    print("\n" + "="*60)
    print(f"Best Model: {best_model_name} (R² = {best_metrics['r2']:.4f})")
    print("="*60)
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Training completed\n")

    try:
        train_df.unpersist()
        test_df.unpersist()
        df_prepared.unpersist()
    except Exception:
        pass
    
    spark.stop()


if __name__ == "__main__":
    BUCKET_NAME = "afc4bd21-75ad-4da3-9fd7-4b0b540a1ccc"
    main(BUCKET_NAME, EXPORT_PLOTS=False)
