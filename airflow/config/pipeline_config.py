"""
Central configuration for all Airflow pipeline DAGs.

All environment-dependent values default to what is defined in docker-compose.yml.
Override at runtime via Airflow Variables (Admin → Variables in the UI) or env vars.
"""

import os

# ---------------------------------------------------------------------------
# Docker container name that runs PySpark / pipeline scripts
# ---------------------------------------------------------------------------
PYTHON_CONTAINER = os.getenv("PIPELINE_PYTHON_CONTAINER", "python")

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "10.5.0.4:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE      = False

# Default bucket (business-level tenant bucket)
DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET", "pulse-bucket-1")

# MinIO path prefixes used by each pipeline stage
MINIO_PREFIX_INGESTED      = "ingested/"
MINIO_PREFIX_MAPPED        = "mapped/"
MINIO_PREFIX_CLEANED       = "cleaned/"
MINIO_PREFIX_TRANSFORMED   = "transformed/"
MINIO_PREFIX_ANALYTICS     = "analytics/"
MINIO_PREFIX_MODELS        = "models/"
MINIO_PREFIX_DRIFT_BASELINES = "models/drift_baselines/"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")

# ---------------------------------------------------------------------------
# PostgreSQL  (used for status updates)
# ---------------------------------------------------------------------------
POSTGRES_SERVER   = os.getenv("POSTGRES_SERVER", "10.5.0.5")
POSTGRES_DB       = os.getenv("POSTGRES_DB", os.getenv("POSTGRES_DATABASE_NAME", "pulse"))
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "10.5.0.11")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ---------------------------------------------------------------------------
# Debezium / CDC
# ---------------------------------------------------------------------------
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://10.5.0.10:8083")

# Default DB URI for CDC mode (override per-dag-run via conf)
DEFAULT_DB_URI    = os.getenv(
    "CDC_DB_URI",
    "postgresql://debezium_user:debezium_pass@10.5.0.5:5432/pulse"
)
DEFAULT_DB_TABLES = os.getenv(
    "CDC_DB_TABLES",
    "orders,payments,inventory,shopping_cart,cart_items,customers,products"
).split(",")

# ---------------------------------------------------------------------------
# API-mode ingestion settings
# ---------------------------------------------------------------------------
# The user's external API URL is business-specific and is stored per onboarding
# record.  It is passed in dag_run.conf["api_url"] when triggering api_streaming.
# There is no meaningful system-wide default — the env var is provided only as
# an emergency override (e.g. for manual testing); leave it empty in production.
FRONTEND_API_URL  = os.getenv("FRONTEND_API_URL", "")
API_POLL_INTERVAL = int(os.getenv("API_POLL_INTERVAL", "30"))   # seconds per poll cycle
# How long (seconds) the initial-mapping subprocess runs to collect schema data.
# After this window the subprocess exits cleanly and the api_streaming DAG takes
# over for production continuous polling.
API_INITIAL_POLL_DURATION = int(os.getenv("API_INITIAL_POLL_DURATION", "120"))

# ---------------------------------------------------------------------------
# Inline downstream processing (streaming modes)
# ---------------------------------------------------------------------------
# When --enable-downstream is passed to run_mapping.py, the downstream
# pipeline (clean → transform → analyze → ML inference) runs inline
# in a background thread immediately after each Spark micro-batch.
# This reduces end-to-end latency from ~10 min (Airflow cron) to ~10 s – 2 min.
# The streaming_downstream DAG remains as a fallback at a reduced interval.
STREAMING_DOWNSTREAM_TIMEOUT = int(os.getenv("STREAMING_DOWNSTREAM_TIMEOUT", "900"))  # 15 min per step

# ---------------------------------------------------------------------------
# Airflow task defaults (applied to every DAG unless overridden)
# ---------------------------------------------------------------------------
DEFAULT_TASK_ARGS = {
    "owner": "pulse",
    "depends_on_past": False,
    "retries": 3,
    "retry_exponential_backoff": True,
    "retry_delay_seconds": 60,          # 1 min → 2 min → 4 min (exponential)
    "max_retry_delay_seconds": 600,     # cap at 10 min
    "execution_timeout_seconds": 3600,  # 1 h hard kill per task
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# KS-test (Kolmogorov-Smirnov drift detection) settings
# ---------------------------------------------------------------------------
KS_ALPHA                = float(os.getenv("KS_ALPHA", "0.05"))     # significance threshold
KS_DRIFT_RATIO_TRIGGER  = float(os.getenv("KS_DRIFT_RATIO", "0.2")) # retrain if ≥20 % features drift
KS_MIN_SAMPLE_SIZE      = int(os.getenv("KS_MIN_SAMPLE", "100"))    # skip KS if fewer rows
KS_BASELINE_MAX_SAMPLES = int(os.getenv("KS_BASELINE_SAMPLES", "5000"))  # rows stored in baseline

# ---------------------------------------------------------------------------
# Model → feature mapping (table in transformed/, columns to KS-test)
# Each entry: model_name → { "table": agg-table-name, "features": [col, ...] }
# ---------------------------------------------------------------------------
MODEL_FEATURE_MAP = {
    # ── General classification ──────────────────────────────────────────────
    "cart_abandonment": {
        "table": "agg_cart_abandonment",
        "features": [
            "abandonment_rate", "avg_cart_value", "avg_items_per_cart",
            "total_abandoned_carts", "total_completed_carts",
        ],
    },
    "customer_churn": {
        "table": "agg_customers",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
            "days_since_last_order", "order_frequency",
        ],
    },
    "customer_segments": {
        "table": "agg_customers",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
            "total_items_purchased", "total_discount_amount",
        ],
    },
    "payment_success": {
        "table": "agg_customers",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
        ],
    },
    "review_sentiment": {
        # agg_reviews schema: review_id, product_id, customer_id, review_date,
        # rating (int), review_title, review_desc, review_sentiment.
        # Training uses TF-IDF text features; drift detection monitors the
        # rating distribution (a rating shift signals changed review quality).
        "table": "agg_reviews",
        "features": ["rating"],
    },
    "stock_status": {
        "table": "agg_inventory_health",
        "features": [
            "stock_health_score", "avg_quantity", "low_stock_count",
            "out_of_stock_count", "overstock_count",
        ],
    },
    # ── General regression ──────────────────────────────────────────────────
    "aov_v2": {
        # Multi-table model (customers + orders + order_items + products).
        # Drift detection uses customer-level features available directly from
        # agg_customers without additional joins.
        "table": "agg_customers",
        "features": [
            "total_orders", "customer_tenure_days", "total_items_purchased",
            "avg_items_per_order", "session_conversion_rate",
            "cart_abandonment_rate", "recency_score",
            "frequency_score", "monetary_score",
        ],
    },
    "clv": {
        "table": "agg_customers",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
            "order_frequency", "days_since_last_order",
        ],
    },
    "restock_quantity": {
        "table": "agg_inventory_health",
        "features": [
            "avg_quantity", "low_stock_count", "total_products",
            "stock_health_score",
        ],
    },
    "safety_stock": {
        "table": "agg_inventory_health",
        "features": [
            "avg_quantity", "low_stock_count", "out_of_stock_count",
        ],
    },
    "session_conversion": {
        "table": "agg_sessions",
        "features": [
            "avg_session_duration", "avg_pages_per_session",
            "conversion_rate", "bounce_rate",
        ],
    },
    "stockout_probability": {
        "table": "agg_inventory_health",
        "features": [
            "out_of_stock_count", "low_stock_count", "avg_quantity",
            "stock_health_score",
        ],
    },
    # ── General clustering ──────────────────────────────────────────────────
    "customer_segment": {
        "table": "agg_customers",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
            "order_frequency", "total_discount_amount",
        ],
    },
    "geo_cluster": {
        "table": "agg_geographic",
        "features": [
            "total_orders", "total_revenue", "total_customers",
            "avg_order_value",
        ],
    },
    "session_behavior": {
        "table": "agg_sessions",
        "features": [
            "avg_session_duration", "avg_pages_per_session",
            "conversion_rate", "bounce_rate",
        ],
    },
    "supplier_performance": {
        "table": "agg_suppliers",
        "features": [
            "total_products", "avg_restock_quantity",
            "total_inventory_value",
        ],
    },
    # ── Specific regression ─────────────────────────────────────────────────
    "demand_forecast": {
        "table": "agg_products",
        "features": [
            "total_sold", "total_revenue", "avg_unit_price",
            "total_orders", "avg_discount",
        ],
    },
    "revenue_forecast": {
        "table": "agg_products",
        "features": [
            "total_revenue", "total_sold", "avg_unit_price",
            "total_orders",
        ],
    },
    "price_optimization": {
        "table": "agg_products",
        "features": [
            "avg_unit_price", "total_sold", "total_revenue",
            "avg_discount",
        ],
    },
    "seasonal_trends": {
        "table": "agg_time_based",
        "features": [
            "total_orders", "total_revenue", "avg_order_value",
        ],
    },
    "delivery_time": {
        "table": "agg_customers",
        "features": [
            "total_orders", "avg_order_value",
        ],
    },
    "campaign_roi": {
        "table": "agg_campaigns",
        "features": [
            "total_spent", "total_revenue_generated", "roi",
            "conversion_rate", "total_impressions", "total_clicks",
        ],
    },
    # ── Specific classification ─────────────────────────────────────────────
    "product_bundling": {
        "table": "agg_product_affinity",
        "features": [
            "co_purchase_count", "support", "confidence", "lift",
        ],
    },
    "fulfillment_risk": {
        "table": "agg_inventory_health",
        "features": [
            "out_of_stock_count", "low_stock_count", "avg_quantity",
        ],
    },
    # ── Specific clustering ─────────────────────────────────────────────────
    "product_affinity": {
        "table": "agg_product_affinity",
        "features": [
            "co_purchase_count", "support", "confidence", "lift",
        ],
    },
    "product_lifecycle": {
        "table": "agg_products",
        "features": [
            "total_sold", "total_revenue", "avg_unit_price",
            "total_orders",
        ],
    },
}

# ---------------------------------------------------------------------------
# General vs specific model lists (used by retrain DAG to pick script)
# ---------------------------------------------------------------------------
GENERAL_MODELS = [
    "cart_abandonment", "customer_churn", "customer_segments",
    "payment_success", "review_sentiment", "stock_status",    # classification (6)
    "aov_v2", "clv", "restock_quantity", "safety_stock",
    "session_conversion", "stockout_probability",              # regression (6)
    "customer_segment", "geo_cluster", "session_behavior",
    "supplier_performance",                                    # clustering (4)
]

SPECIFIC_MODELS = [
    "demand_forecast", "revenue_forecast", "price_optimization",
    "seasonal_trends", "delivery_time", "campaign_roi",        # regression
    "product_bundling", "fulfillment_risk",                    # classification
    "product_affinity", "product_lifecycle",                   # clustering
]
