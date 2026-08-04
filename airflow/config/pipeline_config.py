"""
Central configuration for all Airflow pipeline DAGs.

All environment-dependent values default to what is defined in docker-compose.yml.
Override at runtime via Airflow Variables (Admin → Variables in the UI) or env vars.

Task execution: Kubernetes, not Docker
---------------------------------------
Pipeline steps run as their own Kubernetes Pods (via KubernetesPodOperator,
or via run_k8s_task_pod() below for call sites that need to run several
steps in sequence from inside one Python task), using the pulse-python
image with cleaning/mapping/transformation/analysis/machine-learning
already baked in at build time - see .docker/python/Dockerfile. There is
no host bind mount and no docker.sock dependency: this replaced an earlier
DockerOperator/exec_run()-based design that needed both (see
docs/CLOUD_DEPLOYMENT_GUIDE.md for the history).
"""

import os

# ---------------------------------------------------------------------------
# Task pod image
# ---------------------------------------------------------------------------
PYTHON_IMAGE = os.getenv(
    "PIPELINE_PYTHON_IMAGE", "haq2682/pulse-python:latest"
)

# ---------------------------------------------------------------------------
# Where task pods get created
# ---------------------------------------------------------------------------
# The namespace Airflow itself is running in, injected via the Kubernetes
# Downward API (POD_NAMESPACE env var on pulse-airflow-webserver/scheduler -
# see deployment.yaml). Task pods are created in the same namespace so they
# can reach every other pulse-* service the same way Airflow itself does.
POD_NAMESPACE = os.getenv("POD_NAMESPACE", "default")

# The ServiceAccount task pods run as - see .k8s/bases/rbac.yaml. Pods
# themselves don't need any Kubernetes API permissions (they just run a
# script); this is a plain, unprivileged default identity for them.
TASK_SERVICE_ACCOUNT = "pulse-airflow"

# Labels applied to spawned pods - also what the matching NetworkPolicy
# entries in .k8s/bases/networkpolicy.yaml select on.
TASK_POD_LABELS       = {"app": "pulse-task"}
DB_STREAM_POD_LABELS  = {"app": "pulse-db-stream"}
API_STREAM_POD_LABELS = {"app": "pulse-api-stream"}

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
# Scheduled batch processing (streaming modes)
# ---------------------------------------------------------------------------
# Downstream processing (clean → transform → analyze → ML inference) runs
# as a scheduled Airflow batch job (scheduled_batch_dag, every 10 minutes)
# for db/api streaming tenants.  It is NEVER triggered inline from the
# Spark micro-batch — the streaming layer handles ingestion + mapping only.
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


# ---------------------------------------------------------------------------
# Kubernetes task pod helpers
# ---------------------------------------------------------------------------

def k8s_pipeline_env() -> dict:
    """
    Return the environment dict forwarded into every task pod (KubernetesPodOperator's
    env_vars accepts a plain dict, and run_k8s_task_pod() below uses this too).
    Values are read from the Airflow scheduler's own environment at task-run
    time, so they stay in sync with whatever is configured on
    pulse-airflow-webserver/scheduler in deployment.yaml.
    """
    import os as _os
    return {
        "POSTGRES_USER":     POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "POSTGRES_DB":       POSTGRES_DB,
        "POSTGRES_SERVER":   POSTGRES_SERVER,
        "POSTGRES_DATABASE_NAME": POSTGRES_DB,
        "MINIO_ENDPOINT":    MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY":  MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY":  MINIO_SECRET_KEY,
        "KAFKA_BOOTSTRAP":   KAFKA_BOOTSTRAP,
        "REDIS_HOST":        REDIS_HOST,
        "REDIS_PORT":        str(REDIS_PORT),
        "GEMINI_API_KEY":    _os.getenv("GEMINI_API_KEY", ""),
        "GEMINI_MODEL":      _os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite"),
        "SPARK_MASTER_URL":  _os.getenv("SPARK_MASTER_URL", "spark://pulse-spark-master:7077"),
        "SPARK_SERVER":      _os.getenv("SPARK_MASTER_URL", "spark://pulse-spark-master:7077"),
    }


def k8s_pipeline_env_vars() -> list:
    """
    Same values as k8s_pipeline_env(), as a list[V1EnvVar] instead of a plain
    dict, plus POD_IP sourced from the Kubernetes Downward API.

    For run_k8s_task_pod() ONLY - that function talks to the Kubernetes API
    directly via the raw `kubernetes` client, with no Airflow templating
    involved, so building V1EnvVar objects with plain string values here is
    safe. Do NOT use this for a KubernetesPodOperator's env_vars= - that
    field is templated (Jinja) at task-execution time, and even a value
    with no `{{ }}` syntax at all still gets run through
    render_template_as_native_obj's NativeEnvironment, which silently
    coerces any numeric-looking string (e.g. REDIS_PORT="6379") back into
    a Python int - verified live: this produced a 400 Bad Request from the
    Kubernetes API ("cannot unmarshal number into ... EnvVar...value of
    type string") the first time clean/transform/analyze's task pods
    actually reached pod-creation. See k8s_pipeline_env_templated() and
    k8s_pipeline_pod_ip_runtime_env() for the KubernetesPodOperator-safe
    equivalents.

    Every cleaning/transformation/analysis/machine-learning Spark session
    (cleaning_config.py, transformation/config/spark_config.py,
    analysis_config.py, machine-learning/spark_utils.py) sets
    spark.driver.host from os.getenv("POD_IP") - a bare pod has no DNS
    record for its own hostname, which is what Spark advertises to
    executors by default, and every executor on pulse-spark-worker fails
    with java.net.UnknownHostException without this (same root cause
    already verified live and fixed in mapping/map.py, where POD_IP comes
    from deployment.yaml's Downward API fieldRef on the long-lived
    pulse-api container).
    """
    from kubernetes import client

    env_vars = [
        client.V1EnvVar(name=k, value=str(v))
        for k, v in k8s_pipeline_env().items()
    ]
    env_vars.append(
        client.V1EnvVar(
            name="POD_IP",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
            ),
        )
    )
    return env_vars


def k8s_pipeline_env_templated() -> dict:
    """
    Same values as k8s_pipeline_env(), for a KubernetesPodOperator's env_vars=
    specifically (which IS templated - see the warning in k8s_pipeline_env_vars()
    above). Every value is wrapped in repr() so Jinja's NativeEnvironment
    (render_template_as_native_obj=True on these DAGs) round-trips it back to
    the correct plain string instead of silently coercing a numeric-looking
    one (e.g. REDIS_PORT="6379") into a Python int - verified live:
    NativeEnvironment.from_string("6379").render({}) == 6379 (int), even
    with zero {{ }} template syntax anywhere in the value, which the
    Kubernetes API then rejects with a 400 (EnvVar.value must be a string).
    repr('6379') == "'6379'", and rendering that literal Python string
    expression through ast.literal_eval (what NativeEnvironment does under
    the hood) correctly yields back the plain str '6379' - verified live.
    """
    return {k: repr(str(v)) for k, v in k8s_pipeline_env().items()}


def k8s_pipeline_pod_ip_runtime_env() -> list:
    """
    POD_IP as a pod_runtime_info_envs entry (list[V1EnvVar]) for a
    KubernetesPodOperator. Deliberately NOT part of k8s_pipeline_env_templated()'s
    dict - a dict-based env_vars entry can only hold a literal value
    (see convert_env_vars()'s dict branch), never a valueFrom/fieldRef.
    pod_runtime_info_envs is merged into the operator's own env_vars list at
    construction time but - unlike the dict entries - has no plain .value
    string for Jinja's per-field rendering to touch, so it survives
    render_template_as_native_obj untouched (verified live).
    """
    from kubernetes import client

    return [
        client.V1EnvVar(
            name="POD_IP",
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
            ),
        )
    ]


def _resource_requirements(cpu_request, mem_request, cpu_limit, mem_limit):
    """Small wrapper so call sites below read as plain numbers/strings."""
    from kubernetes.client import V1ResourceRequirements
    return V1ResourceRequirements(
        requests={"cpu": cpu_request, "memory": mem_request},
        limits={"cpu": cpu_limit, "memory": mem_limit},
    )


# Bounded, short-lived steps (clean/transform/analyze/connector-deploy/etc).
# Kept modest given this project's minikube target - see hpa.yaml's sizing
# comment about this being a 4-core/16GB machine. run_k8s_task_pod() applies
# this to every step uniformly (no per-step override) - the limit is sized
# for the heaviest of them (analysis.py: one long-lived driver session
# running dozens of independent analytics computations, thousands of Spark
# jobs/stages total). Verified live at 2Gi: the "base" container itself got
# OOMKilled mid-run (not a graceful JVM OutOfMemoryError - the kernel killed
# it directly) once JVM heap+Metaspace+CodeCache (capped in
# analysis_config.py) plus the Python driver process's own accumulated
# state from thousands of sequential actions exceeded the limit. Cleaning/
# transformation don't need this much - the extra headroom is simply unused
# for them, not harmful.
def TASK_POD_RESOURCES():
    return _resource_requirements("250m", "512Mi", "1000m", "3Gi")


# The 24/7 streaming pods (db_streaming / api_streaming) run a Spark
# Structured Streaming consumer continuously, not a single short script.
def STREAM_POD_RESOURCES():
    return _resource_requirements("500m", "1Gi", "1500m", "3Gi")


def run_k8s_task_pod(name: str, command: list, timeout_seconds: int) -> tuple:
    """
    Run `command` to completion in a fresh, single-container Kubernetes Pod
    (using PYTHON_IMAGE), and return (exit_code, logs).

    This is the Kubernetes-native replacement for what
    `docker_sdk_container.exec_run()` used to do: run one command, block
    until it finishes (or times out), get its exit code and output, then
    clean up - used by call sites that run several short-lived steps in a
    row from inside a single Python task (scheduled_batch_dag.py,
    db_streaming_dag.py's connector-deploy step), where a full
    KubernetesPodOperator-per-step isn't a natural fit. For anything that's
    naturally one Airflow task = one container run, use
    KubernetesPodOperator directly instead (see batch_downstream_dag.py).

    Always deletes the pod when done, whether it succeeded, failed, or
    timed out - never leaves one behind.
    """
    import time
    from kubernetes import client, config as k8s_config

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()

    core_v1 = client.CoreV1Api()

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(generate_name=f"{name}-", labels=TASK_POD_LABELS),
        spec=client.V1PodSpec(
            restart_policy="Never",
            service_account_name=TASK_SERVICE_ACCOUNT,
            containers=[
                client.V1Container(
                    name="task",
                    image=PYTHON_IMAGE,
                    # PYTHON_IMAGE is tagged :latest, which Kubernetes
                    # defaults to imagePullPolicy=Always for - silently
                    # re-pulling from Docker Hub over any locally-built/
                    # freshly-pushed image otherwise. Same fix already
                    # applied to the long-lived Deployments.
                    image_pull_policy="IfNotPresent",
                    command=command,
                    env=k8s_pipeline_env_vars(),
                    resources=TASK_POD_RESOURCES(),
                )
            ],
        ),
    )

    created = core_v1.create_namespaced_pod(namespace=POD_NAMESPACE, body=pod)
    pod_name = created.metadata.name

    try:
        deadline = time.monotonic() + timeout_seconds
        current = None
        while time.monotonic() < deadline:
            current = core_v1.read_namespaced_pod(name=pod_name, namespace=POD_NAMESPACE)
            if current.status.phase in ("Succeeded", "Failed"):
                break
            time.sleep(3)
        else:
            raise TimeoutError(f"Pod {pod_name} did not finish within {timeout_seconds}s")

        logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace=POD_NAMESPACE)
        container_status = current.status.container_statuses[0] if current.status.container_statuses else None
        terminated = container_status.state.terminated if container_status and container_status.state else None
        exit_code = terminated.exit_code if terminated else (0 if current.status.phase == "Succeeded" else 1)
        return exit_code, logs

    finally:
        try:
            core_v1.delete_namespaced_pod(name=pod_name, namespace=POD_NAMESPACE, grace_period_seconds=0)
        except Exception:
            pass
