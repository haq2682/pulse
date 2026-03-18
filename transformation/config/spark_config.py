import os

# ---------------------------------------------------------------------------
# JAR paths — set PYSPARK_SUBMIT_ARGS BEFORE pyspark is imported so the JVM
# starts with the correct driver classpath; no Ivy/Maven download needed.
# ---------------------------------------------------------------------------
_JARS_DIR = "/app/jars"
_JARS_LIST = [
    f"{_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{_JARS_DIR}/delta-spark_2.12-3.0.0.jar",
    f"{_JARS_DIR}/delta-storage-3.0.0.jar",
]
_JARS_STR = ",".join(_JARS_LIST)
_JARS_CP  = ":".join(_JARS_LIST)

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_JARS_CP} --jars {_JARS_STR} pyspark-shell"
    )

if os.environ.get("SPARK_HOME"):
    import findspark
    findspark.init()

from pyspark.sql import SparkSession


def create_spark_session():
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")

    builder = (
        SparkSession.builder
        .appName("Transformation")
        .master(spark_master)
    )

    if not is_local:
        # -----------------------------------------------------------------------
        # Cluster layout: 2 workers × 2 cores × 4 GB RAM each
        #   → 1 executor per worker (2 cores/executor) = 2 executors max
        #   → 4 GB worker − 512 MB OS − 512 MB JVM overhead = ~3 GB safe ceiling
        #   → parallelism/shuffle partitions = 2 executors × 2 cores × 2 = 8–16
        # -----------------------------------------------------------------------
        dynamic_enabled = os.getenv("TRANSFORM_DYNAMIC_ALLOCATION_ENABLED", "true").lower() == "true"
        min_exec     = int(os.getenv("TRANSFORM_MIN_EXECUTORS",     "0"))
        initial_exec = int(os.getenv("TRANSFORM_INITIAL_EXECUTORS", "1"))
        max_exec     = int(os.getenv("TRANSFORM_MAX_EXECUTORS",     "2"))  # hard ceiling: 2 workers

        # Guard against contradictory env overrides.
        if max_exec < max(min_exec, initial_exec):
            max_exec = max(min_exec, initial_exec)

        builder = (
            builder
            .config("spark.dynamicAllocation.enabled",
                    str(dynamic_enabled).lower())
            .config("spark.dynamicAllocation.shuffleTracking.enabled",
                    os.getenv("TRANSFORM_SHUFFLE_TRACKING_ENABLED", "true"))
            .config("spark.dynamicAllocation.minExecutors",     str(min_exec))
            .config("spark.dynamicAllocation.initialExecutors", str(initial_exec))
            .config("spark.dynamicAllocation.maxExecutors",     str(max_exec))
            .config("spark.dynamicAllocation.executorIdleTimeout",
                    os.getenv("TRANSFORM_EXECUTOR_IDLE_TIMEOUT", "120s"))
            .config("spark.dynamicAllocation.cachedExecutorIdleTimeout",
                    os.getenv("TRANSFORM_CACHED_EXECUTOR_IDLE_TIMEOUT", "300s"))
            .config("spark.dynamicAllocation.schedulerBacklogTimeout",
                    os.getenv("TRANSFORM_BACKLOG_TIMEOUT", "1s"))
            .config("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout",
                    os.getenv("TRANSFORM_SUSTAINED_BACKLOG_TIMEOUT", "5s"))
            # 2 cores per executor → 1 executor fills one worker completely
            .config("spark.executor.cores",
                    os.getenv("TRANSFORM_EXECUTOR_CORES", "2"))
            # 2 g execution/storage heap + 512 m JVM overhead = 2.5 g per executor slot
            # leaves ~1.5 g on the worker for OS and Spark worker daemon
            .config("spark.executor.memory",
                    os.getenv("TRANSFORM_EXECUTOR_MEMORY", "2g"))
            .config("spark.executor.memoryOverhead",
                    os.getenv("TRANSFORM_EXECUTOR_MEMORY_OVERHEAD", "512m"))
            # Driver runs on the host machine (16 GB) — 3 g is safe
            .config("spark.driver.memory",
                    os.getenv("TRANSFORM_DRIVER_MEMORY", "3g"))
            .config("spark.driver.maxResultSize",
                    os.getenv("TRANSFORM_DRIVER_MAX_RESULT_SIZE", "1g"))
            # 2 executors × 2 cores = 4 slots; 4× multiplier → 16 partitions
            .config("spark.sql.shuffle.partitions",
                    os.getenv("TRANSFORM_SHUFFLE_PARTITIONS", "16"))
            .config("spark.default.parallelism",
                    os.getenv("TRANSFORM_DEFAULT_PARALLELISM", "16"))
            # Hard cap matches physical core count; prevents starving the host
            .config("spark.cores.max",
                    os.getenv("TRANSFORM_CORES_MAX", "4"))
        )

        if dynamic_enabled:
            # Keep executor.instances consistent with minExecutors to silence warnings
            builder = builder.config("spark.executor.instances", str(max(1, min_exec)))
        else:
            builder = builder.config(
                "spark.executor.instances",
                os.getenv("TRANSFORM_EXECUTOR_INSTANCES", "2")
            )
    else:
        # -----------------------------------------------------------------------
        # Local mode: driver IS the executor; entire 16 GB host is available
        # -----------------------------------------------------------------------
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.executor.instances",  "1")
            .config("spark.executor.cores",
                    os.getenv("TRANSFORM_LOCAL_EXECUTOR_CORES", "4"))
            .config("spark.executor.memory",
                    os.getenv("TRANSFORM_LOCAL_EXECUTOR_MEMORY", "4g"))
            .config("spark.driver.memory",
                    os.getenv("TRANSFORM_LOCAL_DRIVER_MEMORY", "3g"))
            .config("spark.driver.maxResultSize",
                    os.getenv("TRANSFORM_LOCAL_DRIVER_MAX_RESULT_SIZE", "1g"))
            # 4 cores × 2 = 8 partitions is the sweet spot for local
            .config("spark.sql.shuffle.partitions",
                    os.getenv("TRANSFORM_LOCAL_SHUFFLE_PARTITIONS", "8"))
        )

    # ---------------------------------------------------------------------------
    # Common settings (both modes)
    # ---------------------------------------------------------------------------
    builder = (
        builder
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",
                os.getenv("TRANSFORM_ADAPTIVE_SKEW_JOIN", "true"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Disable auto-broadcast to prevent driver OOM on unexpectedly large tables;
        # AQE will still apply runtime broadcast optimisations where safe.
        .config("spark.sql.autoBroadcastJoinThreshold",
                os.getenv("TRANSFORM_AUTO_BROADCAST_THRESHOLD", "-1"))
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold",
                os.getenv("TRANSFORM_ADAPTIVE_AUTO_BROADCAST_THRESHOLD", "-1"))
        .config("spark.network.timeout",
                os.getenv("TRANSFORM_NETWORK_TIMEOUT", "800s"))
        .config("spark.rpc.askTimeout",
                os.getenv("TRANSFORM_RPC_ASK_TIMEOUT", "600s"))
        .config("spark.executor.heartbeatInterval",
                os.getenv("TRANSFORM_HEARTBEAT_INTERVAL", "60s"))
        .config("spark.sql.broadcastTimeout",
                os.getenv("TRANSFORM_BROADCAST_TIMEOUT", "1200"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars",                    _JARS_STR)
        .config("spark.driver.extraClassPath",   _JARS_CP)
        .config("spark.executor.extraClassPath", _JARS_CP)
        .config("spark.hadoop.fs.s3a.endpoint",  os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return builder.getOrCreate()