import os

# ---------------------------------------------------------------------------
# JAR paths — set PYSPARK_SUBMIT_ARGS BEFORE pyspark is imported so the JVM
# starts with the correct driver classpath; no Ivy/Maven download needed.
# ---------------------------------------------------------------------------
# jars/deps/, not jars/ directly - same bug already found+fixed in
# mapping/map.py: the bare "/app/jars" path silently finds none of these,
# and Spark doesn't error on a missing --driver-class-path entry, so this
# goes unnoticed until Delta Lake's catalog class is actually needed at
# runtime (ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog).
_JARS_DIR = "/app/jars/deps"
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
        # Cluster layout: up to 2 workers (HPA-scaled, pulse-spark-worker-hpa's
        # maxReplicas) × 1 core × 2 GB RAM each (SPARK_WORKER_CORES/
        # SPARK_WORKER_MEMORY in deployment.yaml)
        #   → 1 executor per worker (1 core/executor) = 2 executors max
        #   → TRANSFORM_EXECUTOR_MEMORY (1g) + memoryOverhead (384m) = 1.375g
        #     per executor, leaving ~640m of the worker pod's 2Gi container
        #     limit for the Spark worker daemon itself plus OS/JVM headroom
        #   → parallelism/shuffle partitions = 2 executors × 1 core × 8 = 16
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
            # This driver runs as its own KubernetesPodOperator task pod (see
            # airflow/config/pipeline_config.py) - a bare K8s pod has no DNS
            # record for its own hostname, which is what Spark advertises to
            # executors by default. Same root cause (and fix) already
            # verified live in mapping/map.py: without this, every executor
            # on pulse-spark-worker fails with java.net.UnknownHostException.
            # POD_IP comes from a Downward API fieldRef added to this pod's
            # env - see pipeline_config.py's k8s_pipeline_env_vars().
            .config("spark.driver.host", os.getenv("POD_IP", "localhost"))
            # Fixed ports (Spark picks random ephemeral ones by default) so
            # the matching NetworkPolicy rule can allow exactly these from
            # pulse-spark-worker instead of opening every port - same
            # reasoning as mapping/map.py's identical fix.
            .config("spark.driver.port", os.getenv("SPARK_DRIVER_PORT", "7078"))
            .config("spark.driver.blockManager.port", os.getenv("SPARK_DRIVER_BLOCKMANAGER_PORT", "7079"))
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
            # Graceful executor/worker decommissioning: when dynamic allocation
            # (or a spark-worker pod eviction) removes an executor, migrate its
            # RDD/shuffle blocks to a surviving executor first instead of just
            # dropping them - avoids forcing a costly recompute/reshuffle.
            # Requires spark.shuffle.service.enabled=true on the workers
            # (set via SPARK_WORKER_OPTS in deployment.yaml), which is what
            # lets shuffle data outlive the executor that produced it.
            .config("spark.decommission.enabled", "true")
            .config("spark.storage.decommission.enabled", "true")
            .config("spark.storage.decommission.rddBlocks.enabled", "true")
            .config("spark.storage.decommission.shuffleBlocks.enabled", "true")
            # 1 core per executor → 1 executor fills one worker completely
            # (must match SPARK_WORKER_CORES in deployment.yaml: standalone
            # mode can't split one executor across multiple workers, so a
            # value here higher than what a single worker advertises means
            # the master can never place an executor at all)
            .config("spark.executor.cores",
                    os.getenv("TRANSFORM_EXECUTOR_CORES", "1"))
            # 1g execution/storage heap + 384m overhead (Spark's own default
            # floor: max(384m, 10% of executor.memory)) = 1.375g per executor
            # slot - fits inside the worker pod's 2Gi container limit
            # alongside the worker daemon's own footprint (see layout note
            # at the top of this block). MUST stay under ~1.75g here: this
            # executor JVM and the Worker daemon share the same container's
            # memory cgroup, so anything left unbudgeted risks an OOMKill.
            .config("spark.executor.memory",
                    os.getenv("TRANSFORM_EXECUTOR_MEMORY", "1g"))
            .config("spark.executor.memoryOverhead",
                    os.getenv("TRANSFORM_EXECUTOR_MEMORY_OVERHEAD", "384m"))
            # Driver runs on the host machine (16 GB) — 3 g is safe
            .config("spark.driver.memory",
                    os.getenv("TRANSFORM_DRIVER_MEMORY", "3g"))
            .config("spark.driver.maxResultSize",
                    os.getenv("TRANSFORM_DRIVER_MAX_RESULT_SIZE", "1g"))
            # 2 executors × 1 core = 2 slots; 8× multiplier → 16 partitions
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
        # Without an explicit region, the AWS SDK's default credential/region
        # provider chain makes a real network call out to actual AWS
        # infrastructure to auto-detect the bucket's region, even though
        # fs.s3a.endpoint already points it at MinIO - same gap already
        # verified live and fixed in mapping/map.py. Fake region (MinIO
        # doesn't have regions), just enough to satisfy the SDK and skip
        # that discovery call.
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return builder.getOrCreate()