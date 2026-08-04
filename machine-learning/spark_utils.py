import os
from typing import Any

# jars/deps/, not jars/ directly - same bug already found+fixed in
# mapping/map.py: the bare "/app/jars" path silently finds none of these,
# and Spark doesn't error on a missing --driver-class-path entry, so this
# goes unnoticed until Delta Lake's catalog class is actually needed at
# runtime (ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog).
# ML is currently disabled project-wide - this file isn't run, but is kept
# consistent with cleaning/transformation/analysis so it works correctly
# whenever ML is re-enabled.
_JARS_DIR = "/app/jars/deps"
_JARS_LIST = [
    f"{_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
]
_JARS_STR = ",".join(_JARS_LIST)
_JARS_CP = ":".join(_JARS_LIST)

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_JARS_CP} --jars {_JARS_STR} pyspark-shell"
    )

if os.environ.get("SPARK_HOME"):
    import findspark

    findspark.init()
else:
    try:
        import findspark

        findspark.init()
    except Exception:
        pass

from pyspark.sql import SparkSession


def create_ml_spark_session(
    app_name: str,
    extra_configs: dict[str, Any] | None = None,
    log_level: str = "ERROR",
) -> SparkSession:
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")

    builder = SparkSession.builder.appName(app_name).master(spark_master)

    builder = (
        builder
        .config("spark.jars", _JARS_STR)
        .config("spark.driver.extraClassPath", _JARS_CP)
        .config("spark.executor.extraClassPath", _JARS_CP)
    )

    if not is_local:
        builder = (
            builder
            # This driver runs as its own KubernetesPodOperator task pod (see
            # airflow/config/pipeline_config.py) - a bare K8s pod has no DNS
            # record for its own hostname, which is what Spark advertises to
            # executors by default. Same root cause (and fix) already
            # verified live in mapping/map.py: without this, every executor
            # on pulse-spark-worker fails with java.net.UnknownHostException.
            # POD_IP comes from a Downward API fieldRef added to this pod's
            # env - see pipeline_config.py's k8s_pipeline_env_vars(). ML is
            # currently disabled project-wide - not run/tested, kept
            # consistent for when it's re-enabled.
            .config("spark.driver.host", os.getenv("POD_IP", "localhost"))
            # Fixed ports (Spark picks random ephemeral ones by default) so
            # the matching NetworkPolicy rule can allow exactly these from
            # pulse-spark-worker instead of opening every port - same
            # reasoning as mapping/map.py's identical fix.
            .config("spark.driver.port", os.getenv("SPARK_DRIVER_PORT", "7078"))
            .config("spark.driver.blockManager.port", os.getenv("SPARK_DRIVER_BLOCKMANAGER_PORT", "7079"))
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "0")
            .config("spark.dynamicAllocation.initialExecutors", "1")
            # Migrate RDD/shuffle blocks off an executor before it's removed
            # by dynamic allocation or a spark-worker pod eviction, instead
            # of dropping them and forcing a recompute/reshuffle.
            .config("spark.decommission.enabled", "true")
            .config("spark.storage.decommission.enabled", "true")
            .config("spark.storage.decommission.rddBlocks.enabled", "true")
            .config("spark.storage.decommission.shuffleBlocks.enabled", "true")
            # spark.driver.memory only bounds the JVM heap - Metaspace/
            # CodeCache aren't covered and grow with every distinct query
            # plan Spark JIT-compiles. Same mechanism verified live in
            # mapping/map.py's driver; capped here too, centrally, so every
            # ML script inherits it rather than needing this repeated in
            # each of the 6 training/inference scripts.
            .config("spark.driver.extraJavaOptions",
                    "-XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m")
        )
    else:
        builder = builder.config("spark.dynamicAllocation.enabled", "false")

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Without an explicit region, the AWS SDK's default credential/region
        # provider chain makes a real network call out to actual AWS
        # infrastructure to auto-detect the bucket's region, even though
        # fs.s3a.endpoint already points it at MinIO - same gap already
        # verified live and fixed in mapping/map.py. Fake region (MinIO
        # doesn't have regions), just enough to satisfy the SDK and skip
        # that discovery call.
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
