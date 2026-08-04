"""
Configuration module for Spark session and MinIO client setup.
"""

import os

# ---------------------------------------------------------------------------
# JAR paths — set BEFORE pyspark is imported so the JVM gateway starts with
# the correct driver classpath (Delta Lake + S3A), no Maven download needed.
# ---------------------------------------------------------------------------
# jars/deps/, not jars/ directly - same bug already found+fixed in
# mapping/map.py: the bare "/app/jars" path silently finds none of these,
# and Spark doesn't error on a missing --driver-class-path entry, so this
# goes unnoticed until Delta Lake's catalog class is actually needed at
# runtime (ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog).
_JARS_DIR = "/app/jars/deps"
_CLEAN_JARS = [
    f"{_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{_JARS_DIR}/delta-spark_2.12-3.0.0.jar",
    f"{_JARS_DIR}/delta-storage-3.0.0.jar",
]
_CLEAN_JARS_STR = ",".join(_CLEAN_JARS)
_CLEAN_CP = ":".join(_CLEAN_JARS)

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {_CLEAN_CP} --jars {_CLEAN_JARS_STR} pyspark-shell"
    )

# findspark is only needed when Spark is installed as a standalone binary.
if os.environ.get("SPARK_HOME"):
    import findspark
    findspark.init()

from dotenv import load_dotenv, find_dotenv
from minio import Minio
from pyspark.sql import SparkSession

load_dotenv(find_dotenv())


def parse_minio_endpoint(endpoint_url):
    """
    Parse MinIO endpoint URL and strip protocol prefix if present.
    
    The MinIO Python client expects endpoint in format 'hostname:port' without
    protocol prefix. This function handles both formats:
    - With protocol: 'http://localhost:9000' -> 'localhost:9000'
    - Without protocol: 'localhost:9000' -> 'localhost:9000'
    
    Args:
        endpoint_url: MinIO endpoint URL (e.g., 'localhost:9000' or 'http://localhost:9000')
        
    Returns:
        str: Endpoint in 'hostname:port' format
        
    Raises:
        ValueError: If endpoint is empty or invalid after parsing
    """
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT cannot be empty")
    
    # Strip protocol prefix if present
    if "://" in endpoint_url:
        endpoint_url = endpoint_url.split("://", 1)[1]
    
    # Validate that we have a non-empty endpoint after parsing
    endpoint_url = endpoint_url.strip()
    if not endpoint_url:
        raise ValueError("MINIO_ENDPOINT is invalid after removing protocol")
    
    return endpoint_url


def create_spark_session():
    """
    Create and configure a Spark session with S3/MinIO support.

    Returns:
        SparkSession: Configured Spark session
    """
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")

    builder = (
        SparkSession.builder.appName("Cleaning")
        .master(spark_master)
    )

    if not is_local:
        builder = (
            builder
            # This driver runs as its own KubernetesPodOperator task pod (see
            # airflow/config/pipeline_config.py), not a subprocess of another
            # container - but a bare K8s pod still has no DNS record for its
            # own hostname, which is what Spark advertises to executors by
            # default. Same root cause (and fix) already verified live in
            # mapping/map.py: without this, every executor on
            # pulse-spark-worker fails with java.net.UnknownHostException.
            # POD_IP comes from a Downward API fieldRef added to this pod's
            # env - see pipeline_config.py's k8s_pipeline_env_vars().
            .config("spark.driver.host", os.getenv("POD_IP", "localhost"))
            # Fixed ports (Spark picks random ephemeral ones by default) so
            # the matching NetworkPolicy rule can allow exactly these from
            # pulse-spark-worker instead of opening every port - same
            # reasoning as mapping/map.py's identical fix.
            .config("spark.driver.port", os.getenv("SPARK_DRIVER_PORT", "7078"))
            .config("spark.driver.blockManager.port", os.getenv("SPARK_DRIVER_BLOCKMANAGER_PORT", "7079"))
            # spark.blockManager.port sets the default block manager port for
            # BOTH driver and executors - the driver's own is overridden
            # above, so this is effectively the EXECUTOR's block manager
            # port. Left unset, it's random/ephemeral per run. Verified
            # live: approxQuantile (and anything else needing the driver to
            # pull a result back from the executor's block manager, not just
            # the executor registering with the driver) connects OUT from
            # this pod TO the executor on this port - "Connecting to
            # <executor-ip>:<random-port> timed out (120000 ms)" repeated
            # for 30 minutes until the job aborted, because no NetworkPolicy
            # rule can allow a port that changes every run. Fixed so
            # pulse-task-netpol's egress and pulse-spark-worker-netpol's
            # ingress can both pin exactly this port.
            .config("spark.blockManager.port", os.getenv("SPARK_EXECUTOR_BLOCKMANAGER_PORT", "7080"))
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
        )
    else:
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
        )

    spark = (
        builder
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Explicit rather than relying on Spark's own 1g default - this
        # driver runs inside a KubernetesPodOperator task pod capped at
        # TASK_POD_RESOURCES()'s 2Gi limit (pipeline_config.py), with
        # nothing else sharing that cgroup.
        .config("spark.driver.memory", os.getenv("CLEAN_DRIVER_MEMORY", "1g"))
        # spark.driver.memory only bounds the JVM heap - Metaspace/CodeCache
        # aren't covered and grow with every distinct query plan Spark
        # JIT-compiles. This driver processes every table for a business in
        # one long-lived session, each triggering fresh codegen - same
        # mechanism verified live in mapping/map.py's driver (shares
        # pulse-api's container, not this one, but the JVM behavior is
        # identical). Capped here too so it can't silently eat the rest of
        # this pod's 2Gi budget over a run with many tables.
        .config("spark.driver.extraJavaOptions", "-XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m")
        # Use pre-downloaded local JARs — no Maven/internet access needed.
        .config("spark.jars", _CLEAN_JARS_STR)
        .config("spark.driver.extraClassPath", _CLEAN_CP)
        .config("spark.executor.extraClassPath", _CLEAN_CP)
        # S3A/MinIO configuration
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
        .config("inferSchema", "true")
        .config("mergeSchema", "true")
        # Delta Lake extensions
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Write timestamps as MICROS to stay compatible with all readers.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Tolerate TIMESTAMP(NANOS,true) in Parquet files already on disk
        # (written before this fix).  Reads nanos as a Long rather than
        # raising "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))".
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_minio_client():
    """
    Create and configure a MinIO client.

    Returns:
        Minio: Configured MinIO client
    """
    # Parse MINIO_ENDPOINT to strip protocol prefix if present
    minio_endpoint = parse_minio_endpoint(os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    
    minio_client = Minio(
        minio_endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    return minio_client


def get_bucket_name():
    """
    Get the default bucket name.

    Returns:
        str: Bucket name
    """
    return "pulse-bucket-1"
