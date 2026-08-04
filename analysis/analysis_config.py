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

from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession

load_dotenv(find_dotenv())

DB_HOST = os.getenv("POSTGRES_SERVER", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

DB_CONFIG = {
    "host": DB_HOST,
    "port":  DB_PORT,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "url": f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
}

def create_spark_session(app_name="Analysis"):
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")
    
    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
    )
    
    # Only enable dynamic allocation for cluster mode
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
            # port. Left unset, it's random/ephemeral per run, and the
            # driver connecting OUT to the executor to pull back a result
            # (not just the executor registering with the driver) has no
            # NetworkPolicy rule that can allow a port that changes every
            # run - verified live in cleaning/cleaning_config.py's identical
            # fix (approxQuantile hung for 30 minutes on exactly this).
            # Fixed so pulse-task-netpol's egress and
            # pulse-spark-worker-netpol's ingress can both pin this port.
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

    # Apply common configurations
    spark = (
        builder
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Explicit rather than relying on Spark's own 1g default - this
        # driver runs inside a KubernetesPodOperator task pod capped at
        # TASK_POD_RESOURCES()'s 3Gi limit (pipeline_config.py), with
        # nothing else sharing that cgroup. maxResultSize dropped from 2g to
        # stay under this heap - a maxResultSize larger than the heap it's
        # collected into can never actually be honored. Bumped from 1g to
        # 1536m - verified live: analysis.py's single main() runs dozens of
        # independent analytics computations sequentially in one session
        # (thousands of Spark jobs/stages total for a full run), and 1g
        # wasn't enough headroom on top of the accumulated per-execution
        # bookkeeping below once the pod's container limit was raised to
        # 2Gi's replacement - the "base" container was OOMKilled by the
        # kernel directly (not a graceful JVM OutOfMemoryError).
        .config("spark.driver.memory", os.getenv("ANALYSIS_DRIVER_MEMORY", "1536m"))
        .config("spark.driver.maxResultSize", os.getenv("ANALYSIS_DRIVER_MAX_RESULT_SIZE", "512m"))
        # spark.driver.memory only bounds the JVM heap - Metaspace/CodeCache
        # aren't covered and grow with every distinct query plan Spark
        # JIT-compiles. This driver runs every analysis query for a business
        # in one long-lived session - same mechanism verified live in
        # mapping/map.py's driver (shares pulse-api's container, not this
        # one, but the JVM behavior is identical). Bumped from 256m to 384m
        # for Metaspace specifically - analysis.py compiles far more
        # distinct query plans in one session (thousands of stages across
        # dozens of independent computations) than any other stage.
        .config("spark.driver.extraJavaOptions", "-XX:MaxMetaspaceSize=384m -XX:ReservedCodeCacheSize=128m")
        # Spark's own UI/SQL-execution-history listeners retain metadata
        # (job/stage/task info, full SQL execution plans) for every action
        # run in the session, up to their retention caps - with thousands
        # of jobs/stages/executions in a single analysis.py run, that's real
        # accumulated driver heap even under the defaults. This pod is
        # ephemeral and its Spark UI (port 4040) is never exposed by any
        # Service/Ingress, so nothing depends on it - disabled outright
        # rather than just tightening retention.
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        .config("spark.jars", _JARS_STR)
        .config("spark.driver.extraClassPath", _JARS_CP)
        .config("spark.executor.extraClassPath", _JARS_CP)
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
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark