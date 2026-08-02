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
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.jars", _JARS_STR)
        .config("spark.driver.extraClassPath", _JARS_CP)
        .config("spark.executor.extraClassPath", _JARS_CP)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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