import os
from typing import Any

_JARS_DIR = "/app/jars"
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
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "0")
            .config("spark.dynamicAllocation.initialExecutors", "1")
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
