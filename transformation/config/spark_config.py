import os
import findspark
from pyspark.sql import SparkSession

findspark.init()


def create_spark_session():
    spark_master = os.getenv("SPARK_SERVER", "local[*]")
    is_local = spark_master.startswith("local")
    
    builder = (
        SparkSession.builder.appName("Transformation")
        .master(spark_master)
    )
    
    # Only enable dynamic allocation for cluster mode
    if not is_local:
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "0")
            .config("spark.dynamicAllocation.maxExecutors", "8")
            .config("spark.dynamicAllocation.initialExecutors", "2")
        )
    else:
        # Disable dynamic allocation for local mode
        builder = builder.config("spark.dynamicAllocation.enabled", "false")
    
    # Apply common configurations
    builder = (
        builder
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "6g")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
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
    )
    
    return builder.getOrCreate()

        # .config("spark.dynamicAllocation.enabled", "true")
        # .config("spark.dynamicAllocation.minExecutors", "0")
        # .config("spark.dynamicAllocation.maxExecutors", "8")
        # .config("spark.dynamicAllocation.initialExecutors", "1")
        # .config("spark.executor.instances", "2")
        # .config("spark.executor.cores", "2")
        # .config("spark.executor.memory", "6g")
        # .config("spark.driver.memory", "2g")
