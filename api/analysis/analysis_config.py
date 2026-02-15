import os
import findspark
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession

findspark.init()
load_dotenv(find_dotenv())

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

DB_CONFIG = {
    "host": DB_HOST,
    "port":  DB_PORT,
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
            .config("spark.dynamicAllocation.maxExecutors", "8")
            .config("spark.dynamicAllocation.initialExecutors", "1")
        )
    else:
        # Disable dynamic allocation for local mode
        builder = builder.config("spark.dynamicAllocation.enabled", "false")
    
    # Apply common configurations
    spark = (
        builder
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark