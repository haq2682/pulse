"""
Configuration module for Spark session and Database setup.
"""
import os
import findspark
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession

# Initialize findspark and load environment variables
findspark.init()
load_dotenv(find_dotenv())

# Database Configuration
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DATABASE_NAME", "pulse")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "url": f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
}

def create_spark_session(app_name="analysis"):
    """
    Create and configure a Spark session with S3/MinIO and Postgres support.
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # JAR dependencies for MinIO (Hadoop AWS) and Postgres
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3"
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2/")
        # MinIO / S3 Configuration
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark