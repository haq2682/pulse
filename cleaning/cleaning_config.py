"""
Configuration module for Spark session and MinIO client setup.
"""

import os

from dotenv import load_dotenv, find_dotenv
from minio import Minio
import findspark
from pyspark.sql import SparkSession

# Initialize findspark and load environment variables
findspark.init()
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
    spark = (
        SparkSession.builder.appName("Cleaning")
        .master(os.getenv("SPARK_SERVER", "local[*]"))
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "8")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        # S3A/MinIO JAR dependencies for PySpark 3.5.0
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.2.6,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        # S3A/MinIO configuration
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
