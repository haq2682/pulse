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


def create_spark_session():
    """
    Create and configure a Spark session with S3/MinIO support.

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName("Cleaning")
        .master("spark://localhost:7077")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "2")
        # S3A/MinIO JAR dependencies for PySpark 3.5.0
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
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
    return spark


def create_minio_client():
    """
    Create and configure a MinIO client.

    Returns:
        Minio: Configured MinIO client
    """
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
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
