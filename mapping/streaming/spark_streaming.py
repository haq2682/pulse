"""Spark Structured Streaming - Kafka Consumer with Existing Map Integration"""

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, MapType
import psycopg2
from minio import Minio
from dotenv import load_dotenv, find_dotenv

from map import process_all_dataframes, save_dataframes_to_minio
import List as mapping_list

load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
CHECKPOINT_LOCATION = "s3a://pulse-checkpoints/normalize-stream"
OUTPUT_BUCKET = "pulse-bucket-stream"


def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka and S3 support"""
    return (
        SparkSession.builder
        .appName("StreamingNormalization")
        .master("spark://10.5.0.3:7077")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "0")
        .config("spark.dynamicAllocation.maxExecutors", "8")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def get_canonical_schema() -> StructType:
    """Define canonical message schema"""
    return StructType([
        StructField("source_type", StringType()),
        StructField("vendor", StringType()),
        StructField("table", StringType()),
        StructField("schema_version", StringType()),
        StructField("payload", MapType(StringType(), StringType()))
    ])


def load_postgres_schema():
    """Load canonical schema from PostgreSQL"""
    conn = psycopg2.connect(
        host="10.5.0.5",
        database=os.getenv("POSTGRES_DATABASE_NAME"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        """
    )
    columns_info = cur.fetchall()
    cur.close()
    conn.close()
    return columns_info


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read from Kafka topics"""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", "ecom\\..*")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), get_canonical_schema()).alias("data"))
        .select("data.*")
    )


def extract_table_dataframes(batch_df: DataFrame) -> dict:
    """Extract DataFrames per table from batch"""
    if batch_df.rdd.isEmpty():
        return {}
    
    table_names = [row["table"] for row in batch_df.select("table").distinct().collect()]
    all_dataframes = {}
    
    for table_name in table_names:
        table_df = batch_df.filter(col("table") == table_name)
        
        # Get payload keys from first row
        sample = table_df.select("payload").limit(1).collect()
        if not sample:
            continue
        
        payload_keys = list(sample[0]["payload"].keys())
        
        # Extract payload columns
        select_exprs = [col("payload").getItem(k).alias(k) for k in payload_keys]
        payload_df = table_df.select(*select_exprs)
        
        # Use naming convention expected by map.py
        df_name = f"{table_name}_df"
        all_dataframes[df_name] = payload_df
    
    return all_dataframes


def process_microbatch(batch_df: DataFrame, batch_id: int, columns_info, minio_client):
    """Process each micro-batch"""
    print(f"\n{'='*60}")
    print(f"Processing batch {batch_id}")
    print(f"{'='*60}")
    
    all_dataframes = extract_table_dataframes(batch_df)
    
    if not all_dataframes:
        print("No data in batch")
        return
    
    print(f"Tables in batch: {list(all_dataframes.keys())}")
    
    # Call existing mapping function with mode="stream"
    results = process_all_dataframes(
        all_dataframes,
        columns_info,
        mapping_list,
        mode="stream"
    )
    
    # Save results using existing function
    save_dataframes_to_minio(results, minio_client, OUTPUT_BUCKET)
    
    print(f"✅ Batch {batch_id} completed: {len(results)} tables processed")


def run_streaming():
    """Main streaming pipeline"""
    print("Starting Spark Streaming Pipeline")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")
    print(f"Output bucket: {OUTPUT_BUCKET}\n")
    
    # Initialize
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    columns_info = load_postgres_schema()
    print(f"Loaded {len(columns_info)} columns from canonical schema")
    
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    
    # Create bucket if not exists
    if not minio_client.bucket_exists(OUTPUT_BUCKET):
        minio_client.make_bucket(OUTPUT_BUCKET)
        print(f"Created bucket: {OUTPUT_BUCKET}")
    
    # Read stream
    json_stream = read_kafka_stream(spark)
    
    # Process with foreachBatch
    query = (
        json_stream.writeStream
        .foreachBatch(lambda df, id: process_microbatch(df, id, columns_info, minio_client))
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    
    print("Streaming query started. Press Ctrl+C to stop.\n")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("\nStreaming stopped")


if __name__ == "__main__":
    run_streaming()
