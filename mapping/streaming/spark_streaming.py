"""Phase 5: Spark Streaming with Mapping Integration"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, MapType
import json

KAFKA_BOOTSTRAP = "10.5.0.7:9092"
KAFKA_TOPICS = "ecom.*"
CHECKPOINT_DIR = "/tmp/spark_checkpoints"
POSTGRES_URL = "jdbc:postgresql://10.5.0.5:5432/pulse"
POSTGRES_USER = "your_user"
POSTGRES_PASSWORD = "your_password"
MINIO_ENDPOINT = "10.5.0.4:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"


def create_spark_session() -> SparkSession:
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Phase5-KafkaStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def get_schema() -> StructType:
    """Message schema"""
    return StructType([
        StructField("source_type", StringType()),
        StructField("vendor", StringType()),
        StructField("table", StringType()),
        StructField("schema_version", StringType()),
        StructField("timestamp", StringType()),
        StructField("payload", MapType(StringType(), StringType()))
    ])


def read_kafka(spark: SparkSession) -> DataFrame:
    """Read from Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribePattern", KAFKA_TOPICS) \
        .option("startingOffsets", "latest") \
        .load()


def parse_json(df: DataFrame) -> DataFrame:
    """Parse JSON messages"""
    return df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), get_schema()).alias("msg")) \
        .select("msg.*")


def process_batch(batch_df: DataFrame, batch_id: int):
    """Process micro-batch with mapping"""
    if batch_df.isEmpty():
        return
    
    print(f"\nBatch {batch_id}: {batch_df.count()} messages")
    
    # Get unique tables in this batch
    tables = [row.table for row in batch_df.select("table").distinct().collect()]
    
    for table in tables:
        table_df = batch_df.filter(col("table") == table)
        
        # Convert payload map to columns
        from pyspark.sql.functions import map_keys, map_values
        
        # Get all keys from all payloads
        keys = table_df.select(explode(map_keys("payload"))).distinct().collect()
        all_keys = [row[0] for row in keys]
        
        # Extract each key as a column
        result_df = table_df.select(
            *[col("payload")[k].alias(k) for k in all_keys],
            col("timestamp").alias("ingestion_timestamp"),
            col("source_type"),
            col("vendor")
        )
        
        # Apply mapping (if map.py functions are available)
        # mapped_df = apply_mapping_logic(result_df, table)
        
        # Write to storage
        write_to_storage(result_df, table)
        
        print(f"  {table}: {result_df.count()} rows")


def write_to_storage(df: DataFrame, table: str):
    """Write to PostgreSQL and MinIO"""
    # Write to PostgreSQL
    try:
        df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=f"raw_{table}",
                mode="append",
                properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD}
            )
    except Exception as e:
        print(f"PostgreSQL error: {e}")
    
    # Write to MinIO as Parquet
    try:
        df.write \
            .mode("append") \
            .parquet(f"s3a://raw-data/{table}/")
    except Exception as e:
        print(f"MinIO error: {e}")


def run():
    """Main streaming pipeline"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Kafka Streaming Started")
    print(f"Topics: {KAFKA_TOPICS}")
    print(f"Checkpoint: {CHECKPOINT_DIR}\n")
    
    stream = read_kafka(spark)
    parsed = parse_json(stream)
    
    query = parsed \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        print("\nStopped")


if __name__ == "__main__":
    run()
