"""Spark Kafka Consumer - Clean Functional Implementation"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, map_keys, explode
from pyspark.sql.types import StructType, StructField, StringType, MapType

KAFKA_BOOTSTRAP = "10.5.0.7:9092"
KAFKA_TOPICS = "ecom.*"
CHECKPOINT_DIR = "/tmp/spark_checkpoint"
POSTGRES_URL = "jdbc:postgresql://10.5.0.5:5432/pulse"
POSTGRES_USER = "your_user"
POSTGRES_PASS = "your_password"
MINIO_ENDPOINT = "10.5.0.4:9000"
MINIO_KEY = "minioadmin"
MINIO_SECRET = "minioadmin"


def spark_session() -> SparkSession:
    """Create Spark session"""
    return SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def message_schema() -> StructType:
    """Canonical message schema"""
    return StructType([
        StructField("source_type", StringType()),
        StructField("vendor", StringType()),
        StructField("table", StringType()),
        StructField("schema_version", StringType()),
        StructField("timestamp", StringType()),
        StructField("payload", MapType(StringType(), StringType()))
    ])


def read_stream(spark: SparkSession) -> DataFrame:
    """Read Kafka stream"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribePattern", KAFKA_TOPICS) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), message_schema()).alias("msg")) \
        .select("msg.*")


def extract_payload(df: DataFrame) -> DataFrame:
    """Convert payload map to columns"""
    keys = df.select(explode(map_keys("payload"))).distinct().collect()
    cols = [row[0] for row in keys]
    
    return df.select(
        *[col("payload")[k].alias(k) for k in cols],
        col("table"),
        col("timestamp").alias("ingestion_ts"),
        col("source_type"),
        col("vendor")
    )


def write_postgres(df: DataFrame, table: str):
    """Write to PostgreSQL"""
    df.write.jdbc(
        url=POSTGRES_URL,
        table=f"raw_{table}",
        mode="append",
        properties={"user": POSTGRES_USER, "password": POSTGRES_PASS, "driver": "org.postgresql.Driver"}
    )


def write_minio(df: DataFrame, table: str):
    """Write to MinIO"""
    df.write.mode("append").parquet(f"s3a://raw-data/{table}/")


def process_batch(batch_df: DataFrame, batch_id: int):
    """Process micro-batch"""
    if batch_df.isEmpty():
        return
    
    print(f"Batch {batch_id}: {batch_df.count()} rows")
    
    tables = [r.table for r in batch_df.select("table").distinct().collect()]
    
    for table in tables:
        table_df = batch_df.filter(col("table") == table)
        extracted = extract_payload(table_df)
        
        try:
            write_postgres(extracted.drop("table"), table)
            write_minio(extracted.drop("table"), table)
            print(f"  {table}: {extracted.count()} rows written")
        except Exception as e:
            print(f"  {table} error: {e}")


def run():
    """Main pipeline"""
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topics: {KAFKA_TOPICS}\n")
    
    stream = read_stream(spark)
    
    query = stream \
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
