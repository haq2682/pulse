# Real-Time Streaming Architecture: Kafka, Spark Streaming, and Zookeeper

## Table of Contents
1. [Overview](#overview)
2. [Architecture Components](#architecture-components)
3. [Data Flow](#data-flow)
4. [Apache Zookeeper](#apache-zookeeper)
5. [Apache Kafka](#apache-kafka)
6. [Spark Streaming](#spark-streaming)
7. [Data Storage](#data-storage)
8. [Complete Data Journey](#complete-data-journey)
9. [Component Interactions](#component-interactions)

---

## Overview

The Pulse e-commerce data analytics engine uses a **real-time streaming architecture** to ingest, process, and store e-commerce data from multiple sources. The architecture implements a modern data pipeline using:

- **Apache Kafka** for distributed messaging and event streaming
- **Apache Zookeeper** for distributed coordination
- **Apache Spark Streaming** for real-time data processing
- **MinIO** as the data lake for processed data
- **PostgreSQL** for canonical schema storage and metadata

---

## Architecture Components

### Infrastructure (from docker-compose.yml)

```
┌─────────────────────────────────────────────────────────────────┐
│                      Spark Network (10.5.0.0/16)                │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Zookeeper   │  │    Kafka     │  │ Spark Master │         │
│  │  10.5.0.6    │◄─┤  10.5.0.7    │  │  10.5.0.3    │         │
│  │  Port: 2181  │  │  Port: 9092  │  │  Port: 7077  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                            │                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ PostgreSQL   │  │    MinIO     │  │   Python     │         │
│  │  10.5.0.5    │  │  10.5.0.4    │  │  10.5.0.2    │         │
│  │  Port: 5432  │  │  Port: 9000  │  │  Port: 5000  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### High-Level Pipeline

```
┌──────────────┐
│ Data Sources │
└──────┬───────┘
       │
       ├─► Database URI (PostgreSQL/MySQL/MongoDB/MSSQL)
       │   └─► db_ingest_service.py
       │
       └─► External API
           └─► api_ingest_service.py
                    │
                    ▼
           ┌────────────────┐
           │  Kafka Topics  │
           │  (ecom.*)      │
           └────────┬───────┘
                    │
                    ▼
          ┌──────────────────┐
          │ Spark Streaming  │
          │ (spark_streaming │
          │      .py)        │
          └────────┬─────────┘
                   │
                   ├─► Schema Mapping
                   ├─► Data Normalization
                   ├─► ML-based Column Mapping
                   │
                   ▼
          ┌──────────────────┐
          │  MinIO Data Lake │
          │ (pulse-bucket-   │
          │     stream)      │
          └──────────────────┘
```

---

## Apache Zookeeper

### Role in the Architecture

**Apache Zookeeper** (running at `10.5.0.6:2181`) serves as the **distributed coordination service** for Kafka.

### Configuration (docker-compose.yml)
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.7.0
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181
    ZOOKEEPER_TICK_TIME: 2000
  ports:
    - "2181:2181"
```

### Key Responsibilities

1. **Kafka Broker Management**
   - Maintains live list of Kafka brokers
   - Detects when brokers join or leave the cluster
   - Stores broker metadata and configuration

2. **Topic Configuration**
   - Stores topic metadata (partitions, replicas, leaders)
   - Manages topic partition assignments
   - Tracks partition leaders and ISRs (In-Sync Replicas)

3. **Consumer Group Coordination**
   - Tracks consumer group memberships
   - Manages consumer offsets (in older Kafka versions)
   - Coordinates consumer rebalancing

4. **Cluster Health Monitoring**
   - Provides distributed consensus
   - Ensures data consistency across the cluster
   - Facilitates leader election for partitions

### How It Works

```
Kafka (10.5.0.7)  ──KAFKA_ZOOKEEPER_CONNECT──►  Zookeeper (10.5.0.6:2181)
                                                         │
                                                         ├─► Broker Registration
                                                         ├─► Topic Metadata
                                                         ├─► Consumer Groups
                                                         └─► Cluster State
```

In the configuration:
```yaml
kafka:
  environment:
    KAFKA_ZOOKEEPER_CONNECT: 10.5.0.6:2181  # Points to Zookeeper
```

---

## Apache Kafka

### Role in the Architecture

**Apache Kafka** (running at `10.5.0.7:9092`) acts as the **distributed event streaming platform** and message broker, providing:
- High-throughput data ingestion
- Fault-tolerant message delivery
- Decoupling between data producers and consumers
- Event log persistence

### Configuration (docker-compose.yml)
```yaml
kafka:
  image: confluentinc/cp-kafka:7.7.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: 10.5.0.6:2181
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://10.5.0.7:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
```

### Topic Naming Convention

The system uses a hierarchical topic structure:
```
ecom.<table_name>

Examples:
- ecom.customers
- ecom.orders
- ecom.products
- ecom.inventory
- ecom.payments
- ecom.order_items
- ecom.shopping_cart
- ecom.customer_sessions
- ecom.marketing_campaigns
- ecom.suppliers
- ecom.addresses
- ecom.categories
- ecom.reviews
- ecom.wishlist
```

### How Kafka Works in the Pipeline

#### 1. **Producers** (Ingestion Services)

**Database Ingestion** (`db_ingest_service.py`):
```python
# Creates Kafka producer
producer = KafkaProducer(
    bootstrap_servers="10.5.0.7:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    max_in_flight_requests_per_connection=5,
    retries=3
)

# Sends messages in canonical format
message = {
    "source_type": "db",
    "vendor": "custom",
    "table": "customers",
    "schema_version": "v1",
    "timestamp": "2024-01-01T00:00:00Z",
    "payload": {...}  # Actual data
}
producer.send("ecom.customers", value=message)
```

**API Ingestion** (`api_ingest_service.py`):
```python
# Similar producer setup for API data
message = {
    "source_type": "api",
    "vendor": "api_polling",
    "table": "orders",
    "schema_version": "v1",
    "timestamp": "2024-01-01T00:00:00Z",
    "payload": {...}
}
producer.send("ecom.orders", value=message)
```

#### 2. **Topic Auto-Creation**

When ingestion services start, they create topics dynamically:
```python
def create_kafka_topic(kafka_bootstrap: str, topic: str):
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    topic_obj = NewTopic(
        name=topic, 
        num_partitions=1, 
        replication_factor=1
    )
    admin.create_topics([topic_obj])
```

#### 3. **Message Format** (Canonical Schema)

Every message follows a standardized structure (`canonical_message.py`):
```python
{
    "source_type": "db" | "api",      # Data source type
    "vendor": str,                     # Source vendor/system name
    "table": str,                      # Target canonical table
    "schema_version": "v1",            # Schema version
    "timestamp": "ISO-8601",           # Event timestamp
    "payload": {                       # Actual data
        "customer_id": "123",
        "name": "John Doe",
        ...
    }
}
```

#### 4. **Consumer** (Spark Streaming)

Spark Structured Streaming consumes from Kafka:
```python
# Subscribe to all ecom.* topics using pattern
json_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.5.0.7:9092")
    .option("subscribePattern", "ecom\\..*")  # All ecom topics
    .option("startingOffsets", "latest")
    .load()
)
```

### Kafka Benefits in this Architecture

1. **Decoupling**: Producers (DB/API ingest) and consumers (Spark) are independent
2. **Scalability**: Can handle thousands of messages per second
3. **Fault Tolerance**: Messages are persisted to disk (7-day retention)
4. **Multiple Consumers**: Different consumers can read same data
5. **Backpressure Handling**: Spark can read at its own pace
6. **Replay Capability**: Can reprocess historical data using offsets

---

## Spark Streaming

### Role in the Architecture

**Apache Spark Streaming** (`spark_streaming.py`) is the **stream processing engine** that:
- Consumes messages from Kafka topics
- Performs schema mapping and normalization
- Applies ML-based column matching algorithms
- Writes processed data to MinIO data lake

### Configuration

```python
spark = SparkSession.builder \
    .appName("StreamingNormalization") \
    .master("spark://10.5.0.3:7077") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "0") \
    .config("spark.dynamicAllocation.maxExecutors", "8") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "10.5.0.4:9000") \
    .getOrCreate()
```

### Streaming Pipeline

#### 1. **Reading from Kafka**
```python
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.5.0.7:9092")
        .option("subscribePattern", "ecom\\..*")  # All ecom.* topics
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), get_canonical_schema()).alias("data"))
        .select("data.*")
    )
```

#### 2. **Schema Definition**
```python
def get_canonical_schema() -> StructType:
    return StructType([
        StructField("source_type", StringType()),
        StructField("vendor", StringType()),
        StructField("table", StringType()),
        StructField("schema_version", StringType()),
        StructField("payload", MapType(StringType(), StringType()))
    ])
```

#### 3. **Micro-batch Processing**

Spark processes data in micro-batches using `foreachBatch`:

```python
query = (
    json_stream.writeStream
    .foreachBatch(lambda df, batch_id: process_microbatch(
        df, batch_id, columns_info, minio_client
    ))
    .outputMode("append")
    .option("checkpointLocation", "s3a://pulse-checkpoints/normalize-stream")
    .start()
)
```

#### 4. **Processing Logic** (`process_microbatch`)

```python
def process_microbatch(batch_df: DataFrame, batch_id: int, columns_info, minio_client):
    # Step 1: Extract DataFrames by table
    all_dataframes = extract_table_dataframes(batch_df)
    
    # Step 2: Apply schema mapping (from map.py)
    results = process_all_dataframes(
        all_dataframes,
        columns_info,      # PostgreSQL canonical schema
        mapping_list,      # Predefined column mappings
        mode="stream"
    )
    
    # Step 3: Save to MinIO
    save_dataframes_to_minio(results, minio_client, "pulse-bucket-stream")
```

#### 5. **Schema Mapping Algorithms**

The `process_all_dataframes` function applies multiple ML/NLP algorithms sequentially:

```python
# From map.py - Sequential mapping pipeline
1. normalize_dataframe()          # Predefined variant matching
2. rapidfuzz_column_mapping()     # Fuzzy string matching (threshold: 87%)
3. mapping_with_nltk()            # NLTK-based similarity (threshold: 70%)
4. semantic_column_mapping()      # WordNet semantic similarity
5. spacy_column_mapping()         # spaCy NLP matching (threshold: 87%)
6. word2vec_column_mapping()      # Word2Vec embeddings
7. roberta_similarity()           # RoBERTa transformer (threshold: 87%)
8. gpt_schema_mapping()           # GPT-based intelligent mapping
```

Each algorithm tries to map unmapped columns to the canonical schema. The process continues until all columns are mapped or all algorithms are exhausted.

#### 6. **Checkpointing**

Spark maintains checkpoints for fault tolerance:
```python
CHECKPOINT_LOCATION = "s3a://pulse-checkpoints/normalize-stream"
```

This stores:
- Stream processing state
- Kafka offsets (which messages have been processed)
- Metadata for recovery

---

## Data Storage

### Where is Data Stored?

The architecture uses **two different storage systems** for different purposes:

### 1. **PostgreSQL** (10.5.0.5:5432)

**Purpose**: Stores the **canonical schema definition** (metadata), NOT the actual streaming data.

**What's Stored**:
```sql
-- From sql/canonical_schema.sql
CREATE TABLE customers (...);
CREATE TABLE orders (...);
CREATE TABLE products (...);
-- etc.
```

**Usage**:
```python
# spark_streaming.py loads schema metadata
def load_postgres_schema():
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
    """)
    return cur.fetchall()
```

This metadata is used to:
- Define target schema for mapping
- Validate column types
- Guide the ML mapping algorithms

**Important**: PostgreSQL does NOT store the actual streaming data being processed. It only stores the schema definition that guides the mapping process.

### 2. **MinIO Data Lake** (10.5.0.4:9000)

**Purpose**: Stores the **processed/mapped data** from the streaming pipeline.

**What's Stored**:
- Output bucket: `pulse-bucket-stream`
- File format: CSV files
- Files named: `mapped_<table_name>.csv`

**Storage Process** (`save_dataframes_to_minio`):
```python
def save_dataframes_to_minio(results, client, bucket_name):
    for result_key, result_data in results.items():
        table_name = result_data["table_name"]
        final_df = result_data["final_df"]
        
        # Convert Spark DataFrame to Pandas
        pdf = final_df.toPandas()
        
        # Save as CSV
        csv_buffer = BytesIO()
        pdf.to_csv(csv_buffer, index=False)
        
        # Upload to MinIO
        file_name = f"mapped_{table_name}.csv"
        minio_client.put_object(
            bucket_name,           # "pulse-bucket-stream"
            file_name,
            csv_buffer,
            content_type="text/csv"
        )
```

**Examples of stored files**:
```
pulse-bucket-stream/
  ├── mapped_customers.csv
  ├── mapped_orders.csv
  ├── mapped_products.csv
  ├── mapped_inventory.csv
  └── ...
```

### Storage Summary

```
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL (10.5.0.5:5432)                                 │
│  ────────────────────────────                               │
│  Role: Canonical Schema Definition (Metadata)               │
│  Contains:                                                   │
│    • Table schemas (customers, orders, products, etc.)      │
│    • Column definitions and data types                      │
│    • Schema metadata for mapping guidance                   │
│  Does NOT contain: Actual streaming data                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  MinIO Data Lake (10.5.0.4:9000)                           │
│  ────────────────────────────                               │
│  Role: Processed Data Storage                               │
│  Contains:                                                   │
│    • Bucket: pulse-bucket-stream                            │
│    • Format: CSV files                                      │
│    • Files: mapped_<table>.csv                              │
│  This is where actual processed streaming data is stored    │
└─────────────────────────────────────────────────────────────┘
```

---

## Complete Data Journey

### End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: Data Ingestion                                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  External Database (via URI)                External API             │
│         │                                          │                 │
│         ▼                                          ▼                 │
│  db_ingest_service.py                    api_ingest_service.py      │
│         │                                          │                 │
│         ├─► Discovers tables automatically         │                 │
│         ├─► Maps to canonical names                ├─► Polls API     │
│         ├─► Polls for new records (10s)            ├─► Maps tables   │
│         └─► Serializes to JSON                     └─► Formats data  │
│                        │                              │               │
│                        └──────────┬───────────────────┘               │
│                                   ▼                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: Message Streaming (Kafka)                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│                    Kafka Broker (10.5.0.7:9092)                     │
│                               │                                       │
│  ┌────────────────────────────┼────────────────────────────────┐    │
│  │        Zookeeper manages ──┘                                 │    │
│  │                                                               │    │
│  │  Topics (ecom.*):                                            │    │
│  │    • ecom.customers         ┌─────────────────┐             │    │
│  │    • ecom.orders            │  Canonical      │             │    │
│  │    • ecom.products          │  Message Format │             │    │
│  │    • ecom.inventory         │  ─────────────  │             │    │
│  │    • ecom.payments          │  source_type    │             │    │
│  │    • ecom.order_items       │  vendor         │             │    │
│  │    • ecom.shopping_cart     │  table          │             │    │
│  │    • ecom.addresses         │  schema_version │             │    │
│  │    • ecom.categories        │  timestamp      │             │    │
│  │    • ecom.reviews           │  payload        │             │    │
│  │    • ecom.wishlist          └─────────────────┘             │    │
│  │    • ecom.customer_sessions                                  │    │
│  │    • ecom.marketing_campaigns                                │    │
│  │    • ecom.suppliers                                          │    │
│  │                                                               │    │
│  │  Features:                                                    │    │
│  │    • Auto-creation enabled                                    │    │
│  │    • 7-day retention (168 hours)                             │    │
│  │    • Replication factor: 1                                    │    │
│  │    • 1 partition per topic                                    │    │
│  └───────────────────────────────────────────────────────────────┘    │
│                                   │                                   │
│                                   ▼                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: Stream Processing (Spark Streaming)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│              Spark Master (10.5.0.3:7077)                           │
│                        │                                              │
│              spark_streaming.py                                      │
│                        │                                              │
│  ┌─────────────────────┴──────────────────────────────────┐         │
│  │                                                          │         │
│  │  1. Subscribe to ecom.* topics                          │         │
│  │     └─► Pattern: "ecom\\..*"                            │         │
│  │                                                          │         │
│  │  2. Parse canonical message format                      │         │
│  │     └─► Extract: table, payload, source, vendor         │         │
│  │                                                          │         │
│  │  3. Load PostgreSQL schema metadata                     │         │
│  │     └─► Get canonical column definitions                │         │
│  │                                                          │         │
│  │  4. Process micro-batches with foreachBatch             │         │
│  │     └─► Group by table                                  │         │
│  │     └─► Extract payload into DataFrames                 │         │
│  │                                                          │         │
│  │  5. Apply ML-based column mapping (map.py)              │         │
│  │     ├─► Predefined variant matching                     │         │
│  │     ├─► RapidFuzz fuzzy matching (87% threshold)        │         │
│  │     ├─► NLTK semantic similarity (70% threshold)        │         │
│  │     ├─► WordNet semantic mapping                        │         │
│  │     ├─► spaCy NLP matching (87% threshold)              │         │
│  │     ├─► Word2Vec embeddings                             │         │
│  │     ├─► RoBERTa transformers (87% threshold)            │         │
│  │     └─► GPT-4 intelligent mapping (fallback)            │         │
│  │                                                          │         │
│  │  6. Checkpoint progress                                 │         │
│  │     └─► Location: s3a://pulse-checkpoints/...           │         │
│  │     └─► Stores: Kafka offsets, state, metadata          │         │
│  │                                                          │         │
│  └──────────────────────────────────────────────────────────┘         │
│                                   │                                   │
│                                   ▼                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: Data Storage (MinIO Data Lake)                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│                MinIO Object Storage (10.5.0.4:9000)                 │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Bucket: pulse-bucket-stream                             │       │
│  │  ──────────────────────────                              │       │
│  │                                                           │       │
│  │  Files (CSV format):                                     │       │
│  │    • mapped_customers.csv                                │       │
│  │    • mapped_orders.csv                                   │       │
│  │    • mapped_products.csv                                 │       │
│  │    • mapped_inventory.csv                                │       │
│  │    • mapped_payments.csv                                 │       │
│  │    • mapped_order_items.csv                              │       │
│  │    • mapped_shopping_cart.csv                            │       │
│  │    • mapped_addresses.csv                                │       │
│  │    • mapped_categories.csv                               │       │
│  │    • mapped_reviews.csv                                  │       │
│  │    • mapped_wishlist.csv                                 │       │
│  │    • mapped_customer_sessions.csv                        │       │
│  │    • mapped_marketing_campaigns.csv                      │       │
│  │    • mapped_suppliers.csv                                │       │
│  │                                                           │       │
│  │  This is the final destination for processed data        │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component Interactions

### Detailed Interaction Flow

```
┌────────────────┐    registers with    ┌────────────────┐
│     Kafka      │◄────────────────────►│   Zookeeper    │
│  (10.5.0.7)    │  • Broker metadata   │  (10.5.0.6)    │
└────────┬───────┘  • Topic config      └────────────────┘
         │          • Consumer groups
         │
         │ produces to
         │
    ┌────┴─────┐
    │          │
    ▼          ▼
┌─────────┐  ┌─────────┐
│   DB    │  │   API   │
│ Ingest  │  │ Ingest  │
└─────────┘  └─────────┘
    │            │
    │ reads      │ polls
    ▼            ▼
┌─────────┐  ┌─────────┐
│Database │  │External │
│  (URI)  │  │   API   │
└─────────┘  └─────────┘
```

```
┌────────────────┐    subscribes to    ┌────────────────┐
│     Spark      │◄────────────────────►│     Kafka      │
│  Streaming     │  ecom.* topics       │  (10.5.0.7)    │
│  (10.5.0.3)    │                      └────────────────┘
└────────┬───────┘
         │
         │ reads schema from
         ▼
┌────────────────┐
│  PostgreSQL    │
│  (10.5.0.5)    │
│  Schema Only   │
└────────────────┘
         │
         │
         ▼
┌────────────────┐
│  Processes &   │
│  Maps Data     │
└────────┬───────┘
         │
         │ writes to
         ▼
┌────────────────┐
│     MinIO      │
│  Data Lake     │
│  (10.5.0.4)    │
│  Actual Data   │
└────────────────┘
```

### Timing and Polling

1. **DB Ingestion**: Polls every 10 seconds for new records
2. **API Ingestion**: Polls every 10 seconds for new data
3. **Kafka**: Retains messages for 7 days (168 hours)
4. **Spark Streaming**: Processes micro-batches as they arrive
5. **Checkpointing**: Continuous (after each micro-batch)

---

## Summary

### Answer to Original Question

**Q: Is the data coming from database URI or API being stored in PostgreSQL or MinIO?**

**A: The data is stored in MinIO data lake, NOT in PostgreSQL.**

- **PostgreSQL** stores only the **canonical schema definition** (metadata) that guides the mapping process
- **MinIO** stores the **actual processed streaming data** in the `pulse-bucket-stream` bucket as CSV files

### Data Flow Summary

1. **Sources** → External DB (via URI) or External API
2. **Ingestion** → `db_ingest_service.py` or `api_ingest_service.py`
3. **Streaming** → Kafka topics (ecom.*)
4. **Coordination** → Zookeeper manages Kafka cluster
5. **Processing** → Spark Streaming with ML-based column mapping
6. **Storage** → MinIO data lake (pulse-bucket-stream bucket)

### Key Technologies

- **Zookeeper**: Distributed coordination for Kafka cluster
- **Kafka**: Message broker and event streaming platform
- **Spark Streaming**: Real-time stream processing with ML algorithms
- **PostgreSQL**: Schema metadata storage (not data storage)
- **MinIO**: Object storage data lake (actual data storage)

---

## Architecture Advantages

1. **Scalability**: Each component can scale independently
2. **Fault Tolerance**: Kafka persistence, Spark checkpointing, Zookeeper coordination
3. **Real-Time Processing**: Sub-second latency with micro-batch processing
4. **Schema Flexibility**: ML-based mapping handles varying source schemas
5. **Data Lake Architecture**: Flexible storage in MinIO for downstream analytics
6. **Decoupling**: Producers and consumers are independent
7. **Replay Capability**: Can reprocess data using Kafka offsets

---

*This document explains the real-time streaming architecture of the Pulse e-commerce data analytics engine, detailing how Apache Kafka, Zookeeper, and Spark Streaming work together to ingest, process, and store data from multiple sources.*
