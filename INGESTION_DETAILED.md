# Pulse Data Ingestion - Detailed Explanation

## Overview

Pulse supports **two distinct ingestion modes**: **batch processing** for large-scale historical data and **Kafka streaming** for real-time continuous data flow. This document provides an in-depth explanation of how data enters the Pulse system.

---

## Ingestion Modes

### A. Batch Processing (Historical/Bulk Data)

**Purpose:** Load and process large volumes of existing data from static files.

#### Fake Data Generation
- Located in `/faker` directory
- Generates realistic e-commerce data in Excel format for testing
- **14 entity types**: customers, orders, products, suppliers, addresses, categories, inventories, customer_sessions, payments, reviews, shopping_carts, wishlists, marketing_campaigns, order_items
- Creates both raw (`.xlsx`) and mapped (`.csv`) versions
- Can generate thousands of records per entity type

#### Batch Processing Workflow
```
Excel/CSV Files → MinIO Storage (Raw Bucket) → Spark Batch Processing → Cleaning → Mapping → Transformation
```

#### Characteristics
- Processes complete datasets at once
- High throughput for large files (GBs to TBs)
- Scheduled or on-demand execution
- Uses Spark's distributed file reading capabilities
- Ideal for initial data loads, migrations, and periodic bulk imports

---

### B. Kafka Streaming (Real-Time Continuous Data)

**Purpose:** Ingest and process data continuously as it arrives from live sources.

## Streaming Architecture

The streaming system uses **Apache Kafka** as a message broker with **Spark Structured Streaming** as the processing engine. Data flows through standardized topics following a canonical message format.

---

## Streaming Components

### 1. Canonical Message Format

**File:** `mapping/streaming/canonical_message.py`

Every message sent to Kafka follows a standardized JSON structure:

```json
{
  "source_type": "db" | "api",
  "vendor": "custom" | "api_polling" | vendor_name,
  "table": "customers" | "orders" | ... (one of 14 canonical tables),
  "schema_version": "v1",
  "timestamp": "2024-12-10T12:00:00Z",
  "payload": { 
    "customer_id": "C001",
    "name": "John Doe",
    ... actual record data ...
  }
}
```

**Fields Explained:**
- **source_type**: Where the data came from (`db` = database, `api` = REST API)
- **vendor**: Identifies the specific source system (e.g., "Shopify", "WooCommerce", "custom")
- **table**: Canonical table name (normalized across all sources)
- **schema_version**: Message format version for evolution
- **timestamp**: When the message was created (ISO 8601 format)
- **payload**: The actual data record as key-value pairs

**Benefits:**
- Consistent structure regardless of source
- Easy routing via Kafka topics
- Version management for schema evolution
- Metadata for lineage tracking

---

### 2. Kafka Topic Structure

**Setup Script:** `bash/setup_kafka.sh`

Pulse uses **14 predefined topics**, one for each canonical table:

| Topic Name | Purpose |
|------------|---------|
| `ecom.customers` | Customer records |
| `ecom.orders` | Order transactions |
| `ecom.products` | Product catalog |
| `ecom.inventory` | Stock levels |
| `ecom.addresses` | Location data |
| `ecom.categories` | Product categories |
| `ecom.suppliers` | Supplier information |
| `ecom.payments` | Payment transactions |
| `ecom.reviews` | Customer reviews |
| `ecom.wishlist` | Saved items |
| `ecom.shopping_cart` | Active carts |
| `ecom.customer_sessions` | Browsing sessions |
| `ecom.marketing_campaigns` | Marketing data |
| `ecom.order_items` | Order line items |

**Topic Configuration:**
- Naming pattern: `ecom.<canonical_table_name>`
- 1 partition per topic (configurable)
- Replication factor: 1 (increase for production)
- Automatically created during initial setup

---

### 3. Database Ingestion Service

**File:** `mapping/streaming/ingestion/db_ingest_service.py`

**Purpose:** Connect to external databases and stream changes to Kafka in real-time.

#### Features

**1. Auto-Detection of Database Type**
- Supports PostgreSQL, MySQL, MongoDB, Microsoft SQL Server
- Detects DB type from connection URI scheme
- Example URIs:
  ```
  postgresql://user:pass@host:5432/dbname
  mysql://user:pass@host:3306/dbname
  mongodb://user:pass@host:27017/dbname
  mssql+pyodbc://user:pass@host/dbname
  ```

**2. Auto-Discovery of Tables**
- Automatically queries information schema to find all tables
- PostgreSQL: Queries `information_schema.tables`
- MySQL: Uses `SHOW TABLES`
- MongoDB: Lists collections with `list_collection_names()`
- No manual configuration needed!

**3. Intelligent Table Mapping**
- Uses **RapidFuzz** fuzzy matching to map table names to canonical schema
- Handles naming variations automatically
- Examples:
  - `"user"` → `"customers"` (exact match via lookup table)
  - `"customer_info"` → `"customers"` (fuzzy match, 87% similarity)
  - `"order_details"` → `"order_items"` (fuzzy match)
  - `"product_inventory"` → `"inventory"` (fuzzy match)
- Minimum threshold: 85% similarity score
- Falls back to canonical table map for common variations

**4. Change Data Capture (CDC)**
- Tracks last processed timestamp per table
- Only fetches new or updated records
- Uses `updated_at` or `created_at` columns
- SQL query pattern:
  ```sql
  SELECT * FROM table 
  WHERE updated_at > '2024-12-10T11:00:00' 
     OR created_at > '2024-12-10T11:00:00'
  ORDER BY COALESCE(updated_at, created_at) ASC
  ```

**5. Continuous Polling**
- Default: Polls every 10 seconds (configurable)
- Incremental updates only
- Maintains state across restarts
- Handles connection failures gracefully

#### Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Frontend provides database URI                               │
│    Example: "postgresql://user:pass@10.5.0.5:5432/ecommerce"   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Connect & Detect Database Type                               │
│    - Parse URI scheme                                            │
│    - Establish connection                                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Auto-Discover All Tables                                     │
│    Found: ["users", "orders", "product_info", "addresses"]      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Map to Canonical Schema (Fuzzy Matching)                     │
│    - "users" → "customers" (exact match)                         │
│    - "orders" → "orders" (exact match)                           │
│    - "product_info" → "products" (fuzzy, 91% similar)            │
│    - "addresses" → "addresses" (exact match)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Create Kafka Topics                                          │
│    - ecom.customers                                              │
│    - ecom.orders                                                 │
│    - ecom.products                                               │
│    - ecom.addresses                                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Start Continuous Polling (Every 10 seconds)                  │
│                                                                  │
│    For each table:                                               │
│      ① Fetch new/updated records since last_timestamp           │
│      ② Serialize each record                                    │
│      ③ Wrap in canonical message format                         │
│      ④ Send to appropriate Kafka topic                          │
│      ⑤ Update last_timestamp                                    │
│                                                                  │
│    Example:                                                      │
│    - users table: 5 new records → ecom.customers                │
│    - orders table: 12 new records → ecom.orders                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Code Example

```python
from db_ingest_service import ingest_from_uri

# Frontend provides this URI
db_uri = "postgresql://pulse_user:secret@10.5.0.5:5432/ecommerce"
kafka_bootstrap = "10.5.0.7:9092"
poll_interval = 10  # seconds

# Start continuous ingestion
ingest_from_uri(db_uri, poll_interval, kafka_bootstrap)

# Output:
# ✓ Connected to postgres database
# ✓ Discovered 8 tables
# ✓ Mapped 6 tables to canonical schema
# ✓ Created Kafka topics
# [Iteration 1] Polling tables...
#   users: 100 new records → customers
#   orders: 50 new records → orders
#   Total: 150 records sent to Kafka
```

---

### 4. API Ingestion Service

**File:** `mapping/streaming/ingestion/api_ingest_service.py`

**Purpose:** Poll external REST APIs and stream data to Kafka.

#### Features

**1. HTTP Polling**
- Fetches data via HTTP GET requests
- Configurable API endpoint
- Default: Polls every 10 seconds
- Timeout: 10 seconds per request

**2. Expected API Format**

The external API should return JSON in this structure:

```json
{
  "tables": [
    {
      "name": "customer",
      "data": [
        { "customer_id": "C001", "name": "John Doe", "email": "john@example.com" },
        { "customer_id": "C002", "name": "Jane Smith", "email": "jane@example.com" }
      ]
    },
    {
      "name": "order",
      "data": [
        { "order_id": "O001", "customer_id": "C001", "total": 99.99 }
      ]
    }
  ]
}
```

**3. Table Name Mapping**

Similar to database ingestion, uses fuzzy matching to map API table names:

```python
# Predefined mappings
"customer" → "customers"
"users" → "customers"
"carts" → "shopping_cart"
"inventories" → "inventory"
"order_details" → "order_items"

# Fuzzy matching for unknown names
"customer_info" → "customers" (85%+ similarity)
```

**4. Error Resilience**
- Continues on API failures
- Logs errors without stopping
- Automatic retries (3 attempts)
- Graceful degradation

#### Workflow

```
┌───────────────────────────────────────────────────────┐
│ 1. Configure API Endpoint                             │
│    URL: http://external-api.com/api/data             │
└─────────────────────────┬─────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────┐
│ 2. Create Kafka Producer                              │
└─────────────────────────┬─────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────┐
│ 3. Create All Kafka Topics (14 topics)                │
└─────────────────────────┬─────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────┐
│ 4. Start Polling Loop (Every 10 seconds)              │
│                                                        │
│    ① HTTP GET request to API                          │
│    ② Parse JSON response                              │
│    ③ For each table in response:                      │
│       - Map table name to canonical                   │
│       - Extract data records                          │
│       - Wrap each record in canonical message         │
│       - Send to Kafka topic                           │
│    ④ Flush producer                                   │
│    ⑤ Sleep 10 seconds                                 │
│                                                        │
│    Example iteration:                                 │
│    - Fetched 3 tables                                 │
│    - "customer" (50 rows) → ecom.customers            │
│    - "order" (30 rows) → ecom.orders                  │
│    - "product" (100 rows) → ecom.products             │
│    - Total: 180 records sent                          │
└───────────────────────────────────────────────────────┘
```

#### Code Example

```python
from api_ingest_service import run

API_URL = "http://external-system.com/api/data"
POLL_INTERVAL = 10  # seconds
KAFKA_BOOTSTRAP = "10.5.0.7:9092"

# Start continuous polling
run(API_URL, POLL_INTERVAL, KAFKA_BOOTSTRAP)

# Output:
# Starting API ingestion: http://... → Kafka
# Polling every 10s (Ctrl+C to stop)
# Processed 3 tables, 180 rows
# Processed 3 tables, 12 rows
# ...
```

---

### 5. Spark Structured Streaming Consumer

**File:** `mapping/streaming/spark_streaming.py`

**Purpose:** Consume messages from Kafka, apply NLP-based mapping/normalization, and save to MinIO.

#### Architecture

**Spark Cluster Setup:**
- **Master Node**: 10.5.0.3:7077
- **Dynamic Allocation**: 0-8 executors based on workload
- **Packages**: 
  - `spark-sql-kafka-0-10_2.12:3.5.0` (Kafka integration)
  - `hadoop-aws:3.3.4` (S3/MinIO support)
  - `aws-java-sdk-bundle:1.12.262` (AWS SDK)

**Processing Model:**
- **Micro-batch processing**: Processes data in small batches
- **Near real-time**: Typically processes within seconds
- **Stateless**: Each batch is independent
- **Fault tolerant**: Checkpointing for recovery

#### Processing Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Subscribe to Kafka Topics                                │
│    Pattern: "ecom\\..*" (all ecom.* topics)                 │
│    Starting offset: latest                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Read Stream & Parse JSON                                 │
│    - Cast Kafka value to string                             │
│    - Parse JSON to canonical message schema                 │
│    - Extract: source_type, vendor, table, payload           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Process Micro-Batch (foreachBatch)                       │
│                                                              │
│    For each batch (batch_id):                               │
│      ① Group by table name                                  │
│      ② Extract payload columns                              │
│      ③ Create DataFrame per table                           │
│                                                              │
│    Example batch:                                            │
│      - 15 messages for customers                            │
│      - 8 messages for orders                                │
│      - 3 messages for products                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Apply Mapping Algorithms (Reuse from Batch)              │
│                                                              │
│    Uses existing NLP algorithms:                             │
│      - RapidFuzz: Fast fuzzy string matching                │
│      - NLTK: Token-based matching                           │
│      - SpaCy: Semantic similarity                           │
│      - WordNet: Synonym matching                            │
│      - Word2Vec: Embedding-based similarity                 │
│      - RoBERTa: Transformer-based matching                  │
│      - GPT: Large language model mapping                    │
│                                                              │
│    Maps source columns → canonical schema columns           │
│    Example: "cust_name" → "customer_name"                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Save to MinIO                                             │
│    - Format: Parquet (columnar, compressed)                 │
│    - Bucket: pulse-bucket-stream                            │
│    - Path: /customers/part-0001.parquet                     │
│    - Checkpoint: s3a://pulse-checkpoints/normalize-stream   │
└─────────────────────────────────────────────────────────────┘
```

#### Features

**1. Topic Pattern Subscription**
```python
.option("subscribePattern", "ecom\\..*")
```
- Automatically subscribes to all `ecom.*` topics
- No need to specify each topic individually
- New topics are auto-detected

**2. Dynamic Executor Scaling**
```python
.config("spark.dynamicAllocation.enabled", "true")
.config("spark.dynamicAllocation.minExecutors", "0")
.config("spark.dynamicAllocation.maxExecutors", "8")
```
- Scales up when workload increases
- Scales down to 0 when idle (saves resources)
- Automatic load balancing

**3. Checkpoint Mechanism**
```python
.option("checkpointLocation", "s3a://pulse-checkpoints/normalize-stream")
```
- Saves processing progress to MinIO
- Enables exactly-once processing semantics
- Automatic recovery from failures
- Prevents duplicate processing

**4. Integration with Existing Mapping Logic**
- Reuses the same 7 NLP algorithms from batch processing
- Calls `process_all_dataframes()` from `map.py`
- Mode parameter: `mode="stream"` for optimizations
- Consistent results between batch and streaming

#### Example Output

```
Starting Spark Streaming Pipeline
Kafka: 10.5.0.7:9092
Checkpoint: s3a://pulse-checkpoints/normalize-stream
Output bucket: pulse-bucket-stream

Loaded 150 columns from canonical schema
✓ Created bucket: pulse-bucket-stream
Streaming query started.

============================================================
Processing batch 0
============================================================
Tables in batch: ['customers_df', 'orders_df']
Mapped customers_df: 15 records
Mapped orders_df: 8 records
✅ Batch 0 completed: 2 tables processed

============================================================
Processing batch 1
============================================================
Tables in batch: ['products_df']
Mapped products_df: 20 records
✅ Batch 1 completed: 1 tables processed
```

---

## Streaming vs Batch Comparison

| Aspect | Batch Processing | Kafka Streaming |
|--------|------------------|-----------------|
| **Latency** | Minutes to hours | Seconds to minutes |
| **Data Source** | Static files (Excel/CSV) | Live databases, APIs |
| **Data Volume** | Large (GBs to TBs) at once | Continuous (records/second) |
| **Processing** | One-time or scheduled | Continuous 24/7 |
| **Use Case** | Historical loads, migrations | Real-time sync, CDC |
| **Fault Tolerance** | Restart entire job | Checkpoint + replay |
| **Scalability** | Vertical + Horizontal | Horizontal (add consumers) |
| **Resource Usage** | Spiky (high during job) | Consistent (steady state) |
| **Data Freshness** | Stale (batch intervals) | Fresh (seconds old) |

---

## End-to-End Streaming Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                  │
│                                                                   │
│  ┌─────────────────┐         ┌─────────────────┐                │
│  │  PostgreSQL     │         │   REST API      │                │
│  │  MySQL          │         │   (External)    │                │
│  │  MongoDB        │         │                 │                │
│  │  MSSQL          │         └────────┬────────┘                │
│  └────────┬────────┘                  │                          │
└───────────┼───────────────────────────┼──────────────────────────┘
            │                           │
            │ DB Ingest Service         │ API Ingest Service
            │ (Polling every 10s)       │ (Polling every 10s)
            │                           │
            ▼                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                     APACHE KAFKA                                  │
│                                                                   │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐    │
│  │ ecom.customers │  │  ecom.orders   │  │ ecom.products  │    │
│  └────────────────┘  └────────────────┘  └────────────────┘    │
│                                                                   │
│  ... 11 more topics (14 total) ...                              │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                │ Spark Structured Streaming
                                │ (Pattern: ecom\\.*)
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                  SPARK STREAMING CONSUMER                         │
│                                                                   │
│  ① Read from Kafka (micro-batches)                               │
│  ② Parse canonical messages                                      │
│  ③ Group by table                                                │
│  ④ Apply NLP mapping (7 algorithms)                              │
│  ⑤ Normalize to canonical schema                                 │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                     MINIO STORAGE                                 │
│                                                                   │
│  Bucket: pulse-bucket-stream                                     │
│  Format: Parquet (compressed, columnar)                          │
│  Path: /customers/*, /orders/*, etc.                             │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                │ Subsequent Processing
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                   DOWNSTREAM PIPELINE                             │
│                                                                   │
│  ① Data Cleaning (remove duplicates, handle nulls)               │
│  ② Transformation (business logic, joins)                        │
│  ③ Aggregation (RFM, analytics, KPIs)                            │
│  ④ Export to PostgreSQL                                          │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL                                   │
│                                                                   │
│  Tables: customers, orders, products, etc.                       │
│  Aggregations: rfm_segments, product_analytics, etc.             │
└───────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                      REACT FRONTEND                               │
│                                                                   │
│  Dashboards, Analytics, Visualizations                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Key Advantages of Streaming

1. **Low Latency**: Data available within seconds of creation in source system
2. **Continuous Sync**: Always up-to-date with source systems (no stale data)
3. **Scalable**: Kafka handles millions of messages per second
4. **Decoupled**: Source systems don't need to know about Pulse
5. **Resilient**: Built-in retry, checkpointing, and replay capabilities
6. **Flexible**: Easy to add new sources (just produce to Kafka)
7. **No Polling Impact**: Source systems not overloaded (Kafka buffers)
8. **Event-Driven**: React to changes as they happen

---

## Configuration

### Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP=10.5.0.7:9092          # Internal network
KAFKA_BOOTSTRAP_EXTERNAL=localhost:9092 # External access

# MinIO (S3-compatible storage)
MINIO_ENDPOINT=10.5.0.4:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# PostgreSQL
POSTGRES_SERVER=10.5.0.5
POSTGRES_USER=pulse_user
POSTGRES_PASSWORD=pulse_password
POSTGRES_DATABASE_NAME=pulse_db

# Spark
SPARK_MASTER_URL=spark://10.5.0.3:7077
```

### Tuning Parameters

**Database Ingestion:**
```python
POLL_INTERVAL = 10  # seconds between polls
CDC_BATCH_SIZE = 1000  # records per batch
FUZZY_THRESHOLD = 85  # minimum similarity %
```

**API Ingestion:**
```python
POLL_INTERVAL = 10  # seconds between polls
API_TIMEOUT = 10  # seconds per request
RETRY_COUNT = 3  # retries on failure
```

**Spark Streaming:**
```python
MIN_EXECUTORS = 0
MAX_EXECUTORS = 8
INITIAL_EXECUTORS = 1
CHECKPOINT_INTERVAL = "10 seconds"
TRIGGER_INTERVAL = "5 seconds"
```

---

## Monitoring

**Kafka:**
- Web UI: N/A (use CLI tools)
- Topics list: `docker exec kafka kafka-topics --list --bootstrap-server 10.5.0.7:9092`
- Consumer lag: `docker exec kafka kafka-consumer-groups --describe --group <group> --bootstrap-server 10.5.0.7:9092`

**Spark:**
- Master UI: http://localhost:8080
- Application UI: http://localhost:4040 (when running)
- Metrics: Batch processing time, records/second, failures

**MinIO:**
- Console: http://localhost:9001
- Check bucket sizes and object counts
- Monitor S3 API calls

---

## Summary

Pulse's ingestion system provides a flexible, scalable solution for bringing data into the platform:

- **Batch mode** for historical data and bulk loads
- **Streaming mode** for real-time continuous sync
- **Intelligent mapping** using fuzzy matching and NLP
- **Multi-source support** (databases, APIs, files)
- **Fault-tolerant** with checkpointing and retry logic
- **Production-ready** with monitoring and configuration options

The streaming architecture enables Pulse to keep data fresh while maintaining high throughput and low latency, making it suitable for real-time analytics and decision-making.
