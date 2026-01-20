# Mapping Phase - User Guide

## Overview

The mapping phase in Pulse provides three modes for processing data:

1. **Batch Mode** - Process files from MinIO storage
2. **DB Mode** - Stream data from databases
3. **API Mode** - Stream data from API endpoints

All modes apply the same mapping pipeline to normalize data to the canonical schema and save results to the MinIO `mapped/` folder.

## Installation

Ensure you have the required dependencies:

```bash
cd /path/to/pulse
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Create a `.env` file in the project root (or copy from `.env.example`):

```bash
# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Kafka Configuration (for DB and API modes)
KAFKA_BOOTSTRAP=10.5.0.7:9092

# Spark Configuration
SPARK_SERVER=local[*]
# For cluster mode: SPARK_SERVER=spark://10.5.0.3:7077
```

## Usage

### Batch Mode

Process files from the `ingested/` folder in MinIO bucket:

```bash
cd mapping/
python run_mapping.py --mode batch --bucket-name pulse-bucket-1
```

**What happens:**
1. Loads all files (CSV, Excel, Parquet, JSON) from `pulse-bucket-1/ingested/`
2. Processes each file through the mapping pipeline
3. Applies fuzzy matching and ML-based column mapping algorithms
4. Saves normalized results to `pulse-bucket-1/mapped/` as CSV files

**Example:**
```bash
# Process data from the default bucket
python run_mapping.py --mode batch --bucket-name pulse-bucket-1

# The script will:
# - Find files in pulse-bucket-1/ingested/
# - Map columns to canonical schema
# - Save to pulse-bucket-1/mapped/customers.csv, products.csv, etc.
```

### DB Mode

Stream data from a database using Change Data Capture (CDC):

```bash
cd mapping/
python run_mapping.py --mode db \
  --db-uri "postgresql://user:password@host:5432/database" \
  --bucket-name pulse-bucket-1 \
  --poll-interval 10
```

**What happens:**
1. Connects to the database and discovers tables
2. Maps discovered tables to canonical schema tables
3. Polls for new/changed records every `poll-interval` seconds
4. Streams changes to Kafka topics
5. Spark Streaming consumes from Kafka, applies mapping
6. Saves normalized results to `pulse-bucket-1/mapped/` as CSV files

**Supported Databases:**
- PostgreSQL
- MySQL
- MongoDB
- Microsoft SQL Server
- Oracle
- IBM Db2
- Apache Cassandra
- Vitess
- Google Cloud Spanner

**Prerequisites:**
Before using DB mode, the database administrator must complete setup steps. See `README.md` section "Database Administrator Prerequisites".

**Example:**
```bash
# Stream from PostgreSQL
python run_mapping.py --mode db \
  --db-uri "postgresql://pulse_user:password@10.5.0.5:5432/ecommerce" \
  --bucket-name pulse-bucket-1

# Stream from MySQL with custom poll interval
python run_mapping.py --mode db \
  --db-uri "mysql://pulse_user:password@10.5.0.11:3306/ecommerce" \
  --bucket-name pulse-bucket-1 \
  --poll-interval 30

# Stream from MongoDB
python run_mapping.py --mode db \
  --db-uri "mongodb://pulse_user:password@10.5.0.12:27017/ecommerce?replicaSet=rs0" \
  --bucket-name pulse-bucket-1
```

### API Mode

Stream data from an API endpoint:

```bash
cd mapping/
python run_mapping.py --mode api \
  --api-url "http://localhost:5000/api/data" \
  --bucket-name pulse-bucket-1 \
  --poll-interval 10
```

**What happens:**
1. Polls the API endpoint every `poll-interval` seconds
2. Expects JSON response with structure: `{"tables": [{"name": "...", "data": [...]}]}`
3. Maps table names to canonical schema
4. Streams data to Kafka topics
5. Spark Streaming consumes from Kafka, applies mapping
6. Saves normalized results to `pulse-bucket-1/mapped/` as CSV files

**Expected API Response Format:**
```json
{
  "tables": [
    {
      "name": "customers",
      "data": [
        {"customer_id": "C001", "name": "John Doe", "email": "john@example.com"},
        {"customer_id": "C002", "name": "Jane Smith", "email": "jane@example.com"}
      ]
    },
    {
      "name": "products",
      "data": [
        {"product_id": "P001", "name": "Widget", "price": 19.99},
        {"product_id": "P002", "name": "Gadget", "price": 29.99}
      ]
    }
  ]
}
```

**Example:**
```bash
# Stream from local API
python run_mapping.py --mode api \
  --api-url "http://localhost:5000/api/data" \
  --bucket-name pulse-bucket-1

# Stream from remote API with custom poll interval
python run_mapping.py --mode api \
  --api-url "https://api.example.com/v1/data" \
  --bucket-name pulse-bucket-1 \
  --poll-interval 60
```

## Command-Line Options

```
--mode {batch,db,api}
    Required. Mapping mode to use.
    
--bucket-name BUCKET_NAME
    Required. MinIO bucket name where results will be saved.
    
--db-uri DB_URI
    Required for db mode. Database connection URI.
    Format: protocol://user:password@host:port/database
    
--api-url API_URL
    Required for api mode. API endpoint URL.
    
--poll-interval POLL_INTERVAL
    Optional for db/api modes. Polling interval in seconds (default: 10).
    
--kafka-bootstrap KAFKA_BOOTSTRAP
    Optional for db/api modes. Kafka bootstrap servers (default: from env).
```

## Architecture

### Batch Mode Architecture
```
┌─────────────────┐
│ MinIO           │
│ bucket/ingested │
└────────┬────────┘
         │
         ▼
    ┌────────────┐
    │  PySpark   │
    │  Mapping   │
    └────┬───────┘
         │
         ▼
┌─────────────────┐
│ MinIO           │
│ bucket/mapped   │
└─────────────────┘
```

### DB/API Mode Architecture
```
┌──────────┐          ┌───────────────┐
│ Database │          │  API Endpoint │
│  or API  │          └───────────────┘
└────┬─────┘                  │
     │                        │
     ▼                        ▼
┌────────────────────────────────┐
│   Ingestion Service            │
│   (db_ingest / api_ingest)     │
└────────────┬───────────────────┘
             │
             ▼
      ┌──────────────┐
      │    Kafka     │
      │   Topics     │
      └──────┬───────┘
             │
             ▼
┌────────────────────────────────┐
│   Spark Streaming              │
│   (spark_streaming.py)         │
│   - Consumes from Kafka        │
│   - Applies mapping            │
└────────────┬───────────────────┘
             │
             ▼
      ┌─────────────────┐
      │ MinIO           │
      │ bucket/mapped   │
      └─────────────────┘
```

## Mapping Pipeline

All modes use the same mapping pipeline with multiple algorithms:

1. **Initial Normalization** - Match known column variants
2. **RapidFuzz Mapping** - Fuzzy string matching (87% threshold)
3. **NLTK Combination** - Natural language processing (70% threshold)
4. **WordNet Semantic** - Semantic similarity (70% threshold)
5. **spaCy Mapping** - NLP-based matching (87% threshold)
6. **Word2Vec Mapping** - Vector-based semantic similarity
7. **RoBERTa Mapping** - Transformer-based similarity (87% threshold)
8. **GPT Mapping** - LLM-based intelligent mapping (last resort)

## Output

All modes save results to the MinIO bucket in the `mapped/` folder:

```
bucket-name/
├── ingested/          # Input files (for batch mode)
│   ├── customers.csv
│   └── products.xlsx
└── mapped/            # Output files (all modes)
    ├── customers.csv  # Normalized to canonical schema
    ├── products.csv
    ├── orders.csv
    └── ...
```

## Troubleshooting

### Batch Mode Issues

**Problem:** No files found in ingested folder  
**Solution:** Ensure files are uploaded to `bucket-name/ingested/`

**Problem:** File format not supported  
**Solution:** Use CSV, Excel (.xlsx), Parquet, or JSON formats

### DB Mode Issues

**Problem:** Connection refused  
**Solution:** Check database host, port, and firewall rules

**Problem:** Permission denied  
**Solution:** Ensure the database user has appropriate permissions (see README.md)

**Problem:** No tables mapped  
**Solution:** Check if table names match canonical schema or can be fuzzy-matched

### API Mode Issues

**Problem:** API endpoint unreachable  
**Solution:** Check API URL and network connectivity

**Problem:** Invalid JSON response  
**Solution:** Ensure API returns the expected format (see example above)

### General Issues

**Problem:** Kafka connection failed  
**Solution:** Verify Kafka is running and KAFKA_BOOTSTRAP env var is correct

**Problem:** MinIO connection failed  
**Solution:** Check MinIO credentials and endpoint in .env file

**Problem:** Spark job fails  
**Solution:** Check Spark configuration and available resources

## Testing

Run CLI validation tests:

```bash
cd mapping/
python test_run_mapping.py
```

This validates the command-line interface without requiring full infrastructure.

## Support

For issues or questions:
1. Check this guide and README.md
2. Review the troubleshooting section
3. Open an issue in the repository with error details

## Security Summary

✅ **CodeQL Analysis:** 0 vulnerabilities found  
✅ **Code Review:** All feedback addressed  
✅ **Best Practices:** Follows secure coding guidelines
