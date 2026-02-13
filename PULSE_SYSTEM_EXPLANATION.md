# Pulse Repository - Comprehensive System Explanation

**Last Updated**: 2025-02-13  
**Target Audience**: Developers, DevOps Engineers, Data Engineers

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Main Scripts Structure and Arguments](#main-scripts-structure-and-arguments)
3. [Database Tables and Purpose](#database-tables-and-purpose)
4. [MinIO Storage Architecture](#minio-storage-architecture)
5. [Overall Workflow: Onboarding to Dashboard](#overall-workflow-onboarding-to-dashboard)
6. [Business ID Concept](#business-id-concept)
7. [Data Flow Through the System](#data-flow-through-the-system)
8. [Current Limitations and Workarounds](#current-limitations-and-workarounds)

---

## System Overview

**Pulse** is a web-based E-Commerce Data Analytics Engine that:
- Ingests e-commerce data from multiple sources (batch files, databases, APIs)
- Performs data cleaning, transformation, and aggregation using Apache Spark
- Generates analytics, insights, predictions, and forecasts using machine learning
- Presents results through a ReactJS frontend dashboard

**Technology Stack**:
- **Frontend**: ReactJS (Vite)
- **Backend API**: FastAPI (Python)
- **Data Processing**: Apache Spark (PySpark)
- **Storage**: MinIO (S3-compatible object storage)
- **Database**: PostgreSQL (metadata)
- **Streaming**: Apache Kafka + Debezium (CDC)
- **Orchestration**: Apache NiFi
- **ML Framework**: PySpark MLlib

---

## Main Scripts Structure and Arguments

### 1. **cleaning.py** (`cleaning/cleaning.py`)

**Purpose**: Orchestrates the complete data cleaning process for e-commerce data.

**Structure**:
```python
def main():
    # 1. Initialize Spark and MinIO
    spark = create_spark_session()
    minio_client = create_minio_client()
    bucket_name = get_bucket_name()  # Returns "pulse-bucket-1"
    
    # 2. Load data from MinIO (mapped/ directory)
    dataframes = load_data_from_minio(spark, minio_client, bucket_name, table_names)
    
    # 3-18. Perform cleaning operations:
    # - Cast data types
    # - Merge tables
    # - Handle duplicates and nulls
    # - Remove outliers
    # - Validate dates/timestamps
    # - Clean gibberish patterns
    # - Text cleaning
    
    # 19. Save cleaned data to MinIO (cleaned/ directory)
    save_data_to_minio(dataframes, minio_client, bucket_name)
```

**Command-Line Arguments**:
- ❌ **Currently NOT accepted** - bucket_name is hardcoded in `cleaning_config.py`
- **Bucket Name Source**: `cleaning_config.py::get_bucket_name()` → Returns `"pulse-bucket-1"`
- **Configuration**: Reads from environment variables via `.env` file

**Input**: MinIO `{bucket}/mapped/*.csv` files  
**Output**: MinIO `{bucket}/cleaned/*.csv` files

**Tables Processed**:
```python
[
    "addresses", "categories", "customer_sessions", "customers",
    "inventory", "marketing_campaigns", "order_items", "orders",
    "payments", "products", "reviews", "shopping_cart",
    "cart_items", "suppliers", "wishlist"
]
```

---

### 2. **transformation.py** (`transformation/transformation.py`)

**Purpose**: Transforms cleaned data and creates aggregations for analytics.

**Structure**:
```python
def main():
    spark = create_spark_session()
    minio_client = create_minio_client()
    
    # Load from cleaned/ directory
    dataframes = load_data_from_minio(spark, minio_client, BUCKET_NAME)
    
    # Apply transformations
    transform_orders(dataframes)
    transform_customers(dataframes)
    transform_campaigns(dataframes)
    # ... more transformations
    
    # Create aggregations
    aggregate_customers(dataframes)
    aggregate_products(spark, dataframes)
    time_based_aggregations(dataframes)
    geographic_aggregations(dataframes)
    # ... more aggregations
    
    # Export to MinIO (transformed/ directory as Parquet)
    export_to_minio(
        dataframes,
        sql_schema_path="/app/sql/agg_schema.sql",
        enforce_schemas=True,
        preserve_types=True,
        compression='snappy'
    )
```

**Command-Line Arguments**:
- ❌ **Currently NOT accepted** - bucket_name is hardcoded
- **Bucket Name Source**: `transformation/config/minio_config.py::BUCKET_NAME` → `"pulse-bucket-1"`
- **Configuration**: Reads from environment variables via `.env` file

**Input**: MinIO `{bucket}/cleaned/*.csv` files  
**Output**: MinIO `{bucket}/transformed/*.parquet` files (with agg_ prefix)

**Key Aggregations Created**:
- `agg_customers`, `agg_orders`, `agg_products`
- `agg_daily_aggregations`, `agg_weekly_aggregations`, `agg_monthly_aggregations`
- `agg_country_aggregations`, `agg_state_aggregations`, `agg_city_aggregations`
- `agg_rfm_segmentation`, `agg_product_affinity`
- `agg_cart_abandonment_analysis`, `agg_inventory_health`
- `agg_global_aggregations`

---

### 3. **analysis.py** (`analysis/analysis.py`)

**Purpose**: Performs advanced analytics on aggregated data to generate insights.

**Structure**:
```python
def main():
    spark = create_spark_session("Ecommerce_Analysis_Main")
    
    # Load aggregated tables from transformed/ directory
    dataframes = get_agg_tables(spark)
    
    # Perform analytics:
    # - Core KPIs over time (daily/weekly/monthly)
    # - Customer analytics (cohort, retention, segmentation)
    # - Product analytics (performance, categories, trends)
    # - Geographic analytics
    # - Campaign performance
    # - Session analytics
    
    # Export results back to MinIO (analytics/ directory)
    export_analytics_to_minio(...)
```

**Command-Line Arguments**:
- ❌ **Currently NOT accepted** - bucket_name from environment variable
- **Bucket Name Source**: `analysis_utils.py::get_agg_tables()` → `os.getenv("MINIO_BUCKET", "pulse-bucket-1")`
- **Configuration**: Environment variable `MINIO_BUCKET` or defaults to `"pulse-bucket-1"`

**Input**: MinIO `{bucket}/transformed/*.parquet` files  
**Output**: MinIO `{bucket}/analytics/*.parquet` files (analytics results)

**File Size**: 329.3 KB (large file with extensive analytics logic)

---

### 4. **infer_all.py** (`machine-learning/infer_all.py`)

**Purpose**: Runs inference on all trained ML models (general and specific).

**Structure**:
```python
def main():
    parser = argparse.ArgumentParser(description='Run inference on general and specific models')
    parser.add_argument('--bucket-name', type=str, required=True, help='S3 bucket name')
    args = parser.parse_args()
    
    # Run all general model inferences
    general_infer(args.bucket_name)
    
    # Run all specific model inferences
    specific_infer(args.bucket_name)

if __name__ == "__main__":
    main()
```

**Command-Line Arguments**:
- ✅ **ACCEPTS ARGUMENTS** via `argparse`
- **Required**: `--bucket-name` (string)

**Usage Example**:
```bash
python machine-learning/infer_all.py --bucket-name my-business-bucket
```

**Sub-modules** (`general/infer.py` and `specific/infer.py`):
```python
# machine-learning/general/infer.py
def main(BUCKET_NAME):
    # Classification models
    cart_abandonment(BUCKET_NAME)
    customer_churn(BUCKET_NAME)
    customer_segments(BUCKET_NAME)
    payment_success(BUCKET_NAME)
    review_sentiment(BUCKET_NAME)
    stock_status(BUCKET_NAME)
    
    # Regression models
    aov(BUCKET_NAME)
    clv(BUCKET_NAME)
    restock_quantity(BUCKET_NAME)
    revenue_forecast(BUCKET_NAME)
    safety_stock(BUCKET_NAME)
    session_conversion(BUCKET_NAME)
    stockout_probability(BUCKET_NAME)
    
    # Clustering models
    customer_segment(BUCKET_NAME)
    geo_cluster(BUCKET_NAME)
    session_behavior(BUCKET_NAME)
    supplier_performance(BUCKET_NAME)

if __name__ == "__main__":
    BUCKET_NAME = "pulse-bucket-1"  # Fallback default
    main(BUCKET_NAME)
```

**General ML Models** (20 models):
- **Classification**: cart_abandonment, customer_churn, customer_segments, payment_success, review_sentiment, stock_status
- **Regression**: aov, clv, restock_quantity, revenue_forecast, safety_stock, session_conversion, stockout_probability
- **Clustering**: customer_segment, geo_cluster, session_behavior, supplier_performance

**Specific ML Models** (8 models):
- **Classification**: fulfillment_risk, product_bundling
- **Regression**: campaign_roi, delivery_time, demand_forecasting, price_optimization
- **Clustering**: product_affinity, product_lifecycle

**Input**: MinIO `{bucket}/transformed/*.parquet` files  
**Output**: MinIO `{bucket}/ml-predictions/*.parquet` files

---

## Database Tables and Purpose

### PostgreSQL Schema (`sql/schema.sql`)

#### 1. **users**
**Purpose**: Store user account information for authentication and authorization.

```sql
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NULL,
    reset_token TEXT NULL,
    reset_token_expires TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Fields**:
- `user_id`: Unique identifier (UUID)
- `email`: Unique email for login
- `password_hash`: Hashed password (bcrypt/argon2)
- `reset_token`: Password reset token

---

#### 2. **businesses**
**Purpose**: Store business entities owned by users. Each business gets its own MinIO bucket.

```sql
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_name VARCHAR(255) NOT NULL,
    business_region VARCHAR(100),
    business_currency VARCHAR(50),
    ingestion_type VARCHAR(50) CHECK (ingestion_type IN ('batch', 'db', 'api')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

**Key Fields**:
- `business_id`: **Unique identifier (UUID) - ALSO USED AS MINIO BUCKET NAME**
- `user_id`: Owner of the business
- `business_name`: Display name
- `business_region`: ISO country code (e.g., "US", "IN")
- `business_currency`: ISO currency code (e.g., "USD", "EUR")
- `ingestion_type`: How data is ingested (`batch`, `db`, `api`)

**Critical Relationship**:
```
business_id = MinIO bucket name
Example: business_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
         MinIO bucket = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

---

#### 3. **admins**
**Purpose**: Store admin accounts with elevated privileges.

```sql
CREATE TABLE admins (
    admin_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    reset_token VARCHAR(500),
    reset_token_expires TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

#### 4. **onboarding**
**Purpose**: Track user onboarding progress through the multi-step wizard.

```sql
CREATE TABLE onboarding (
    onboarding_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_id VARCHAR(50) NULL,
    current_step VARCHAR(100) NOT NULL DEFAULT 'business' 
        CHECK (current_step IN ('business', 'data-type', 'connect', 'mapping', 'mapping-in-progress')),
    ingestion_type VARCHAR(50) NULL CHECK (ingestion_type IN ('batch', 'db', 'api')),
    is_completed BOOLEAN DEFAULT FALSE,
    mapping_status VARCHAR(50) NULL CHECK (mapping_status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    mapping_error TEXT NULL,
    mapping_started_at TIMESTAMP NULL,
    mapping_completed_at TIMESTAMP NULL,
    mapping_results JSONB NULL,
    manual_mappings JSONB NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id)
);
```

**Onboarding Steps**:
1. `business`: Create business entity (name, region, currency)
2. `data-type`: Select ingestion type (batch/db/api)
3. `connect`: Upload files or connect to data source
4. `mapping`: Map uploaded data to canonical schema
5. `mapping-in-progress`: Mapping process running

**Mapping Status Lifecycle**:
- `pending` → `running` → `completed` (success)
- `pending` → `running` → `failed` (error with `mapping_error`)
- `pending` → `cancelled` (user cancelled)

---

#### 5. **uploaded_files**
**Purpose**: Track files uploaded to MinIO during onboarding.

```sql
CREATE TABLE uploaded_files (
    file_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(100),
    s3_key VARCHAR(1000) NOT NULL,
    upload_status VARCHAR(50) DEFAULT 'uploading' 
        CHECK (upload_status IN ('uploading', 'completed', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
);
```

**Key Fields**:
- `s3_key`: Full MinIO path (e.g., `{business_id}/ingested/orders.csv`)
- `upload_status`: Upload lifecycle (`uploading` → `completed` or `failed`)

**Cascade Behavior**: When a business is deleted, all uploaded files are also deleted.

---

#### 6. **nifi_schemas**
**Purpose**: Store Avro schemas for NiFi data validation.

```sql
CREATE TABLE IF NOT EXISTS nifi_schemas (
    schema_name VARCHAR(255) PRIMARY KEY,
    schema_text TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Default Schema**: `pulse_schema` (Avro record format)

---

## MinIO Storage Architecture

### MinIO Overview
- **Type**: S3-compatible object storage
- **Container IP**: 10.5.0.4
- **Ports**: 9000 (API), 9001 (Console UI)
- **Credentials**: From environment variables (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`)

### Bucket Structure

#### **Multi-Tenant Architecture**
Each business gets its own bucket:

```
MinIO Root
├── pulse-bucket-1/              # Default/legacy bucket (hardcoded)
│   ├── ingested/                # Raw uploaded files
│   ├── mapped/                  # Schema-mapped CSV files
│   ├── cleaned/                 # Cleaned CSV files
│   ├── transformed/             # Aggregated Parquet files (agg_*)
│   ├── analytics/               # Analytics results
│   └── ml-predictions/          # ML model predictions
│
├── {business-id-1}/             # Business-specific bucket (UUID)
│   ├── ingested/
│   ├── mapped/
│   ├── cleaned/
│   ├── transformed/
│   └── ml-predictions/
│
└── {business-id-2}/             # Another business
    └── ...
```

### Directory Purposes

#### 1. **ingested/** (Input Layer)
**Created by**: Frontend file upload OR NiFi data ingestion  
**Format**: Original uploaded format (CSV, Excel, JSON, Parquet)  
**Path Pattern**: `{business_id}/ingested/{filename}.{ext}`  
**Example**: `a1b2c3d4-e5f6/ingested/orders.csv`

**Contains**: Raw, unprocessed data as uploaded by the user.

---

#### 2. **mapped/** (Schema Mapping Layer)
**Created by**: `mapping/run_mapping.py` script  
**Format**: CSV files conforming to canonical schema  
**Path Pattern**: `{business_id}/mapped/{table_name}.csv`  
**Example**: `a1b2c3d4-e5f6/mapped/orders.csv`

**Purpose**: 
- Maps user's custom column names to Pulse's standardized schema
- Ensures all downstream processes work with consistent field names
- Example mapping: User's "OrderID" → Pulse's "order_id"

**Canonical Tables** (15 tables):
```
addresses, categories, customer_sessions, customers,
inventory, marketing_campaigns, order_items, orders,
payments, products, reviews, shopping_cart, cart_items,
suppliers, wishlist
```

---

#### 3. **cleaned/** (Data Cleaning Layer)
**Created by**: `cleaning/cleaning.py` script  
**Format**: CSV files  
**Path Pattern**: `{business_id}/cleaned/{table_name}.csv`  
**Example**: `a1b2c3d4-e5f6/cleaned/orders.csv`

**Cleaning Operations**:
- Schema casting (enforce data types)
- Duplicate removal
- Null value handling (drop/fill/impute)
- Outlier removal
- Date/timestamp normalization
- Gibberish pattern detection
- Text cleaning (whitespace, mixed scripts, non-ASCII)
- Numeric string validation

---

#### 4. **transformed/** (Aggregation Layer)
**Created by**: `transformation/transformation.py` script  
**Format**: Parquet files (Snappy compression)  
**Path Pattern**: `{business_id}/transformed/agg_{table_name}.parquet`  
**Example**: `a1b2c3d4-e5f6/transformed/agg_orders.parquet`

**Purpose**:
- Pre-compute aggregations for fast dashboard loading
- Enforce SQL schema from `sql/agg_schema.sql`
- Store in columnar format (Parquet) for efficient analytics queries

**Aggregation Types**:

**Entity Aggregations** (13 tables):
```
agg_customers           - Customer-level metrics
agg_orders              - Order-level metrics
agg_products            - Product-level metrics
agg_order_items         - Line item metrics
agg_payments            - Payment metrics
agg_marketing_campaigns - Campaign metrics
agg_suppliers           - Supplier metrics
agg_inventory           - Inventory metrics
agg_customer_sessions   - Session metrics
agg_wishlist            - Wishlist metrics
agg_shopping_cart       - Cart metrics
agg_cart_items          - Cart item metrics
agg_reviews             - Review metrics
```

**Time-Based Aggregations** (3 tables):
```
agg_daily_aggregations   - Day-level metrics
agg_weekly_aggregations  - Week-level metrics
agg_monthly_aggregations - Month-level metrics
```

**Geographic Aggregations** (3 tables):
```
agg_country_aggregations - Country-level metrics
agg_state_aggregations   - State/province-level metrics
agg_city_aggregations    - City-level metrics
```

**Advanced Analytics** (7 tables):
```
agg_categories               - Category performance
agg_cart_abandonment_analysis - Cart abandonment insights
agg_product_inventory_health - Inventory health scores
agg_supplier_inventory_health - Supplier performance
agg_rfm_segmentation         - RFM customer segments
agg_rfm_segment_summary      - RFM segment summaries
agg_product_affinity         - Product affinity matrix
agg_top_product_pairs        - Frequently bought together
agg_product_recommendations  - Product recommendation scores
agg_category_affinity        - Category cross-sell matrix
agg_global_aggregations      - Global KPIs
```

**Total**: 29 aggregation tables

---

#### 5. **analytics/** (Advanced Analytics Layer)
**Created by**: `analysis/analysis.py` script  
**Format**: Parquet files  
**Path Pattern**: `{business_id}/analytics/{analysis_name}.parquet`

**Purpose**: Store results of complex analytical computations (cohort analysis, trend analysis, forecasts).

---

#### 6. **ml-predictions/** (Machine Learning Layer)
**Created by**: `machine-learning/infer_all.py` script  
**Format**: Parquet files  
**Path Pattern**: `{business_id}/ml-predictions/{model_name}.parquet`  
**Example**: `a1b2c3d4-e5f6/ml-predictions/customer_churn.parquet`

**Contains**: Predictions from 28 ML models (20 general + 8 specific).

---

### Data Access Pattern

All scripts use a common pattern to access MinIO:

```python
# 1. Create MinIO client
from minio import Minio
minio_client = Minio(
    endpoint="minio:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False  # HTTP, not HTTPS
)

# 2. Read from MinIO using Spark
df = spark.read.csv(f"s3a://{bucket_name}/cleaned/orders.csv", header=True)

# 3. Write to MinIO using Spark
df.write.parquet(f"s3a://{bucket_name}/transformed/agg_orders.parquet", mode="overwrite")

# 4. Direct MinIO operations (Python SDK)
minio_client.put_object(
    bucket_name=bucket_name,
    object_name="cleaned/orders.csv",
    data=csv_buffer,
    length=len(csv_buffer.getvalue())
)
```

**Spark S3A Configuration** (in all scripts):
```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

---

## Overall Workflow: Onboarding to Dashboard

### Complete User Journey

```
┌──────────────────────────────────────────────────────────────────┐
│                     USER REGISTRATION                             │
│  POST /auth/register → users table                               │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                  STEP 1: CREATE ONBOARDING                        │
│  POST /onboarding/create → onboarding table                      │
│  current_step = 'business'                                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                  STEP 2: CREATE BUSINESS                          │
│  POST /onboarding/create-business                                │
│  → INSERT INTO businesses (business_id, user_id, ...)            │
│  → CREATE MinIO bucket: {business_id}                            │
│  → UPDATE onboarding SET business_id, current_step='data-type'   │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                 STEP 3: SELECT INGESTION TYPE                     │
│  POST /onboarding/select-ingestion-type                          │
│  → UPDATE onboarding SET ingestion_type ('batch'|'db'|'api')     │
│  → UPDATE businesses SET ingestion_type                          │
│  → UPDATE onboarding SET current_step='connect'                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                ┌────────┴────────┬────────────┐
                │                 │            │
             BATCH              DB          API
                │                 │            │
                ↓                 ↓            ↓
┌──────────────────────┐  ┌──────────────┐  ┌──────────────┐
│  UPLOAD FILES        │  │  CONFIGURE   │  │  CONFIGURE   │
│  (CSV/Excel/JSON)    │  │  DB CDC      │  │  API POLL    │
│                      │  │  CONNECTION  │  │  ENDPOINT    │
│  POST /upload-chunk  │  │              │  │              │
│  → MinIO:            │  │  Debezium    │  │  NiFi HTTP   │
│  {biz}/ingested/     │  │  → Kafka     │  │  → Kafka     │
│                      │  │  → MinIO     │  │  → MinIO     │
└──────────┬───────────┘  └──────┬───────┘  └──────┬───────┘
           │                     │                  │
           └──────────┬──────────┴──────────────────┘
                      │
                      ↓
┌──────────────────────────────────────────────────────────────────┐
│                    STEP 4: MAPPING PHASE                          │
│  Triggered automatically after file upload or streaming          │
│  Script: mapping/run_mapping.py                                  │
│                                                                   │
│  1. Read from: {business_id}/ingested/*                          │
│  2. Algorithm detects column mappings:                           │
│     - Fuzzy matching (user columns → canonical schema)           │
│     - ML-based similarity                                        │
│     - Manual corrections via UI                                  │
│  3. Write to: {business_id}/mapped/*.csv                         │
│                                                                   │
│  Status updates: onboarding.mapping_status                       │
│    'pending' → 'running' → 'completed' (or 'failed')             │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                   PIPELINE STAGE 1: CLEANING                      │
│  Script: cleaning/cleaning.py                                    │
│  Container: python (10.5.0.2)                                    │
│                                                                   │
│  Input:  {business_id}/mapped/*.csv                              │
│  Output: {business_id}/cleaned/*.csv                             │
│                                                                   │
│  Operations:                                                      │
│  - Schema casting                                                │
│  - Table merging (joins on foreign keys)                         │
│  - Duplicate removal                                             │
│  - Null handling (drop/fill/impute)                              │
│  - Outlier detection & removal                                   │
│  - Date/timestamp normalization                                  │
│  - Gibberish detection & cleaning                                │
│  - Text cleaning (whitespace, scripts, ASCII)                    │
│  - Data validation                                               │
│                                                                   │
│  Duration: ~5-15 minutes (depends on data size)                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│               PIPELINE STAGE 2: TRANSFORMATION                    │
│  Script: transformation/transformation.py                        │
│  Container: python (10.5.0.2)                                    │
│                                                                   │
│  Input:  {business_id}/cleaned/*.csv                             │
│  Output: {business_id}/transformed/agg_*.parquet                 │
│                                                                   │
│  Operations:                                                      │
│  1. Transform raw tables:                                        │
│     - Enrich orders (metrics, derived fields)                    │
│     - Enrich customers (lifetime value, segments)                │
│     - Enrich products (performance scores)                       │
│                                                                   │
│  2. Create aggregations (29 tables):                             │
│     - Time-based (daily/weekly/monthly)                          │
│     - Geographic (country/state/city)                            │
│     - Advanced analytics (RFM, affinity, inventory)              │
│                                                                   │
│  3. Schema enforcement:                                           │
│     - Read sql/agg_schema.sql                                    │
│     - Enforce column presence & types                            │
│     - Write as Parquet (Snappy compression)                      │
│                                                                   │
│  Duration: ~10-30 minutes (depends on data size)                 │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                 PIPELINE STAGE 3: ANALYSIS                        │
│  Script: analysis/analysis.py                                    │
│  Container: python (10.5.0.2)                                    │
│                                                                   │
│  Input:  {business_id}/transformed/agg_*.parquet                 │
│  Output: {business_id}/analytics/*.parquet                       │
│                                                                   │
│  Analytics Generated:                                             │
│  - Core KPIs (revenue, orders, customers, AOV)                   │
│  - Customer cohort analysis                                      │
│  - Customer retention curves                                     │
│  - Product performance rankings                                  │
│  - Category performance                                          │
│  - Geographic heatmaps                                           │
│  - Campaign ROI analysis                                         │
│  - Session funnel analysis                                       │
│                                                                   │
│  Duration: ~5-20 minutes                                         │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│              PIPELINE STAGE 4: ML INFERENCE                       │
│  Script: machine-learning/infer_all.py --bucket-name {biz_id}    │
│  Container: python (10.5.0.2)                                    │
│                                                                   │
│  Input:  {business_id}/transformed/agg_*.parquet                 │
│  Output: {business_id}/ml-predictions/*.parquet                  │
│                                                                   │
│  Models Executed (28 total):                                     │
│                                                                   │
│  GENERAL MODELS (20):                                            │
│  Classification:                                                 │
│    - cart_abandonment, customer_churn, customer_segments         │
│    - payment_success, review_sentiment, stock_status             │
│  Regression:                                                     │
│    - aov, clv, restock_quantity, revenue_forecast                │
│    - safety_stock, session_conversion, stockout_probability      │
│  Clustering:                                                     │
│    - customer_segment, geo_cluster, session_behavior             │
│    - supplier_performance                                        │
│                                                                   │
│  SPECIFIC MODELS (8):                                            │
│  Classification:                                                 │
│    - fulfillment_risk, product_bundling                          │
│  Regression:                                                     │
│    - campaign_roi, delivery_time, demand_forecasting             │
│    - price_optimization                                          │
│  Clustering:                                                     │
│    - product_affinity, product_lifecycle                         │
│                                                                   │
│  Duration: ~20-60 minutes (model loading + inference)            │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                      DASHBOARD READY                              │
│  Frontend: React app (port 5173)                                │
│  API: FastAPI (port 8000)                                        │
│                                                                   │
│  Data Sources:                                                    │
│  - PostgreSQL: Metadata (businesses, users)                      │
│  - MinIO: Analytics results, ML predictions                      │
│                                                                   │
│  Dashboard Features:                                              │
│  - KPI cards (revenue, orders, customers)                        │
│  - Time-series charts (daily/weekly/monthly trends)              │
│  - Customer segments (RFM analysis)                              │
│  - Product performance tables                                    │
│  - Geographic maps                                               │
│  - Campaign performance                                          │
│  - ML predictions (churn risk, forecasts)                        │
│                                                                   │
│  API Endpoint: GET /analytics/get-businesses?userId={user_id}    │
│  Returns: List of businesses for user                            │
│           User selects business → Load data from {business_id}   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Business ID Concept

### What is business_id?

**business_id** is a **UUID (Universally Unique Identifier)** that serves as:
1. **Primary key** in the `businesses` table
2. **MinIO bucket name** for data isolation
3. **Tenant identifier** for multi-tenancy

### Example

```
User: john@example.com (user_id: 123e4567-e89b-12d3-a456-426614174000)
Business: "Acme Corp" (business_id: a1b2c3d4-e5f6-7890-abcd-ef1234567890)

MinIO Structure:
a1b2c3d4-e5f6-7890-abcd-ef1234567890/
  ├── ingested/
  │   ├── orders.csv
  │   └── customers.csv
  ├── mapped/
  │   ├── orders.csv
  │   └── customers.csv
  ├── cleaned/
  │   ├── orders.csv
  │   └── customers.csv
  ├── transformed/
  │   ├── agg_orders.parquet
  │   └── agg_customers.parquet
  └── ml-predictions/
      ├── customer_churn.parquet
      └── revenue_forecast.parquet
```

### Why UUID?

- **Globally unique**: No collisions across users/businesses
- **No sequential IDs**: Prevents enumeration attacks
- **URL-safe**: Can be used in S3 bucket names
- **Database-friendly**: Supported by PostgreSQL UUID type

### Multi-Tenancy Pattern

```python
# API retrieves business_id from authenticated user
@router.get("/dashboard")
async def get_dashboard(user_id: str, db=Depends(get_db)):
    # Get user's businesses
    businesses = db.execute(
        "SELECT business_id, business_name FROM businesses WHERE user_id = :user_id",
        {"user_id": user_id}
    ).fetchall()
    
    # User selects a business
    selected_business_id = request.params.get("business_id")
    
    # Load data from MinIO using business_id as bucket
    analytics_data = load_from_minio(
        bucket_name=selected_business_id,
        path="analytics/kpis.parquet"
    )
    
    return analytics_data
```

---

## Data Flow Through the System

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├─────────────────┬───────────────────┬───────────────────────────┤
│ Batch Files     │ Database (CDC)    │ API Endpoints             │
│ (CSV, Excel,    │ (PostgreSQL,      │ (REST, GraphQL)           │
│  JSON, Parquet) │  MySQL, etc.)     │                           │
└────────┬────────┴────────┬──────────┴────────┬──────────────────┘
         │                 │                   │
         ↓                 ↓                   ↓
    ┌────────────────────────────────────────────┐
    │           INGESTION LAYER                  │
    │  - FastAPI (file upload) OR                │
    │  - Apache NiFi (batch/streaming)           │
    │  - Debezium (database CDC → Kafka)         │
    └────────────────────┬───────────────────────┘
                         │
                         ↓
              MinIO: {business_id}/ingested/
                         │
                         ↓
    ┌────────────────────────────────────────────┐
    │         MAPPING LAYER                      │
    │  Script: mapping/run_mapping.py            │
    │  - Column name mapping (fuzzy match)       │
    │  - Schema conformance                      │
    │  - Data type inference                     │
    └────────────────────┬───────────────────────┘
                         │
                         ↓
              MinIO: {business_id}/mapped/
                         │
                         ↓
    ┌────────────────────────────────────────────┐
    │         CLEANING LAYER                     │
    │  Script: cleaning/cleaning.py              │
    │  - Duplicates, nulls, outliers             │
    │  - Date normalization                      │
    │  - Text cleaning                           │
    └────────────────────┬───────────────────────┘
                         │
                         ↓
              MinIO: {business_id}/cleaned/
                         │
                         ↓
    ┌────────────────────────────────────────────┐
    │       TRANSFORMATION LAYER                 │
    │  Script: transformation/transformation.py  │
    │  - Feature engineering                     │
    │  - Aggregations (29 tables)                │
    │  - Schema enforcement                      │
    └────────────────────┬───────────────────────┘
                         │
                         ↓
              MinIO: {business_id}/transformed/
                         │
                ┌────────┴────────┐
                │                 │
                ↓                 ↓
    ┌─────────────────┐  ┌──────────────────┐
    │ ANALYSIS LAYER  │  │ ML INFERENCE     │
    │ analysis.py     │  │ infer_all.py     │
    │ - KPIs          │  │ - 28 models      │
    │ - Cohorts       │  │ - Predictions    │
    │ - Retention     │  │ - Forecasts      │
    └────────┬────────┘  └────────┬─────────┘
             │                    │
             ↓                    ↓
    MinIO: analytics/    ml-predictions/
             │                    │
             └─────────┬──────────┘
                       │
                       ↓
         ┌────────────────────────────┐
         │   PRESENTATION LAYER       │
         │  - FastAPI (REST API)      │
         │  - React Frontend          │
         │  - Dashboard Visualizations│
         └────────────────────────────┘
```

### Data Transformations at Each Stage

| Stage | Input Format | Output Format | Key Operations | Row Count Change |
|-------|-------------|---------------|----------------|------------------|
| **Ingestion** | User files | CSV/JSON | File upload, validation | No change |
| **Mapping** | CSV/JSON | CSV (canonical) | Column mapping | No change |
| **Cleaning** | CSV | CSV | Deduplication, null removal | ↓ (rows removed) |
| **Transformation** | CSV | Parquet | Aggregation, joins | ↓ (aggregated) |
| **Analysis** | Parquet | Parquet | Complex analytics | Variable |
| **ML Inference** | Parquet | Parquet | Predictions | ≈ (row per entity) |

### Example: Order Processing

```
1. User uploads: "my_orders.xlsx"
   → MinIO: {biz}/ingested/my_orders.xlsx

2. Mapping detects:
   "OrderID" → "order_id"
   "CustomerEmail" → "customer_email"
   "OrderDate" → "order_placed_at"
   → MinIO: {biz}/mapped/orders.csv

3. Cleaning:
   - Remove duplicates (10,000 → 9,500 rows)
   - Fill null shipping addresses
   - Convert dates to ISO 8601
   → MinIO: {biz}/cleaned/orders.csv

4. Transformation:
   - Join with customers, products
   - Calculate: total_revenue, avg_order_value
   - Create time aggregations
   → MinIO: {biz}/transformed/agg_orders.parquet
   → MinIO: {biz}/transformed/agg_daily_aggregations.parquet

5. Analysis:
   - Cohort analysis: Customers by signup month
   - Retention: Month-over-month retention rates
   → MinIO: {biz}/analytics/cohort_retention.parquet

6. ML Inference:
   - Revenue forecast: Next 30 days
   - Churn prediction: Customers at risk
   → MinIO: {biz}/ml-predictions/revenue_forecast.parquet
   → MinIO: {biz}/ml-predictions/customer_churn.parquet
```

---

## Current Limitations and Workarounds

### 1. ❌ Hardcoded Bucket Names

**Problem**: 
- `cleaning.py`, `transformation.py`, `analysis.py` all hardcode `"pulse-bucket-1"`
- Cannot process different businesses without code changes

**Current Code**:
```python
# cleaning/cleaning_config.py
def get_bucket_name():
    return "pulse-bucket-1"  # HARDCODED

# transformation/config/minio_config.py
BUCKET_NAME = "pulse-bucket-1"  # HARDCODED
```

**Impact**:
- Multi-tenancy not fully implemented
- All users share same bucket (data isolation risk)
- Scripts only work for default bucket

**Workaround Options**:

#### Option A: Environment Variable (Quick Fix)
```python
# cleaning/cleaning_config.py
def get_bucket_name():
    return os.getenv("MINIO_BUCKET", "pulse-bucket-1")

# Usage:
export MINIO_BUCKET="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
python cleaning/cleaning.py
```

#### Option B: Command-Line Argument (Best Practice)
```python
# cleaning/cleaning.py
import argparse

def main():
    parser = argparse.ArgumentParser(description='Data cleaning pipeline')
    parser.add_argument('--bucket-name', type=str, required=True,
                       help='MinIO bucket name (business_id)')
    args = parser.parse_args()
    
    bucket_name = args.bucket_name
    # ... rest of code

# Usage:
python cleaning/cleaning.py --bucket-name a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

#### Option C: API-Triggered Execution
```python
# api/routers/pipeline.py
@router.post("/run-cleaning")
async def run_cleaning(business_id: str, db=Depends(get_db)):
    # Verify business exists
    business = db.execute(
        "SELECT * FROM businesses WHERE business_id = :bid",
        {"bid": business_id}
    ).fetchone()
    
    if not business:
        raise HTTPException(404, "Business not found")
    
    # Run cleaning with subprocess
    subprocess.run([
        "python", "/app/cleaning/cleaning.py",
        "--bucket-name", business_id
    ])
```

**Recommended Solution**: 
Implement **Option B** for all scripts (`cleaning.py`, `transformation.py`, `analysis.py`) to match `infer_all.py` which already accepts `--bucket-name`.

---

### 2. ❌ No Pipeline Orchestration

**Problem**:
- Scripts must be run manually in correct order
- No automatic triggering after onboarding
- No dependency management

**Current Flow**:
```bash
# Manual execution (correct order required)
python mapping/run_mapping.py           # Step 1
python cleaning/cleaning.py             # Step 2 (depends on Step 1)
python transformation/transformation.py # Step 3 (depends on Step 2)
python analysis/analysis.py             # Step 4 (depends on Step 3)
python machine-learning/infer_all.py --bucket-name {biz_id}  # Step 5 (depends on Step 3)
```

**Workaround Options**:

#### Option A: Shell Script
```bash
#!/bin/bash
# pipeline.sh
BUSINESS_ID=$1

if [ -z "$BUSINESS_ID" ]; then
  echo "Usage: ./pipeline.sh <business_id>"
  exit 1
fi

set -e  # Exit on error

echo "Running mapping..."
python mapping/run_mapping.py --bucket-name $BUSINESS_ID

echo "Running cleaning..."
python cleaning/cleaning.py --bucket-name $BUSINESS_ID

echo "Running transformation..."
python transformation/transformation.py --bucket-name $BUSINESS_ID

echo "Running analysis..."
python analysis/analysis.py --bucket-name $BUSINESS_ID

echo "Running ML inference..."
python machine-learning/infer_all.py --bucket-name $BUSINESS_ID

echo "Pipeline complete!"
```

#### Option B: Apache Airflow (Production)
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'pulse_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Triggered manually
)

mapping = BashOperator(
    task_id='mapping',
    bash_command='python /app/mapping/run_mapping.py --bucket-name {{ dag_run.conf["business_id"] }}',
    dag=dag
)

cleaning = BashOperator(
    task_id='cleaning',
    bash_command='python /app/cleaning/cleaning.py --bucket-name {{ dag_run.conf["business_id"] }}',
    dag=dag
)

transformation = BashOperator(
    task_id='transformation',
    bash_command='python /app/transformation/transformation.py --bucket-name {{ dag_run.conf["business_id"] }}',
    dag=dag
)

# ... more tasks

mapping >> cleaning >> transformation >> analysis >> ml_inference
```

#### Option C: FastAPI Background Tasks
```python
from fastapi import BackgroundTasks
import subprocess

@router.post("/trigger-pipeline")
async def trigger_pipeline(
    business_id: str,
    background_tasks: BackgroundTasks,
    db=Depends(get_db)
):
    def run_pipeline(biz_id):
        steps = [
            ["python", "mapping/run_mapping.py", "--bucket-name", biz_id],
            ["python", "cleaning/cleaning.py", "--bucket-name", biz_id],
            ["python", "transformation/transformation.py", "--bucket-name", biz_id],
            ["python", "analysis/analysis.py", "--bucket-name", biz_id],
            ["python", "machine-learning/infer_all.py", "--bucket-name", biz_id],
        ]
        
        for step in steps:
            result = subprocess.run(step, capture_output=True)
            if result.returncode != 0:
                # Log error, update status in DB
                break
    
    background_tasks.add_task(run_pipeline, business_id)
    return {"status": "Pipeline started"}
```

---

### 3. ⚠️ Configuration Management

**Problem**:
- Each script has separate config files
- Duplicate code for MinIO/Spark setup
- Environment variables scattered

**Current Structure**:
```
cleaning/cleaning_config.py       - Spark + MinIO config
transformation/config/minio_config.py  - MinIO config
transformation/config/spark_config.py  - Spark config
analysis/analysis_config.py        - Spark config
machine-learning/*/config.py       - Model-specific configs
```

**Recommended**: Centralize configuration
```
config/
  ├── __init__.py
  ├── spark.py       - Shared Spark session factory
  ├── minio.py       - Shared MinIO client factory
  ├── database.py    - PostgreSQL connection
  └── settings.py    - Environment variable loading (python-decouple)
```

---

### 4. ⚠️ Error Handling & Logging

**Problem**:
- Scripts print to stdout (not centralized logs)
- No error recovery mechanisms
- Failed pipelines require manual debugging

**Recommendation**:
```python
import logging
import sys

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/var/log/pulse/{script_name}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

try:
    # Pipeline logic
    logger.info(f"Starting cleaning for bucket: {bucket_name}")
    # ...
except Exception as e:
    logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
    # Update database status
    update_pipeline_status(business_id, "failed", str(e))
    raise
```

---

### 5. ✅ What's Working Well

**infer_all.py**:
- ✅ Accepts `--bucket-name` argument
- ✅ Passes bucket_name to all sub-modules
- ✅ Can be called with any business_id

**Mapping Pipeline**:
- ✅ Uses business_id from CONFIG
- ✅ Updates database status (onboarding.mapping_status)
- ✅ Error handling with status updates

**Database Schema**:
- ✅ Well-designed multi-tenancy (business_id)
- ✅ Proper foreign keys and constraints
- ✅ Automatic timestamp triggers

**MinIO Structure**:
- ✅ Clear directory hierarchy (ingested → mapped → cleaned → transformed)
- ✅ Supports multi-tenant buckets
- ✅ S3-compatible (portable)

---

## Next Steps for Full Multi-Tenancy

1. **Add CLI Arguments** (Priority: HIGH)
   ```bash
   python cleaning/cleaning.py --bucket-name {business_id}
   python transformation/transformation.py --bucket-name {business_id}
   python analysis/analysis.py --bucket-name {business_id}
   ```

2. **Create Orchestration** (Priority: MEDIUM)
   - Option: Shell script for MVP
   - Option: Airflow for production

3. **Centralize Configuration** (Priority: MEDIUM)
   - Single config module for Spark/MinIO
   - Environment variable validation

4. **Add Monitoring** (Priority: LOW)
   - Pipeline execution logs
   - Metrics (execution time, row counts)
   - Alerting on failures

5. **API Integration** (Priority: HIGH)
   - POST /pipeline/run endpoint
   - Background task execution
   - Real-time status updates

---

## Summary

### How to Pass bucket_name to Each Script

**Current State**:
| Script | Accepts CLI Args? | Bucket Name Source | How to Pass |
|--------|-------------------|-------------------|-------------|
| `cleaning.py` | ❌ No | Hardcoded `"pulse-bucket-1"` | **Needs modification** |
| `transformation.py` | ❌ No | Hardcoded `"pulse-bucket-1"` | **Needs modification** |
| `analysis.py` | ❌ No | Env var or default | Set `MINIO_BUCKET` env var |
| `infer_all.py` | ✅ Yes | `--bucket-name` argument | `--bucket-name {biz_id}` |

**Recommended Changes**:
Add argparse to `cleaning.py`, `transformation.py`, `analysis.py` to accept `--bucket-name`.

---

### What business_id Represents

- **Database**: Primary key in `businesses` table
- **MinIO**: Bucket name for data isolation
- **Multi-Tenancy**: Unique identifier for each customer/tenant
- **Format**: UUID v4 (e.g., `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
- **Lifecycle**: Created during onboarding, persists forever

---

### How Data Flows

```
User Upload → Ingested (raw) → Mapped (schema) → Cleaned (quality) 
→ Transformed (aggregated) → Analytics (insights) + ML Predictions (forecasts)
→ Dashboard (visualizations)
```

Each stage stored in MinIO under `{business_id}/{stage}/` directory.

---

**For questions or clarifications, see**:
- `IMPLEMENTATION_ISSUES_AND_RECOMMENDATIONS.md` - Known issues
- `NIFI_SETUP_GUIDE.md` - Data ingestion architecture
- `ML_MODELS_DOCUMENTATION.md` - Machine learning models
