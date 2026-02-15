# Pulse System - Quick Reference Guide

## 🚀 Running the Pipeline

### Current State (Hardcoded Bucket)
```bash
# These scripts use "pulse-bucket-1" hardcoded
python cleaning/cleaning.py                    # No args accepted
python transformation/transformation.py        # No args accepted  
python analysis/analysis.py                    # No args accepted

# This one DOES accept arguments ✅
python machine-learning/infer_all.py --bucket-name {business_id}
```

### Recommended: Add CLI Arguments
```bash
# After modification (see PULSE_SYSTEM_EXPLANATION.md)
python cleaning/cleaning.py --bucket-name {business_id}
python transformation/transformation.py --bucket-name {business_id}
python analysis/analysis.py --bucket-name {business_id}
python machine-learning/infer_all.py --bucket-name {business_id}
```

### Environment Variable Workaround
```bash
# For analysis.py (reads MINIO_BUCKET env var)
export MINIO_BUCKET="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
python analysis/analysis.py
```

---

## 📊 Data Flow Cheat Sheet

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  INGESTED  │────▶│   MAPPED   │────▶│  CLEANED   │────▶│TRANSFORMED │
│  (raw)     │     │ (schema)   │     │ (quality)  │     │  (agg)     │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
     │                   │                  │                   │
     │                   │                  │                   ▼
     │                   │                  │            ┌──────────────┐
     │                   │                  │            │  ANALYTICS   │
     │                   │                  │            │  (insights)  │
     │                   │                  │            └──────────────┘
     │                   │                  │                   │
     ▼                   ▼                  ▼                   ▼
  CSV/Excel/JSON      CSV files         CSV files        Parquet files
  Original format     15 tables         15 tables        29 agg tables
```

**Stages**:
1. **Ingested**: User uploads → `{bucket}/ingested/`
2. **Mapped**: Schema mapping → `{bucket}/mapped/*.csv`
3. **Cleaned**: Data cleaning → `{bucket}/cleaned/*.csv`
4. **Transformed**: Aggregations → `{bucket}/transformed/agg_*.parquet`
5. **Analytics**: Advanced analytics → `{bucket}/analytics/*.parquet`
6. **ML Predictions**: Model inference → `{bucket}/ml-predictions/*.parquet`

---

## 🗂️ MinIO Directory Structure

```
{business_id}/                    # UUID bucket name
├── ingested/                     # Raw uploaded files
│   ├── orders.csv
│   ├── customers.xlsx
│   └── products.json
├── mapped/                       # Schema-mapped CSV files (15 tables)
│   ├── orders.csv
│   ├── customers.csv
│   ├── products.csv
│   └── ... (12 more tables)
├── cleaned/                      # Cleaned CSV files (15 tables)
│   └── (same structure as mapped/)
├── transformed/                  # Aggregated Parquet files (29 tables)
│   ├── agg_customers.parquet
│   ├── agg_orders.parquet
│   ├── agg_daily_aggregations.parquet
│   ├── agg_rfm_segmentation.parquet
│   └── ... (25 more tables)
├── analytics/                    # Analytics results
│   └── cohort_retention.parquet
└── ml-predictions/               # ML model outputs (28 models)
    ├── customer_churn.parquet
    ├── revenue_forecast.parquet
    └── ... (26 more models)
```

---

## 🗃️ Database Tables (PostgreSQL)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| **users** | User accounts | `user_id`, `email`, `password_hash` |
| **businesses** | Business entities | `business_id` (=bucket), `user_id`, `business_name` |
| **onboarding** | Onboarding progress | `business_id`, `current_step`, `mapping_status` |
| **uploaded_files** | File metadata | `business_id`, `file_name`, `s3_key` |
| **admins** | Admin accounts | `admin_id`, `email` |
| **nifi_schemas** | Avro schemas | `schema_name`, `schema_text` |

### Key Relationships
```
users (1) ──< (N) businesses
             └──< (N) uploaded_files
users (1) ──< (1) onboarding
businesses (1) ──< (N) uploaded_files
```

---

## 🔑 What is business_id?

- **Type**: UUID v4 (e.g., `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
- **Created**: During onboarding (Step 2: Create Business)
- **Used as**:
  1. Primary key in `businesses` table
  2. **MinIO bucket name** (data isolation)
  3. Tenant identifier (multi-tenancy)

### Example
```
User: john@example.com
Business: "Acme Corp"
business_id: a1b2c3d4-e5f6-7890-abcd-ef1234567890

MinIO Bucket Name: a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

---

## 📝 Script Summary

### 1. cleaning.py
- **Input**: `{bucket}/mapped/*.csv` (15 tables)
- **Output**: `{bucket}/cleaned/*.csv` (15 tables)
- **Operations**: Dedup, nulls, outliers, dates, text cleaning
- **CLI Args**: ❌ None (hardcoded bucket)

### 2. transformation.py
- **Input**: `{bucket}/cleaned/*.csv` (15 tables)
- **Output**: `{bucket}/transformed/agg_*.parquet` (29 tables)
- **Operations**: Transformations + aggregations
- **CLI Args**: ❌ None (hardcoded bucket)

### 3. analysis.py
- **Input**: `{bucket}/transformed/agg_*.parquet` (29 tables)
- **Output**: `{bucket}/analytics/*.parquet`
- **Operations**: KPIs, cohorts, retention, trends
- **CLI Args**: ❌ None (uses env var `MINIO_BUCKET`)

### 4. infer_all.py
- **Input**: `{bucket}/transformed/agg_*.parquet`
- **Output**: `{bucket}/ml-predictions/*.parquet` (28 models)
- **Operations**: ML inference (classification, regression, clustering)
- **CLI Args**: ✅ `--bucket-name {business_id}`

---

## 🔧 How to Modify Scripts for Multi-Tenancy

### Add argparse to cleaning.py

```python
# cleaning/cleaning.py
import argparse

def main():
    # Add argument parser
    parser = argparse.ArgumentParser(description='Data cleaning pipeline')
    parser.add_argument('--bucket-name', type=str, required=True,
                       help='MinIO bucket name (business_id)')
    args = parser.parse_args()
    
    # Use args.bucket_name instead of get_bucket_name()
    spark = create_spark_session()
    minio_client = create_minio_client()
    bucket_name = args.bucket_name  # ← Change here
    
    # Rest of code remains same
    dataframes = load_data_from_minio(spark, minio_client, bucket_name, table_names)
    # ...
```

### Add argparse to transformation.py

```python
# transformation/transformation.py
import argparse

def main():
    parser = argparse.ArgumentParser(description='Data transformation pipeline')
    parser.add_argument('--bucket-name', type=str, required=True,
                       help='MinIO bucket name (business_id)')
    args = parser.parse_args()
    
    spark = create_spark_session()
    minio_client = create_minio_client()
    
    # Use args.bucket_name instead of BUCKET_NAME constant
    dataframes = load_data_from_minio(spark, minio_client, args.bucket_name)
    # ...
    export_to_minio(dataframes, ...)  # Update to use args.bucket_name
```

### Add argparse to analysis.py

```python
# analysis/analysis.py (around line 45)
import argparse

def main():
    parser = argparse.ArgumentParser(description='Analytics pipeline')
    parser.add_argument('--bucket-name', type=str, required=True,
                       help='MinIO bucket name (business_id)')
    args = parser.parse_args()
    
    spark = create_spark_session("Ecommerce_Analysis_Main")
    
    # Pass bucket_name to get_agg_tables
    dataframes = get_agg_tables(spark, bucket_name=args.bucket_name)
    # ...
```

### Update analysis_utils.py

```python
# analysis/analysis_utils.py
def get_agg_tables(spark, bucket_name=None):
    """Load aggregated tables from MinIO."""
    if bucket_name is None:
        bucket_name = os.getenv("MINIO_BUCKET", "pulse-bucket-1")
    
    minio_client = get_minio_client()
    # Rest of code uses bucket_name
```

---

## 🎯 Complete Pipeline Execution

### Option 1: Manual Execution
```bash
BUSINESS_ID="a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Step 1: Mapping (already supports bucket)
python mapping/run_mapping.py  # Edit CONFIG in file

# Step 2: Cleaning
python cleaning/cleaning.py --bucket-name $BUSINESS_ID

# Step 3: Transformation
python transformation/transformation.py --bucket-name $BUSINESS_ID

# Step 4: Analysis
python analysis/analysis.py --bucket-name $BUSINESS_ID

# Step 5: ML Inference
python machine-learning/infer_all.py --bucket-name $BUSINESS_ID
```

### Option 2: Shell Script
```bash
#!/bin/bash
# run_pipeline.sh
BUSINESS_ID=$1

if [ -z "$BUSINESS_ID" ]; then
  echo "Usage: ./run_pipeline.sh <business_id>"
  exit 1
fi

set -e

echo "🧹 Cleaning..."
python cleaning/cleaning.py --bucket-name $BUSINESS_ID

echo "🔄 Transforming..."
python transformation/transformation.py --bucket-name $BUSINESS_ID

echo "📊 Analyzing..."
python analysis/analysis.py --bucket-name $BUSINESS_ID

echo "🤖 ML Inference..."
python machine-learning/infer_all.py --bucket-name $BUSINESS_ID

echo "✅ Pipeline complete!"
```

---

## 🌐 API Endpoints

### Get User's Businesses
```bash
GET /analytics/get-businesses?userId={user_id}

Response:
{
  "businesses": [
    {
      "business_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "business_name": "Acme Corp"
    }
  ]
}
```

### Onboarding Flow
```bash
1. POST /onboarding/create
   { "userId": "..." }
   
2. POST /onboarding/create-business
   { "userId": "...", "businessName": "...", "businessRegion": "...", "businessCurrency": "..." }
   → Creates business_id and MinIO bucket
   
3. POST /onboarding/upload-chunk
   (Upload files to MinIO)
   
4. Mapping runs automatically
   → Updates onboarding.mapping_status
```

---

## 🧠 ML Models (28 Total)

### General Models (20)
**Classification** (6): cart_abandonment, customer_churn, customer_segments, payment_success, review_sentiment, stock_status  
**Regression** (7): aov, clv, restock_quantity, revenue_forecast, safety_stock, session_conversion, stockout_probability  
**Clustering** (4): customer_segment, geo_cluster, session_behavior, supplier_performance

### Specific Models (8)
**Classification** (2): fulfillment_risk, product_bundling  
**Regression** (4): campaign_roi, delivery_time, demand_forecasting, price_optimization  
**Clustering** (2): product_affinity, product_lifecycle

---

## 📦 Canonical Tables (15)

```
addresses, categories, customer_sessions, customers,
inventory, marketing_campaigns, order_items, orders,
payments, products, reviews, shopping_cart, cart_items,
suppliers, wishlist
```

These are the standardized schema tables that all user data is mapped to.

---

## 🎨 Aggregation Tables (29)

**Entity-Level** (13): agg_customers, agg_orders, agg_products, agg_order_items, agg_payments, agg_marketing_campaigns, agg_suppliers, agg_inventory, agg_customer_sessions, agg_wishlist, agg_shopping_cart, agg_cart_items, agg_reviews

**Time-Based** (3): agg_daily_aggregations, agg_weekly_aggregations, agg_monthly_aggregations

**Geographic** (3): agg_country_aggregations, agg_state_aggregations, agg_city_aggregations

**Advanced** (10): agg_categories, agg_cart_abandonment_analysis, agg_product_inventory_health, agg_supplier_inventory_health, agg_rfm_segmentation, agg_rfm_segment_summary, agg_product_affinity, agg_top_product_pairs, agg_product_recommendations, agg_category_affinity, agg_global_aggregations

---

## 🐛 Known Issues

1. **Hardcoded Buckets**: `cleaning.py`, `transformation.py`, `analysis.py` use `"pulse-bucket-1"`
2. **No Orchestration**: Scripts must be run manually in order
3. **Scattered Config**: Multiple config files across modules
4. **Limited Error Handling**: No centralized logging or recovery

**See**: `IMPLEMENTATION_ISSUES_AND_RECOMMENDATIONS.md` for details

---

## 📚 Full Documentation

For complete details, see:
- **PULSE_SYSTEM_EXPLANATION.md** - Comprehensive system guide
- **IMPLEMENTATION_ISSUES_AND_RECOMMENDATIONS.md** - Known issues
- **NIFI_SETUP_GUIDE.md** - Data ingestion architecture
- **ML_MODELS_DOCUMENTATION.md** - Machine learning models

---

**Last Updated**: 2025-02-13
