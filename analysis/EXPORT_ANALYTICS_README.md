# Analytics Export to MinIO - Implementation Guide

## Overview

This document describes the implementation of analytics export functionality to MinIO data lake from the Pulse analytics engine. The system exports **188 analytics and insights** organized into three main dictionaries.

## Implementation Summary

### What Was Done

1. **Added Export Call** to `analysis_final.py` (line 7788+)
   - Automatically exports all generated analytics to MinIO after analysis completion
   - Uses the existing `export_analytics_to_minio()` function from `analysis_export_utils.py`
   - Exports in Parquet format by default (configurable to CSV or JSON)
   - Uses parallel processing with 8 workers for optimal performance

2. **Leveraged Existing Infrastructure**
   - `analysis_export_config.py`: Contains category mappings and MinIO configuration
   - `analysis_export_utils.py`: Contains export functions and serialization logic
   - `analysis_config.py`: Contains MinIO connection settings

### Analytics Breakdown (Total: 188)

#### 1. Analysis Dictionary (Customer & General Analytics): 130 items

**Business Health (4)**
- `business_health_daily`, `business_health_weekly`, `business_health_monthly`
- `low_margin_categories`

**Customer Acquisition & Growth (12)**
- `new_customers_daily`, `new_customers_weekly`, `new_customers_monthly`
- `cumulative_customers_daily`, `cumulative_customers_weekly`, `cumulative_customers_monthly`
- `customer_account_status_distribution_daily`, `customer_account_status_distribution_weekly`, `customer_account_status_distribution_monthly`
- `geo_acquisition`
- `new_customers_geo_acquisition_daily`, `new_customers_geo_acquisition_monthly`

**Customer Demographics (5)**
- `customer_age_group_distribution`
- `customer_city_distribution`
- `customer_state_distribution`
- `customer_country_distribution`
- `customer_age_group_spending`

**Customer Preferences (2)**
- `gender_category_preference`
- `gender_product_preference`

**Customer Segmentation (6)**
- `new_vs_returning_customer_country`, `new_vs_returning_customer_city`, `new_vs_returning_customer_state`
- `rfm_segment_summary`
- `customer_overall_health_summary`
- `high_intent_non_buyers`

**Customer Engagement (3)**
- `customer_engagement`
- `customer_engagement_summary`
- `session_to_order_analysis`

**Customer Value (11)**
- `top_customers_by_revenue`
- `top_customers_by_profit`
- `clv_summary`
- `customer_profit_per_segment`
- `segment_aov_by_rfm`
- `session_conversion_distribution`
- `cart_abandonment_distribution`
- `discount_customers`, `discount_customers_summary`
- `correlation_discount_vs_clv`
- `high_discount_customers`

**Revenue Analysis (6)**
- `rev_by_country_city`
- `rev_by_customer_segment`
- `rev_by_rfm_segment`
- `rev_by_segment_label`
- `rev_by_referrer`
- `rev_by_device`

**AOV Trends (3)**
- `aov_trend_daily`, `aov_trend_weekly`, `aov_trend_monthly`

**Churn & Risk (2)**
- `churn_risk_summary`
- `high_clv_at_risk`

**Cohort Analysis (3)**
- `customers_cohorts`
- `signup_cohort_summary`
- `customer_cohort_retention`

**Cross-dimensional Analysis (3)**
- `rfm_churn_crosstab`
- `seg_referrer_crosstab`
- `seg_device_crosstab`

**Payment Analysis (10)**
- `payment_method_vs_clv_churn`
- `payment_method_summary`
- `payment_counts_by_country_method`
- `payment_counts_by_state_method`
- `payment_method_success_rates`
- `payment_method_success_rates_by_country`
- `payment_method_aov`
- `refund_rate_by_payment_method`
- `refund_rate_by_product`
- `refund_rate_by_month`
- `time_to_refund_by_payment_method`

**Marketing Campaigns (7)**
- `campaign_performance_summary`
- `campaign_product_contribution`
- `campaign_ltv`
- `campaign_wasteful_campaigns`
- `campaign_margin_profile`
- `campaign_performance`
- `device_conversion_rates`

**Referrer & Channel (2)**
- `referrer_source_summary`
- `referrer_churn_summary`

**Conversion Funnel (6)**
- `high_value_funnel`
- `funnel_summary`
- `high_value_vs_regular`
- `funnel_by_device`
- `funnel_by_referrer`
- `abandoned_vs_converted`

**Cart Behavior (7)**
- `cart_behavior_summary`
- `high_value_abandoners`
- `cart_overall_stats`
- `cart_status_distribution`
- `cart_abandon_summary`
- `cart_value_stats`
- `high_value_abandoned_carts`

**Time to Purchase (3)**
- `time_to_purchase_overall`
- `time_to_purchase_by_tier`
- `time_to_purchase_buckets`

**Wishlist (9)**
- `wishlist_overall_summary`
- `wishlist_by_product`
- `wishlist_by_customer`
- `wishlist_time_to_purchase_stats`
- `wishlist_time_to_purchase_distribution`
- `abandoned_wishlist_items`
- `abandoned_wishlist_by_customer`
- `abandoned_wishlist_by_product`
- `wishlist_adds_by_month`

**Review Analysis (5)**
- `review_velocity_daily`, `review_velocity_weekly`, `review_velocity_monthly`
- `sentiment_by_category`
- `product_monthly_rating_trends`

**Rating Analysis (4)**
- `low_rated_product_monthly_trends_rating_only`
- `rating_tier_per_product`
- `rating_tier_sales_velocity`

**Operations & Fulfillment (16)**
- `processing_by_category`, `processing_by_subcategory`
- `processing_by_hour`
- `processing_by_day_of_week`
- `weekend_vs_weekday`
- `delivery_days_by_country`, `delivery_days_by_state`, `delivery_days_by_city`
- `ontime_delivery_by_country`, `ontime_delivery_by_state`, `ontime_delivery_by_city`
- `shipping_efficiency_by_country`, `shipping_efficiency_by_state`, `shipping_efficiency_by_city`
- `processing_by_season`, `processing_by_season_and_status`

**Inventory Carrying Cost (1)**
- `inventory_carrying_cost_overall`

#### 2. Product_Analysis Dictionary: 46 items

**Product Performance (5)**
- `best_selling_products`
- `highest_margin_products`
- `low_margin_high_traffic_products`
- `product_performance_score`
- `low_conversion_products`

**Product Trends (5)**
- `category_monthly_trends`
- `product_monthly_trends`
- `product_calendar_month_seasonality`
- `category_calendar_month_seasonality`
- `product_monthly_rating_trends`

**Category Analysis (5)**
- `category_revenue_share`
- `low_performing_categories`
- `category_popularity_score`
- `category_profitability`
- `category_view_patterns`

**Seasonality (2)**
- `category_monthly_seasonality`
- `category_peak_season`

**Product Lifecycle (2)**
- `product_lifecycle_segments`
- `product_lifecycle_summary`

**Stock & Inventory (10)**
- `out_of_stock_products`
- `product_stockout_risk`
- `product_stockout_replenishment`
- `product_dead_stock`
- `product_inventory_health`
- `product_inventory_critical`
- `inventory_stock_status`
- `days_of_supply`
- `overstock_analysis`
- `excess_inventory_not_selling`

**Reorder Management (2)**
- `sku_reorder_urgency`
- `reorder_point_breach_frequency`

**Supplier-Product Relations (4)**
- `supplier_product_performance`
- `stockout_rate_by_product`
- `supplier_ranking_core`
- `supplier_stockout_impact_on_products`

**Product Discovery (6)**
- `category_affinity_pairs`
- `category_affinity_top_per_category`
- `product_affinity_pairs`
- `product_affinity_top_per_product`
- `precomputed_product_recommendations`
- `precomputed_reco_coverage`

**Product Views & Ratings (3)**
- `product_rating_summary`
- `top_view_to_purchase_products`

**Reserved Stock (1)**
- `reserved_vs_available`

**Checkout Optimization (3)**
- `checkout_dropoff_reasons`
- `checkout_dropoff_buckets`
- `checkout_dropoff_by_device_and_reason`

#### 3. Supplier_Analysis Dictionary: 12 items

**Supplier Performance (4)**
- `supplier_reliability`
- `supplier_fulfillment_performance`
- `supplier_revenue_contribution`
- `supplier_profit_margin`

**Supplier Inventory (2)**
- `supplier_stockouts`
- `stockout_rate_by_supplier`

**Storage & Costs (4)**
- `storage_cost_efficiency_by_supplier`
- `inventory_carrying_cost_by_supplier`
- `inventory_carrying_cost_by_product`
- `margin_erosion_risk`

**Supplier Operations (2)**
- `supplier_days_since_last_restock`
- `supplier_contract_expiry`

## MinIO Export Structure

Analytics are organized in MinIO with the following structure:

```
s3://[bucket-name]/
├── analytics/
│   ├── kpis/
│   │   ├── business_health_daily.parquet
│   │   ├── clv_summary.parquet
│   │   └── ...
│   ├── customer_analytics/
│   │   ├── new_customers_daily.parquet
│   │   ├── customer_engagement.parquet
│   │   └── ...
│   ├── product_analytics/
│   │   ├── best_selling_products.parquet
│   │   ├── product_monthly_trends.parquet
│   │   └── ...
│   ├── supplier_analytics/
│   │   ├── supplier_reliability.parquet
│   │   ├── supplier_stockouts.parquet
│   │   └── ...
│   ├── marketing_analytics/
│   │   ├── campaign_performance_summary.parquet
│   │   └── ...
│   ├── revenue_analytics/
│   │   ├── rev_by_country_city.parquet
│   │   └── ...
│   ├── funnel_analytics/
│   │   ├── high_value_funnel.parquet
│   │   └── ...
│   ├── payment_analytics/
│   │   ├── payment_method_success_rates.parquet
│   │   └── ...
│   ├── review_analytics/
│   │   ├── review_velocity_daily.parquet
│   │   └── ...
│   ├── operations_analytics/
│   │   ├── processing_by_category.parquet
│   │   └── ...
│   ├── wishlist_analytics/
│   │   ├── wishlist_overall_summary.parquet
│   │   └── ...
│   ├── cart_analytics/
│   │   ├── cart_overall_stats.parquet
│   │   └── ...
│   └── metadata/
│       └── export_metadata.json
```

## Configuration

### Required Environment Variables

Add these to your `.env` file:

```bash
# MinIO Configuration
MINIO_ENDPOINT=your_minio_endpoint        # e.g., "localhost:9000"
MINIO_ACCESS_KEY=your_minio_access_key    # MinIO access key
MINIO_SECRET_KEY=your_minio_secret_key    # MinIO secret key

# Optional: Specify custom bucket name (defaults to "pulse-bucket-1")
ANALYTICS_BUCKET_NAME=pulse-bucket-1
```

### Export Configuration Options

The export can be customized in `analysis_final.py`:

```python
export_result = export_analytics_to_minio(
    analysis=analysis,
    product_analysis=product_analysis,
    supplier_analysis=supplier_analysis,
    business_id=None,              # Use specific bucket or None for default
    file_format="parquet",         # Options: "parquet", "csv", "json"
    parallel=True,                 # Enable parallel uploads
    max_workers=8                  # Number of parallel workers
)
```

### Supported File Formats

1. **Parquet** (default, recommended)
   - Compressed columnar format
   - Best for large-scale analytics
   - Smallest file size
   - Fast query performance

2. **CSV**
   - Human-readable format
   - Compatible with Excel and other tools
   - Larger file size

3. **JSON**
   - Structured data format
   - Good for APIs and integrations
   - Line-delimited JSON format

## Usage

### Running the Export

```bash
# Navigate to the analysis directory
cd /home/user/pulse/analysis

# Run the analysis pipeline (export happens automatically)
python analysis_final.py
```

### Export Output

The export process will:
1. Print progress for each analytics export
2. Show success (✅), skip (⏭️), or error (❌) status for each item
3. Generate a summary with total counts:
   - Total analytics found
   - Successfully exported
   - Skipped (not generated)
   - Failed
4. Export metadata to `analytics/metadata/export_metadata.json`

### Metadata File

The metadata file contains:
- Last updated timestamp
- Bucket name
- Counts per dictionary
- List of exported keys
- Export statistics

## Verification

### List Exported Analytics

Use the provided utility function:

```python
from analysis_export_utils import list_exported_analytics

# List all analytics
all_analytics = list_exported_analytics()

# List analytics from specific category
kpi_analytics = list_exported_analytics(category="kpis")
```

### Access from Other Applications

Analytics can be accessed from any application with MinIO/S3 access:

```python
import pandas as pd
from minio import Minio

# Create MinIO client
client = Minio(
    "localhost:9000",
    access_key="your_access_key",
    secret_key="your_secret_key",
    secure=False
)

# Read a specific analytic
response = client.get_object("pulse-bucket-1", "analytics/kpis/business_health_daily.parquet")
df = pd.read_parquet(response)
```

### Query with Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnalyticsQuery").getOrCreate()

# Read directly from MinIO
df = spark.read.parquet("s3a://pulse-bucket-1/analytics/kpis/business_health_daily.parquet")
df.show()
```

## Architecture

### Export Flow

```
analysis_final.py
    ↓
[Generate Analytics] → analysis{}, product_analysis{}, supplier_analysis{}
    ↓
export_analytics_to_minio()
    ↓
[Categorize Analytics] → analytics/[category]/[key].[format]
    ↓
[Parallel Upload] → MinIO Data Lake
    ↓
[Generate Metadata] → analytics/metadata/export_metadata.json
```

### Components

1. **analysis_final.py**: Main analytics generation and export orchestration
2. **analysis_export_config.py**: Category mappings and MinIO client creation
3. **analysis_export_utils.py**: Export functions, serialization, and utilities
4. **analysis_config.py**: Database and MinIO connection configuration
5. **analysis_utils.py**: Helper utilities for data loading and processing

## Performance Considerations

- **Parallel Processing**: Uses ThreadPoolExecutor for concurrent uploads (default: 8 workers)
- **Compression**: Parquet format uses Snappy compression by default
- **Memory Efficiency**: Converts Spark DataFrames to Pandas only during serialization
- **Empty DataFrames**: Automatically skips empty or null DataFrames

## Error Handling

The export system handles:
- Missing or None DataFrames (skipped)
- Empty DataFrames (skipped)
- Upload failures (logged and tracked)
- Network errors (reported in summary)

Failed exports are logged in the metadata file and printed in the summary report.

## Future Enhancements

Potential improvements:
1. Add incremental export (only changed analytics)
2. Support for partitioned exports by date
3. Compression format options (gzip, brotli)
4. Export scheduling and automation
5. Data lake catalog integration
6. Query interface for analytics

## Support

For issues or questions:
1. Check the error messages in the export summary
2. Verify MinIO environment variables are set correctly
3. Check MinIO server is accessible
4. Review logs in `analytics/metadata/export_metadata.json`

## Version History

- **v1.0** (2026-02-10): Initial implementation
  - Export of all 188 analytics to MinIO
  - Support for Parquet, CSV, and JSON formats
  - Parallel upload with configurable workers
  - Comprehensive metadata tracking
