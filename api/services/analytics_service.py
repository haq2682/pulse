"""
Analytics Service for fetching and serving analytics data from MinIO.

This service handles:
- Fetching analytics parquet files from MinIO
- Caching analytics data for performance
- Organizing analytics by category
- Progressive loading of large datasets
"""

import os
import io
import json
from typing import Dict, List, Optional, Any
from minio import Minio
from minio.error import S3Error
import pandas as pd
import pyarrow.parquet as pq
import math
from datetime import datetime, timedelta


class AnalyticsService:
    """Service for fetching analytics data from MinIO."""
    
    # Analytics categories and their files (188 total)
    ANALYTICS_CATEGORIES = {
        "kpis": [
            "business_health_daily",
            "business_health_weekly",
            "business_health_monthly",
            "clv_summary",
            "funnel_summary",
            # ADDED: was entirely missing from previous schema
            "cart_abandon_summary",
            "session_to_order_analysis",
            "customer_engagement_summary",
        ],

        "customer_analytics": [
            "customer_account_status_distribution_daily",
            "customer_account_status_distribution_weekly",
            "customer_account_status_distribution_monthly",
            "new_customers_daily",
            "new_customers_weekly",
            "new_customers_monthly",
            "cumulative_customers_daily",
            "cumulative_customers_weekly",
            "cumulative_customers_monthly",
            "new_customers_geo_acquisition_daily",
            "new_customers_geo_acquisition_monthly",
            "customer_age_group_distribution",
            "customer_city_distribution",
            "customer_state_distribution",
            "customer_country_distribution",
            "customer_age_group_spending",
            "new_vs_returning_customer_country",
            "new_vs_returning_customer_city",
            "new_vs_returning_customer_state",
            "customer_engagement",
            "session_conversion_distribution",
            "cart_abandonment_distribution",
            "top_customers_by_revenue",
            "top_customers_by_profit",
            # FIX: code does NOT .select() on disc_df — it retains all agg_customers columns
            "discount_customers",
            "discount_customers_summary",
            "correlation_discount_vs_clv",
            "high_discount_customers",
            "cart_behavior_summary",
            "high_value_abandoners",
            "churn_risk_summary",
            "high_clv_at_risk",
            "rfm_segment_summary",
            "high_intent_non_buyers",
            "customer_overall_health_summary",
            "customers_cohorts",
            "signup_cohort_summary",
            "customer_cohort_retention",
            "rfm_churn_crosstab",
            "seg_referrer_crosstab",
            "seg_device_crosstab",
            "payment_method_vs_clv_churn",
            "payment_method_summary",
            "referrer_source_summary",
            "referrer_churn_summary",
            "customer_profit_per_segment",
            "gender_category_preference",
            "gender_product_preference",
        ],

        "product_analytics": [
            "best_selling_products",
            "product_monthly_trends",
            "category_monthly_trends",
            "product_calendar_month_seasonality",
            "category_calendar_month_seasonality",
            "highest_margin_products",
            "low_margin_high_traffic_products",
            "out_of_stock_products",
            "low_conversion_products",
            "product_rating_summary",
            "category_view_patterns",
            "top_view_to_purchase_products",
            "product_performance_score",
            "category_revenue_share",
            "low_performing_categories",
            "category_popularity_score",
            "category_profitability",
            "category_monthly_seasonality",
            "category_peak_season",
            "product_lifecycle_segments",
            "product_lifecycle_summary",
            "product_stockout_risk",
            "product_stockout_replenishment",
            "product_dead_stock",
            "product_inventory_health",
            "product_inventory_critical",
            "supplier_product_performance",
            "stockout_rate_by_product",
            "supplier_ranking_core",
            "supplier_stockout_impact_on_products",
            "category_affinity_pairs",
            "category_affinity_top_per_category",
            "product_affinity_pairs",
            "product_affinity_top_per_product",
            "precomputed_product_recommendations",
            "precomputed_reco_coverage",
            # FIX: primary path selects product_id, (supplier_id), available_stock, current_stock, minimum_stock_level, days_of_supply, stock_status_computed.
            "inventory_stock_status",
            # FIX: primary path selects product_id, (supplier_id), available_stock, avg_daily_sales, days_of_supply.
            "days_of_supply",
            # FIX: breach column is reorder_point_breach_count (not reorder_point_breach).
            "sku_reorder_urgency",
            # FIX: primary path uses stock_health_score, reorder_urgency, days_of_supply, stock_status
            "reorder_point_breach_frequency",
            "overstock_analysis",
            "reserved_vs_available",
            "excess_inventory_not_selling",
            "margin_erosion_risk",
            "inventory_carrying_cost_by_product",
        ],

        "supplier_analytics": [
            "stockout_rate_by_supplier",
            "storage_cost_efficiency_by_supplier",
            "inventory_carrying_cost_by_supplier",
            "supplier_reliability",
            "supplier_stockouts",
            "supplier_fulfillment_performance",
            "supplier_revenue_contribution",
            "supplier_profit_margin",
            "supplier_days_since_last_restock",
            "supplier_contract_expiry",
        ],

        "marketing_analytics": [
            "campaign_performance_summary",
            "campaign_product_contribution",
            "campaign_ltv",
            "campaign_customer_ltv_summary",
            # FIX: code builds this by filtering campaign_performance_summary and adding 2 columns.
            "campaign_wasteful_campaigns",
            "campaign_margin_profile",
            "campaign_performance",
        ],

        "revenue_analytics": [
            "low_margin_categories",
            "rev_by_country_city",
            "rev_by_customer_segment",
            "rev_by_rfm_segment",
            "rev_by_segment_label",
            "rev_by_referrer",
            "rev_by_device",
            "aov_trend_daily",
            "aov_trend_weekly",
            "aov_trend_monthly",
            "segment_aov_by_rfm",
            "inventory_carrying_cost_overall",
        ],

        "funnel_analytics": [
            "high_value_funnel",
            "high_value_vs_regular",
            "funnel_by_device",
            "funnel_by_referrer",
            "abandoned_vs_converted",
            # FIX: these 3 were wrongly placed under product_analytics in the previous schema.
            "checkout_dropoff_reasons",
            "checkout_dropoff_buckets",
            "checkout_dropoff_by_device_and_reason",
            "device_conversion_rates",
        ],

        "payment_analytics": [
            "payment_counts_by_country_method",
            "payment_counts_by_state_method",
            "payment_method_success_rates",
            "payment_method_success_rates_by_country",
            "payment_method_aov",
            "refund_rate_by_payment_method",
            "refund_rate_by_product",
            "refund_rate_by_month",
            "time_to_refund_by_payment_method",
        ],

        "review_analytics": [
            "review_velocity_daily",
            "review_velocity_weekly",
            "review_velocity_monthly",
            "sentiment_by_category",
            "product_monthly_rating_trends",
            "low_rated_product_monthly_trends_rating_only",
            "rating_tier_per_product",
            "rating_tier_sales_velocity",
        ],

        "operations_analytics": [
            "processing_by_category",
            "processing_by_subcategory",
            "processing_by_hour",
            "processing_by_day_of_week",
            "weekend_vs_weekday",
            "delivery_days_by_country",
            "delivery_days_by_state",
            "delivery_days_by_city",
            "ontime_delivery_by_country",
            "ontime_delivery_by_state",
            "ontime_delivery_by_city",
            "shipping_efficiency_by_country",
            "shipping_efficiency_by_state",
            "shipping_efficiency_by_city",
            "processing_by_season",
            "processing_by_season_and_status",
        ],

        "wishlist_analytics": [
            "wishlist_overall_summary",
            "wishlist_by_product",
            "wishlist_by_customer",
            "wishlist_time_to_purchase_stats",
            "wishlist_time_to_purchase_distribution",
            "abandoned_wishlist_items",
            "abandoned_wishlist_by_customer",
            "abandoned_wishlist_by_product",
            "wishlist_adds_by_month",
        ],

        "cart_analytics": [
            "cart_overall_stats",
            "cart_status_distribution",
            "cart_value_stats",
            "high_value_abandoned_carts",
            "time_to_purchase_overall",
            # FIX: groupby key is cart_value_tier OR cart_size_category (whichever column exists).
            "time_to_purchase_by_tier",
            "time_to_purchase_buckets",
        ],

        "geo_analytics": [
            "geo_acquisition",
        ],
    }
    
    def __init__(self):
        """Initialize MinIO client."""
        self.minio_client = self._create_minio_client()
        self.cache = {}  # Simple in-memory cache
        self.cache_duration = timedelta(minutes=5)
    
    def _sanitize(self, obj):
        """Recursively replace nan/inf floats with None."""
        if isinstance(obj, dict):
            return {k: self._sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    def _create_minio_client(self):
        """Create MinIO client with environment variables."""
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        # Strip protocol if present
        if "://" in minio_endpoint:
            minio_endpoint = minio_endpoint.split("://", 1)[1]
        
        return Minio(
            minio_endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )
    
    def _get_cache_key(self, business_id: str, file_name: str) -> str:
        """Generate cache key."""
        return f"{business_id}:{file_name}"
    
    def _is_cache_valid(self, cache_entry: Dict) -> bool:
        """Check if cache entry is still valid."""
        if not cache_entry:
            return False
        cached_at = cache_entry.get("cached_at")
        if not cached_at:
            return False
        return datetime.now() - cached_at < self.cache_duration
    
    async def fetch_analytics_file(self, business_id: str, file_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch a single analytics parquet file from MinIO.
        
        Args:
            business_id: Business ID (bucket name)
            file_name: Name of the analytics file (without .parquet extension)
            
        Returns:
            DataFrame or None if file doesn't exist
        """
        # Check cache first
        cache_key = self._get_cache_key(business_id, file_name)
        if cache_key in self.cache and self._is_cache_valid(self.cache[cache_key]):
            return self.cache[cache_key]["data"]
        
        try:
            found_category = next(
                (category for category, values in self.ANALYTICS_CATEGORIES.items() if file_name in values),
                None
            )
            # Path in MinIO: analytics/{file_name}.parquet
            object_path = f"analytics/{found_category}/{file_name}.parquet"
            
            # Get object from MinIO
            response = self.minio_client.get_object(business_id, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            
            # Read parquet from bytes
            df = pd.read_parquet(io.BytesIO(data))
            
            # Cache the result
            self.cache[cache_key] = {
                "data": df,
                "cached_at": datetime.now()
            }
            
            return df
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise
        except Exception as e:
            print(f"Error fetching analytics file {file_name}: {e}")
            raise
    
    async def fetch_category_analytics(self, business_id: str, category: str) -> Dict[str, Any]:
        """
        Fetch all analytics for a specific category.
        
        Args:
            business_id: Business ID (bucket name)
            category: Category name (e.g., "customer_acquisition")
            
        Returns:
            Dict with analytics data
        """
        if category not in self.ANALYTICS_CATEGORIES:
            raise ValueError(f"Invalid category: {category}")
        
        file_names = self.ANALYTICS_CATEGORIES[category]
        results = {}
        
        for file_name in file_names:
            df = await self.fetch_analytics_file(business_id, file_name)
            if df is not None:
                # Convert DataFrame to dict for JSON serialization
                results[file_name] = self._sanitize({
                    "data": df.where(pd.notna(df), None).to_dict(orient="records"),
                    "columns": list(df.columns),
                    "row_count": len(df),
                    "fetched_at": datetime.now().isoformat()
                })
        
        return {
            "category": category,
            "analytics": results,
            "total_count": len(results)
        }
    
    async def fetch_all_analytics(self, business_id: str, categories: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Fetch all analytics or specified categories.
        
        Args:
            business_id: Business ID (bucket name)
            categories: Optional list of categories to fetch (default: all)
            
        Returns:
            Dict with all analytics organized by category
        """
        if categories is None:
            categories = list(self.ANALYTICS_CATEGORIES.keys())
        
        results = {}
        for category in categories:
            if category in self.ANALYTICS_CATEGORIES:
                results[category] = await self.fetch_category_analytics(business_id, category)
        
        # Calculate totals
        total_analytics = sum(
            cat_data["total_count"] 
            for cat_data in results.values()
        )
        
        return {
            "business_id": business_id,
            "categories": results,
            "total_categories": len(results),
            "total_analytics": total_analytics,
            "fetched_at": datetime.now().isoformat()
        }
    
    async def list_available_analytics(self, business_id: str) -> List[str]:
        """
        List all available analytics files in MinIO for a business.
        
        Args:
            business_id: Business ID (bucket name)
            
        Returns:
            List of available analytics file names
        """
        try:
            objects = self.minio_client.list_objects(
                business_id,
                prefix="analytics/",
                recursive=True
            )

            available_files = []

            for obj in objects:
                if obj.is_dir:
                    continue

                if not obj.object_name.endswith(".parquet"):
                    continue

                # Extract only the filename (last part of path)
                file_name = obj.object_name.split("/")[-1].replace(".parquet", "")

                available_files.append(file_name)

            return available_files

        except Exception as e:
            print(f"Error listing analytics: {e}")
            return []

    
    def clear_cache(self, business_id: Optional[str] = None):
        """Clear cache for specific business or all."""
        if business_id:
            # Clear cache for specific business
            keys_to_remove = [k for k in self.cache.keys() if k.startswith(f"{business_id}:")]
            for key in keys_to_remove:
                del self.cache[key]
        else:
            # Clear all cache
            self.cache.clear()
