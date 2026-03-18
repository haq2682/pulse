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
from pathlib import Path
import hashlib
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
        self.redis_cache_ttl_seconds = int(os.getenv("ANALYTICS_REDIS_CACHE_TTL_SECONDS", "900"))
        self.revision_cache_ttl_seconds = int(os.getenv("ANALYTICS_REVISION_CACHE_TTL_SECONDS", "20"))
        self.redis_client = self._create_redis_client()
        self.expected_schema = self._load_expected_schema()
        self._revision_cache: Dict[str, Dict[str, Any]] = {}

    def _create_redis_client(self):
        """Create Redis client for analytics response caching (best-effort)."""
        try:
            import redis

            host = os.getenv("REDIS_HOST", "10.5.0.11")
            port = int(os.getenv("REDIS_PORT", "6379"))
            db = int(os.getenv("ANALYTICS_REDIS_DB", "0"))
            password = os.getenv("REDIS_PASSWORD") or None

            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_timeout=3,
                socket_connect_timeout=3,
            )
            client.ping()
            return client
        except Exception as e:
            print(f"Warning: Redis analytics cache disabled: {e}")
            return None

    def _handle_redis_error(self, err: Exception, context: str, business_id: Optional[str] = None) -> None:
        business_part = f" for {business_id}" if business_id else ""
        print(f"Warning: Redis {context} failed{business_part}: {err}")
        msg = str(err).lower()
        if any(token in msg for token in ["connection reset", "broken pipe", "connection refused", "timed out"]):
            self.redis_client = None
            print("Warning: Redis analytics cache disabled after connection failure")

    def _load_expected_schema(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Load expected analytics schema from ANALYTICS_SCHEMA.txt.

        File format includes human-readable header text plus a JSON payload.
        Returns an empty mapping when parsing fails (non-fatal).
        """
        try:
            repo_root = Path(__file__).resolve().parents[2]
            schema_path = repo_root / "ANALYTICS_SCHEMA.txt"
            if not schema_path.exists():
                return {}

            raw = schema_path.read_text(encoding="utf-8")
            start = raw.find("{")
            end = raw.rfind("}")
            if start == -1 or end == -1 or end <= start:
                return {}

            payload = raw[start : end + 1]
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                return parsed
            return {}
        except Exception as e:
            print(f"Warning: Could not parse ANALYTICS_SCHEMA.txt: {e}")
            return {}
    
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

    def _get_cached_file_entry_if_fresh(self, business_id: str, file_name: str):
        """
        Return cached DataFrame only if in-memory cache is valid AND object metadata
        in MinIO has not changed since it was cached.
        """
        cache_key = self._get_cache_key(business_id, file_name)
        cache_entry = self.cache.get(cache_key)
        if not cache_entry or not self._is_cache_valid(cache_entry):
            return None

        cached_version = cache_entry.get("object_version")
        if not cached_version:
            return None

        found_category = next(
            (category for category, values in self.ANALYTICS_CATEGORIES.items() if file_name in values),
            None
        )
        if not found_category:
            return None

        object_path = f"analytics/{found_category}/{file_name}.parquet"
        try:
            stat = self.minio_client.stat_object(business_id, object_path)
            current_version = (
                stat.last_modified.isoformat() if stat.last_modified else "",
                int(stat.size or 0),
            )
            if current_version == cached_version:
                return cache_entry.get("data")
        except Exception:
            return None

        return None

    def _get_business_analytics_revision(self, business_id: str) -> str:
        """
        Build a deterministic revision hash from analytics parquet metadata.
        Any file add/modify/remove changes this revision and automatically
        invalidates response-level Redis caches.
        """
        now = datetime.now()
        revision_entry = self._revision_cache.get(business_id)
        if revision_entry:
            cached_at = revision_entry.get("cached_at")
            if cached_at and (now - cached_at).total_seconds() < self.revision_cache_ttl_seconds:
                return revision_entry.get("revision", "empty")

        records: list[str] = []
        try:
            objects = self.minio_client.list_objects(
                business_id,
                prefix="analytics/",
                recursive=True,
            )
            for obj in objects:
                if not obj.object_name.endswith(".parquet"):
                    continue
                lm = obj.last_modified.isoformat() if obj.last_modified else ""
                records.append(f"{obj.object_name}|{lm}|{int(obj.size or 0)}")
        except Exception as e:
            # If metadata listing fails, force a non-cacheable pseudo-revision.
            fallback = f"error-{int(now.timestamp())}"
            self._revision_cache[business_id] = {"revision": fallback, "cached_at": now}
            print(f"Warning: Could not compute analytics revision for {business_id}: {e}")
            return fallback

        if not records:
            revision = "empty"
        else:
            joined = "\n".join(sorted(records))
            revision = hashlib.sha256(joined.encode("utf-8")).hexdigest()

        self._revision_cache[business_id] = {"revision": revision, "cached_at": now}
        return revision

    def _build_redis_all_key(self, business_id: str, categories: List[str], revision: str) -> str:
        categories_key = ",".join(sorted(categories))
        return f"analytics:all:{business_id}:{revision}:{categories_key}"

    def _clear_redis_business_keys(self, business_id: str) -> None:
        if not self.redis_client:
            return
        try:
            pattern = f"analytics:*:{business_id}:*"
            for key in self.redis_client.scan_iter(match=pattern, count=500):
                self.redis_client.delete(key)
        except Exception as e:
            self._handle_redis_error(e, "clear", business_id)
    
    async def fetch_analytics_file(self, business_id: str, file_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch a single analytics parquet file from MinIO.
        
        Args:
            business_id: Business ID (bucket name)
            file_name: Name of the analytics file (without .parquet extension)
            
        Returns:
            DataFrame or None if file doesn't exist
        """
        cached_df = self._get_cached_file_entry_if_fresh(business_id, file_name)
        if cached_df is not None:
            return cached_df
        
        try:
            found_category = next(
                (category for category, values in self.ANALYTICS_CATEGORIES.items() if file_name in values),
                None
            )
            if not found_category:
                return None
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
            cache_key = self._get_cache_key(business_id, file_name)
            stat = self.minio_client.stat_object(business_id, object_path)
            self.cache[cache_key] = {
                "data": df,
                "cached_at": datetime.now(),
                "object_version": (
                    stat.last_modified.isoformat() if stat.last_modified else "",
                    int(stat.size or 0),
                ),
            }
            
            return df
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise
        except Exception as e:
            print(f"Error fetching analytics file {file_name}: {e}")
            raise
    
    async def fetch_category_analytics(
        self,
        business_id: str,
        category: str,
        file_names: Optional[List[str]] = None,
        row_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
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
        
        category_files = self.ANALYTICS_CATEGORIES[category]
        if file_names:
            selected_files = [f for f in file_names if f in category_files]
            file_names = selected_files
        else:
            file_names = category_files
        results = {}
        file_errors = {}
        
        for file_name in file_names:
            try:
                df = await self.fetch_analytics_file(business_id, file_name)
                if df is not None:
                    df_for_payload = df.head(row_limit) if row_limit and row_limit > 0 else df
                    expected_cols = (
                        self.expected_schema.get(category, {}).get(file_name)
                        if self.expected_schema
                        else None
                    )
                    if expected_cols:
                        missing_cols = [c for c in expected_cols if c not in df.columns]
                        if missing_cols:
                            file_errors[file_name] = (
                                "Missing required columns: " + ", ".join(missing_cols)
                            )
                            print(
                                f"Warning: Skipping analytics file {file_name} for business {business_id} "
                                f"due to missing columns: {missing_cols}"
                            )
                            continue

                    # Convert DataFrame to dict for JSON serialization
                    results[file_name] = self._sanitize({
                        "data": df_for_payload.where(pd.notna(df_for_payload), None).to_dict(orient="records"),
                        "columns": list(df_for_payload.columns),
                        "row_count": len(df),
                        "fetched_at": datetime.now().isoformat()
                    })
            except Exception as e:
                # A single corrupt/incompatible parquet should not break the
                # whole category or the entire dashboard page.
                file_errors[file_name] = str(e)
                print(
                    f"Warning: Failed to load analytics file {file_name} "
                    f"for business {business_id}: {e}"
                )
        
        return {
            "category": category,
            "analytics": results,
            "total_count": len(results),
            "file_errors": file_errors,
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
        else:
            categories = [c for c in categories if c in self.ANALYTICS_CATEGORIES]

        revision = self._get_business_analytics_revision(business_id)

        # Fast path: serve Redis cached response for this exact category set +
        # current MinIO analytics revision.
        if self.redis_client and categories:
            redis_key = self._build_redis_all_key(business_id, categories, revision)
            try:
                cached_payload = self.redis_client.get(redis_key)
                if cached_payload:
                    parsed = json.loads(cached_payload)
                    parsed["cache"] = {
                        "source": "redis",
                        "revision": revision,
                    }
                    return parsed
            except Exception as e:
                self._handle_redis_error(e, "read", business_id)
        
        results = {}
        for category in categories:
            if category in self.ANALYTICS_CATEGORIES:
                try:
                    results[category] = await self.fetch_category_analytics(business_id, category)
                except Exception as e:
                    # Keep API responsive even if one category has an
                    # unexpected failure.
                    print(
                        f"Warning: Failed to load analytics category {category} "
                        f"for business {business_id}: {e}"
                    )
                    results[category] = {
                        "category": category,
                        "analytics": {},
                        "total_count": 0,
                        "file_errors": {"_category": str(e)},
                    }
        
        # Calculate totals
        total_analytics = sum(
            cat_data["total_count"] 
            for cat_data in results.values()
        )
        
        payload = {
            "business_id": business_id,
            "categories": results,
            "total_categories": len(results),
            "total_analytics": total_analytics,
            "fetched_at": datetime.now().isoformat(),
            "cache": {
                "source": "minio",
                "revision": revision,
            },
        }

        if self.redis_client and categories:
            redis_key = self._build_redis_all_key(business_id, categories, revision)
            try:
                self.redis_client.setex(
                    redis_key,
                    self.redis_cache_ttl_seconds,
                    json.dumps(payload, default=str, allow_nan=False),
                )
            except Exception as e:
                self._handle_redis_error(e, "write", business_id)

        return payload
    
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
            if business_id in self._revision_cache:
                del self._revision_cache[business_id]
            self._clear_redis_business_keys(business_id)
        else:
            # Clear all cache
            self.cache.clear()
            self._revision_cache.clear()
            if self.redis_client:
                try:
                    for key in self.redis_client.scan_iter(match="analytics:*", count=1000):
                        self.redis_client.delete(key)
                except Exception as e:
                    self._handle_redis_error(e, "clear")
