import os
from typing import Optional
from minio import Minio
from analysis_config import MINIO_CONFIG

SUPPORTED_FORMATS = ["parquet", "csv", "json"]

ANALYTICS_CATEGORIES = {
    "kpis": [
        "business_health_daily", "business_health_weekly", "business_health_monthly",
        "clv_summary", "funnel_summary", "cart_abandon_summary", "session_to_order_analysis",
        "customer_engagement_summary",
    ],
    "customer_analytics": [
        "customer_account_status_distribution_daily", "customer_account_status_distribution_weekly",
        "customer_account_status_distribution_monthly", "new_customers_daily", "new_customers_weekly",
        "new_customers_monthly", "cumulative_customers_daily", "cumulative_customers_weekly",
        "cumulative_customers_monthly", "new_customers_geo_acquisition_daily",
        "new_customers_geo_acquisition_monthly", "customer_age_group_distribution",
        "customer_city_distribution", "customer_state_distribution", "customer_country_distribution",
        "customer_age_group_spending", "new_vs_returning_customer_country",
        "new_vs_returning_customer_city", "customer_engagement", "session_conversion_distribution",
        "cart_abandonment_distribution", "top_customers_by_revenue", "top_customers_by_profit",
        "discount_customers", "discount_customers_summary", "correlation_discount_vs_clv",
        "high_discount_customers", "cart_behavior_summary", "high_value_abandoners",
        "churn_risk_summary", "high_clv_at_risk", "rfm_segment_summary", "high_intent_non_buyers",
        "customer_overall_health_summary", "customers_cohorts", "signup_cohort_summary",
        "customer_cohort_retention", "rfm_churn_crosstab", "seg_referrer_crosstab",
        "seg_device_crosstab", "payment_method_vs_clv_churn", "payment_method_summary",
        "referrer_source_summary", "referrer_churn_summary", "customer_profit_per_segment",
    ],
    "product_analytics": [
        "best_selling_products", "product_monthly_trends", "category_monthly_trends",
        "product_calendar_month_seasonality", "category_calendar_month_seasonality",
        "highest_margin_products", "low_margin_high_traffic_products", "out_of_stock_products",
        "low_conversion_products", "product_rating_summary", "category_view_patterns",
        "top_view_to_purchase_products", "product_performance_score", "category_revenue_share",
        "low_performing_categories", "category_popularity_score", "category_profitability",
        "category_monthly_seasonality", "category_peak_season", "product_lifecycle_segments",
        "product_lifecycle_summary", "product_stockout_risk", "product_stockout_replenishment",
        "product_dead_stock", "product_inventory_health", "product_inventory_critical",
        "supplier_product_performance", "stockout_rate_by_product", "supplier_ranking_core",
        "supplier_stockout_impact_on_products", "category_affinity_pairs",
        "category_affinity_top_per_category", "product_affinity_pairs",
        "product_affinity_top_per_product", "product_affinity_top5_candidates",
        "precomputed_product_recommendations", "precomputed_reco_coverage",
        "inventory_stock_status", "days_of_supply", "sku_reorder_urgency",
        "reorder_point_breach_frequency", "overstock_analysis", "reserved_vs_available",
        "excess_inventory_not_selling", "aging_inventory_slow_movers", "margin_erosion_risk",
    ],
    "supplier_analytics": [
        "stockout_rate_by_supplier", "storage_cost_efficiency_by_supplier",
        "inventory_carrying_cost_by_supplier", "supplier_reliability", "supplier_stockouts",
        "supplier_fulfillment_performance", "supplier_revenue_contribution",
        "supplier_profit_margin", "supplier_days_since_last_restock", "supplier_contract_expiry",
    ],
    "marketing_analytics": [
        "campaign_performance_summary", "campaign_product_contribution", "campaign_ltv",
        "campaign_customer_ltv_summary", "campaign_wasteful_campaigns", "campaign_margin_profile",
        "campaign_performance",
    ],
    "revenue_analytics": [
        "low_margin_categories", "rev_by_country_city", "rev_by_customer_segment",
        "rev_by_rfm_segment", "rev_by_segment_label", "rev_by_referrer", "rev_by_device",
        "aov_trend_daily", "aov_trend_weekly", "aov_trend_monthly", "segment_aov_by_rfm",
        "inventory_carrying_cost_overall", "inventory_carrying_cost_by_product",
    ],
    "funnel_analytics": [
        "high_value_funnel", "high_value_vs_regular", "funnel_by_device", "funnel_by_referrer",
        "abandoned_vs_converted", "checkout_dropoff_reasons", "checkout_dropoff_buckets",
        "checkout_dropoff_by_device_and_reason", "device_conversion_rates",
    ],
    "payment_analytics": [
        "payment_counts_by_country_method", "payment_counts_by_state_method",
        "payment_method_success_rates", "payment_method_success_rates_by_country",
        "payment_method_aov", "refund_rate_by_payment_method", "refund_rate_by_product",
        "refund_rate_by_month", "time_to_refund_by_payment_method",
    ],
    "review_analytics": [
        "review_velocity_daily", "review_velocity_weekly", "review_velocity_monthly",
        "sentiment_by_category", "product_monthly_rating_trends",
        "low_rated_product_monthly_trends_rating_only", "rating_tier_per_product",
        "rating_tier_sales_velocity",
    ],
    "operations_analytics": [
        "processing_by_category", "processing_by_subcategory", "processing_by_hour",
        "processing_by_day_of_week", "weekend_vs_weekday", "delivery_days_by_country",
        "delivery_days_by_state", "delivery_days_by_city", "ontime_delivery_by_country",
        "ontime_delivery_by_state", "ontime_delivery_by_city", "shipping_efficiency_by_country",
        "shipping_efficiency_by_state", "shipping_efficiency_by_city", "processing_by_season",
        "processing_by_season_and_status", "shipping_cost_outliers",
    ],
    "wishlist_analytics": [
        "wishlist_overall_summary", "wishlist_by_product", "wishlist_time_to_purchase_stats",
        "wishlist_time_to_purchase_distribution", "abandoned_wishlist_items",
        "abandoned_wishlist_by_customer", "abandoned_wishlist_by_product", "wishlist_adds_by_month",
    ],
    "cart_analytics": [
        "cart_overall_stats", "cart_status_distribution", "cart_value_stats",
        "high_value_abandoned_carts", "time_to_purchase_overall", "time_to_purchase_by_tier",
        "time_to_purchase_buckets",
    ],
}


def create_minio_client() -> Minio: 
    if not all([MINIO_CONFIG["endpoint"], MINIO_CONFIG["access_key"], MINIO_CONFIG["secret_key"]]):
        raise ValueError("Missing required MinIO environment variables")
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=False
    )


def get_bucket_name(business_id: Optional[str] = None) -> str:
    if business_id: 
        return business_id
    return os.getenv("ANALYTICS_BUCKET_NAME", "pulse-bucket-1")


def ensure_bucket_exists(minio_client: Minio, bucket_name: str) -> bool:
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"✅ Created bucket: {bucket_name}")
    return True


def get_category_for_key(key: str) -> str:
    for category, keys in ANALYTICS_CATEGORIES.items():
        if key in keys:
            return category
    return "other"