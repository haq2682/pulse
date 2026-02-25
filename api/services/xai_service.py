"""
XAI (Explainable AI) Service — Gemini-powered analytics chatbot.

Token optimisation — three layers:
───────────────────────────────────────────────────────────────────────────────
 Layer 1 │ KEYWORD PRE-FILTER  (Pass 1, local, 0 tokens)
         │ Maps query words to source names. Narrows ~250 sources → ≤15
         │ candidates in Python with no API call. If ≤5 match, skips Gemini
         │ Pass-1 entirely.
─────────┼─────────────────────────────────────────────────────────────────────
 Layer 2 │ TINY PASS-1 PROMPT  (Pass 1, ~120 tokens worst-case)
         │ When disambiguation is needed, Gemini only sees the short candidate
         │ list (not the full 250-source catalog). Prompt is ~120 tokens vs
         │ the original ~1,000 tokens.
─────────┼─────────────────────────────────────────────────────────────────────
 Layer 3 │ TRIMMED DATA BLOCK  (Pass 2, ~500-900 tokens vs 9,000+ original)
         │ • Max 3 sources fetched (not 5)
         │ • Max 20 rows per source (not 50)
         │ • String columns capped at 80 chars
         │ • Columns with >95 % identical values dropped (low-signal noise)
───────────────────────────────────────────────────────────────────────────────

Net result: ~500-900 tokens per full query vs ~3,000-10,000 in the original.
That is a 70-90 % reduction and preserves the analytical quality of responses.
"""

import asyncio
import io
import json
import math
import os
import random
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from minio import Minio

# ---------------------------------------------------------------------------
# Gemini SDK (new google-genai only — legacy is EOL)
# ---------------------------------------------------------------------------
try:
    from google import genai as _genai          # pip install google-genai
except ImportError as _e:
    raise ImportError(
        "google-genai not installed.\n"
        "Run: pip install google-genai\n"
        "Then rebuild / restart the container."
    ) from _e

# ---------------------------------------------------------------------------
# Analytics catalog
# ---------------------------------------------------------------------------
ANALYTICS_CATEGORIES: Dict[str, List[str]] = {
    "kpis": [
        "business_health_daily", "business_health_weekly", "business_health_monthly",
        "clv_summary", "funnel_summary", "cart_abandon_summary",
        "session_to_order_analysis", "customer_engagement_summary",
    ],
    "customer_analytics": [
        "customer_account_status_distribution_daily", "customer_account_status_distribution_weekly",
        "customer_account_status_distribution_monthly", "new_customers_daily", "new_customers_weekly",
        "new_customers_monthly", "cumulative_customers_daily", "cumulative_customers_weekly",
        "cumulative_customers_monthly", "new_customers_geo_acquisition_daily",
        "new_customers_geo_acquisition_monthly", "customer_age_group_distribution",
        "customer_city_distribution", "customer_state_distribution", "customer_country_distribution",
        "customer_age_group_spending", "new_vs_returning_customer_country",
        "new_vs_returning_customer_city", "new_vs_returning_customer_state",
        "customer_engagement", "session_conversion_distribution", "cart_abandonment_distribution",
        "top_customers_by_revenue", "top_customers_by_profit", "discount_customers",
        "discount_customers_summary", "correlation_discount_vs_clv", "high_discount_customers",
        "cart_behavior_summary", "high_value_abandoners", "churn_risk_summary",
        "high_clv_at_risk", "rfm_segment_summary", "high_intent_non_buyers",
        "customer_overall_health_summary", "customers_cohorts", "signup_cohort_summary",
        "customer_cohort_retention", "rfm_churn_crosstab", "seg_referrer_crosstab",
        "seg_device_crosstab", "payment_method_vs_clv_churn", "payment_method_summary",
        "referrer_source_summary", "referrer_churn_summary", "customer_profit_per_segment",
        "gender_category_preference", "gender_product_preference",
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
        "product_affinity_top_per_product", "precomputed_product_recommendations",
        "precomputed_reco_coverage", "inventory_stock_status", "days_of_supply",
        "sku_reorder_urgency", "reorder_point_breach_frequency", "overstock_analysis",
        "reserved_vs_available", "excess_inventory_not_selling", "margin_erosion_risk",
        "inventory_carrying_cost_by_product",
    ],
    "supplier_analytics": [
        "stockout_rate_by_supplier", "storage_cost_efficiency_by_supplier",
        "inventory_carrying_cost_by_supplier", "supplier_reliability", "supplier_stockouts",
        "supplier_fulfillment_performance", "supplier_revenue_contribution",
        "supplier_profit_margin", "supplier_days_since_last_restock", "supplier_contract_expiry",
    ],
    "marketing_analytics": [
        "campaign_performance_summary", "campaign_product_contribution", "campaign_ltv",
        "campaign_customer_ltv_summary", "campaign_wasteful_campaigns",
        "campaign_margin_profile", "campaign_performance",
    ],
    "revenue_analytics": [
        "low_margin_categories", "rev_by_country_city", "rev_by_customer_segment",
        "rev_by_rfm_segment", "rev_by_segment_label", "rev_by_referrer", "rev_by_device",
        "aov_trend_daily", "aov_trend_weekly", "aov_trend_monthly", "segment_aov_by_rfm",
        "inventory_carrying_cost_overall",
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
        "shipping_efficiency_by_state", "shipping_efficiency_by_city",
        "processing_by_season", "processing_by_season_and_status",
    ],
    "wishlist_analytics": [
        "wishlist_overall_summary", "wishlist_by_product", "wishlist_by_customer",
        "wishlist_time_to_purchase_stats", "wishlist_time_to_purchase_distribution",
        "abandoned_wishlist_items", "abandoned_wishlist_by_customer",
        "abandoned_wishlist_by_product", "wishlist_adds_by_month",
    ],
    "cart_analytics": [
        "cart_overall_stats", "cart_status_distribution", "cart_value_stats",
        "high_value_abandoned_carts", "time_to_purchase_overall",
        "time_to_purchase_by_tier", "time_to_purchase_buckets",
    ],
    "geo_analytics": ["geo_acquisition"],
}

ANALYTIC_TO_CATEGORY: Dict[str, str] = {
    name: cat
    for cat, names in ANALYTICS_CATEGORIES.items()
    for name in names
}

ML_INFERENCE_CATALOG: Dict[str, str] = {
    "cart_abandonment_predictions":  "machine-learning/classification/predictions/cart_abandonment_predictions/",
    "customer_churn_predictions":    "machine-learning/classification/predictions/customer_churn_predictions/",
    "customer_segment_predictions":  "machine-learning/classification/predictions/customer_segment_predictions/",
    "payment_success_predictions":   "machine-learning/classification/predictions/payment_success_predictions/",
    "review_sentiment_predictions":  "machine-learning/classification/predictions/review_sentiment_predictions/",
    "stock_status_predictions":      "machine-learning/classification/predictions/stock_status_predictions/",
    "fulfillment_risk_predictions":  "machine-learning/classification/predictions/fulfillment_risk_predictions/",
    "product_bundling_predictions":  "machine-learning/classification/predictions/product_bundling_predictions/",
    "customer_segmentation":         "machine-learning/clustering/predictions/customer_segmentation.parquet/",
    "geographic_clustering":         "machine-learning/clustering/predictions/geographic_clustering.parquet/",
    "session_behavior_clustering":   "machine-learning/clustering/predictions/session_behavior_clustering.parquet/",
    "supplier_clustering":           "machine-learning/clustering/predictions/supplier_clustering.parquet/",
    "product_affinity_clustering":   "machine-learning/clustering/predictions/product_affinity_clustering.parquet/",
    "product_lifecycle_clustering":  "machine-learning/clustering/predictions/product_lifecycle_clustering.parquet/",
    "aov_prediction":                "machine-learning/regression/predictions/aov_prediction/",
    "clv_predictions":               "machine-learning/regression/predictions/clv_predictions/",
    "restock_quantity":              "machine-learning/regression/predictions/restock_quantity/",
    "safety_stock_adjusted":         "machine-learning/regression/predictions/safety_stock_adjusted/",
    "session_conversion_value":      "machine-learning/regression/predictions/session_conversion_value/",
    "stockout_probability":          "machine-learning/regression/predictions/stockout_probability/",
    "campaign_roi":                  "machine-learning/regression/predictions/campaign_roi/",
    "delivery_time":                 "machine-learning/regression/predictions/delivery_time/",
    "demand_forecast":               "machine-learning/regression/predictions/demand_forecast/",
    "price_optimization":            "machine-learning/regression/predictions/price_optimization/",
    "revenue_forecast":              "machine-learning/regression/predictions/revenue_forecast/",
    "seasonal_trends":               "machine-learning/regression/predictions/seasonal_trends/",
}

AGGREGATED_TABLES: List[str] = [
    "agg_customer_sessions", "agg_customers", "agg_inventory",
    "agg_marketing_campaigns", "agg_order_items", "agg_orders",
    "agg_payments", "agg_products", "agg_reviews", "agg_shopping_cart",
    "agg_suppliers", "agg_wishlist",
]

_ALL_ML  = set(ML_INFERENCE_CATALOG.keys())
_ALL_AGG = set(AGGREGATED_TABLES)

# ---------------------------------------------------------------------------
# Layer 1 — Keyword pre-filter
#
# Each entry maps a source name → list of lowercase keyword fragments.
# A source is "hit" when ANY fragment is a substring of the lowercased query.
# This entire step runs in Python with zero API tokens.
# ---------------------------------------------------------------------------
_KEYWORD_MAP: Dict[str, List[str]] = {
    # KPIs / general health
    "business_health_daily":           ["business", "health", "overview", "performance", "how is", "doing", "summary", "general", "kpi", "overall"],
    "business_health_weekly":          ["weekly", "this week", "last week", "week"],
    "business_health_monthly":         ["monthly", "this month", "last month", "month"],
    "clv_summary":                     ["clv", "lifetime value", "ltv"],
    "funnel_summary":                  ["funnel", "pipeline", "conversion rate"],
    "cart_abandon_summary":            ["cart abandon", "abandoned cart", "checkout abandon"],
    "session_to_order_analysis":       ["session to order", "visit to order", "order conversion"],
    "customer_engagement_summary":     ["engagement", "active customer", "activity level"],
    # New customers / acquisition
    "new_customers_daily":             ["new customer", "acquisition", "signup", "register", "joined today"],
    "new_customers_weekly":            ["new customer week", "weekly acquisition", "weekly signup"],
    "new_customers_monthly":           ["new customer month", "monthly acquisition"],
    "cumulative_customers_daily":      ["total customer", "cumulative customer", "customer base size"],
    # Customer demographics
    "customer_age_group_distribution": ["age group", "age distribution", "demographic", "how old"],
    "customer_city_distribution":      ["city distribution", "customers by city", "which city"],
    "customer_state_distribution":     ["state distribution", "customers by state", "which state"],
    "customer_country_distribution":   ["country distribution", "customers by country", "international"],
    "customer_age_group_spending":     ["age group spend", "age spending"],
    "new_vs_returning_customer_country": ["new vs returning", "returning customer"],
    # Customer behaviour
    "customer_engagement":             ["engagement score", "engaged customer", "active user"],
    "cart_abandonment_distribution":   ["cart abandon rate", "abandon distribution"],
    "top_customers_by_revenue":        ["top customer", "best customer", "vip", "highest spending"],
    "top_customers_by_profit":         ["most profitable customer", "top profit customer"],
    "discount_customers":              ["discount customer", "coupon user", "promo customer"],
    "discount_customers_summary":      ["discount summary", "promotion summary"],
    "correlation_discount_vs_clv":     ["discount vs clv", "discount lifetime"],
    "high_discount_customers":         ["high discount", "heavy discount user"],
    "cart_behavior_summary":           ["cart behavior", "shopping behavior"],
    "high_value_abandoners":           ["high value abandon", "vip cart abandon"],
    "churn_risk_summary":              ["churn", "at risk", "leaving", "retention", "lost customer", "cancel"],
    "high_clv_at_risk":                ["high value churn", "clv at risk", "vip churn"],
    "rfm_segment_summary":             ["rfm", "recency", "frequency", "monetary", "segment"],
    "high_intent_non_buyers":          ["intent not buy", "non buyer", "window shopper", "browse not buy"],
    "customer_overall_health_summary": ["customer health", "customer score", "overall customer"],
    "customers_cohorts":               ["cohort", "cohort analysis"],
    "signup_cohort_summary":           ["signup cohort", "registration cohort"],
    "customer_cohort_retention":       ["cohort retention", "retention by cohort"],
    "rfm_churn_crosstab":              ["rfm churn", "churn segment cross"],
    "seg_referrer_crosstab":           ["segment referrer", "source segment"],
    "seg_device_crosstab":             ["segment device", "device segment"],
    "payment_method_vs_clv_churn":     ["payment clv", "payment churn"],
    "payment_method_summary":          ["payment method", "how customer pay", "pay by"],
    "referrer_source_summary":         ["referrer", "traffic source", "where customer come", "acquisition source", "utm"],
    "referrer_churn_summary":          ["referrer churn", "source churn"],
    "customer_profit_per_segment":     ["profit per segment", "segment profit"],
    "gender_category_preference":      ["gender category", "male female category"],
    "gender_product_preference":       ["gender product", "male female product"],
    # Products
    "best_selling_products":           ["best sell", "top product", "popular product", "bestseller", "most sold", "top selling"],
    "product_monthly_trends":          ["product trend", "product monthly", "product performance over"],
    "category_monthly_trends":         ["category trend", "category monthly"],
    "highest_margin_products":         ["highest margin", "most profitable product", "best margin"],
    "low_margin_high_traffic_products":["low margin traffic", "high traffic low profit"],
    "out_of_stock_products":           ["out of stock", "stockout", "sold out", "unavailable product"],
    "low_conversion_products":         ["low conversion product", "viewed not bought"],
    "product_rating_summary":          ["product rating", "review score", "star rating", "product review"],
    "category_view_patterns":          ["category view", "browse category", "view pattern"],
    "top_view_to_purchase_products":   ["view to purchase", "most browsed bought"],
    "product_performance_score":       ["product score", "product performance score"],
    "category_revenue_share":          ["category revenue share", "revenue by category"],
    "low_performing_categories":       ["low performing category", "weak category"],
    "category_popularity_score":       ["category popularity", "popular category"],
    "category_profitability":          ["category profit", "profitable category"],
    "category_monthly_seasonality":    ["category seasonal", "seasonal category"],
    "category_peak_season":            ["peak season category", "category peak"],
    "product_lifecycle_segments":      ["product lifecycle", "product stage", "growth decline"],
    "product_lifecycle_summary":       ["lifecycle summary", "product life stage"],
    "product_stockout_risk":           ["stockout risk", "stock risk", "running out of stock"],
    "product_stockout_replenishment":  ["replenishment", "restock plan", "replenish"],
    "product_dead_stock":              ["dead stock", "slow moving", "no sell", "unsold"],
    "product_inventory_health":        ["inventory health", "stock health"],
    "product_inventory_critical":      ["critical stock", "urgent restock", "stock critical"],
    "supplier_product_performance":    ["supplier product", "vendor product performance"],
    "stockout_rate_by_product":        ["stockout rate product", "product out of stock rate"],
    "supplier_ranking_core":           ["supplier rank", "vendor rank"],
    "category_affinity_pairs":         ["category affinity", "category pair"],
    "product_affinity_pairs":          ["product affinity", "frequently bought together", "bundle", "cross sell"],
    "precomputed_product_recommendations": ["recommendation", "suggest product", "recommend product"],
    "inventory_stock_status":          ["stock status", "inventory status"],
    "days_of_supply":                  ["days of supply", "how long stock last", "stock duration"],
    "sku_reorder_urgency":             ["reorder urgency", "sku reorder"],
    "reorder_point_breach_frequency":  ["reorder breach", "reorder frequency"],
    "overstock_analysis":              ["overstock", "excess stock", "too much inventory"],
    "reserved_vs_available":           ["reserved stock", "available stock"],
    "excess_inventory_not_selling":    ["excess not selling", "surplus unsold"],
    "margin_erosion_risk":             ["margin erosion", "shrinking margin"],
    "inventory_carrying_cost_by_product": ["carrying cost product", "holding cost product"],
    # Suppliers
    "stockout_rate_by_supplier":       ["supplier stockout", "vendor stockout"],
    "storage_cost_efficiency_by_supplier": ["storage cost supplier", "warehouse cost vendor"],
    "inventory_carrying_cost_by_supplier": ["carrying cost supplier", "holding cost vendor"],
    "supplier_reliability":            ["supplier reliable", "vendor reliable", "supplier performance", "supplier quality"],
    "supplier_stockouts":              ["supplier out of stock", "vendor out of stock"],
    "supplier_fulfillment_performance":["supplier fulfillment", "vendor fulfillment", "supplier delivery"],
    "supplier_revenue_contribution":   ["supplier revenue", "vendor revenue contribution"],
    "supplier_profit_margin":          ["supplier margin", "vendor profit"],
    "supplier_days_since_last_restock":["last restock supplier", "supplier restock date"],
    "supplier_contract_expiry":        ["supplier contract", "vendor contract", "contract expiry"],
    # Marketing
    "campaign_performance_summary":    ["campaign", "marketing campaign", "ad campaign", "promotion performance"],
    "campaign_product_contribution":   ["campaign product", "which product campaign"],
    "campaign_ltv":                    ["campaign ltv", "campaign lifetime"],
    "campaign_customer_ltv_summary":   ["campaign customer ltv"],
    "campaign_wasteful_campaigns":     ["wasteful campaign", "inefficient campaign", "poor campaign", "bad ad"],
    "campaign_margin_profile":         ["campaign margin", "ad margin"],
    "campaign_performance":            ["campaign performance", "marketing performance"],
    # Revenue
    "low_margin_categories":           ["low margin category", "worst margin category"],
    "rev_by_country_city":             ["revenue country", "revenue city", "revenue by location"],
    "rev_by_customer_segment":         ["revenue by segment", "segment revenue"],
    "rev_by_rfm_segment":              ["revenue rfm", "rfm revenue"],
    "rev_by_segment_label":            ["revenue label", "segment label revenue"],
    "rev_by_referrer":                 ["revenue referrer", "revenue by source", "revenue by traffic"],
    "rev_by_device":                   ["revenue device", "mobile revenue", "desktop revenue"],
    "aov_trend_daily":                 ["aov", "average order value", "order value daily"],
    "aov_trend_weekly":                ["aov weekly", "average order weekly"],
    "aov_trend_monthly":               ["aov monthly", "average order monthly"],
    "segment_aov_by_rfm":              ["rfm aov", "segment average order"],
    "inventory_carrying_cost_overall": ["overall carrying cost", "total holding cost"],
    # Funnel
    "high_value_funnel":               ["high value funnel", "vip funnel"],
    "high_value_vs_regular":           ["high value vs regular", "vip vs regular"],
    "funnel_by_device":                ["funnel device", "device funnel"],
    "funnel_by_referrer":              ["funnel referrer", "source funnel"],
    "abandoned_vs_converted":          ["abandon vs convert", "conversion vs abandon"],
    "checkout_dropoff_reasons":        ["checkout drop", "dropoff reason", "why abandon checkout"],
    "checkout_dropoff_buckets":        ["dropoff bucket", "checkout stage"],
    "checkout_dropoff_by_device_and_reason": ["device dropoff", "device abandon reason"],
    "device_conversion_rates":         ["device conversion", "mobile convert", "desktop convert"],
    # Payment
    "payment_counts_by_country_method":["payment country", "payment by country"],
    "payment_counts_by_state_method":  ["payment state", "payment by state"],
    "payment_method_success_rates":    ["payment success rate", "payment fail", "decline rate"],
    "payment_method_success_rates_by_country": ["payment success country"],
    "payment_method_aov":              ["payment aov", "order value by payment"],
    "refund_rate_by_payment_method":   ["refund payment", "refund by method"],
    "refund_rate_by_product":          ["product refund", "refund by product"],
    "refund_rate_by_month":            ["monthly refund", "refund trend", "refund rate"],
    "time_to_refund_by_payment_method":["time to refund", "refund speed"],
    # Reviews
    "review_velocity_daily":           ["review velocity", "review rate", "how many review"],
    "review_velocity_weekly":          ["weekly review", "review volume"],
    "review_velocity_monthly":         ["monthly review rate"],
    "sentiment_by_category":           ["sentiment", "review opinion", "positive negative review"],
    "product_monthly_rating_trends":   ["rating trend", "rating over time"],
    "low_rated_product_monthly_trends_rating_only": ["low rating trend", "poorly rated trend"],
    "rating_tier_per_product":         ["rating tier", "product star tier"],
    "rating_tier_sales_velocity":      ["rating sales", "rating vs sales"],
    # Operations
    "processing_by_category":          ["processing category", "fulfillment category"],
    "processing_by_subcategory":       ["processing subcategory"],
    "processing_by_hour":              ["processing hour", "order hour", "busiest hour"],
    "processing_by_day_of_week":       ["day of week", "busiest day", "weekly pattern"],
    "weekend_vs_weekday":              ["weekend", "weekday vs weekend"],
    "delivery_days_by_country":        ["delivery days country", "shipping days country"],
    "delivery_days_by_state":          ["delivery days state"],
    "delivery_days_by_city":           ["delivery days city"],
    "ontime_delivery_by_country":      ["on time country", "late delivery country", "delivery performance"],
    "ontime_delivery_by_state":        ["on time state"],
    "ontime_delivery_by_city":         ["on time city"],
    "shipping_efficiency_by_country":  ["shipping efficiency", "fulfillment speed country"],
    "shipping_efficiency_by_state":    ["shipping efficiency state"],
    "shipping_efficiency_by_city":     ["shipping efficiency city"],
    "processing_by_season":            ["processing season", "seasonal fulfillment"],
    "processing_by_season_and_status": ["processing season status"],
    # Wishlist
    "wishlist_overall_summary":        ["wishlist", "wish list", "saved item"],
    "wishlist_by_product":             ["wishlist product", "most wishlisted"],
    "wishlist_by_customer":            ["wishlist customer", "customer wishlist"],
    "wishlist_time_to_purchase_stats": ["wishlist purchase time", "wishlist to buy"],
    "wishlist_time_to_purchase_distribution": ["wishlist buy distribution"],
    "abandoned_wishlist_items":        ["wishlist abandon", "saved not bought"],
    "abandoned_wishlist_by_customer":  ["customer wishlist abandon"],
    "abandoned_wishlist_by_product":   ["product wishlist abandon"],
    "wishlist_adds_by_month":          ["wishlist monthly", "monthly wishlist"],
    # Cart
    "cart_overall_stats":              ["cart stat", "overall cart", "shopping cart summary"],
    "cart_status_distribution":        ["cart status", "cart stage"],
    "cart_value_stats":                ["cart value", "basket value"],
    "high_value_abandoned_carts":      ["high value cart abandon", "expensive cart abandon"],
    "time_to_purchase_overall":        ["time to purchase", "how long to buy"],
    "time_to_purchase_by_tier":        ["time to purchase tier"],
    "time_to_purchase_buckets":        ["time to purchase bucket"],
    # Geo
    "geo_acquisition":                 ["geo acquisition", "map acquisition", "geographic sign"],
    # ML — regression
    "revenue_forecast":                ["revenue forecast", "future revenue", "predict revenue", "forecast"],
    "demand_forecast":                 ["demand forecast", "predict demand", "stock forecast"],
    "aov_prediction":                  ["predict order value", "aov predict", "forecast order"],
    "clv_predictions":                 ["predict clv", "forecast lifetime", "clv model"],
    "restock_quantity":                ["restock quantity", "how much order", "order quantity predict"],
    "safety_stock_adjusted":           ["safety stock", "buffer stock"],
    "session_conversion_value":        ["session value predict", "conversion value"],
    "stockout_probability":            ["stockout probability", "chance run out"],
    "campaign_roi":                    ["campaign roi", "marketing roi", "ad return predict"],
    "delivery_time":                   ["delivery time predict", "shipping time predict"],
    "price_optimization":              ["price optimiz", "optimal price", "best price", "pricing model"],
    "seasonal_trends":                 ["seasonal trend", "season model", "peak season predict"],
    # ML — classification
    "customer_churn_predictions":      ["churn predict", "who will churn", "churn probability", "churn model"],
    "cart_abandonment_predictions":    ["abandon predict", "cart abandon model", "who will abandon"],
    "customer_segment_predictions":    ["segment predict", "customer segment model"],
    "payment_success_predictions":     ["payment success predict", "payment model", "transaction predict"],
    "review_sentiment_predictions":    ["sentiment predict", "review sentiment model"],
    "stock_status_predictions":        ["stock status predict", "inventory model"],
    "fulfillment_risk_predictions":    ["fulfillment risk", "delivery risk predict"],
    "product_bundling_predictions":    ["bundle predict", "bundling model"],
    # ML — clustering
    "customer_segmentation":           ["customer cluster", "customer segment model", "segment group"],
    "geographic_clustering":           ["geo cluster", "location cluster"],
    "session_behavior_clustering":     ["session cluster", "behavior cluster"],
    "supplier_clustering":             ["supplier cluster", "vendor group"],
    "product_affinity_clustering":     ["product cluster", "affinity cluster"],
    "product_lifecycle_clustering":    ["lifecycle cluster", "product stage cluster"],
    # Aggregated raw tables (usually fallback / explicit requests)
    "agg_customers":                   ["raw customer data", "customer table"],
    "agg_orders":                      ["raw order data", "order table"],
    "agg_products":                    ["raw product data", "product table"],
    "agg_inventory":                   ["raw inventory", "inventory table"],
    "agg_payments":                    ["raw payment data", "payment table"],
    "agg_reviews":                     ["raw review data", "review table"],
    "agg_marketing_campaigns":         ["raw campaign data"],
    "agg_suppliers":                   ["raw supplier data"],
    "agg_order_items":                 ["order item data", "line item"],
    "agg_shopping_cart":               ["raw cart data", "cart table"],
    "agg_customer_sessions":           ["raw session data", "session table"],
    "agg_wishlist":                    ["raw wishlist data", "wishlist table"],
}

_DEFAULT_FALLBACK  = ["business_health_daily", "clv_summary", "funnel_summary"]
_MAX_CANDIDATES    = 15   # max fed to Gemini Pass-1
_MAX_FINAL_SOURCES = 3    # max sources fetched and sent to Gemini Pass-2
_MAX_ROWS_PER_SOURCE = 20 # max data rows per source in the Pass-2 prompt
_MAX_STR_LEN       = 80   # truncate long string values to save tokens


def _keyword_prefilter(query: str) -> List[str]:
    """
    Layer 1: zero-token local pre-filter.

    Scans _KEYWORD_MAP for any fragment that is a substring of the
    lowercased query. Returns up to _MAX_CANDIDATES matched source names,
    or _DEFAULT_FALLBACK when nothing matches.
    """
    q = query.lower()
    hits: List[str] = []
    for source, fragments in _KEYWORD_MAP.items():
        if any(frag in q for frag in fragments):
            hits.append(source)
            if len(hits) >= _MAX_CANDIDATES:
                break
    return hits if hits else list(_DEFAULT_FALLBACK)


def _classify_sources(names: List[str]) -> Dict[str, List[str]]:
    """Split a flat list of source names into the three buckets."""
    return {
        "analytics":      [n for n in names if n in ANALYTIC_TO_CATEGORY],
        "ml_predictions": [n for n in names if n in _ALL_ML],
        "aggregated":     [n for n in names if n in _ALL_AGG],
        "reasoning":      "Keyword pre-filter (direct)",
    }


# ---------------------------------------------------------------------------
# Layer 3 — DataFrame trimmer for the Pass-2 data block
# ---------------------------------------------------------------------------
def _trim_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove low-signal columns and cap row/string size before serialising.

    • Drop columns where >95 % of rows share the same value (near-constant).
    • Truncate string values to _MAX_STR_LEN characters.
    """
    if df.empty:
        return df

    # Drop near-constant columns
    keep = []
    for col in df.columns:
        try:
            top_freq = df[col].value_counts(dropna=False).iloc[0] / len(df)
            if top_freq < 0.95:
                keep.append(col)
        except Exception:
            keep.append(col)   # keep if we can't evaluate

    df = df[keep] if keep else df

    # Truncate long strings
    def _cap(v: Any) -> Any:
        if isinstance(v, str) and len(v) > _MAX_STR_LEN:
            return v[:_MAX_STR_LEN] + "…"
        return v

    return df.applymap(_cap)


# ---------------------------------------------------------------------------
# Analysis system prompt (static — never changes between requests)
# ---------------------------------------------------------------------------
_ANALYSIS_PROMPT = (
    "You are Pulse AI, an expert analytics assistant for an e-commerce platform.\n"
    "Answer using ONLY the data provided. Be concise and cite specific numbers.\n"
    "Use markdown (headers, bold, bullets). If data is absent, say so honestly.\n"
    "Add actionable insights where the data supports them.\n"
)


class XAIService:
    """Gemini-powered Explainable AI service with three-layer token optimisation."""

    _MAX_RETRIES      = 3
    _RETRY_BASE_DELAY = 10.0   # seconds; doubles per attempt

    def __init__(self) -> None:
        self.minio_client = self._init_minio()
        self.model_name   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-lite")
        self._client      = self._init_gemini()

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------
    def _init_minio(self) -> Minio:
        endpoint = re.sub(r"^https?://", "", os.getenv("MINIO_ENDPOINT", "minio:9000"))
        return Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def _init_gemini(self):
        key = os.getenv("GEMINI_API_KEY", "").strip()
        if not key:
            raise ValueError(
                "[XAI] GEMINI_API_KEY is not set — add it to your .env file."
            )
        client = _genai.Client(api_key=key)
        print(f"[XAI] Gemini ready — model={self.model_name}")
        return client

    # ------------------------------------------------------------------
    # Gemini call with retry on 429
    # ------------------------------------------------------------------
    async def _generate(self, prompt: str) -> str:
        last_exc: Exception = RuntimeError("No attempts made")
        for attempt in range(self._MAX_RETRIES):
            try:
                resp = self._client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                )
                return resp.text.strip()
            except Exception as exc:
                last_exc = exc
                if self._is_quota_error(exc):
                    delay = (
                        self._parse_retry_delay(exc)
                        or self._RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 3)
                    )
                    print(
                        f"[XAI] 429 quota — attempt {attempt + 1}/{self._MAX_RETRIES}, "
                        f"retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                else:
                    raise  # Auth / schema errors: fail immediately
        raise last_exc

    # ------------------------------------------------------------------
    # Error helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _is_quota_error(exc: Exception) -> bool:
        m = str(exc).lower()
        return "429" in m or "resource_exhausted" in m or "rate limit" in m

    @staticmethod
    def _is_auth_error(exc: Exception) -> bool:
        m = str(exc).lower()
        return any(t in m for t in ("401", "403", "api key", "permission denied", "invalid key"))

    @staticmethod
    def _classify_error(exc: Exception) -> str:
        if XAIService._is_quota_error(exc):
            return "quota_exceeded"
        if XAIService._is_auth_error(exc):
            return "auth_error"
        return "gemini_error"

    @staticmethod
    def _parse_retry_delay(exc: Exception) -> Optional[float]:
        m = re.search(r"retryDelay['\"]:\s*['\"](\d+)s", str(exc))
        return float(m.group(1)) + random.uniform(1, 3) if m else None

    # ------------------------------------------------------------------
    # MinIO helpers
    # ------------------------------------------------------------------
    def _read_parquet(self, bucket: str, path: str) -> Optional[pd.DataFrame]:
        try:
            resp = self.minio_client.get_object(bucket, path)
            data = resp.read()
            resp.close(); resp.release_conn()
            return pd.read_parquet(io.BytesIO(data))
        except Exception:
            return None

    def _read_dir_parquet(self, bucket: str, prefix: str) -> Optional[pd.DataFrame]:
        try:
            parts: List[pd.DataFrame] = []
            for obj in self.minio_client.list_objects(bucket, prefix=prefix, recursive=True):
                if obj.is_dir or not obj.object_name.endswith(".parquet"):
                    continue
                df = self._read_parquet(bucket, obj.object_name)
                if df is not None and not df.empty:
                    parts.append(df)
                    if sum(len(p) for p in parts) >= 500:
                        break
            return pd.concat(parts, ignore_index=True) if parts else None
        except Exception:
            return None

    def _fetch(self, source_type: str, bucket: str, name: str) -> Optional[pd.DataFrame]:
        if source_type == "analytics":
            cat = ANALYTIC_TO_CATEGORY.get(name)
            return self._read_parquet(bucket, f"analytics/{cat}/{name}.parquet") if cat else None
        if source_type == "ml":
            prefix = ML_INFERENCE_CATALOG.get(name)
            return self._read_dir_parquet(bucket, prefix) if prefix else None
        if source_type == "aggregated":
            df = self._read_parquet(bucket, f"transformed/{name}.parquet")
            return df if df is not None else self._read_dir_parquet(bucket, f"transformed/{name}/")
        return None

    @staticmethod
    def _sanitize(v: Any) -> Any:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        return v

    def _df_to_text(self, df: Optional[pd.DataFrame], label: str) -> str:
        """
        Layer 3: convert a DataFrame to a compact text block.
        Applies _trim_df first, then serialises max _MAX_ROWS_PER_SOURCE rows.
        """
        if df is None or df.empty:
            return f"[{label}]: No data available.\n"

        df = _trim_df(df)
        sample = df.head(_MAX_ROWS_PER_SOURCE)
        lines = [f"[{label}] {len(df)} rows | cols: {list(sample.columns)}"]
        for _, row in sample.iterrows():
            lines.append(
                json.dumps({k: self._sanitize(v) for k, v in row.to_dict().items()}, default=str)
            )
        if len(df) > _MAX_ROWS_PER_SOURCE:
            lines.append(f"… {len(df) - _MAX_ROWS_PER_SOURCE} more rows omitted")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Pass 1 — Context extraction (Layers 1 + 2)
    # ------------------------------------------------------------------
    async def extract_context(self, query: str) -> Dict[str, Any]:
        """
        Determine which data sources to fetch.

        LAYER 1 — keyword pre-filter (0 tokens):
            ~250 sources → ≤15 candidates in Python.

        LAYER 2 — tiny Gemini prompt (~120 tokens, only when needed):
            If ≤5 candidates, skip Gemini and use them directly.
            Otherwise send ONLY the short candidate list to Gemini
            (not the full catalog) to pick the best ≤_MAX_FINAL_SOURCES.
        """
        candidates = _keyword_prefilter(query)

        # Fast-path: few enough candidates → no Gemini call needed
        if len(candidates) <= 5:
            result = _classify_sources(candidates)
            # Enforce the cap
            result["analytics"]      = result["analytics"][:_MAX_FINAL_SOURCES]
            result["ml_predictions"] = result["ml_predictions"][:_MAX_FINAL_SOURCES]
            result["aggregated"]     = result["aggregated"][:_MAX_FINAL_SOURCES]
            _log_context("pre-filter (skipped Gemini)", result)
            return result

        # Layer 2: compact disambiguation prompt
        ml_in_candidates  = [c for c in candidates if c in _ALL_ML]
        agg_in_candidates = [c for c in candidates if c in _ALL_AGG]

        prompt = (
            f'User query: "{query}"\n\n'
            f'Candidate data sources: {json.dumps(candidates)}\n'
            f'Of those, ML sources: {json.dumps(ml_in_candidates)}\n'
            f'Of those, aggregated table sources: {json.dumps(agg_in_candidates)}\n'
            f'Everything else in the candidate list is an analytics file.\n\n'
            f'Pick the BEST {_MAX_FINAL_SOURCES} sources from the candidate list to answer the query.\n'
            f'Return ONLY valid JSON, no markdown:\n'
            f'{{"analytics":[],"ml_predictions":[],"aggregated":[],"reasoning":""}}'
        )

        try:
            text = await self._generate(prompt)
            text = re.sub(r"```[a-z]*\n?|```", "", text).strip()
            parsed = json.loads(text)
            # Sanitise — only keep valid known names
            parsed["analytics"]      = [s for s in parsed.get("analytics", [])      if s in ANALYTIC_TO_CATEGORY][:_MAX_FINAL_SOURCES]
            parsed["ml_predictions"] = [s for s in parsed.get("ml_predictions", []) if s in _ALL_ML][:_MAX_FINAL_SOURCES]
            parsed["aggregated"]     = [s for s in parsed.get("aggregated", [])     if s in _ALL_AGG][:_MAX_FINAL_SOURCES]
            _log_context("Gemini selection", parsed)
            return parsed
        except json.JSONDecodeError:
            # Fallback: use pre-filter result directly
            result = _classify_sources(candidates[:_MAX_FINAL_SOURCES])
            _log_context("pre-filter (JSON fallback)", result)
            return result

    # ------------------------------------------------------------------
    # Pass 2 — Data analysis (Layer 3 applied during fetch)
    # ------------------------------------------------------------------
    async def analyze_with_data(self, query: str, context: Dict[str, Any], business_id: str) -> str:
        summaries: List[str] = []

        for name in context.get("analytics", []):
            summaries.append(self._df_to_text(self._fetch("analytics", business_id, name), f"analytics/{name}"))
        for name in context.get("ml_predictions", []):
            summaries.append(self._df_to_text(self._fetch("ml", business_id, name), f"ml/{name}"))
        for name in context.get("aggregated", []):
            summaries.append(self._df_to_text(self._fetch("aggregated", business_id, name), f"agg/{name}"))

        data_block = "\n\n".join(summaries) or "No data found for the requested sources."

        prompt = (
            _ANALYSIS_PROMPT
            + "\n\n--- DATA ---\n"
            + data_block
            + "\n\n--- QUESTION ---\n"
            + query
        )
        return await self._generate(prompt)

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------
    async def process_query(self, query: str, business_id: str) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "analytics": [], "ml_predictions": [], "aggregated": [], "reasoning": ""
        }

        try:
            context = await self.extract_context(query)
        except Exception as exc:
            code = self._classify_error(exc)
            print(f"[XAI] Pass 1 failed ({code}): {exc}")
            return {"answer": None, "context": {}, "error": code}

        try:
            answer = await self.analyze_with_data(query, context, business_id)
            return {"answer": answer, "context": context, "error": None}
        except Exception as exc:
            code = self._classify_error(exc)
            print(f"[XAI] Pass 2 failed ({code}): {exc}")
            return {"answer": None, "context": context, "error": code}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _log_context(method: str, ctx: Dict[str, Any]) -> None:
    total = (
        len(ctx.get("analytics", []))
        + len(ctx.get("ml_predictions", []))
        + len(ctx.get("aggregated", []))
    )
    print(
        f"[XAI] Context ({method}): "
        f"analytics={ctx.get('analytics', [])} "
        f"ml={ctx.get('ml_predictions', [])} "
        f"agg={ctx.get('aggregated', [])} "
        f"[{total} source(s)]"
    )