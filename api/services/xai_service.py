"""
XAI (Explainable AI) Service — Gemini-powered analytics chatbot.

Two-pass Gemini approach:
  1. CONTEXT EXTRACTION — Parse the user query to identify which data sources
     (analytics tables, ML predictions, aggregated files) are relevant.
  2. DATA ANALYSIS — Fetch the identified data from MinIO, feed it to Gemini
     with the query, and produce a human-readable answer.

Data locations in MinIO (bucket = businessId):
  - <bucket>/transformed/                           → aggregated parquet files
  - <bucket>/analytics/<category>/<analytic>.parquet → analytics files
  - <bucket>/machine-learning/<type>/predictions/<name>/  → ML inference files
"""

import io
import json
import math
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from google import genai
from minio import Minio
from minio.error import S3Error

# ---------------------------------------------------------------------------
# Analytics category → file mapping (mirrored from AnalyticsService)
# ---------------------------------------------------------------------------
ANALYTICS_CATEGORIES = {
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
    "geo_analytics": [
        "geo_acquisition",
    ],
}

# Flat lookup: analytic_name → category
ANALYTIC_TO_CATEGORY = {}
for cat, files in ANALYTICS_CATEGORIES.items():
    for f in files:
        ANALYTIC_TO_CATEGORY[f] = cat

# All analytics as a flat list for the system prompt
ALL_ANALYTICS = list(ANALYTIC_TO_CATEGORY.keys())

# ---------------------------------------------------------------------------
# ML inference catalog
# ---------------------------------------------------------------------------
ML_INFERENCE_CATALOG = {
    # Classification
    "cart_abandonment_predictions": "machine-learning/classification/predictions/cart_abandonment_predictions/",
    "customer_churn_predictions": "machine-learning/classification/predictions/customer_churn_predictions/",
    "customer_segment_predictions": "machine-learning/classification/predictions/customer_segment_predictions/",
    "payment_success_predictions": "machine-learning/classification/predictions/payment_success_predictions/",
    "review_sentiment_predictions": "machine-learning/classification/predictions/review_sentiment_predictions/",
    "stock_status_predictions": "machine-learning/classification/predictions/stock_status_predictions/",
    "fulfillment_risk_predictions": "machine-learning/classification/predictions/fulfillment_risk_predictions/",
    "product_bundling_predictions": "machine-learning/classification/predictions/product_bundling_predictions/",
    # Clustering
    "customer_segmentation": "machine-learning/clustering/predictions/customer_segmentation.parquet/",
    "geographic_clustering": "machine-learning/clustering/predictions/geographic_clustering.parquet/",
    "session_behavior_clustering": "machine-learning/clustering/predictions/session_behavior_clustering.parquet/",
    "supplier_clustering": "machine-learning/clustering/predictions/supplier_clustering.parquet/",
    "product_affinity_clustering": "machine-learning/clustering/predictions/product_affinity_clustering.parquet/",
    "product_lifecycle_clustering": "machine-learning/clustering/predictions/product_lifecycle_clustering.parquet/",
    # Regression
    "aov_prediction": "machine-learning/regression/predictions/aov_prediction/",
    "clv_predictions": "machine-learning/regression/predictions/clv_predictions/",
    "restock_quantity": "machine-learning/regression/predictions/restock_quantity/",
    "safety_stock_adjusted": "machine-learning/regression/predictions/safety_stock_adjusted/",
    "session_conversion_value": "machine-learning/regression/predictions/session_conversion_value/",
    "stockout_probability": "machine-learning/regression/predictions/stockout_probability/",
    "campaign_roi": "machine-learning/regression/predictions/campaign_roi/",
    "delivery_time": "machine-learning/regression/predictions/delivery_time/",
    "demand_forecast": "machine-learning/regression/predictions/demand_forecast/",
    "price_optimization": "machine-learning/regression/predictions/price_optimization/",
    "revenue_forecast": "machine-learning/regression/predictions/revenue_forecast/",
    "seasonal_trends": "machine-learning/regression/predictions/seasonal_trends/",
}

# Aggregated table names (in transformed/)
AGGREGATED_TABLES = [
    "agg_customer_sessions", "agg_customers", "agg_inventory",
    "agg_marketing_campaigns", "agg_order_items", "agg_orders",
    "agg_payments", "agg_products", "agg_reviews", "agg_shopping_cart",
    "agg_suppliers", "agg_wishlist",
]


class XAIService:
    """Gemini-powered Explainable AI service for analytics chat."""

    def __init__(self):
        self.minio_client = self._create_minio_client()
        api_key = os.getenv("GEMINI_API_KEY", "")
        self.gemini_client = genai.Client(api_key=api_key)
        self.model_name = "gemini-2.0-flash"

    # ------------------------------------------------------------------
    # MinIO helpers
    # ------------------------------------------------------------------
    def _create_minio_client(self) -> Minio:
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        if "://" in endpoint:
            endpoint = endpoint.split("://", 1)[1]
        return Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def _read_parquet_from_minio(self, bucket: str, path: str, max_rows: int = 500) -> Optional[pd.DataFrame]:
        """Read a single parquet object from MinIO, capped to *max_rows*."""
        try:
            resp = self.minio_client.get_object(bucket, path)
            data = resp.read()
            resp.close()
            resp.release_conn()
            df = pd.read_parquet(io.BytesIO(data))
            return df.head(max_rows)
        except Exception:
            return None

    def _read_directory_parquet(self, bucket: str, prefix: str, max_rows: int = 500) -> Optional[pd.DataFrame]:
        """Read all parquet parts under a MinIO prefix and concatenate."""
        try:
            objects = list(self.minio_client.list_objects(bucket, prefix=prefix, recursive=True))
            parts = []
            for obj in objects:
                if obj.is_dir or not obj.object_name.endswith(".parquet"):
                    continue
                df = self._read_parquet_from_minio(bucket, obj.object_name, max_rows=max_rows)
                if df is not None and not df.empty:
                    parts.append(df)
                    if sum(len(p) for p in parts) >= max_rows:
                        break
            if not parts:
                return None
            combined = pd.concat(parts, ignore_index=True)
            return combined.head(max_rows)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------
    def _fetch_analytics(self, bucket: str, analytic_name: str) -> Optional[pd.DataFrame]:
        """Fetch an analytics parquet file."""
        category = ANALYTIC_TO_CATEGORY.get(analytic_name)
        if not category:
            return None
        path = f"analytics/{category}/{analytic_name}.parquet"
        return self._read_parquet_from_minio(bucket, path)

    def _fetch_ml_prediction(self, bucket: str, inference_name: str) -> Optional[pd.DataFrame]:
        """Fetch ML inference parquet (directory of part files)."""
        prefix = ML_INFERENCE_CATALOG.get(inference_name)
        if not prefix:
            return None
        return self._read_directory_parquet(bucket, prefix)

    def _fetch_aggregated(self, bucket: str, table_name: str) -> Optional[pd.DataFrame]:
        """Fetch an aggregated table parquet from transformed/."""
        path = f"transformed/{table_name}.parquet"
        df = self._read_parquet_from_minio(bucket, path)
        if df is not None:
            return df
        # Try directory style
        return self._read_directory_parquet(bucket, f"transformed/{table_name}/")

    def _sanitize_value(self, v):
        """Make a value JSON-safe."""
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        return v

    def _df_to_summary(self, df: pd.DataFrame, name: str, max_rows: int = 50) -> str:
        """Convert a dataframe to a compact text summary for the LLM."""
        if df is None or df.empty:
            return f"[{name}]: No data available.\n"
        lines = [f"[{name}] — {len(df)} rows, columns: {list(df.columns)}"]
        # Include first N rows as records
        sample = df.head(max_rows)
        for _, row in sample.iterrows():
            record = {k: self._sanitize_value(v) for k, v in row.to_dict().items()}
            lines.append(json.dumps(record, default=str))
        if len(df) > max_rows:
            lines.append(f"... ({len(df) - max_rows} more rows omitted)")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Gemini Pass 1: Context extraction
    # ------------------------------------------------------------------
    CONTEXT_EXTRACTION_PROMPT = f"""You are a context extraction engine for an e-commerce analytics platform called Pulse.
Your job is to analyze the user's query and determine which data sources are needed to answer it.

Available data sources:

1. ANALYTICS FILES (analytics/<category>/<name>.parquet):
   Categories and files: {json.dumps({cat: files for cat, files in ANALYTICS_CATEGORIES.items()}, indent=0)}

2. ML PREDICTIONS (machine-learning/<type>/predictions/<name>/):
   {json.dumps(list(ML_INFERENCE_CATALOG.keys()))}

3. AGGREGATED TABLES (transformed/<name>.parquet):
   {json.dumps(AGGREGATED_TABLES)}

Respond ONLY with valid JSON (no markdown, no explanation) in this exact format:
{{
  "analytics": ["file_name1", "file_name2"],
  "ml_predictions": ["prediction_name1"],
  "aggregated": ["table_name1"],
  "reasoning": "Brief explanation of why these sources are relevant"
}}

Rules:
- Pick ONLY the most relevant sources (max 5 total across all categories).
- If the query is general (e.g. "how is my business doing?"), pick a few KPI/summary sources.
- If no data sources match, return empty arrays.
- Never invent source names that aren't in the lists above.
"""

    @staticmethod
    def _is_quota_error(exc: Exception) -> bool:
        """Return True when the exception is a Gemini quota / rate-limit error."""
        msg = str(exc)
        return "429" in msg or "RESOURCE_EXHAUSTED" in msg or "quota" in msg.lower()

    async def extract_context(self, query: str) -> Dict[str, Any]:
        """Pass 1: Use Gemini to identify which data sources to fetch."""
        try:
            response = self.gemini_client.models.generate_content(
                model=self.model_name,
                contents=[
                    {"role": "user", "parts": [{"text": self.CONTEXT_EXTRACTION_PROMPT + f"\n\nUser query: {query}"}]}
                ],
            )
            text = response.text.strip()
            # Strip markdown code fence if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()
            return json.loads(text)
        except Exception as e:
            if self._is_quota_error(e):
                raise
            print(f"[XAI] Context extraction error: {e}")
            return {"analytics": [], "ml_predictions": [], "aggregated": [], "reasoning": str(e)}

    # ------------------------------------------------------------------
    # Gemini Pass 2: Data analysis
    # ------------------------------------------------------------------
    async def analyze_with_data(self, query: str, context: Dict[str, Any], business_id: str) -> str:
        """Pass 2: Fetch data from MinIO based on context, send to Gemini for analysis."""
        data_summaries = []

        # Fetch analytics
        for name in context.get("analytics", []):
            df = self._fetch_analytics(business_id, name)
            data_summaries.append(self._df_to_summary(df, f"analytics/{name}"))

        # Fetch ML predictions
        for name in context.get("ml_predictions", []):
            df = self._fetch_ml_prediction(business_id, name)
            data_summaries.append(self._df_to_summary(df, f"ml/{name}"))

        # Fetch aggregated
        for name in context.get("aggregated", []):
            df = self._fetch_aggregated(business_id, name)
            data_summaries.append(self._df_to_summary(df, f"aggregated/{name}"))

        data_block = "\n\n".join(data_summaries) if data_summaries else "No data was found for the requested sources."

        system_prompt = """You are Pulse AI — an expert analytics assistant for an e-commerce analytics platform.
You answer questions about the user's business using the provided data.

Guidelines:
- Be concise, specific, and data-driven.
- Reference actual numbers from the data.
- If data is missing or empty, say so honestly.
- Use markdown formatting for readability (headers, bullet points, bold numbers).
- When discussing trends, highlight key takeaways.
- Do NOT hallucinate data that isn't in the provided datasets.
- If you can derive actionable insights, include them.
"""

        user_content = f"""Here is the relevant data for this business:

{data_block}

---
User question: {query}

Please analyze the data and provide a clear, helpful answer."""

        try:
            response = self.gemini_client.models.generate_content(
                model=self.model_name,
                contents=[
                    {"role": "user", "parts": [{"text": system_prompt + "\n\n" + user_content}]}
                ],
            )
            return response.text.strip()
        except Exception as e:
            print(f"[XAI] Analysis error: {e}")
            raise

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------
    async def process_query(self, query: str, business_id: str) -> Dict[str, Any]:
        """
        Run the full two-pass pipeline:
        1. Extract context from query
        2. Fetch data & analyze

        Returns dict with keys: answer, context, error
        """
        # Pass 1
        try:
            context = await self.extract_context(query)
        except Exception as e:
            if self._is_quota_error(e):
                return {
                    "answer": None,
                    "context": {},
                    "error": "quota_exceeded",
                }
            context = {"analytics": [], "ml_predictions": [], "aggregated": [], "reasoning": str(e)}

        # Pass 2
        try:
            answer = await self.analyze_with_data(query, context, business_id)
            return {
                "answer": answer,
                "context": context,
                "error": None,
            }
        except Exception as e:
            if self._is_quota_error(e):
                return {
                    "answer": None,
                    "context": context,
                    "error": "quota_exceeded",
                }
            return {
                "answer": None,
                "context": context,
                "error": str(e),
            }
