"""
Forecasting Service — reads ML inference results from the business-owned MinIO bucket.

Each business's bucket (named after its businessId) contains inference output at:
  machine-learning/{type}/predictions/{output_name}/     (directory-partitioned parquet)
  machine-learning/{type}/predictions/{output_name}.parquet  (single parquet file)

This service is intentionally read-only and never exposes raw exception details
to callers — every fetch returns None on any storage error so the caller can
gracefully skip missing inferences.
"""

import io
import math
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from minio import Minio
from minio.error import S3Error


class ForecastingService:
    """Fetch ML inference results from a business's MinIO bucket."""

    # ------------------------------------------------------------------
    # Catalog  (26 unique output locations; 27 inference files because
    # infer_aov.py v1 and infer_aov_v2.py both write to aov_prediction/)
    # ------------------------------------------------------------------
    INFERENCE_CATALOG: Dict[str, Dict[str, str]] = {
        # ── GENERAL CLASSIFICATION ────────────────────────────────────
        "cart_abandonment_predictions": {
            "label": "Cart Abandonment Predictions",
            "path": "machine-learning/classification/predictions/cart_abandonment_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "Predicts which active carts are likely to be abandoned with risk scores.",
        },
        "customer_churn_predictions": {
            "label": "Customer Churn Predictions",
            "path": "machine-learning/classification/predictions/customer_churn_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "Classifies each customer's churn risk (High / Medium / Low).",
        },
        "customer_segment_predictions": {
            "label": "Customer Segment Predictions",
            "path": "machine-learning/classification/predictions/customer_segment_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "RFM-based segment classification for every customer.",
        },
        "payment_success_predictions": {
            "label": "Payment Success Predictions",
            "path": "machine-learning/classification/predictions/payment_success_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "Predicts whether a payment will succeed or fail.",
        },
        "review_sentiment_predictions": {
            "label": "Review Sentiment Predictions",
            "path": "machine-learning/classification/predictions/review_sentiment_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "Classifies review text as Positive / Neutral / Negative.",
        },
        "stock_status_predictions": {
            "label": "Stock Status Predictions",
            "path": "machine-learning/classification/predictions/stock_status_predictions/",
            "path_type": "directory",
            "group": "general_classification",
            "description": "Predicts stock health: In Stock, Low Stock, Out of Stock, or Overstock.",
        },
        # ── GENERAL CLUSTERING ────────────────────────────────────────
        "customer_segmentation": {
            "label": "Customer Segmentation (RFM Clustering)",
            "path": "machine-learning/clustering/predictions/customer_segmentation.parquet/",
            "path_type": "directory",
            "group": "general_clustering",
            "description": "K-Means clusters customers by RFM metrics into semantic personas.",
        },
        "geographic_clustering": {
            "label": "Geographic Sales Clustering",
            "path": "machine-learning/clustering/predictions/geographic_clustering.parquet/",
            "path_type": "directory",
            "group": "general_clustering",
            "description": "Groups geographic regions by sales performance and market potential.",
        },
        "session_behavior_clustering": {
            "label": "Session Behavior Clustering",
            "path": "machine-learning/clustering/predictions/session_behavior_clustering.parquet/",
            "path_type": "directory",
            "group": "general_clustering",
            "description": "Clusters user sessions into behavior personas (Quick Buyers, Researchers, etc.).",
        },
        "supplier_clustering": {
            "label": "Supplier Performance Clustering",
            "path": "machine-learning/clustering/predictions/supplier_clustering.parquet/",
            "path_type": "directory",
            "group": "general_clustering",
            "description": "Segments suppliers into performance tiers (Strategic Partners, Risk Suppliers, etc.).",
        },
        # ── GENERAL REGRESSION ───────────────────────────────────────
        "aov_prediction": {
            "label": "Average Order Value Prediction",
            "path": "machine-learning/regression/predictions/aov_prediction/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "Predicts each customer's next average order value with confidence intervals.",
        },
        "clv_predictions": {
            "label": "Customer Lifetime Value Prediction",
            "path": "machine-learning/regression/predictions/clv_predictions/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "Forecasts 1-year customer lifetime value with confidence intervals.",
        },
        "restock_quantity": {
            "label": "Restock Quantity Prediction",
            "path": "machine-learning/regression/predictions/restock_quantity/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "Recommends optimal restock quantities and expected 30-day demand per product.",
        },
        "safety_stock_adjusted": {
            "label": "Safety Stock Adjustment",
            "path": "machine-learning/regression/predictions/safety_stock_adjusted/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "ML-adjusted safety stock levels accounting for demand variability.",
        },
        "session_conversion_value": {
            "label": "Session Conversion Value Prediction",
            "path": "machine-learning/regression/predictions/session_conversion_value/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "Predicts order value if a session converts, with engagement recommendations.",
        },
        "stockout_probability": {
            "label": "Stockout Probability Prediction",
            "path": "machine-learning/regression/predictions/stockout_probability/",
            "path_type": "directory",
            "group": "general_regression",
            "description": "Forecasts stockout risk levels and expected days until stockout per product.",
        },
        # ── SPECIFIC CLASSIFICATION ───────────────────────────────────
        "fulfillment_risk_predictions": {
            "label": "Order Fulfillment Risk",
            "path": "machine-learning/classification/predictions/fulfillment_risk_predictions/",
            "path_type": "directory",
            "group": "specific_classification",
            "description": "Classifies fulfillment risk (Low / Medium / High / Critical) for each order.",
        },
        "product_bundling_predictions": {
            "label": "Product Bundling Predictions",
            "path": "machine-learning/classification/predictions/product_bundling_predictions/",
            "path_type": "directory",
            "group": "specific_classification",
            "description": "Identifies complementary product pairs with bundle category and affinity scores.",
        },
        # ── SPECIFIC CLUSTERING ───────────────────────────────────────
        "product_affinity_clustering": {
            "label": "Product Affinity Clustering",
            "path": "machine-learning/clustering/predictions/product_affinity_clustering.parquet/",
            "path_type": "directory",
            "group": "specific_clustering",
            "description": "Clusters products by co-purchase patterns for cross-sell recommendations.",
        },
        "product_lifecycle_clustering": {
            "label": "Product Lifecycle Clustering",
            "path": "machine-learning/clustering/predictions/product_lifecycle_clustering.parquet/",
            "path_type": "directory",
            "group": "specific_clustering",
            "description": "Assigns each product to a lifecycle stage: Introduction, Growth, Maturity, or Decline.",
        },
        # ── SPECIFIC REGRESSION ───────────────────────────────────────
        "campaign_roi": {
            "label": "Campaign ROI Prediction",
            "path": "machine-learning/regression/predictions/campaign_roi/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Predicts ROI, revenue, and conversions for each marketing campaign.",
        },
        "delivery_time": {
            "label": "Delivery Time Prediction",
            "path": "machine-learning/regression/predictions/delivery_time/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Predicts delivery days and expected delivery date per order.",
        },
        "demand_forecast": {
            "label": "Product Demand Forecast",
            "path": "machine-learning/regression/predictions/demand_forecast/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Forecasts product demand units with seasonality and trend adjustments.",
        },
        "price_optimization": {
            "label": "Price Optimization",
            "path": "machine-learning/regression/predictions/price_optimization/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Recommends optimal prices based on elasticity, competitor range, and expected units.",
        },
        "revenue_forecast": {
            "label": "Revenue Forecast",
            "path": "machine-learning/regression/predictions/revenue_forecast/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Forecasts total revenue and order count for the next 30-day period.",
        },
        "seasonal_trends": {
            "label": "Seasonal Trends Forecast",
            "path": "machine-learning/regression/predictions/seasonal_trends/",
            "path_type": "directory",
            "group": "specific_regression",
            "description": "Predicts seasonal index, season classification, and estimated revenue by month.",
        },
    }

    # Group display metadata
    GROUP_META: Dict[str, Dict[str, str]] = {
        "general_classification": {"label": "General Classification", "icon": "pi-tag"},
        "general_clustering":     {"label": "General Clustering",     "icon": "pi-sitemap"},
        "general_regression":     {"label": "General Regression",     "icon": "pi-chart-line"},
        "specific_classification": {"label": "Specific Classification", "icon": "pi-check-circle"},
        "specific_clustering":    {"label": "Specific Clustering",    "icon": "pi-th-large"},
        "specific_regression":    {"label": "Specific Regression",    "icon": "pi-arrow-up-right"},
    }

    def __init__(self) -> None:
        self.minio_client = self._create_minio_client()
        self._cache: Dict[str, Dict] = {}
        self._ttl = timedelta(minutes=5)

    # ------------------------------------------------------------------
    # MinIO client
    # ------------------------------------------------------------------

    def _create_minio_client(self) -> Minio:
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        if "://" in endpoint:
            endpoint = endpoint.split("://", 1)[1]
        return Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False,
        )

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_key(self, business_id: str, inference_name: str) -> str:
        return f"{business_id}:{inference_name}"

    def _cache_valid(self, entry: Optional[Dict]) -> bool:
        if not entry:
            return False
        cached_at = entry.get("cached_at")
        return bool(cached_at and datetime.now() - cached_at < self._ttl)

    # ------------------------------------------------------------------
    # DataFrame sanitisation
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize(obj: Any) -> Any:
        """Replace NaN/Inf with None recursively."""
        if isinstance(obj, dict):
            return {k: ForecastingService._sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [ForecastingService._sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    # ------------------------------------------------------------------
    # Low-level parquet reader
    # ------------------------------------------------------------------

    def _read_parquet_bytes(self, business_id: str, object_path: str) -> Optional[pd.DataFrame]:
        """Download a single parquet object and return a DataFrame, or None."""
        try:
            response = self.minio_client.get_object(business_id, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            return pd.read_parquet(io.BytesIO(data))
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchBucket"):
                return None
            print(f"[ForecastingService] S3Error reading {object_path}: {exc.code}")
            return None
        except Exception as exc:  # pragma: no cover
            print(f"[ForecastingService] Error reading {object_path}: {type(exc).__name__}")
            return None

    # ------------------------------------------------------------------
    # Public: fetch a single inference
    # ------------------------------------------------------------------

    async def fetch_inference(
        self, business_id: str, inference_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch ML inference results for one inference type.

        Returns a dict with keys: data, columns, row_count, meta — or None if
        the inference does not exist for this business.
        """
        if inference_name not in self.INFERENCE_CATALOG:
            return None

        cache_key = self._cache_key(business_id, inference_name)
        if cache_key in self._cache and self._cache_valid(self._cache[cache_key]):
            return self._cache[cache_key]["data"]

        meta = self.INFERENCE_CATALOG[inference_name]
        path = meta["path"]
        path_type = meta["path_type"]

        try:
            if path_type == "file":
                df = self._read_parquet_bytes(business_id, path)
            else:
                # Directory: list all parquet objects under the prefix
                try:
                    objects = list(
                        self.minio_client.list_objects(
                            business_id, prefix=path, recursive=True
                        )
                    )
                except S3Error as exc:
                    if exc.code in ("NoSuchKey", "NoSuchBucket"):
                        return None
                    print(f"[ForecastingService] S3Error listing {path}: {exc.code}")
                    return None

                parquet_files = [
                    o.object_name
                    for o in objects
                    if not o.is_dir and o.object_name.endswith(".parquet")
                ]

                if not parquet_files:
                    return None

                frames = [
                    self._read_parquet_bytes(business_id, p)
                    for p in parquet_files
                ]
                frames = [f for f in frames if f is not None and not f.empty]
                if not frames:
                    return None
                df = pd.concat(frames, ignore_index=True)

            if df is None or df.empty:
                return None

            result: Dict[str, Any] = {
                "data": self._sanitize(
                    df.where(pd.notna(df), None).to_dict(orient="records")
                ),
                "columns": list(df.columns),
                "row_count": len(df),
                "meta": {
                    "inference_name": inference_name,
                    "label": meta["label"],
                    "description": meta["description"],
                    "group": meta["group"],
                    "model_type": meta.get("path_type"),
                },
                "fetched_at": datetime.now().isoformat(),
            }

            self._cache[cache_key] = {"data": result, "cached_at": datetime.now()}
            return result

        except Exception as exc:  # pragma: no cover
            print(
                f"[ForecastingService] Unexpected error fetching {inference_name}: "
                f"{type(exc).__name__}"
            )
            return None

    # ------------------------------------------------------------------
    # Public: fetch all inferences (or a subset by group)
    # ------------------------------------------------------------------

    async def fetch_all_inferences(
        self,
        business_id: str,
        groups: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch all available ML inference results for a business.

        Missing inferences are silently skipped — callers receive only what
        actually exists in the bucket.
        """
        names = [
            name
            for name, meta in self.INFERENCE_CATALOG.items()
            if groups is None or meta["group"] in groups
        ]

        results: Dict[str, Any] = {}
        for name in names:
            result = await self.fetch_inference(business_id, name)
            if result is not None:
                results[name] = result

        return {
            "business_id": business_id,
            "inferences": results,
            "available_count": len(results),
            "total_catalog": len(names),
            "groups": self.GROUP_META,
            "fetched_at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self, business_id: Optional[str] = None) -> None:
        if business_id:
            keys = [k for k in self._cache if k.startswith(f"{business_id}:")]
            for k in keys:
                del self._cache[k]
        else:
            self._cache.clear()
