"""
Canonical Kafka message format for Pulse e-commerce streaming.
Functional approach - simple functions, no classes.
"""

from typing import Dict, Any
from datetime import datetime
import json


VALID_SOURCES = ["db", "api"]
VALID_TABLES = [
    "addresses",
    "categories",
    "customer_sessions",
    "customers",
    "inventory",
    "marketing_campaigns",
    "order_items",
    "orders",
    "payments",
    "products",
    "reviews",
    "shopping_cart",
    "suppliers",
    "wishlist",
]
TOPIC_MAP = {table: f"ecom.{table}" for table in VALID_TABLES}


def create_message(
    table: str,
    payload: Dict[str, Any],
    source_type: str = "api",
    vendor: str = "custom",
    schema_version: str = "v1",
) -> Dict[str, Any]:
    """Create canonical message."""
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table: {table}")
    if source_type not in VALID_SOURCES:
        raise ValueError(f"Invalid source_type: {source_type}")

    return {
        "source_type": source_type,
        "vendor": vendor,
        "table": table,
        "schema_version": schema_version,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload,
    }


def get_topic(table: str) -> str:
    """Get Kafka topic name for table."""
    return f"ecom.{table}"


def validate_message(message: Dict[str, Any]) -> bool:
    """Validate message structure."""
    required = ["source_type", "vendor", "table", "schema_version", "payload"]
    return all(field in message for field in required)


def to_json(message: Dict[str, Any]) -> str:
    """Convert message to JSON string."""
    return json.dumps(message)


def from_json(json_str: str) -> Dict[str, Any]:
    """Parse JSON string to message."""
    return json.loads(json_str)
