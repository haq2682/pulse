"""
Canonical Kafka message format for Pulse e-commerce streaming.
Functional approach - simple functions, no classes.
Supports CDC operations for database ingestion.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import json


VALID_SOURCES = ["db", "api"]
VALID_TABLES = [
    "addresses",
    "cart_items",
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

# CDC operations for database ingestion: c=create, u=update, d=delete, r=read/snapshot
VALID_CDC_OPERATIONS = ["c", "u", "d", "r", "create", "update", "delete", "read"]

TOPIC_MAP = {table: f"ecom.{table}" for table in VALID_TABLES}


def create_message(
    table: str,
    payload: Dict[str, Any],
    source_type: str = "api",
    vendor: str = "custom",
    schema_version: str = "v1",
    operation: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create canonical message with optional CDC operation for database ingestion.
    
    Args:
        table: Target table name
        payload: Data payload
        source_type: Source type (db, api)
        vendor: Vendor identifier
        schema_version: Schema version
        operation: CDC operation for db source (c=create, u=update, d=delete, r=read)
        
    Returns:
        Canonical message dictionary
    """
    if table not in VALID_TABLES:
        raise ValueError(f"Invalid table: {table}")
    if source_type not in VALID_SOURCES:
        raise ValueError(f"Invalid source_type: {source_type}")
    if operation and operation not in VALID_CDC_OPERATIONS:
        raise ValueError(f"Invalid CDC operation: {operation}")

    message = {
        "source_type": source_type,
        "vendor": vendor,
        "table": table,
        "schema_version": schema_version,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload,
    }
    
    # Add operation field for CDC messages (used with db source type)
    if operation:
        message["operation"] = operation
    
    return message


def get_topic(table: str) -> str:
    """Get Kafka topic name for table."""
    return f"ecom.{table}"


def validate_message(message: Dict[str, Any]) -> bool:
    """Validate message structure."""
    required = ["source_type", "vendor", "table", "schema_version", "payload"]
    return all(field in message for field in required)


def validate_cdc_message(message: Dict[str, Any]) -> bool:
    """Validate CDC message structure including operation field."""
    if not validate_message(message):
        return False
    # Validate operation whenever it's present, regardless of source_type
    if message.get("operation"):
        return message["operation"] in VALID_CDC_OPERATIONS
    return True


def to_json(message: Dict[str, Any]) -> str:
    """Convert message to JSON string."""
    return json.dumps(message)


def from_json(json_str: str) -> Dict[str, Any]:
    """Parse JSON string to message."""
    return json.loads(json_str)


def from_debezium(debezium_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Debezium CDC payload to canonical format.

    Args:
        debezium_payload: Debezium message payload

    Returns:
        Canonical message dictionary
    """
    op_map = {"c": "c", "u": "u", "d": "d", "r": "r"}

    op = debezium_payload.get("op")
    source = debezium_payload.get("source", {})
    table = source.get("table")
    data = debezium_payload.get("after") or debezium_payload.get("before")

    if not all([op, table, data]):
        raise ValueError("Invalid Debezium message: missing required fields")

    return create_message(
        table=table,
        payload=data,
        source_type="db",
        vendor="debezium",
        operation=op_map.get(op, "c"),
    )


def is_debezium_format(message: Dict[str, Any]) -> bool:
    """Check if message is in Debezium format."""
    if not isinstance(message, dict):
        return False
    return "op" in message and "source" in message
