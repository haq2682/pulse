"""API Ingestion Service - Polls external API and streams to Kafka.

Incremental fetching
--------------------
Every successful poll records its start timestamp in Redis under the key
``api_last_poll:{business_id}``.  On the next poll that timestamp is sent
to the user's API as the ``updated_since`` query parameter::

    GET https://api.example.com/data?updated_since=2026-03-10T10:30:00Z

This means each polling cycle only transfers records that were created or
modified since the previous poll — not the entire dataset.  For reference:

* Shopify equivalent  : ``updated_at_min``  (callers must adapt their API to map it)
* WooCommerce         : ``modified_after``   (same)
* The pulse ingest contract uses ``updated_since`` as the canonical name.

Degradation
-----------
If the user's API ignores ``updated_since`` it simply returns all records;
downstream Delta MERGE deduplication handles repeated rows harmlessly so
there is no correctness risk, just increased bandwidth.

If Redis is unavailable the service falls back to a full poll every cycle
(original behaviour) and logs a warning.
"""

import os
import time
import json
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from rapidfuzz import fuzz, process
import redis as redis_lib
from ..canonical_message import create_message, VALID_TABLES
from .api_validation import validate_api_data, get_expected_format_example

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.5.0.7:9092")
POLL_INTERVAL = 10
API_URL = "http://localhost:5000/api/data"

_REDIS_HOST = os.getenv("REDIS_HOST", "redis")
_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


TABLE_MAP = {
    "customer": "customers",
    "users": "customers",
    "user": "customers",
    "address": "addresses",
    "product": "products",
    "items": "products",
    "inventories": "inventory",
    "stock": "inventory",
    "order": "orders",
    "review": "reviews",
    "ratings": "reviews",
    "category": "categories",
    "wishlists": "wishlist",
    "payment": "payments",
    "transactions": "payments",
    "orderitems": "order_items",
    "order_details": "order_items",
    "shopping_carts": "shopping_cart",
    "cart": "shopping_cart",
    "carts": "shopping_cart",
    "cart_items": "cart_items",
    "cartitems": "cart_items",
    "cart_item": "cart_items",
    "shopping_cart_items": "cart_items",
    "sessions": "customer_sessions",
    "campaigns": "marketing_campaigns",
    "supplier": "suppliers",
    "vendors": "suppliers",
}


def map_table_name(name: str) -> Optional[str]:
    """Map table name to canonical schema using exact or fuzzy matching"""
    lower = name.lower().strip()

    if lower in VALID_TABLES:
        return lower
    if lower in TABLE_MAP:
        return TABLE_MAP[lower]

    match = process.extractOne(lower, VALID_TABLES, scorer=fuzz.ratio, score_cutoff=85)
    return match[0] if match else None


def create_topic(bootstrap: str, topic: str):
    """Create Kafka topic if doesn't exist"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        )
        admin.close()
    except TopicAlreadyExistsError:
        pass


def create_producer(bootstrap: str) -> KafkaProducer:
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def _append_updated_since(url: str, ts: str) -> str:
    """Return *url* with ``updated_since=<ts>`` appended to its query string.

    Handles URLs that already have query parameters correctly so existing
    parameters are preserved.
    """
    parsed   = urlparse(url)
    qs       = parse_qs(parsed.query, keep_blank_values=True)
    qs["updated_since"] = [ts]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def fetch_data(api_url: str, updated_since: Optional[str] = None) -> Optional[Dict]:
    """Fetch and validate data from API.

    Args:
        api_url: The user's external API endpoint.
        updated_since: ISO 8601 UTC timestamp (e.g. ``"2026-03-10T10:30:00Z"``).
            When provided, appended as ``?updated_since=<ts>`` so the user's
            API can return only records modified after that point.  If the API
            ignores the parameter it returns all records — harmless because
            downstream Delta MERGE deduplicates by primary key.
    """
    request_url = _append_updated_since(api_url, updated_since) if updated_since else api_url

    try:
        response = requests.get(request_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Validate data format
        try:
            validated_data = validate_api_data(data)
            # Convert back to dict for processing
            return validated_data.model_dump()
        except ValueError as e:
            print(f"API data validation error: {e}")
            print(f"Expected format: {json.dumps(get_expected_format_example(), indent=2)}")
            return None

    except Exception as e:
        print(f"API error: {e}")
        return None


def process_table(
    producer: KafkaProducer,
    table_data: Dict,
    api_url: str,
    kafka_bootstrap: str,
    topic_prefix: str,
    created_topics: set,
) -> int:
    """Process and send table data to Kafka, return row count"""
    # Use table_name (validated by pydantic)
    name = table_data.get("table_name")
    if not name:
        print("Warning: table_name missing in table data")
        return 0

    canonical = map_table_name(name)
    if not canonical:
        print(f"Unmapped table: {name}")
        return 0

    rows = table_data.get("data", [])
    topic = f"{topic_prefix}.{canonical}"

    if topic not in created_topics:
        create_topic(kafka_bootstrap, topic)
        created_topics.add(topic)

    for row in rows:
        message = create_message(
            table=canonical, payload=row, source_type="api", vendor="api_polling"
        )
        producer.send(topic, value=message)

    return len(rows)


def run(api_url: str, poll_interval: int, kafka_bootstrap: str, business_id: str = ""):
    """Main polling loop with incremental ``updated_since`` watermark.

    On every successful poll the start-of-poll timestamp is persisted to Redis
    under ``api_last_poll:{business_id}`` and sent as the ``updated_since``
    query parameter on the *next* poll.  This ensures only new or modified
    records are fetched after the first (baseline) poll.

    Using the *start* of the poll window (not the end) as the watermark
    prevents a data gap: any record written by the source system while our
    HTTP request was in-flight is captured by the overlapping window.

    On restart, the watermark is read from Redis so the service resumes from
    the last checkpoint rather than performing a full re-fetch.

    Args:
        api_url: The user's external REST endpoint.
        poll_interval: Seconds to wait between polling cycles.
        kafka_bootstrap: Kafka broker address(es).
        business_id: Tenant's business_id / MinIO bucket name.  Used to scope
            the Redis watermark key so different tenants never share state.
    """
    print(f"Starting API ingestion: {api_url} → Kafka")
    if business_id:
        print(f"Tenant: {business_id} (incremental watermark enabled)")
    else:
        print("No business_id — watermark disabled, full poll every cycle")

    producer = create_producer(kafka_bootstrap)
    topic_prefix = business_id or "ecom"
    created_topics: set = set()

    print(f"Polling every {poll_interval}s (Ctrl+C to stop)\n")

    # ── Initialise Redis watermark ─────────────────────────────────────────
    _watermark_key  = f"api_last_poll:{business_id}" if business_id else None
    _redis_client: Optional[redis_lib.Redis] = None
    last_poll_ts: Optional[str] = None

    if _watermark_key:
        try:
            _redis_client = redis_lib.Redis(
                host=_REDIS_HOST, port=_REDIS_PORT, decode_responses=True
            )
            last_poll_ts = _redis_client.get(_watermark_key)
            if last_poll_ts:
                print(f"Resuming from checkpoint: updated_since={last_poll_ts}")
            else:
                print("No previous checkpoint — performing baseline full poll")
        except Exception as _redis_err:
            print(f"⚠️  Redis unavailable — watermark disabled, falling back to full poll: {_redis_err}")
            _redis_client = None

    try:
        while True:
            # Record poll-start BEFORE the HTTP request so the watermark
            # window begins before the request, preventing gaps.
            poll_start_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            data = fetch_data(api_url, updated_since=last_poll_ts)

            if data and "tables" in data:
                total = sum(
                    process_table(
                        producer,
                        t,
                        api_url,
                        kafka_bootstrap,
                        topic_prefix,
                        created_topics,
                    )
                    for t in data["tables"]
                )
                producer.flush()

                mode_label = (
                    f"incremental (since {last_poll_ts})"
                    if last_poll_ts
                    else "full (baseline)"
                )
                print(
                    f"[{poll_start_str}] {mode_label} — "
                    f"{len(data['tables'])} tables, {total} rows"
                )

                # Advance the watermark only after a successful poll so a
                # network error or empty response does not skip any records.
                if _watermark_key and _redis_client:
                    try:
                        _redis_client.set(_watermark_key, poll_start_str)
                        last_poll_ts = poll_start_str
                    except Exception as _save_err:
                        print(f"⚠️  Could not save poll watermark to Redis: {_save_err}")
            else:
                print(f"[{poll_start_str}] No data or empty response — watermark not advanced")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        producer.close()
        print("\nStopped")


if __name__ == "__main__":
    run(API_URL, POLL_INTERVAL, KAFKA_BOOTSTRAP)
