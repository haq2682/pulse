"""API Ingestion Service - Polls external API and streams to Kafka"""

import time
import json
import requests
from typing import Dict, Any, Optional
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from rapidfuzz import fuzz, process
from canonical_message import create_message, VALID_TABLES

KAFKA_BOOTSTRAP = "10.5.0.7:9092"
POLL_INTERVAL = 10
API_URL = "http://localhost:5000/api/data"


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


def fetch_data(api_url: str) -> Optional[Dict]:
    """Fetch data from API"""
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"API error: {e}")
        return None


def process_table(producer: KafkaProducer, table_data: Dict, api_url: str) -> int:
    """Process and send table data to Kafka, return row count"""
    name = table_data.get("name")
    if not name:
        return 0

    canonical = map_table_name(name)
    if not canonical:
        print(f"Unmapped table: {name}")
        return 0

    rows = table_data.get("data", [])
    topic = f"ecom.{canonical}"

    for row in rows:
        message = create_message(
            table=canonical, payload=row, source_type="api", vendor="api_polling"
        )
        producer.send(topic, value=message)

    return len(rows)


def run(api_url: str, poll_interval: int, kafka_bootstrap: str):
    """Main polling loop"""
    print(f"Starting API ingestion: {api_url} → Kafka")

    producer = create_producer(kafka_bootstrap)

    for table in VALID_TABLES:
        create_topic(kafka_bootstrap, f"ecom.{table}")

    print(f"Polling every {poll_interval}s (Ctrl+C to stop)\n")

    try:
        while True:
            data = fetch_data(api_url)

            if data and "tables" in data:
                total = sum(process_table(producer, t, api_url) for t in data["tables"])
                producer.flush()
                print(f"Processed {len(data['tables'])} tables, {total} rows")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        producer.close()
        print("\nStopped")


if __name__ == "__main__":
    run(API_URL, POLL_INTERVAL, KAFKA_BOOTSTRAP)
