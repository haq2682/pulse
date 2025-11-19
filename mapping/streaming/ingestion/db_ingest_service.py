"""
Database ingestion service: DB URI → Kafka
Auto-discovers tables, maps to canonical schema, creates topics dynamically.
Frontend will provide database URI - for now using dummy variable.
"""

import time
import json
from typing import Dict, Any, List, Optional
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from rapidfuzz import fuzz, process
from db_connector import (
    get_connection,
    fetch_new_records,
    get_last_timestamp,
    discover_tables,
)
from canonical_message import create_message, get_topic, VALID_TABLES


# Canonical table mapping (matches your map.py df_to_table)
CANONICAL_TABLE_MAP = {
    "customers": "customers",
    "customer": "customers",
    "users": "customers",
    "user": "customers",
    "addresses": "addresses",
    "address": "addresses",
    "products": "products",
    "product": "products",
    "inventories": "inventory",
    "inventory": "inventory",
    "orders": "orders",
    "order": "orders",
    "reviews": "reviews",
    "review": "reviews",
    "categories": "categories",
    "category": "categories",
    "wishlists": "wishlist",
    "wishlist": "wishlist",
    "payments": "payments",
    "payment": "payments",
    "order_items": "order_items",
    "orderitems": "order_items",
    "shopping_carts": "shopping_cart",
    "shopping_cart": "shopping_cart",
    "cart": "shopping_cart",
    "customer_sessions": "customer_sessions",
    "sessions": "customer_sessions",
    "marketing_campaigns": "marketing_campaigns",
    "campaigns": "marketing_campaigns",
    "suppliers": "suppliers",
    "supplier": "suppliers",
}


def map_to_canonical_table(table_name: str, threshold: int = 85) -> Optional[str]:
    """
    Map discovered table name to canonical schema name using exact match + fuzzy matching.

    Args:
        table_name: Discovered table name
        threshold: Minimum similarity score for fuzzy matching (0-100)

    Returns:
        Canonical table name or None if no match
    """
    table_lower = table_name.lower()

    # Step 1: Try exact match
    canonical = CANONICAL_TABLE_MAP.get(table_lower)
    if canonical and canonical in VALID_TABLES:
        return canonical

    # Step 2: Try fuzzy matching against VALID_TABLES
    match = process.extractOne(
        table_lower, VALID_TABLES, scorer=fuzz.ratio, score_cutoff=threshold
    )

    if match:
        best_match, score = match[0], match[1]
        print(f"  Fuzzy match: {table_name} → {best_match} (score: {score:.1f})")
        return best_match

    return None


def create_kafka_topic(kafka_bootstrap: str, topic: str):
    """Create Kafka topic if it doesn't exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        topic_obj = NewTopic(name=topic, num_partitions=1, replication_factor=1)
        admin.create_topics([topic_obj])
        admin.close()
        print(f"✓ Created topic: {topic}")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"⚠ Topic creation warning: {e}")


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_in_flight_requests_per_connection=5,
        retries=3,
    )


def serialize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert record values to JSON-serializable types."""
    serialized = {}
    for key, value in record.items():
        if hasattr(value, "isoformat"):
            serialized[key] = value.isoformat()
        elif isinstance(value, (int, float, str, bool, type(None))):
            serialized[key] = value
        else:
            serialized[key] = str(value)
    return serialized


def send_records_to_kafka(
    producer: KafkaProducer,
    records: List[Dict],
    canonical_table: str,
    vendor: str = "custom",
):
    """Send records to Kafka with canonical message format."""
    for record in records:
        message = create_message(
            table=canonical_table,
            payload=serialize_record(record),
            source_type="db",
            vendor=vendor,
        )
        topic = get_topic(canonical_table)
        producer.send(topic, value=message)

    producer.flush()


def ingest_from_uri(
    db_uri: str, poll_interval: int = 10, kafka_bootstrap: str = "localhost:9092"
):
    """
    Main ingestion function: discovers all tables from URI and ingests to Kafka.

    Args:
        db_uri: Database connection URI (from frontend)
        poll_interval: Seconds between polls
        kafka_bootstrap: Kafka bootstrap servers
    """
    print(f"\n{'='*60}")
    print(f"Starting DB Ingestion")
    print(f"{'='*60}")
    print(f"Database URI: {db_uri[:30]}...")
    print(f"Kafka: {kafka_bootstrap}")
    print(f"Poll interval: {poll_interval}s\n")

    # Connect and discover tables
    conn, db_type = get_connection(db_uri)
    print(f"✓ Connected to {db_type} database")

    discovered_tables = discover_tables(conn, db_type)
    print(f"✓ Discovered {len(discovered_tables)} tables: {discovered_tables}\n")

    # Map to canonical schema
    table_mappings = {}
    for table in discovered_tables:
        canonical = map_to_canonical_table(table)
        if canonical:
            table_mappings[table] = canonical
            print(f"  {table:20s} → {canonical}")
        else:
            print(f"  {table:20s} → (skipped - not in canonical schema)")

    print(f"\n✓ Mapped {len(table_mappings)} tables to canonical schema\n")

    if not table_mappings:
        print("⚠ No tables matched canonical schema. Exiting.")
        return

    # Create Kafka topics
    print("Creating Kafka topics...")
    for canonical_table in set(table_mappings.values()):
        topic = get_topic(canonical_table)
        create_kafka_topic(kafka_bootstrap, topic)

    print(f"\n{'='*60}")
    print("Starting continuous ingestion...")
    print(f"{'='*60}\n")

    # Initialize tracking
    producer = create_producer(kafka_bootstrap)
    last_timestamps = {table: None for table in table_mappings.keys()}

    try:
        iteration = 0
        while True:
            iteration += 1
            print(f"[Iteration {iteration}] Polling tables...")

            total_records = 0
            for table, canonical_table in table_mappings.items():
                try:
                    records = fetch_new_records(
                        conn, db_type, table, last_timestamps[table]
                    )

                    if records:
                        send_records_to_kafka(producer, records, canonical_table)
                        last_timestamps[table] = get_last_timestamp(records)
                        total_records += len(records)
                        print(
                            f"  {table}: {len(records)} new records → {canonical_table}"
                        )

                except Exception as e:
                    print(f"  {table}: Error - {e}")

            if total_records == 0:
                print(f"  No new records")
            else:
                print(f"  Total: {total_records} records sent to Kafka")

            print()
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\n\nStopping ingestion...")
    finally:
        producer.close()
        if db_type != "mongo":
            conn.close()
        else:
            conn.close()


# ============================================================================
# DUMMY DATABASE URI - Replace this with frontend input later
# ============================================================================

DUMMY_DB_URI = "postgresql://user:password@localhost:5432/ecommerce"

# When frontend is ready, replace above with:
# db_uri = request.json.get('database_uri')  # From React frontend


if __name__ == "__main__":
    # For testing, use dummy URI
    # In production, this will come from frontend API call

    print("\n⚠️  Using DUMMY database URI for testing")
    print("   Replace DUMMY_DB_URI variable when frontend is ready\n")

    # You can override with actual URI for testing:
    # DUMMY_DB_URI = "postgresql://youruser:yourpass@10.5.0.5:5432/pulse"

    KAFKA_BOOTSTRAP = "10.5.0.7:9092"  # From docker-compose
    POLL_INTERVAL = 10

    ingest_from_uri(DUMMY_DB_URI, POLL_INTERVAL, KAFKA_BOOTSTRAP)
