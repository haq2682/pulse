#!/bin/bash
# Purge all ecom.* Kafka topics by briefly setting retention to 1 second.
# The Debezium connector must be deleted BEFORE running this script.
set -e

BOOTSTRAP="10.5.0.7:9092"
WAIT_SECONDS=20

echo "=== Listing ecom.* topics ==="
TOPICS=$(docker exec kafka /usr/bin/kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep "^ecom\.")
echo "$TOPICS"
echo ""

echo "=== Setting retention.ms=1000 on all ecom.* topics ==="
for topic in $TOPICS; do
  echo "  Purging: $topic"
  docker exec kafka /usr/bin/kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name "$topic" \
    --alter --add-config retention.ms=1000
done

echo ""
echo "Waiting ${WAIT_SECONDS}s for broker to drop messages..."
sleep "$WAIT_SECONDS"

echo ""
echo "=== Restoring default retention on all ecom.* topics ==="
for topic in $TOPICS; do
  echo "  Restoring: $topic"
  docker exec kafka /usr/bin/kafka-configs \
    --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name "$topic" \
    --alter --delete-config retention.ms
done

echo ""
echo "=== Verifying message counts ==="
for topic in $TOPICS; do
  COUNT=$(docker exec kafka /usr/bin/kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "$BOOTSTRAP" \
    --topic "$topic" --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}')
  echo "  $topic: $COUNT messages"
done

echo ""
echo "Done. All ecom.* topics purged."
