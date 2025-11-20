#!/bin/bash
set -e

echo "=== Kafka Infrastructure Setup (Integrated) ==="
echo ""

# Check if network exists, create if not
if ! docker network inspect spark-network >/dev/null 2>&1; then
    echo "Creating spark-network..."
    docker network create --subnet=10.5.0.0/16 spark-network
else
    echo "✓ spark-network already exists"
fi

# echo ""
# echo "Starting Kafka services (Zookeeper + Kafka)..."
# docker-compose up -d zookeeper kafka

# echo ""
# echo "Waiting for Zookeeper to be ready..."
# timeout=30
# counter=0
# until docker exec zookeeper sh -c 'echo ruok | nc localhost 2181' | grep -q imok 2>/dev/null; do
#     sleep 1
#     counter=$((counter + 1))
#     if [ $counter -ge $timeout ]; then
#         echo "❌ Zookeeper failed to start"
#         exit 1
#     fi
# done
# echo "✓ Zookeeper is ready"

# echo ""
# echo "Waiting for Kafka to be ready..."
# timeout=60
# counter=0
# until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
#     sleep 2
#     counter=$((counter + 2))
#     if [ $counter -ge $timeout ]; then
#         echo "❌ Kafka failed to start"
#         exit 1
#     fi
# done
# echo "✓ Kafka is ready"

echo ""
echo "Creating ecommerce topics..."
topics=("ecom.addresses" "ecom.categories" "ecom.customer_sessions" "ecom.customers" "ecom.inventory" "ecom.marketing_campaigns" "ecom.order_items" "ecom.orders" "ecom.payments" "ecom.products" "ecom.reviews" "ecom.shopping_cart" "ecom.suppliers" "ecom.wishlist")
for topic in "${topics[@]}"; do
    if docker exec kafka kafka-topics --create --topic "$topic" --bootstrap-server 10.5.0.7:9092 --partitions 1 --replication-factor 1 2>&1 | grep -q "already exists"; then
        echo "✓ $topic exists"
    else
        echo "✓ Created $topic"
    fi
done

echo ""
echo "Listing all topics..."
docker exec kafka kafka-topics --list --bootstrap-server 10.5.0.7:9092

echo ""
echo "✅ Complete"
echo "   Kafka Broker: 10.5.0.7:9092 (internal) / localhost:9092 (external)"
echo "   Zookeeper: 10.5.0.6:2181"