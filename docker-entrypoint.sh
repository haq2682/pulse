#!/bin/bash
set -e

echo "=== Starting Python Application Container ==="

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until pg_isready -h postgresql -p 5432 -U ${POSTGRES_USER}; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "✓ PostgreSQL is ready"

# Wait for MinIO
echo "Waiting for MinIO..."
until curl -sf http://localhost:9000/minio/health/live > /dev/null; do
  echo "MinIO is unavailable - sleeping"
  sleep 2
done
echo "✓ MinIO is ready"

# Wait for Kafka
echo "Waiting for Kafka..."
until timeout 5 bash -c 'cat < /dev/null > /dev/tcp/kafka/9092' 2>/dev/null; do
  echo "Kafka is unavailable - sleeping"
  sleep 2
done
echo "✓ Kafka is ready"

# Wait for Spark Master
echo "Waiting for Spark Master..."
until curl -sf http://localhost:8080 > /dev/null; do
  echo "Spark Master is unavailable - sleeping"
  sleep 2
done
echo "✓ Spark Master is ready"

echo ""
echo "=== All dependencies ready ==="
echo ""

# Execute the main command (can be overridden in docker-compose)
exec "$@"