#!/bin/bash
set -e

echo "=========================================="
echo "Complete Infrastructure Setup"
echo "Started at: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

# Phase 1: Start core services
echo "=== Phase 1: Starting Core Services ==="
echo "Starting PostgreSQL, MinIO, Zookeeper, and Kafka..."
docker compose up -d postgresql minio zookeeper kafka

echo ""
echo "Waiting for services to be ready..."
# sleep 30
echo "✓ Core services started"
echo ""

# Phase 2: Start Spark
echo "=== Phase 2: Starting Spark ==="
echo "Starting Spark Master and Workers..."
docker compose up -d spark_master

echo ""
echo "Waiting for Spark to be ready..."
# sleep 20
echo "✓ Spark services started"
echo ""

#Phase 3: Start Python service
echo "=== Phase 3: Starting Python Service ==="
echo "Starting Python application..."
docker compose up -d python

echo ""
echo "Waiting for Python service to be ready..."
# sleep 10
echo "✓ Python service started"
echo ""

echo "=== Phase 4: Setup Kafka ==="
echo "Setting up Kafka..."
sleep 30
./bash/setup_kafka.sh

echo "=== Phase 5: Start Worker ==="
echo "Starting Spark Worker 1..."
./bash/start_worker_linux.sh
sleep 10
echo "Starting Spark Worker 2..."
./bash/start_worker_linux.sh

echo ""
echo "=========================================="
echo "✅ SETUP COMPLETE"
echo "=========================================="
echo ""
echo "All services are running!"
echo "Completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""