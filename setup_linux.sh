#!/bin/bash
set -e

echo "=========================================="
echo "Complete Infrastructure Setup"
echo "Started at: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

# Phase 1: Start core services
echo "=== Phase 1: Starting Core Services ==="
echo "Starting PostgreSQL, MinIO, Redis, Zookeeper, and Kafka..."
docker compose up -d postgresql minio redis zookeeper kafka

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

# Phase 5: Start FastAPI Backend
echo "=== Phase 5: Starting FastAPI Backend ==="
echo "Starting FastAPI backend..."
docker compose up -d api

# Phase 6: Start React Frontend
echo "=== Phase 6: Starting React Frontend ==="
echo "Starting React frontend..."
docker compose up -d frontend

echo "=== Phase 7: Start Worker ==="
echo "Starting Spark Worker 1..."
./bash/start_worker_linux.sh
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