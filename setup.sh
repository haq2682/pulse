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
docker compose up -d --build postgresql minio redis zookeeper kafka

echo ""
echo "Waiting for core services to be ready..."
# sleep 30
echo "✓ Core services started"
echo ""

# Phase 2: Start Spark
echo "=== Phase 2: Starting Spark ==="
echo "Starting Spark Master..."
docker compose up -d --build spark_master

echo ""
echo "Waiting for Spark to be ready..."
# sleep 20
echo "✓ Spark services started"
echo ""

# Phase 3: Start Python service
echo "=== Phase 3: Starting Python Service ==="
echo "Starting Python application..."
docker compose up -d --build python

echo ""
echo "Waiting for Python service to be ready..."
# sleep 10
echo "✓ Python service started"
echo ""

# Phase 4: Setup Kafka
echo "=== Phase 4: Setup Kafka Topics & Config ==="
echo "Setting up Kafka..."
./bash/setup_kafka.sh
echo "✓ Kafka setup complete"
echo ""

# Phase 5: Start Debezium
echo "=== Phase 5: Starting Debezium ==="
echo "Starting Debezium connector..."
docker compose up -d --build debezium

echo ""
echo "✓ Debezium started"
echo ""

# Phase 6: Start FastAPI Backend
echo "=== Phase 6: Starting FastAPI Backend ==="
echo "Starting API backend..."
docker compose up -d --build api

echo ""
echo "✓ API started"
echo ""

# Phase 7: Start NiFi
echo "=== Phase 7: Starting Apache NiFi ==="
echo "Starting NiFi..."
docker compose up -d --build nifi

echo ""
echo "✓ NiFi started"
echo ""

# Phase 8: Start React Frontend
echo "=== Phase 8: Starting React Frontend ==="
echo "Starting frontend..."
docker compose up -d --build frontend

echo ""
echo "✓ Frontend started"
echo ""

# Phase 9: Start Spark Workers
echo "=== Phase 9: Starting Spark Workers ==="
echo "Starting Spark Worker 1..."
./bash/start_worker.sh

echo "Starting Spark Worker 2..."
./bash/start_worker.sh

chmod -R +x .

echo ""
echo "=========================================="
echo "✅ SETUP COMPLETE"
echo "=========================================="
echo ""
echo "All services are running!"
echo "Completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

echo "Service URLs:"
echo "  - Frontend:     http://localhost:5173"
echo "  - API:          http://localhost:8000"
echo "  - Spark UI:     http://localhost:8080"
echo "  - MinIO:        http://localhost:9001"
echo "  - NiFi UI:      https://localhost:8443/nifi"
echo "  - Debezium:     http://localhost:8083"
echo "  - PostgreSQL:   localhost:5432"
echo "  - Redis:        localhost:6379"
echo "  - Kafka:        localhost:9092"
echo ""
