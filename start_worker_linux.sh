#!/bin/bash
set -e  # Exit on any error

# Load environment variables
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo ".env file not found!"
  exit 1
fi

# Generate a unique worker name
WORKER_NAME="spark_worker_$(date +%s)"

# Run the Spark worker container
sudo docker run -d \
  --network spark-network \
  --name "$WORKER_NAME" \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_WORKER_MEMORY=4g \
  -e MINIO_ENDPOINT="$MINIO_ENDPOINT" \
  -e MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  -e MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  -v "$(pwd)/scripts:/opt/spark/scripts" \
  apache/spark:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://10.5.0.2:7077 \
  >> log.txt 2>&1

echo "Spark worker started with name: $WORKER_NAME"

