#!/bin/bash
set -e

if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo ".env file not found!"
  exit 1
fi

WORKER_NAME="spark_worker_$(date +%s)"

docker run -d \
  --network spark-network \
  --name "$WORKER_NAME" \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_WORKER_MEMORY=2g \
  -e PYSPARK_PYTHON=python3.10 \
  -e PYSPARK_DRIVER_PYTHON=python3.10 \
  -e SPARK_WORKER_OPTS="-Dspark.shuffle.service.enabled=true" \
  -e MINIO_ENDPOINT="$MINIO_ENDPOINT" \
  -e MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  -e MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  -v "$(pwd)/scripts:/opt/spark/scripts" \
  spark-master-py310 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://10.5.0.3:7077 >> log.txt 2>&1

echo "Spark worker started with name: $WORKER_NAME"
