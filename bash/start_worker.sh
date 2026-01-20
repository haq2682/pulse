#!/bin/bash
set -e

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

if [ ! -f .env ]; then
  echo ".env file not found!"
  exit 1
fi

set -a
source .env
set +a

WORKER_NAME="spark-worker-$(hostname)-$(date +%s)"

docker run -d \
  --restart unless-stopped \
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
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://10.5.0.3:7077 \
  >> log.txt 2>&1

echo "Spark worker started: $WORKER_NAME"
