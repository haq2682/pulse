#!/bin/bash

export MSYS_NO_PATHCONV=1 
export MSYS2_ARG_CONV_EXCL="*"

source .env

docker run -d \
  --network spark-network \
  --name "$WORKER_NAME" \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_WORKER_MEMORY=6g \
  -e PYSPARK_PYTHON=python3.10 \
  -e PYSPARK_DRIVER_PYTHON=python3.10 \
  -e SPARK_WORKER_OPTS="-Dspark.shuffle.service.enabled=true" \
  -e MINIO_ENDPOINT="$MINIO_ENDPOINT" \
  -e MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  -e MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  -v "$(pwd)/scripts:/opt/spark/scripts" \
  spark-master-py310 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://10.5.0.2:7077 >> log.txt 2>&1