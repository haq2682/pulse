#!/bin/bash

source .env

docker run -d \
  --network spark-network \
  --name spark_worker_$(date +%s) \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_WORKER_MEMORY=2g \
  -e MINIO_ENDPOINT=${MINIO_ENDPOINT} \
  -e MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY} \
  -e MINIO_SECRET_KEY=${MINIO_SECRET_KEY} \
  -v $(pwd)/scripts:/opt/spark/scripts \
  apache/spark:3.5.0 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://10.5.0.2:7077 \
  >> log.txt 2>&1