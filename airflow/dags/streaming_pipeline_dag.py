"""
Streaming Pipeline DAG

Submits the Spark Structured Streaming orchestrator as a long-running Spark job.
The job is submitted to the Spark master in cluster mode so it keeps running
independently; Airflow only manages the submission step.

Real-time pipeline flow (all via Spark Structured Streaming + Kafka):
  Kafka/Debezium → spark_streaming (mapping) → streaming_cleaning
                 → streaming_transformation → streaming_analysis
                 → streaming_ml_inference

Trigger this DAG manually or via an @once schedule to (re)start the streaming job.
Replaces NiFi streaming flows and the standalone scheduled_ml_training loop.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

BUCKET_NAME = "pulse-bucket-1"
SPARK_MASTER = "spark://10.5.0.3:7077"
TRIGGER_INTERVAL = "10 seconds"

# spark-submit command that launches the unified streaming orchestrator.
# --deploy-mode client keeps logging visible in Airflow task logs.
SPARK_SUBMIT_CMD = (
    f"spark-submit "
    f"--master {SPARK_MASTER} "
    f"--deploy-mode client "
    f"--conf spark.sql.streaming.schemaInference=true "
    f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
    f"--conf spark.hadoop.fs.s3a.access.key=${{MINIO_ACCESS_KEY}} "
    f"--conf spark.hadoop.fs.s3a.secret.key=${{MINIO_SECRET_KEY}} "
    f"/app/streaming_orchestrator.py "
    f"--bucket-name {BUCKET_NAME} "
    f"--trigger-interval '{TRIGGER_INTERVAL}'"
)

# Separate command for the real-time ingestion layer (Kafka → mapping).
SPARK_STREAMING_INGEST_CMD = (
    f"spark-submit "
    f"--master {SPARK_MASTER} "
    f"--deploy-mode client "
    f"--conf spark.sql.streaming.schemaInference=true "
    f"--conf spark.hadoop.fs.s3a.endpoint=${{MINIO_ENDPOINT}} "
    f"--conf spark.hadoop.fs.s3a.access.key=${{MINIO_ACCESS_KEY}} "
    f"--conf spark.hadoop.fs.s3a.secret.key=${{MINIO_SECRET_KEY}} "
    f"/app/mapping/streaming/spark_streaming.py "
    f"--bucket-name {BUCKET_NAME}"
)

default_args = {
    "owner": "pulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="streaming_pipeline",
    default_args=default_args,
    description=(
        "Start real-time Spark Structured Streaming pipeline: "
        "Kafka ingestion → mapping → cleaning → transformation → analysis → ML inference"
    ),
    schedule_interval=None,  # trigger manually or via external trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "real-time", "spark"],
) as dag:

    start_ingestion_stream = BashOperator(
        task_id="start_kafka_ingestion_stream",
        bash_command=SPARK_STREAMING_INGEST_CMD,
    )

    start_processing_pipeline = BashOperator(
        task_id="start_processing_pipeline",
        bash_command=SPARK_SUBMIT_CMD,
    )

    # Ingestion stream must be running before the processing pipeline reads mapped data
    start_ingestion_stream >> start_processing_pipeline
