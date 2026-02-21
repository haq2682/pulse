FROM apache/airflow:2.9.2-python3.10

USER root

# Install Java (required for spark-submit)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install Python dependencies needed by the DAGs and pipeline modules
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt \
    && pip install --no-cache-dir \
        apache-airflow-providers-apache-spark \
        pyspark==3.5.0
