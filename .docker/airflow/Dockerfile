FROM python-py310 AS airflow-base

ENV AIRFLOW_VERSION=2.8.0
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

# Install Airflow + the DockerOperator provider.
# The provider uses the Python Docker SDK (docker package) over the socket
# mounted at /var/run/docker.sock — no CLI binary required.
RUN pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-docker" \
    --constraint "${CONSTRAINT_URL}"