ARG BASE_IMAGE=python-py310
FROM ${BASE_IMAGE} AS airflow-base

ENV AIRFLOW_VERSION=2.8.0
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

# Install Airflow + the Kubernetes provider (KubernetesPodOperator, plus the
# raw `kubernetes` client used directly by pipeline_config.run_k8s_task_pod).
# Tasks run as their own Kubernetes Pods via the in-cluster ServiceAccount
# token - no docker.sock, no host bind mounts. Replaces the earlier
# apache-airflow-providers-docker setup (see docs/CLOUD_DEPLOYMENT_GUIDE.md).
RUN pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-cncf-kubernetes" \
    --constraint "${CONSTRAINT_URL}"