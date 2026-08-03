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

# DAGs/plugins/config/utils - docker-compose bind-mounts these live from
# ./airflow/ (see docker-compose.yml), but Kubernetes deployment.yaml has no
# live host bind mount equivalent, so they need to be baked into the image
# instead, same as note 5 in deployment.yaml's header for the other
# pipeline directories. Paths match AIRFLOW__CORE__DAGS_FOLDER/
# PLUGINS_FOLDER and PYTHONPATH set on pulse-airflow-webserver/scheduler.
COPY ./airflow/dags /opt/airflow/dags
COPY ./airflow/plugins /opt/airflow/plugins
COPY ./airflow/config /opt/airflow/config
COPY ./airflow/utils /opt/airflow/utils