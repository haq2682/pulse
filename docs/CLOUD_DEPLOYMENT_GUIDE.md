# Cloud Deployment Guide

This guide reflects the current Pulse implementation after the Airflow pipeline
was migrated away from shell-based `docker exec` usage for most task execution.

It explains two things:

1. What is already production-safe on a single Docker host
2. What still must change before Pulse is truly cloud-native on AWS, GCP, or Azure

> Current status: the DAG layer is now suitable for single-host production on
> Docker Compose or a VM, but it is not yet cloud-ready.

---

## Table of Contents

1. [Current Architecture Snapshot](#1-current-architecture-snapshot)
2. [What Was Recently Improved](#2-what-was-recently-improved)
3. [What Is Production-Ready Today](#3-what-is-production-ready-today)
4. [What Still Blocks a True Cloud Deployment](#4-what-still-blocks-a-true-cloud-deployment)
5. [Required Cloud Changes by Area](#5-required-cloud-changes-by-area)
6. [Recommended Target Architecture](#6-recommended-target-architecture)
7. [Migration Order](#7-migration-order)

---

## 1. Current Architecture Snapshot

The current deployment model is still centered on a single Docker host.

### Airflow execution model

- Airflow runs with `LocalExecutor`
- Most pipeline steps now use `DockerOperator`
- A few host-coupled operations still use Docker SDK `exec_run()`
- Airflow talks to the Docker daemon through `/var/run/docker.sock`

### Current DAG runtime behavior

| DAG | Current runtime model | Notes |
|---|---|---|
| `batch_downstream` | `DockerOperator` | Spawns one fresh container per task |
| `ml_retrain` | `DockerOperator` | Spawns one fresh container per task |
| `api_streaming` | `DockerOperator` | Long-running streaming container |
| `db_streaming` | `DockerOperator` + Docker SDK `exec_run()` | Streaming container + Debezium bootstrap in existing `python` container |
| `scheduled_batch_dag` | Docker SDK `exec_run()` | Reuses the long-running `python` container for step execution |

### Current Docker assumptions

The Airflow deployment currently depends on all of the following:

- a local Docker daemon
- `/var/run/docker.sock` mounted into Airflow containers
- the Docker network `spark-network`
- the local image `python-py310`
- the environment variable `PULSE_APP_HOST_PATH`
- bind mounts from the host into task containers for:
  - `cleaning/`
  - `transformation/`
  - `analysis/`
  - `machine-learning/`
  - `mapping/`
  - `jars/deps/`

That works reliably on one machine, but those assumptions do not translate to
ECS, EKS, GKE, AKS, or serverless container platforms.

---

## 2. What Was Recently Improved

The current guide should be read in the context of the recent Airflow runtime
hardening work.

### 2.1 `DockerOperator` replaced shell-based task execution

These DAGs now launch real task containers instead of using shell commands like
`docker exec python ...`:

- `airflow/dags/batch_downstream_dag.py`
- `airflow/dags/ml_retrain_dag.py`
- `airflow/dags/api_streaming_dag.py`
- `airflow/dags/db_streaming_dag.py`

### 2.2 Airflow now includes the Docker provider

The Airflow image installs:

- `apache-airflow`
- `apache-airflow-providers-docker`

This means Airflow no longer depends on the Docker CLI binary inside the
container.

### 2.3 Shared container runtime config was centralized

`airflow/config/pipeline_config.py` now provides:

- `PYTHON_IMAGE`
- `SPARK_NETWORK`
- `APP_HOST_PATH`
- `docker_pipeline_env()`
- `docker_app_mounts()`

This keeps all container launch settings consistent across DAGs.

### 2.4 Container lifecycle behavior was hardened

All `DockerOperator` tasks now use:

- `auto_remove="force"`
- `do_xcom_push=False`
- `mount_tmp_dir=False`

This avoids two production problems:

- failed containers piling up on disk
- large task logs being written into the Airflow metadata DB as XCom payloads

### 2.5 `PULSE_APP_HOST_PATH` is now mandatory

`APP_HOST_PATH` no longer silently falls back to a developer-specific path.
That is the correct production behavior because a wrong bind mount is worse
than an explicit startup failure.

---

## 3. What Is Production-Ready Today

Pulse is now suitable for single-host production when deployed on:

- a Linux VM
- a dedicated server
- a single Docker host behind Nginx or a load balancer

### Why it is production-ready in that model

- most Airflow tasks run in isolated task containers
- task container cleanup is enforced
- Airflow metadata growth from stdout/stderr is reduced
- runtime image/network/mount behavior is centralized
- required host-path configuration is explicit
- the Airflow image no longer needs the Docker CLI

### Local production prerequisites

At minimum, the host must provide:

```env
PULSE_APP_HOST_PATH=/absolute/path/to/pulse
```

Airflow must also be rebuilt so the Docker provider is present:

```bash
docker-compose build airflow-webserver airflow-scheduler
docker-compose up -d airflow-webserver airflow-scheduler
```

---

## 4. What Still Blocks a True Cloud Deployment

Despite the improvements above, the current design is still host-bound.

### Blocker 1 — Docker socket dependency

Airflow still requires `/var/run/docker.sock`.

That is usually unavailable or undesirable in:

- ECS Fargate
- Cloud Run
- GKE Autopilot
- locked-down managed Kubernetes clusters

### Blocker 2 — Host bind mounts

Task containers depend on `PULSE_APP_HOST_PATH` and host bind mounts.

Cloud task containers should not rely on:

- a specific absolute host path
- live source code mounted from the VM
- local JAR directories mounted from the host

In cloud deployments, code and dependencies should come from:

- the image itself
- a remote artifact store such as S3, GCS, or Azure Blob
- or a packaged wheel/zip release artifact

### Blocker 3 — Shared named-container reuse still exists

Two workflows still depend on a long-running container with a stable name:

- `scheduled_batch_dag.py` reuses the `python` container with Docker SDK `exec_run()`
- `db_streaming_dag.py` uses Docker SDK `exec_run()` to bootstrap Debezium from that same container

That pattern does not map cleanly to ephemeral cloud pods or tasks.

### Blocker 4 — Airflow still uses `LocalExecutor`

`LocalExecutor` keeps task execution tied to one node. That is acceptable on a
single host, but not for cloud-native horizontal scaling.

### Blocker 5 — Core infrastructure is still local

The stack still assumes local Docker services for:

- PostgreSQL
- MinIO
- Kafka / Zookeeper
- Debezium / Kafka Connect
- Spark standalone

Those all need managed or cloud-native replacements.

---

## 5. Required Cloud Changes by Area

### 5.1 Airflow task execution

#### Current state

The current strategy is:

- `DockerOperator` for most DAG tasks
- Docker SDK `exec_run()` for host-coupled flows

#### Cloud target

Replace host Docker execution with one of:

- `KubernetesPodOperator`
- `EcsRunTaskOperator`
- `EksPodOperator`
- a queue-driven worker service

#### Recommended mapping

| Current workflow | Cloud replacement |
|---|---|
| `batch_downstream` | `KubernetesPodOperator` or `EcsRunTaskOperator` per step |
| `ml_retrain` | `KubernetesPodOperator` or `EcsRunTaskOperator` per step |
| `api_streaming` | Dedicated long-running service on ECS/Kubernetes, not a perpetual Airflow task container |
| `db_streaming` | Dedicated long-running streaming service + separate connector bootstrap job |
| `scheduled_batch_dag` | Real task containers per step, not `exec_run()` into a shared container |

#### Files that still need redesign for cloud

- `airflow/dags/scheduled_batch_dag.py`
- `airflow/dags/db_streaming_dag.py`
- `airflow/dags/api_streaming_dag.py`
- `airflow/dags/batch_downstream_dag.py`
- `airflow/dags/ml_retrain_dag.py`

#### Important design rule

Do not mount live code from the host in a cloud deployment.

Instead:

- bake code into the image, or
- package jobs and fetch them from remote storage

### 5.2 Airflow platform

#### Current state

- `LocalExecutor`
- scheduler and webserver in Docker Compose
- local DAG folders mounted into the container

#### Cloud target

Use one of:

- Airflow Helm chart with `KubernetesExecutor`
- managed Airflow such as MWAA, Cloud Composer, or Astronomer
- `CeleryExecutor` if using a worker queue model

#### Required changes

1. Build a production Airflow image
2. Include the required providers in that image
3. Stop depending on local DAG bind mounts
4. Move variables, connections, and secrets into a managed backend

### 5.3 PostgreSQL and object storage

#### PostgreSQL

Replace the local PostgreSQL container with:

- AWS RDS
- GCP Cloud SQL
- Azure Database for PostgreSQL

Update all configuration that still assumes fixed `10.5.0.x` addresses or local
Docker-network names.

#### Object storage

Replace MinIO with:

- S3
- GCS
- Azure Blob Storage

The application already uses S3-compatible semantics, so the migration path is
straightforward, but any MinIO-specific Spark endpoint or path-style settings
should be removed.

### 5.4 Kafka and Debezium

#### Current state

Kafka, Zookeeper, and Debezium are local Docker services.

#### Cloud target

Use:

- AWS MSK
- Confluent Cloud
- self-managed Kafka Connect on ECS/EKS if Debezium remains custom

#### Required changes

1. Replace local bootstrap addresses and fixed IP assumptions
2. Move Debezium connector management out of host-coupled container execution
3. Decide whether connector lifecycle belongs in Airflow or a separate service/API

### 5.5 Spark runtime

#### Current state

Spark still assumes a standalone cluster on the Docker network.

#### Cloud target

Use one of:

- EMR Serverless
- Dataproc Serverless
- Spark on Kubernetes

#### Required changes

1. Remove assumptions about `spark://spark-master:7077`
2. Move Spark dependencies and JAR delivery into a cloud-compatible packaging model
3. Ensure Spark drivers and executors can reach Kafka and object storage securely

### 5.6 API and frontend

#### API

Deploy FastAPI as:

- ECS Fargate service
- Kubernetes deployment
- Cloud Run container

#### Frontend

Deploy the frontend as:

- S3 + CloudFront
- GCS + CDN
- Azure Static Web Apps
- Vercel / Netlify

#### Recommended cleanup

- make API base URLs environment-driven
- tighten CORS to production origins
- ensure a stable `/health` endpoint exists for probes

### 5.7 Secrets and configuration

#### Current state

The stack still uses `.env` files and Compose environment blocks.

#### Cloud target

Use managed secrets:

- AWS Secrets Manager or SSM Parameter Store
- GCP Secret Manager
- Azure Key Vault

Move these out of flat env files:

- database credentials
- object storage credentials
- Kafka credentials
- Airflow admin/API secrets
- JWT and application secrets

### 5.8 Observability

Before a cloud rollout, add:

- centralized logs
- metrics dashboards
- task and DAG alerts
- container restart/crash visibility
- Kafka lag monitoring
- database pool monitoring

For Airflow specifically, track:

- task duration
- retry counts
- tenant-level failure rate
- stuck streaming jobs

---

## 6. Recommended Target Architecture

For AWS, the cleanest direction is:

| Area | Recommended target |
|---|---|
| Airflow | MWAA or Helm-based Airflow on EKS |
| Batch task execution | `EcsRunTaskOperator` or `KubernetesPodOperator` |
| Streaming ingestion | Dedicated ECS/EKS services, not perpetual Airflow task containers |
| PostgreSQL | RDS |
| Object storage | S3 |
| Kafka | MSK or Confluent Cloud |
| Debezium | Kafka Connect on ECS/EKS or a managed connector platform |
| Spark | EMR Serverless or Spark on EKS |
| Secrets | Secrets Manager |
| Frontend | S3 + CloudFront |
| API | ECS Fargate or EKS |

If staying on a VM-based deployment for now, keep the current implementation
and focus on:

- backups
- monitoring
- TLS
- secret rotation
- database hardening
- resource limits for Airflow and Spark

---

## 7. Migration Order

Follow this order to minimize rework and risk.

| Step | Change | Why first |
|---|---|---|
| 1 | Move secrets to a managed store | Removes env-file dependency early |
| 2 | Migrate PostgreSQL to managed DB | Unblocks API and Airflow portability |
| 3 | Migrate MinIO to managed object storage | Unblocks batch, ML, and Spark portability |
| 4 | Build immutable production images for API, Airflow, and pipeline runtime | Removes host bind-mount dependency |
| 5 | Replace `scheduled_batch_dag.py` `exec_run()` pattern | Removes shared-container dependency |
| 6 | Replace Debezium bootstrap `exec_run()` in `db_streaming_dag.py` | Removes the second shared-container dependency |
| 7 | Move `batch_downstream` and `ml_retrain` to cloud task operators | Makes batch compute cloud-native |
| 8 | Move streaming jobs out of perpetual Airflow task containers | Better operational model for 24/7 streams |
| 9 | Replace local Kafka, Debezium, and Spark services | Removes core local infra dependencies |
| 10 | Move Airflow off `LocalExecutor` | Enables horizontal scaling |
| 11 | Add cloud observability, alerting, and autoscaling | Final production hardening |

---

## Final Recommendation

Use the current implementation for:

- Docker Compose production on a single host
- pilot deployments
- controlled VM-based environments

Do **not** treat the current implementation as cloud-ready yet.

The main remaining gap is no longer raw `docker exec`; it is the broader set of
host assumptions:

- Docker socket access
- host bind mounts
- a named shared `python` container
- `LocalExecutor`
- local infrastructure services

Once those are removed, Pulse can be migrated cleanly to ECS, EKS, GKE, or AKS.
