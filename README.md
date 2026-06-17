# Pulse — E-Commerce Data Analytics Engine

Pulse is a web-based application that ingests e-commerce data and performs
cleaning, transformation, aggregation, and optimization, then derives
analytics, insights, predictions, and forecasts from it. It is built on a
Big Data stack — Python and Apache Spark are the core frameworks — alongside
ReactJS and FastAPI.

## Table of Contents

- [Tech Stack](#tech-stack)
- [Features and Components](#features-and-components)
- [Minimum System Requirements](#minimum-system-requirements)
- [Environment Setup](#environment-setup)
- [Installation](#installation)
- [Database Setup, Migrations, and Seed Data](#database-setup-migrations-and-seed-data)
- [Running the Project](#running-the-project)
- [Tests](#tests)
- [Cleanup](#cleanup)
- [Troubleshooting](#troubleshooting)

## Tech Stack

| Layer               | Technologies                                                                                                                                              |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend             | Python 3.10.19, FastAPI, SQLAlchemy, Pydantic v2, Uvicorn                                                                                                 |
| Big data / pipeline | PySpark 3.5.0 (Hadoop 3), Delta Lake, Apache Kafka + Zookeeper (Confluent 7.7.0), Debezium 3.4.0 (CDC), Apache NiFi, Apache Airflow 2.8.0 (LocalExecutor) |
| Storage             | PostgreSQL (Bitnami image), MinIO (S3-compatible object storage), Redis 7                                                                                 |
| ML / NLP            | sentence-transformers, spaCy, gensim, torch (CPU build), google-generativeai (Gemini)                                                                     |
| Frontend            | React 19, Vite, Tailwind CSS 4, PrimeReact, Chart.js, axios, react-router 7                                                                               |
| Infra               | Docker Compose, Nginx (reverse proxy / TLS), Node 25 (frontend build stage), Java 17/21 (Spark / Debezium base images)                                    |

## Features and Components

- **`api/`** — FastAPI backend, the main application entrypoint (port 8000)
- **`frontend/`** — React/Vite single-page app (port 5173)
- **`cleaning/`, `mapping/`, `transformation/`, `analysis/`, `machine-learning/`**
  — PySpark-based data pipeline stages
- **`airflow/`** — DAGs for batch, streaming, and ML-retrain orchestration
- **`nifi/`** — data ingestion flow templates
- **CDC ingestion via Debezium**, supporting PostgreSQL, MySQL, MariaDB,
  MongoDB, SQL Server, Oracle, Db2, Vitess, Spanner, and Informix as
  external source connectors. Cassandra is not supported by the system.
- **Reverse proxy / TLS termination** via Nginx

## Minimum System Requirements

- **Docker / Docker Compose**: The entire tech stack runs on Docker and Docker Compose.
- **OS:** Linux and MacOS are preferred, but the system can run on Windows as well.
- **RAM & CPU:** At least 8GB RAM, requires tweaking of resources being used by Spark Workers and Apache NiFi. 16GB RAM is Recommended. At least 4 Cores of CPU are required. 8 Cores of CPU are recommended.
- **Required host ports:** `5173`, `8000`, `5000`, `5432`, `9000`, `9001`,
  `8080`, `7077`, `4040`, `2181`, `9092`, `8083`, `6379`, `8081`, `10000`,
  `8443`, `8090`, plus configurable Nginx ports (defaults `8082` / `9443`)

## Environment Setup

1. Copy the environment template:
   
   ```bash
   cp .env.example .env
   ```

2. Fill in the variables that have **no default anywhere in the codebase**:
   `SECRET_KEY`, `NIFI_ADMIN_USER`, `NIFI_ADMIN_PASSWORD`,
   `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`.
   
   Also set `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE_NAME`,
   and `POSTGRES_SERVER` — these are required for the API to start
   (`api/config.py` has no default for them), even though
   `airflow/config/pipeline_config.py` falls back to `postgres` /
   `postgres` / `pulse` / `10.5.0.5` if they're unset for the Airflow
   pipeline code path.

3. Generate secrets:
   
   ```bash
   # SECRET_KEY
   python -c "import secrets; print(secrets.token_hex(32))"
   
   # AIRFLOW_FERNET_KEY
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

4. Optional integrations (these have defaults or are blank if unused):
   Gemini API key, SMTP settings, Google OAuth credentials, Hugging Face
   token.

5. If you plan to use the Debezium Oracle or Spanner connectors, place the
   required files **before** the first `docker compose up`:
   
   - Oracle: `jars/ojdbc8.jar`
   - Spanner: `jars/gcp-credentials.json`

## Installation

Dependencies are installed automatically as part of the Docker image build
— no separate manual install step is required for the containerized
workflow:

```dockerfile
# .docker/python/Dockerfile
python3 -m pip install --no-cache-dir -r requirements.txt
python3 -m pip install --no-cache-dir -r packages.txt
```

To run pipeline scripts locally, outside Docker:

```bash
pip install -r requirements.txt
```

To work on the frontend outside its container:

```bash
cd frontend
npm ci
```

## Database Setup, Migrations, and Seed Data

### PostgreSQL initialization

PostgreSQL is initialized automatically on first container start.
`.docker/postgresql/Dockerfile` copies every file in `sql/` into
`/docker-entrypoint-initdb.d/`:

```dockerfile
COPY ./sql/* /docker-entrypoint-initdb.d/
```

**Ordering risk (verified):** Postgres runs `/docker-entrypoint-initdb.d/`
scripts in alphabetical filename order. `add_api_url_column.sql` and
`add_pipeline_mode_columns.sql` sort *before* `schema.sql`, but they
`ALTER TABLE` the `onboarding` and `pipeline_status` tables that
`schema.sql` creates. On a fresh database, these two files will attempt to
run before their target tables exist.

### Schema migrations

There is no migration framework (e.g. Alembic) in use. `sql/add_*.sql`
files are plain incremental patches — despite being auto-copied into the
init directory above, they appear (per their own header comments) to be
intended for manual application against an *existing* database, not for
fresh-init use.

### Airflow metadata database

Airflow's own metadata database is initialized by the `airflow-init`
service in `docker compose.yml`:

```bash
airflow db migrate
airflow db upgrade
airflow connections create-default-connections
airflow users create --username ${AIRFLOW_ADMIN_USER:-admin} --password ${AIRFLOW_ADMIN_PASSWORD:-admin} --firstname Pulse --lastname Admin --role Admin --email admin@pulse.local
```

### Seed data

Synthetic e-commerce data lives in `faker/*.xlsx`, generated via the
`faker/faker.ipynb` notebook.

## Running the Project

### Full stack — first-time setup

1. Build the shared `python-py310` base image first. The `api`,
   `spark_master`, and `airflow` Dockerfiles all build `FROM python-py310`,
   so it must exist before the other images can build:
   
   ```bash
   docker compose up -d --build python
   ```

2. Build and start the rest of the stack:
   
   ```bash
   docker compose up -d --build
   ```
   
   This brings up every service defined in `docker compose.yml`, including
   the API, frontend, `python` worker (port 5000), PostgreSQL, MinIO,
   Spark, Kafka, Zookeeper, Debezium, Redis, NiFi, Airflow, and Nginx.

3. Point the MinIO client at the running MinIO instance (replace the
   placeholders with your `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` values
   from `.env`):
   
   ```bash
   mc alias set local http://localhost:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>
   ```

4. Create the buckets the pipeline expects. The `minio-init` service
   already creates `pulse-bucket-1`, `pulse-test-bucket`, and
   `pulse-checkpoints` automatically on startup, so these commands are
   normally just a no-op confirmation:
   
   ```bash
   mc mb local/pulse-bucket-1
   mc mb local/pulse-checkpoints
   ```

5. Seed the bucket with the sample data shipped in this repo:
   
   ```bash
   mc cp --recursive ./buckets/pulse-bucket-1 local/pulse-bucket-1
   ```

6. Make the worker startup scripts executable:
   
   ```bash
   chmod +x bash/*.sh
   ```

7. Start a Spark worker. Before running, open the script and adjust
   `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` to match your host's
   available resources:
   
   ```bash
   ./bash/start_worker_linux.sh   # Linux
   ./bash/start_worker.sh         # Windows (Git Bash)
   ```

8. Import and configure the NiFi batch-mode flow (see the two subsections
   below).

### Importing the NiFi batch-mode flow

Open the NiFi UI at `http://localhost:8081/nifi` and log in with
`NIFI_ADMIN_USER` / `NIFI_ADMIN_PASSWORD` from `.env`.

> **Version note:** `.docker/nifi/Dockerfile` builds from
> `apache/nifi:latest` and downloads companion NARs pinned to version
> `2.7.2`, so the running image resolves to the NiFi 2.x line (the
> `latest` tag is floating and may resolve to a newer NiFi 2.x release on
> rebuild). NiFi 2.x removed the legacy XML "Template" feature in favor of
> importing/exporting Process Groups as JSON flow definitions, which is
> the format of `nifi/templates/pulse_batch_mode_v2.json`.

1. On the canvas, drag the **Process Group** icon from the top toolbar
   onto the canvas.
2. In the "Add Process Group" dialog, switch to the **Import** option for
   a local flow definition file.
3. Browse to and select `nifi/templates/pulse_batch_mode_v2.json`, then
   confirm the import.
4. The `pulse_batch_mode` process group appears on the canvas.

### Configuring the flow's controller services

After import, all 13 controller services defined in the flow start in a
**disabled** state, and sensitive properties (passwords, secret keys) are
never carried over by a flow-definition export. Right-click the imported
process group, choose **Configure**, then open the **Controller Services**
tab.

| Controller service                                                                                                                                             | Needs manual input after import?                                                                                                                                                                                                                            |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DBCPConnectionPool`                                                                                                                                           | Yes — set **Database Password** to your `POSTGRES_PASSWORD`. The Connection URL is pre-set to `jdbc:postgresql://10.5.0.5:5432/pulse` and the user to `postgres`; update these if your `.env` uses a different `POSTGRES_DATABASE_NAME` or `POSTGRES_USER`. |
| `AWSCredentialsProviderControllerService`                                                                                                                      | Yes — set **Access Key ID** / **Secret Access Key** to your `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`. This service backs the `PutS3Object` processor, which already points at `http://10.5.0.4:9000` (MinIO's internal container address).                   |
| `RedisConnectionPoolService`                                                                                                                                   | No — pre-set to `10.5.0.11:6379`, matching the Redis container's internal address.                                                                                                                                                                          |
| `DatabaseTableSchemaRegistry`                                                                                                                                  | No — wired to `DBCPConnectionPool` automatically.                                                                                                                                                                                                           |
| `RedisDistributedMapCacheClientService`                                                                                                                        | No — wired to `RedisConnectionPoolService` automatically.                                                                                                                                                                                                   |
| `ExcelReader`, `CSVReader`, `JsonTreeReader`, `ParquetReader`, `CSVRecordSetWriter`, `JsonRecordSetWriter`, `ParquetRecordSetWriter`, `StandardHttpContextMap` | No — self-contained format/schema settings, no credentials.                                                                                                                                                                                                 |

Enable the services with no dependencies first (`DBCPConnectionPool`,
`RedisConnectionPoolService`), then the ones that reference them
(`DatabaseTableSchemaRegistry`, `RedisDistributedMapCacheClientService`),
then the rest. You can also select all services and use the lightning-bolt
**Enable** action, retrying once the dependency services report valid.

### Subsequent startups

Once the images are built and the buckets are seeded, you don't need to
repeat the steps above — just bring the stack back up:

```bash
docker compose up -d
```

**Production-like startup:** Airflow images must be rebuilt before first
production use, so the Docker provider is present:

```bash
docker compose build airflow-webserver airflow-scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

## Cleanup

The standard Compose command would be:

```bash
docker compose down
```

A Kafka topic cleanup script is provided. **Delete the Debezium connector
first**, then run:

```bash
./bash/purge_kafka_topics.sh
```

This purges all `ecom.*` Kafka topics.

## Troubleshooting

- **Verify Debezium is running:**
  
  ```bash
  curl http://localhost:8083/
  ```

- **Verify Nginx health:**
  
  ```bash
  curl http://localhost:8082/health
  ```

- **Confirm Redis is running:**
  
  ```bash
  docker ps | grep redis
  ```

- If Redis is unavailable, the mapping pipeline automatically falls back to
  full polls (no data loss).
