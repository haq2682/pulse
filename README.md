# Pulse — E-Commerce Data Analytics Engine

Pulse is a web-based application that ingests e-commerce data and performs
cleaning, transformation, aggregation, and optimization, then derives
analytics, insights, predictions, and forecasts from it. It is built on a
Big Data stack — Python and Apache Spark are the core frameworks — alongside
ReactJS and FastAPI.

## Table of Contents

- [Tech Stack](#tech-stack)
- [Features and Components](#features-and-components)
- [Two Ways to Run This](#two-ways-to-run-this)
- [DevOps Pipeline Setup (Kubernetes + GitOps)](#devops-pipeline-setup-kubernetes--gitops)
- [Local Development (Docker Compose)](#local-development-docker-compose)
  - [Minimum System Requirements](#minimum-system-requirements)
  - [Environment Setup](#environment-setup)
  - [Installation](#installation)
  - [Database Setup, Migrations, and Seed Data](#database-setup-migrations-and-seed-data)
  - [Running the Project](#running-the-project)
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
| Local dev infra     | Docker Compose, Nginx (reverse proxy / TLS), Node 25 (frontend build stage), Java 17/21 (Spark / Debezium base images)                                    |
| DevOps / GitOps     | Ansible (host bootstrap), Terraform + Helm (cluster infra), Kubernetes / Minikube, ArgoCD + Argo CD Image Updater, HashiCorp Vault + External Secrets Operator, Prometheus (kube-prometheus-stack) + Grafana, GitHub Actions |

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
- **`.ansible/`, `terraform/`, `.k8s/`, `vault/`** — the production-style
  deployment path: a single Ansible playbook provisions a host all the way
  up to a running Minikube cluster, Terraform installs every piece of
  cluster infrastructure via Helm, and ArgoCD then keeps the `production`
  namespace continuously in sync with `.k8s/bases` straight from this repo.
  See [DevOps Pipeline Setup](#devops-pipeline-setup-kubernetes--gitops)
  below.

## Two Ways to Run This

| | Docker Compose | Kubernetes + GitOps |
|---|---|---|
| **Best for** | Quick local development, iterating on one service at a time | Something closer to how this would actually run in production — the full pipeline this repo is really built around |
| **Setup effort** | One `.env` file, a few `docker compose` commands | Ansible playbook, then Terraform, then a handful of one-time bootstrap scripts |
| **Autoscaling / self-healing / monitoring** | No | Yes — HPA, ArgoCD self-heal, Prometheus + Grafana |
| **Secrets** | Plain `.env` file | HashiCorp Vault, synced into the cluster by External Secrets Operator |
| **How you deploy a code change** | `docker compose up -d --build <service>` | `git push` → CI builds & pushes images → ArgoCD Image Updater picks up the new tag automatically |
| **Where to start** | [Local Development](#local-development-docker-compose) below | [DevOps Pipeline Setup](#devops-pipeline-setup-kubernetes--gitops) below |

Both paths run the exact same application code — this only changes *how*
it's deployed and operated.

## DevOps Pipeline Setup (Kubernetes + GitOps)

This provisions a full GitOps pipeline on a single Linux host: Ansible
installs everything the host needs and brings up a Minikube cluster,
Terraform installs every piece of cluster infrastructure via Helm, HashiCorp
Vault holds the application secrets, and ArgoCD continuously syncs the
`production` namespace to whatever is committed in this repo's `.k8s/bases`.

**Requirements:** a Linux host (Ubuntu/Debian - the Ansible playbook uses
`apt`), at least 4 CPU cores and 16GB RAM (this runs Minikube plus every
piece of infra below, on top of the full application stack once deployed -
noticeably heavier than the Docker Compose path), and a Docker Hub account.

### 1. Bootstrap the host with Ansible

Installs Docker, kubelet/kubeadm/kubectl, Minikube (and starts it, with the
`metrics-server` and `ingress` addons enabled), Helm, the `argocd` CLI, and
Terraform - all in one playbook run:

```bash
cd .ansible
ansible-playbook -i inventory.ini install-deps.yaml --ask-become-pass
```

Check `inventory.ini` matches your actual host/user first (it targets
`127.0.0.1` by default, i.e. the machine you're running this on).

### 2. Install cluster infrastructure with Terraform

```bash
cd terraform
terraform init
terraform apply -target=kubernetes_namespace.vault
cd ..
```

Only the `vault` namespace first, on its own - Vault's pod mounts a TLS
certificate Secret unconditionally (next step), which has to already exist
in that namespace before Vault's own Helm release ever installs, or its
pod fails to start at all.

```bash
openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
  -keyout vault.key -out vault.crt \
  -subj "/CN=vault.vault.svc.cluster.local/O=pulse-dev" \
  -addext "subjectAltName=DNS:vault,DNS:vault.vault,DNS:vault.vault.svc,DNS:vault.vault.svc.cluster.local,DNS:vault-internal,DNS:vault-internal.vault,DNS:vault-internal.vault.svc,DNS:vault-internal.vault.svc.cluster.local,DNS:vault-0.vault-internal,DNS:vault-0.vault-internal.vault.svc.cluster.local,DNS:localhost,IP:127.0.0.1"
kubectl create secret tls vault-tls -n vault --cert=vault.crt --key=vault.key
```

Move `vault.key`/`vault.crt` out of the repo directory afterward, same
treatment as every other bootstrap secret here - see
`docs/SECRETS_MANAGEMENT.md`'s TLS section for the full detail (including
why `global.tlsDisable` alone doesn't work and what would silently break
if this cert - or the Vault `storage` stanza next to it in
`terraform/resource.tf` - were handled naively).

Now the rest of the infrastructure:

```bash
cd terraform
terraform apply
cd ..
```

This installs 5 Helm releases - ArgoCD, Argo CD Image Updater, HashiCorp
Vault (TLS-enabled, using the cert just created), External Secrets
Operator, and kube-prometheus-stack (Prometheus + Grafana + Alertmanager) -
each into its own namespace (`argocd`, `vault`, `monitoring`;
`external-secrets` itself installs into `kube-system`), plus the
`dev`/`staging` namespaces. It does **not** create the `production`
namespace - ArgoCD creates that itself in step 6.

### 3. Bootstrap Vault

Vault starts empty and sealed - it needs to be initialized once, then
unsealed, then configured with the KV secrets engine, Kubernetes auth, and
the actual application secret values, seeded from your `.env` file. Every
script here is idempotent (safe to re-run) and fully unattended - none of
them prompt for a key or token, they read `vault-init-output.json`
themselves:

```bash
cd vault/scripts
./01-init-vault.sh          # writes vault-init-output.json - move it to
                             # secure storage (e.g. ~/.vault-pulse/) right
                             # after this, per the script's own output
./02-unseal-vault.sh
./03-enable-kv-and-k8s-auth.sh
./04-apply-policies-and-roles.sh
cd ../..
./vault/scripts/05-seed-secrets-from-env.sh   # run from the repo root - needs your .env
```

See `docs/SECRETS_MANAGEMENT.md` for the full architecture, how to rotate a
secret afterward, and the exact severity of losing `vault-init-output.json`
(≥3 of its 5 unseal keys, or its root token, and this Vault's data is gone).

### 4. Load the TLS certificate

One self-signed certificate covers `pulse-engine.com` and every subdomain
used below (`*.pulse-engine.com`). It has to be loaded as a Secret named
`pulse-tls` into **every namespace** that has an Ingress referencing it -
`production` (the main app) and `argocd` (the ArgoCD UI):

```bash
openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
  -keyout pulse-engine.key -out pulse-engine.crt \
  -subj "/CN=pulse-engine.com/O=pulse-dev" \
  -addext "subjectAltName=DNS:pulse-engine.com,DNS:*.pulse-engine.com"

kubectl create secret tls pulse-tls -n production --cert=pulse-engine.crt --key=pulse-engine.key
kubectl create secret tls pulse-tls -n argocd     --cert=pulse-engine.crt --key=pulse-engine.key
```

Move `pulse-engine.key`/`.crt` out of the repo directory afterward (same
treatment as the Vault keys) - see `docs/INGRESS_ACCESS.md`, which also
covers the `/etc/hosts` entries you'll need for your browser to resolve
`pulse-engine.com` and friends to the cluster at all.

### 5. Configure CI and push

CI (`.github/workflows/ci.yaml`) builds and pushes every custom image to
Docker Hub on every push to `main`, tagged both `latest` and an immutable
`sha-<commit>` (the tag Argo CD Image Updater watches for). It needs two
repository secrets, set under the GitHub repo's Settings → Secrets and
variables → Actions:

| Secret | Value |
|---|---|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | A Docker Hub access token with write/push scope |

Then commit and push everything from steps 1-5 (review `git status` first -
this is a lot of new and modified files):

```bash
git add .
git status   # review before committing - never commit vault-init-output.json,
             # pulse-engine.key, terraform.tfstate, or .env (all gitignored,
             # but worth a second look before a push this size)
git commit -m "Add Kubernetes/GitOps deployment pipeline"
git push origin main
```

This is the step that actually makes ArgoCD's sync target real - until
something is pushed, `.k8s/bases` doesn't exist on the `main` branch that
`.k8s/argocd/argocd.yaml` points at.

### 6. Point ArgoCD at this repo

```bash
kubectl apply -f .k8s/argocd/argocd.yaml
```

This creates the `production` namespace (via `CreateNamespace=true`) and
starts syncing every object in `.k8s/bases` into it - roughly 90 objects on
first sync: 14 Deployments, 2 Jobs, 15 Services, PVCs, the HPAs, the
NetworkPolicies, the ExternalSecrets that pull real values out of Vault, and
the ServiceMonitors that wire everything up to Prometheus. `selfHeal: true`
means any manual `kubectl edit`/`delete` against these resources gets
reverted back to what's in git on the next sync - this repo, not `kubectl`,
is meant to be the source of truth from this point on.

### 7. Verify

```bash
kubectl get application -n argocd pulse         # HEALTHY / Synced, once images actually exist on Docker Hub
kubectl get pods -n production
```

Then see `docs/INGRESS_ACCESS.md` for the actual URLs (including ArgoCD's
own UI, now reachable over HTTPS through the same Ingress) and
`docs/SECRETS_MANAGEMENT.md` for the secrets architecture in more depth.

### Optional: the Vertical Pod Autoscaler

`.k8s/bases/vpa.yaml`'s 7 `VerticalPodAutoscaler` objects need a controller
that isn't part of any of the above - it's not a Helm chart, so it isn't in
`terraform/resource.tf`:

```bash
git clone https://github.com/kubernetes/autoscaler.git
cd autoscaler/vertical-pod-autoscaler
./hack/vpa-up.sh
```

Every VPA here uses `updateMode: "Off"` (recommendation-only, never evicts
a running pod), so this is safe to skip entirely - ArgoCD will just show
those 7 objects as permanently `OutOfSync` until it's installed.

## Local Development (Docker Compose)

The fastest way to run the whole stack on one machine. See [Two Ways to
Run This](#two-ways-to-run-this) above if you're deciding between this and
the full Kubernetes pipeline.

### Minimum System Requirements

- **Docker / Docker Compose**: The entire tech stack runs on Docker and Docker Compose.
- **OS:** Linux and MacOS are preferred, but the system can run on Windows as well.
- **RAM & CPU:** At least 8GB RAM, requires tweaking of resources being used by Spark Workers and Apache NiFi. 16GB RAM is Recommended. At least 4 Cores of CPU are required. 8 Cores of CPU are recommended.
- **Required host ports:** `5173`, `8000`, `5000`, `5432`, `9000`, `9001`,
  `8080`, `7077`, `4040`, `2181`, `9092`, `8083`, `6379`, `8081`, `10000`,
  `8443`, `8090`, plus configurable Nginx ports (defaults `8082` / `9443`)

### Environment Setup

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

### Installation

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

### Database Setup, Migrations, and Seed Data

### PostgreSQL initialization

PostgreSQL is initialized automatically on first container start.
`.docker/postgresql/Dockerfile` copies each file in `sql/` into
`/docker-entrypoint-initdb.d/` individually:

```dockerfile
COPY ./sql/add_api_url_column.sql /docker-entrypoint-initdb.d/
COPY ./sql/add_pipeline_mode_columns.sql /docker-entrypoint-initdb.d/
COPY ./sql/create_airflow_database.sql /docker-entrypoint-initdb.d/
COPY ./sql/create_cleaning_state_table.sql /docker-entrypoint-initdb.d/
# COPY ./sql/create_debezium_user.sh /docker-entrypoint-initdb.d/
COPY ./sql/schema.sql /docker-entrypoint-initdb.d/
```

**Ordering risk (verified):** Postgres runs `/docker-entrypoint-initdb.d/`
scripts in alphabetical filename order. `add_api_url_column.sql` and
`add_pipeline_mode_columns.sql` sort *before* `schema.sql`, but they
`ALTER TABLE` the `onboarding` and `pipeline_status` tables that
`schema.sql` creates. On a fresh database, these two files will attempt to
run before their target tables exist.

### Debezium user creation

`sql/create_debezium_user.sh` would normally run as part of
`/docker-entrypoint-initdb.d/` (alphabetically between
`create_cleaning_state_table.sql` and `schema.sql`), creating the
`debezium_user` role that Debezium uses to read the WAL for CDC. Its
`COPY` line in `.docker/postgresql/Dockerfile` is commented out because
the script's bash heredoc breaks on Windows checkouts (CRLF line
endings), even though it runs fine on Linux/macOS.

Run the equivalent commands manually instead, after the `postgresql`
container is up, substituting the values from your `.env`
(`POSTGRES_USER`, `POSTGRES_DATABASE_NAME`, `DEBEZIUM_PASSWORD`):

```bash
docker exec -it postgresql psql -U <POSTGRES_USER> -d <POSTGRES_DATABASE_NAME>
```

```sql
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'debezium_user'
    ) THEN
        CREATE USER debezium_user WITH
            REPLICATION
            LOGIN
            PASSWORD '<DEBEZIUM_PASSWORD>';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO debezium_user;
```

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

### Running the Project

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

### Cleanup

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

### Troubleshooting

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
