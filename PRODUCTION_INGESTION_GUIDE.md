# Production Ingestion Setup Guide

Complete step-by-step guide for setting up **API mode** and **Database URI mode**
in production for both the **MongoDB Test System** and the **Pulse Project**.

---

## Table of Contents

1. [MongoDB Test System — Database URI Ingestion](#1-mongodb-test-system--database-uri-ingestion)
2. [Pulse Project — Database URI Ingestion (PostgreSQL)](#2-pulse-project--database-uri-ingestion-postgresql)
3. [Pulse Project — API Mode Ingestion](#3-pulse-project--api-mode-ingestion)
4. [Triggering the Pipelines](#4-triggering-the-pipelines)
5. [Monitoring & Operations](#5-monitoring--operations)
6. [Security Hardening Checklist](#6-security-hardening-checklist)

---

## 1. MongoDB Test System — Database URI Ingestion

MongoDB acts as a CDC source.  Debezium tails the oplog via the replica-set
change stream.

### 1.1 Prerequisites

- MongoDB must run as a **replica set** (change streams require it).
- If your instance is standalone, initialise a single-node replica set:

```javascript
// mongosh
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
})
```

Verify:

```javascript
rs.status()   // state should be PRIMARY
```

### 1.2 Create a Dedicated Debezium User

Never use the `root` account in production.  Connect as root once to create a
least-privilege user:

```javascript
use admin
db.createUser({
  user: "debezium",
  pwd: "REPLACE_WITH_STRONG_PASSWORD",
  roles: [
    { role: "read",           db: "YOUR_DATABASE" }, // read source collections
    { role: "read",           db: "local"          }, // read oplog
    { role: "clusterMonitor", db: "admin"          }  // isMaster, serverStatus
  ]
})
```

Verify the user can connect and read the oplog:

```bash
mongosh "mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@localhost:27017/YOUR_DATABASE?authSource=admin&replicaSet=rs0" \
  --eval "db.runCommand({ isMaster: 1 })"
```

### 1.3 Build the Connection URI

```
mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0
```

For TLS (recommended in production):

```
mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0&tls=true
```

### 1.4 What the Connector Will Do

Once triggered (see §4), Debezium creates a Kafka Connect connector that:

- Takes an **initial snapshot** of all specified collections (operation `r`)
- Then streams every INSERT (`c`), UPDATE (`u`), and DELETE (`d`) in real time
- Publishes to Kafka topics: `ecom.customers`, `ecom.orders`, etc.

### 1.5 MongoDB — No Publication Needed

Unlike PostgreSQL, MongoDB does **not** require publications or WAL
configuration.  The replica-set oplog is always available.

---

## 2. Pulse Project — Database URI Ingestion (PostgreSQL)

Pulse's own PostgreSQL instance (or any external PostgreSQL) can feed the
streaming pipeline via Debezium logical replication.

### 2.1 Enable Logical Replication on the PostgreSQL Server

Add to `postgresql.conf` (or pass as environment variables in Docker Compose):

```ini
wal_level             = logical
max_wal_senders       = 4
max_replication_slots = 4
```

In the Pulse `docker-compose.yml` the Bitnami image accepts these as
environment variables:

```yaml
postgresql:
  environment:
    - POSTGRESQL_WAL_LEVEL=logical
    - POSTGRESQL_MAX_WAL_SENDERS=4
    - POSTGRESQL_MAX_REPLICATION_SLOTS=4
```

Restart PostgreSQL after changing `wal_level`.

### 2.2 Create a Dedicated Replication User

`CREATE USER` creates a **server-level role** — run this once per PostgreSQL
instance regardless of how many databases you monitor:

```sql
CREATE USER debezium_user
  WITH REPLICATION LOGIN
  PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';
```

### 2.3 Grant Per-Database Permissions

For **each database** you want to monitor, connect to that database and run:

```sql
-- Connect to the target database first
\c your_database

-- Schema access
GRANT USAGE ON SCHEMA public TO debezium_user;

-- Table read access (list only the tables Debezium should monitor)
GRANT SELECT ON TABLE orders, customers, products, payments, inventory TO debezium_user;

-- Create a scoped publication (not FOR ALL TABLES — limits blast radius)
CREATE PUBLICATION debezium_pub FOR TABLE orders, customers, products, payments, inventory;
```

> **Multiple databases on the same server**: The user is created once. Run the
> `GRANT` and `CREATE PUBLICATION` steps separately for each database.
>
> **Multiple servers**: Create the user on each server independently.

### 2.4 Verify the Setup

```bash
# Test connectivity
psql "postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>" \
  -c "SELECT pg_is_in_recovery(), current_setting('wal_level');"

# Expected output:
#  pg_is_in_recovery | current_setting
# -------------------+-----------------
#  f                 | logical
```

### 2.5 Build the Connection URI

```
postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>
```

For TLS:

```
postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>?sslmode=require
```

### 2.6 Replication Slot Monitoring

Unconsumed replication slots hold WAL on disk indefinitely.  Monitor them:

```sql
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM   pg_replication_slots;
```

If Debezium is stopped for an extended period, drop the slot manually to
prevent disk exhaustion:

```sql
SELECT pg_drop_replication_slot('slot_name');
```

---

## 3. Pulse Project — API Mode Ingestion

The API mode polls an HTTP endpoint every N seconds and feeds data to Kafka.

### 3.1 The /ingest/stream Endpoint (Now Implemented)

Pulse exposes `GET /ingest/stream` on the API container (`10.5.0.9:8000`).
This endpoint reads files from a business's MinIO `ingested/` folder and
returns them in the format expected by `api_ingest_service.py`.

**Endpoint:** `GET http://10.5.0.9:8000/ingest/stream`

**Query parameters:**

| Parameter    | Required | Description |
|---|---|---|
| `business_id` | Yes | MinIO bucket name (e.g. `pulse-bucket-1`) |
| `since`       | No  | ISO-8601 datetime — return only newer files |
| `limit`       | No  | Max records per table per poll (default 500) |

**Example:**

```
GET /ingest/stream?business_id=pulse-bucket-1&limit=500
```

**Response (matches `api_validation.APIDataFormat`):**

```json
{
  "tables": [
    {
      "table_name": "orders",
      "data": [
        { "order_id": "101", "customer_id": "C1", "amount": 250 },
        { "order_id": "102", "customer_id": "C2", "amount": 180 }
      ]
    },
    {
      "table_name": "customers",
      "data": [
        { "customer_id": "C1", "name": "Alice", "email": "alice@example.com" }
      ]
    }
  ]
}
```

Returns `{ "tables": [] }` when there are no new files to serve.

**Incremental delivery:** Each file is served exactly once per business stream.
Served file keys are stored in Redis (`ingest_stream:{business_id}:served`).
To re-serve all files:

```bash
redis-cli DEL "ingest_stream:pulse-bucket-1:served"
```

### 3.2 Using an External API Endpoint Instead

If the data source is an external service (not PULSE itself), pass that URL
when triggering the DAG.  The external API must return the same format:

```json
{
  "tables": [
    { "table_name": "orders", "data": [ {...}, {...} ] }
  ]
}
```

Supported file-name aliases are resolved automatically (`users` → `customers`,
`transactions` → `payments`, etc.).

### 3.3 Environment Variables for API Mode

Set these in the API container environment (`.env` or `docker-compose.yml`):

```env
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Optionally override defaults used by pipeline_config.py
FRONTEND_API_URL=http://10.5.0.9:8000/ingest/stream?business_id=pulse-bucket-1
CDC_POLL_INTERVAL=30
```

---

## 4. Triggering the Pipelines

### 4.1 API Mode — Trigger the DAG

```bash
curl -s -X POST "http://localhost:8080/api/v1/dags/api_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "airflow:airflow" \
  -d '{
    "conf": {
      "bucket":        "pulse-bucket-1",
      "api_url":       "http://10.5.0.9:8000/ingest/stream?business_id=pulse-bucket-1",
      "poll_interval": 30
    }
  }'
```

For an external API endpoint, replace `api_url` with the external URL:

```bash
"api_url": "https://your-ecommerce-system.com/api/data"
```

### 4.2 Database URI Mode — Trigger the DAG

**PostgreSQL example:**

```bash
curl -s -X POST "http://localhost:8080/api/v1/dags/db_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "airflow:airflow" \
  -d '{
    "conf": {
      "bucket":    "pulse-bucket-1",
      "db_uri":    "postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>",
      "db_tables": "orders,customers,products,payments,inventory"
    }
  }'
```

**MongoDB example:**

```bash
curl -s -X POST "http://localhost:8080/api/v1/dags/db_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "airflow:airflow" \
  -d '{
    "conf": {
      "bucket":    "pulse-bucket-1",
      "db_uri":    "mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0",
      "db_tables": "orders,customers,products"
    }
  }'
```

### 4.3 Supported URI Schemes

| Scheme | Database | Notes |
|---|---|---|
| `postgresql://` | PostgreSQL | Requires WAL logical, publication |
| `mysql://`      | MySQL      | Requires binlog enabled |
| `mariadb://`    | MariaDB    | Requires binlog enabled |
| `mongodb://`    | MongoDB    | Requires replica set |
| `mssql://`      | SQL Server | Requires CDC enabled on tables |
| `oracle://`     | Oracle     | Requires LogMiner or XStream |
| `db2://`        | IBM Db2    | Requires SQL Replication |
| `cassandra://`  | Cassandra  | Requires commit log CDC |
| `vitess://`     | Vitess     | Via VStream gRPC |
| `spanner://`    | Spanner    | Via change streams, needs GCP credentials |
| `informix://`   | Informix   | Via CDC API |

---

## 5. Monitoring & Operations

### 5.1 Verify Kafka Topics Are Receiving Data

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecom.orders \
  --from-beginning \
  --max-messages 5
```

### 5.2 Check Debezium Connector Status

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq .

# Check a specific connector's status
curl -s http://localhost:8083/connectors/pulse-bucket-1-connector/status | jq .

# Expected: {"state": "RUNNING", ...}
```

Restart a failed connector:

```bash
curl -s -X POST http://localhost:8083/connectors/pulse-bucket-1-connector/restart
```

### 5.3 Monitor the API Streaming Pipeline

```bash
# Watch api_ingest_service.py logs
docker logs python --follow --tail=50 | grep -E "Processed|API error|validation"

# Watch Airflow task logs
docker logs airflow-scheduler --follow --tail=30
```

### 5.4 Monitor the Ingest Stream Endpoint

```bash
# Manually poll the endpoint (same as api_ingest_service.py does)
curl -s "http://localhost:8000/ingest/stream?business_id=pulse-bucket-1" | jq '.tables | map({table: .table_name, rows: (.data | length)})'
```

### 5.5 Check Redis State for Ingest Stream

```bash
# How many files have been served for a business
docker exec redis redis-cli SCARD "ingest_stream:pulse-bucket-1:served"

# List served file keys
docker exec redis redis-cli SMEMBERS "ingest_stream:pulse-bucket-1:served"

# Reset — re-serve all files on the next poll
docker exec redis redis-cli DEL "ingest_stream:pulse-bucket-1:served"
```

---

## 6. Security Hardening Checklist

### Credentials

- [ ] Replace all default passwords (`debezium_pass`, `minioadmin`, `airflow`) before going live
- [ ] Store database URIs in **Airflow Connections** (`Admin → Connections`), not plain in DAG conf
- [ ] Store MinIO credentials in environment variables injected at runtime, not in source code
- [ ] Use a strong `SECRET_KEY` in the Pulse API `.env`

### Network

- [ ] Do **not** expose PostgreSQL port 5432, MongoDB port 27017, or Kafka port 9092 to the public internet
- [ ] Place Debezium (Kafka Connect) and source databases on the same private Docker network
- [ ] Use `sslmode=require` in PostgreSQL URIs and `tls=true` in MongoDB URIs
- [ ] Restrict Debezium REST API (port 8083) to internal network only

### Principle of Least Privilege

- [ ] PostgreSQL: use `debezium_user` with `REPLICATION LOGIN` and explicit `GRANT SELECT` per table — not a superuser
- [ ] MongoDB: use roles `read` (source db), `read` (local), `clusterMonitor` (admin) — not `root`
- [ ] Debezium user has **no write permissions** on source databases

### Operational

- [ ] Monitor PostgreSQL replication slots — unconsumed slots cause disk growth
- [ ] Set MongoDB oplog size appropriate to expected change rate (`--oplogSizeMB`)
- [ ] Set up alerting on Kafka consumer lag for `ecom.*` topics
- [ ] Enable Airflow email alerts for failed tasks (`email_on_failure=True` in task defaults)
- [ ] Rotate Airflow `airflow:airflow` default credentials immediately
