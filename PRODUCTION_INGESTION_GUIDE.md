# Production Ingestion Setup Guide

Step-by-step guide for setting up **API mode** and **Database URI (CDC) mode** ingestion.

---

## Table of Contents

1. [What's Already Pre-configured](#1-whats-already-pre-configured)
2. [Database-Specific Setup](#2-database-specific-setup)
3. [API Mode Ingestion](#3-api-mode-ingestion)
4. [Triggering the Pipelines](#4-triggering-the-pipelines)
5. [Monitoring & Operations](#5-monitoring--operations)
6. [Security Hardening Checklist](#6-security-hardening-checklist)

---

## 1. What's Already Pre-configured

The following are handled automatically by `docker-compose.yml` and init
scripts — **no manual action needed** for these:

| What | How |
|---|---|
| PostgreSQL WAL logical replication | `POSTGRESQL_WAL_LEVEL=logical` in `docker-compose.yml` |
| `debezium_user` on Pulse PostgreSQL | `sql/create_debezium_user.sh` runs on first container start; password from `DEBEZIUM_PASSWORD` in `.env` (default: `debezium_changeme`) |
| Cassandra CDC service | `cassandra` + `debezium-cassandra` services already defined in `docker-compose.yml`; commit-log volume shared automatically |
| Cassandra connector config | `conf/debezium-cassandra.properties` already mounted into the `debezium-cassandra` container |
| Debezium connectors installed | All connectors (PostgreSQL, MySQL, MariaDB, MongoDB, SQL Server, Oracle, Db2, Vitess, Spanner, Informix) are installed in `.docker/debezium/Dockerfile` |

**Two files you must place manually before starting the stack** (only if you
use those sources):

| File | Required for | Where to place |
|---|---|---|
| `ojdbc8.jar` | Oracle CDC | `./jars/ojdbc8.jar` |
| `gcp-credentials.json` | Spanner CDC | `./jars/gcp-credentials.json` |

These are mounted into the `debezium` container by `docker-compose.yml`.
Download `ojdbc8.jar` from
https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html.

---

## 2. Database-Specific Setup

### 2.1 Pulse's Own PostgreSQL (Internal)

The `debezium_user` is created automatically with `REPLICATION` login and
`SELECT` on all public tables. Nothing to do except set `DEBEZIUM_PASSWORD` in
`.env` before the first `docker compose up`.

If you need to monitor only specific tables you can create a scoped publication
(connect to the `pulse` database):

```sql
CREATE PUBLICATION debezium_pub
  FOR TABLE orders, customers, products, payments, inventory;
```

Monitor replication slots to avoid unbounded disk growth:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM   pg_replication_slots;

-- Drop a slot when Debezium is stopped for an extended period:
SELECT pg_drop_replication_slot('slot_name');
```

### 2.2 External PostgreSQL

On the external server, run once as superuser:

```sql
-- Enable logical replication (restart required if changing wal_level)
ALTER SYSTEM SET wal_level = logical;

-- Create a least-privilege replication user
CREATE USER debezium_user
  WITH REPLICATION LOGIN
  PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

-- Per database: grant access and create a publication
\c your_database
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE orders, customers, products, payments, inventory TO debezium_user;
CREATE PUBLICATION debezium_pub
  FOR TABLE orders, customers, products, payments, inventory;
```

Verify:

```bash
psql "postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>" \
  -c "SELECT current_setting('wal_level');"
# Expected: logical
```

Connection URI format:

```
postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>
# With TLS:
postgresql://debezium_user:REPLACE_WITH_STRONG_PASSWORD@<host>:5432/<database>?sslmode=require
```

### 2.3 MongoDB

MongoDB must run as a **replica set** (required for change streams).
Initialize a single-node replica set if your instance is standalone:

```javascript
// mongosh
rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] })
rs.status()  // state should be PRIMARY
```

Create a least-privilege Debezium user:

```javascript
use admin
db.createUser({
  user: "debezium",
  pwd: "REPLACE_WITH_STRONG_PASSWORD",
  roles: [
    { role: "read",           db: "YOUR_DATABASE" },
    { role: "read",           db: "local"          },
    { role: "clusterMonitor", db: "admin"          }
  ]
})
```

Connection URI format:

```
mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0
# With TLS:
mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0&tls=true
```

### 2.4 MySQL / MariaDB

Enable binary logging on the server (`my.cnf`):

```ini
[mysqld]
server-id         = 1
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
```

Create a user:

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY 'REPLACE_WITH_STRONG_PASSWORD';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

Connection URI format:

```
mysql://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:3306/<database>
mariadb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:3306/<database>
```

### 2.5 Cassandra

The `cassandra` and `debezium-cassandra` services are already defined in
`docker-compose.yml`. You only need to:

1. Edit `conf/debezium-cassandra.properties` and set:
   - `debezium.source.cassandra.password=YOUR_PASSWORD`
   - `debezium.source.cassandra.keyspace=YOUR_KEYSPACE`

2. After starting the stack, enable CDC on each table you want to capture:

```cql
-- cqlsh
ALTER TABLE your_keyspace.orders   WITH cdc = true;
ALTER TABLE your_keyspace.payments WITH cdc = true;
```

### 2.6 Oracle

Place `ojdbc8.jar` at `./jars/ojdbc8.jar` before starting the stack (see §1).
No other manual steps — the connector JARs are already in the Debezium image.

Connection URI format:

```
oracle://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:1521/<service_name>
```

### 2.7 Google Spanner

Place your service-account `gcp-credentials.json` at `./jars/gcp-credentials.json`
before starting the stack (see §1). Create a change stream on the Spanner database:

```sql
CREATE CHANGE STREAM pulse_change_stream FOR ALL;
```

Connection URI format:

```
spanner://projects/<project>/instances/<instance>/databases/<database>
```

---

## 3. API Mode Ingestion

API mode polls an HTTP endpoint every N seconds and feeds data to Kafka.

### 3.1 Pulse's Built-in Ingest Endpoint

The API container exposes `GET /ingest/stream` at `http://10.5.0.9:8000`.

**Query parameters:**

| Parameter     | Required | Description |
|---|---|---|
| `business_id` | Yes | MinIO bucket name (e.g. `pulse-bucket-1`) |
| `since`       | No  | ISO-8601 datetime — only return newer files |
| `limit`       | No  | Max records per table per poll (default 500) |

**Response format:**

```json
{
  "tables": [
    { "table_name": "orders",    "data": [ { "order_id": "101", ... } ] },
    { "table_name": "customers", "data": [ { "customer_id": "C1", ... } ] }
  ]
}
```

Returns `{ "tables": [] }` when there are no new files.

Each file is served exactly once (Redis tracks served keys at
`ingest_stream:{business_id}:served`). To re-serve all files:

```bash
docker exec redis redis-cli DEL "ingest_stream:pulse-bucket-1:served"
```

### 3.2 External API Endpoint

Any external API can be used as the source as long as it returns the same
format shown above. Pass its URL as `api_url` when triggering the DAG.
Table name aliases (`users` → `customers`, `transactions` → `payments`, etc.)
are resolved automatically.

---

## 4. Triggering the Pipelines

> **Airflow UI**: `http://localhost:8090`  
> **Airflow REST API**: `http://localhost:8090/api/v1/...`  
> Default credentials: `admin` / `admin` — change these before going live.

### 4.1 API Mode

```bash
curl -s -X POST "http://localhost:8090/api/v1/dags/api_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "conf": {
      "bucket":        "pulse-bucket-1",
      "api_url":       "http://10.5.0.9:8000/ingest/stream?business_id=pulse-bucket-1",
      "poll_interval": 30
    }
  }'
```

For an external API, replace `api_url` with the external URL.

### 4.2 Database URI Mode

**PostgreSQL example:**

```bash
curl -s -X POST "http://localhost:8090/api/v1/dags/db_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
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
curl -s -X POST "http://localhost:8090/api/v1/dags/db_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "conf": {
      "bucket":    "pulse-bucket-1",
      "db_uri":    "mongodb://debezium:REPLACE_WITH_STRONG_PASSWORD@<host>:27017/<database>?authSource=admin&replicaSet=rs0",
      "db_tables": "orders,customers,products"
    }
  }'
```

### 4.3 Supported URI Schemes

| Scheme | Database | Manual prerequisite |
|---|---|---|
| `postgresql://` | PostgreSQL | WAL logical + publication (automated for internal DB) |
| `mysql://`      | MySQL      | Binlog ROW format enabled |
| `mariadb://`    | MariaDB    | Binlog ROW format enabled |
| `mongodb://`    | MongoDB    | Replica set initialised |
| `mssql://`      | SQL Server | CDC enabled on tables |
| `oracle://`     | Oracle     | `ojdbc8.jar` in `./jars/` |
| `db2://`        | IBM Db2    | SQL Replication enabled |
| `cassandra://`  | Cassandra  | CDC per table + `conf/debezium-cassandra.properties` updated |
| `vitess://`     | Vitess     | VStream gRPC enabled |
| `spanner://`    | Spanner    | Change stream created + `gcp-credentials.json` in `./jars/` |
| `informix://`   | Informix   | CDC API enabled |

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

# Check a specific connector
curl -s http://localhost:8083/connectors/pulse-bucket-1-connector/status | jq .
# Expected: {"state": "RUNNING", ...}

# Restart a failed connector
curl -s -X POST http://localhost:8083/connectors/pulse-bucket-1-connector/restart
```

### 5.3 Monitor Logs

```bash
# Python pipeline container
docker logs python --follow --tail=50

# Airflow scheduler
docker logs airflow-scheduler --follow --tail=30
```

### 5.4 Check the Ingest Stream Endpoint

```bash
curl -s "http://localhost:8000/ingest/stream?business_id=pulse-bucket-1" \
  | jq '.tables | map({table: .table_name, rows: (.data | length)})'
```

### 5.5 Redis State for API Ingest Stream

```bash
# Files served so far
docker exec redis redis-cli SCARD "ingest_stream:pulse-bucket-1:served"

# Reset (re-serve all files on next poll)
docker exec redis redis-cli DEL "ingest_stream:pulse-bucket-1:served"
```

---

## 6. Security Hardening Checklist

### Credentials

- [ ] Set `DEBEZIUM_PASSWORD` in `.env` to a strong password before first `docker compose up`
- [ ] Replace default MinIO credentials (`minioadmin`) in `.env`
- [ ] Replace default Airflow credentials (`admin`/`admin`) after first login
- [ ] Use a strong `AIRFLOW_FERNET_KEY` and `AIRFLOW_SECRET_KEY` in `.env`
- [ ] Store database URIs in **Airflow Connections** (`Admin → Connections`), not plain in DAG `conf`

### Network

- [ ] Do **not** expose PostgreSQL (5432), MongoDB (27017), or Kafka (9092) to the public internet
- [ ] Restrict Debezium REST API (port 8083) to the internal Docker network
- [ ] Use `sslmode=require` in PostgreSQL URIs and `tls=true` in MongoDB URIs

### Principle of Least Privilege

- [ ] PostgreSQL: `debezium_user` has `REPLICATION LOGIN` and explicit `SELECT` only — not superuser
- [ ] MongoDB: roles `read` (source db + local) and `clusterMonitor` only — not `root`
- [ ] Debezium user has **no write permissions** on source databases

### Operational

- [ ] Monitor PostgreSQL replication slots — unconsumed slots cause unbounded disk growth (see §2.1)
- [ ] Set MongoDB oplog size appropriate to expected change rate (`--oplogSizeMB`)
- [ ] Set up alerting on Kafka consumer lag for `ecom.*` topics
- [ ] Enable Airflow email alerts for failed tasks (`email_on_failure=True` in task defaults)
