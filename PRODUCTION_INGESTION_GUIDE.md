# Production Ingestion Setup Guide

Everything in the stack is pre-configured. This guide only lists what you must
do manually **on your source system** before triggering a pipeline.

---

## 1. Before Starting the Stack

Set these in `.env`:

```env
DEBEZIUM_PASSWORD=<strong_password>   # creates debezium_user on Pulse's PostgreSQL
```

If you use Oracle or Spanner, also place the following files (they are mounted
into the Debezium container by `docker-compose.yml`):

| File | Source |
|---|---|
| `./jars/ojdbc8.jar` | https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html |
| `./jars/gcp-credentials.json` | Your GCP service-account key |

---

## 2. Source Database Setup

Only needed for **external** databases. Skip this section if you are only
ingesting from Pulse's own PostgreSQL (already configured automatically).

### PostgreSQL (external)

```sql
-- Run on the external server as superuser, then restart PostgreSQL
ALTER SYSTEM SET wal_level = logical;

-- Create the replication user
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD '<password>';

-- Per database you want to monitor:
\c <your_database>
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE <table1>, <table2> TO debezium_user;
CREATE PUBLICATION debezium_pub FOR TABLE <table1>, <table2>;
```

### MongoDB

```javascript
// Initialize a replica set if your instance is standalone
rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] })

// Create a least-privilege user
use admin
db.createUser({
  user: "debezium", pwd: "<password>",
  roles: [
    { role: "read",           db: "<your_database>" },
    { role: "read",           db: "local" },
    { role: "clusterMonitor", db: "admin" }
  ]
})
```

### MySQL / MariaDB

Add to `my.cnf` and restart MySQL:

```ini
[mysqld]
server-id        = 1
log_bin          = mysql-bin
binlog_format    = ROW
binlog_row_image = FULL
```

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY '<password>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

### Cassandra

Edit `conf/debezium-cassandra.properties` (already mounted into the container):

```properties
debezium.source.cassandra.password=<password>
debezium.source.cassandra.keyspace=<your_keyspace>
```

After starting the stack, enable CDC on each table you want to capture:

```cql
ALTER TABLE <your_keyspace>.<table> WITH cdc = true;
```

### SQL Server

Enable CDC on the database and each table you want to monitor:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '<table>', @role_name = NULL;
```

### Oracle

Place `./jars/ojdbc8.jar` (see §1). No other changes needed in this repo.

### Google Spanner

Place `./jars/gcp-credentials.json` (see §1), then create a change stream:

```sql
CREATE CHANGE STREAM pulse_change_stream FOR ALL;
```

### IBM Db2 / Vitess / Informix

Enable the database-side CDC feature (SQL Replication for Db2, VStream gRPC
for Vitess, CDC API for Informix) according to the Debezium documentation for
that connector. No changes needed in this repo.

---

## 3. Trigger the Pipelines

> Airflow UI: `http://localhost:8090` — default credentials `admin` / `admin`

### API mode

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

Use any external API URL in `api_url` as long as it returns
`{ "tables": [ { "table_name": "...", "data": [...] } ] }`.

### Database URI mode

```bash
curl -s -X POST "http://localhost:8090/api/v1/dags/db_streaming/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:admin" \
  -d '{
    "conf": {
      "bucket":    "pulse-bucket-1",
      "db_uri":    "<scheme>://<user>:<password>@<host>:<port>/<database>",
      "db_tables": "orders,customers,products"
    }
  }'
```

Supported schemes: `postgresql://`, `mysql://`, `mariadb://`, `mongodb://`,
`mssql://`, `oracle://`, `db2://`, `cassandra://`, `vitess://`, `spanner://`,
`informix://`

---

## 4. Security Checklist

- [ ] Set strong values for `DEBEZIUM_PASSWORD`, `MINIO_ROOT_PASSWORD`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY` in `.env`
- [ ] Change Airflow default credentials (`admin`/`admin`) after first login
- [ ] Do **not** expose ports 5432, 27017, 9092, or 8083 to the public internet
- [ ] Use `sslmode=require` / `tls=true` in database URIs
- [ ] Grant Debezium users the minimum required privileges only (no write access)
- [ ] Monitor PostgreSQL replication slots — unused slots cause unbounded WAL growth
- [ ] Store production database URIs in Airflow Connections (`Admin → Connections`), not in DAG `conf`
