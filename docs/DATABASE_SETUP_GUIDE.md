# Database Setup Guide — Pulse Real-Time Ingestion

This guide explains how to configure each supported database so that Pulse can ingest data from it in real time using Debezium Change Data Capture (CDC).

There are **two sides** to set up:

- **Remote database (your side):** The source database from which data will be ingested. A database administrator must configure this database and create the `debezium_user` before entering the connection URI in Pulse.
- **Pulse system (our side):** The infrastructure that reads change events, processes them, and loads them into the analytics pipeline.

---

## How It Works

```
Your database                              Pulse system
─────────────────                          ────────────────────────────────────────
Transaction log  ──→  Debezium  ──→  Kafka  ──→  Spark Streaming  ──→  MinIO / Analytics
(WAL / binlog /        (reads log           (event     (mapping,              (dashboards,
 oplog / CDC)           as debezium_user)    bus)        cleaning, ML)          reports)
```

Debezium connects to your database using the `debezium_user` credentials you enter in the Pulse onboarding wizard. It reads the database's native change log — not application queries — so there is no impact on your database performance.

---

## Supported Databases

| # | Database | URI scheme | Default port | CDC mechanism |
|---|----------|-----------|---------|---------------|
| 1 | PostgreSQL | `postgresql://` | 5432 | Logical replication (WAL) |
| 2 | MySQL | `mysql://` | 3306 | Binary log (binlog) |
| 3 | MariaDB | `mariadb://` | 3306 | Binary log (binlog) |
| 4 | MongoDB | `mongodb://` | 27017 | Change streams (oplog) |
| 5 | SQL Server | `mssql://` | 1433 | SQL Server CDC tables |
| 6 | Oracle | `oracle://` | 1521 | LogMiner / XStream |
| 7 | IBM Db2 | `db2://` | 50000 | ASN Capture (SQL Replication) |
| 8 | Vitess | `vitess://` | 15991 | VStream gRPC |
| 9 | Google Spanner | `spanner://` | N/A | Change streams |
| 10 | Informix | `informix://` | 9088 | CDC API (syscdcv1) |
| 11 | Cassandra | `cassandra://` | 9042 | Commit log CDC |

---

## Connector Plugin Status — What the Pulse Dockerfile Already Installs

The Debezium container is built from `.docker/debezium/Dockerfile`. It extends `quay.io/debezium/connect:3.4` and adds the community connectors that are not in the base image. The table below shows the connector status for each database and any **extra Pulse-side action** required beyond just having the plugin installed.

| Database | Connector plugin | How it gets installed | Extra Pulse-side requirement |
|----------|-----------------|----------------------|------------------------------|
| PostgreSQL | `debezium-connector-postgres` | Pre-installed in base image | None |
| MySQL | `debezium-connector-mysql` | Pre-installed in base image | None |
| MariaDB | `debezium-connector-mariadb` | Downloaded in Dockerfile (Maven Central) | None — standalone connector added in 3.4 |
| MongoDB | `debezium-connector-mongodb` | Pre-installed in base image | None |
| SQL Server | `debezium-connector-sqlserver` | Pre-installed in base image | None |
| Oracle | `debezium-connector-oracle` | Pre-installed in base image (JARs only) | ⚠️ **`ojdbc8.jar` must be manually supplied** — Oracle's JDBC driver cannot be redistributed. Download it from Oracle and place at `./jars/ojdbc8.jar`. |
| IBM Db2 | `debezium-connector-db2` | Pre-installed in base image | None |
| Vitess | `debezium-connector-vitess` | Downloaded in Dockerfile (Maven Central) | None |
| Google Spanner | `debezium-connector-spanner` | Downloaded in Dockerfile (Maven Central) | ⚠️ **GCP credentials JSON file must be supplied** — place at `./jars/gcp-credentials.json` before starting the stack. |
| Informix | `debezium-connector-informix` | Downloaded in Dockerfile (Maven Central) | None |
| Cassandra | ⚠️ **Cannot use Kafka Connect** | Not installable in Kafka Connect | ⚠️ **Separate Debezium Server container required** (see section 11) |

---

## URI Format

```
scheme://debezium_user:password@host:port/database
```

Examples:

```
postgresql://debezium_user:secret@db.example.com:5432/ecommerce
mysql://debezium_user:secret@db.example.com:3306/shop
mariadb://debezium_user:secret@db.example.com:3306/shop
mongodb://debezium_user:secret@db.example.com:27017/orders?replicaSet=rs0&authSource=admin
mssql://debezium_user:secret@db.example.com:1433/sales
oracle://debezium_user:secret@db.example.com:1521/ORCL
db2://debezium_user:secret@db.example.com:50000/ECOMDB
vitess://vtgate.example.com:15991/commerce
spanner://my-gcp-project/my-instance/my-database
informix://debezium_user:secret@db.example.com:9088/stores
cassandra://debezium_user:secret@db.example.com:9042/ecommerce
```

---

## Pulse System Side — One-Time Setup

These steps are done **once** when deploying Pulse. If you are using the provided `docker-compose.yml`, most of this is automatic.

### Start the services

```bash
docker-compose up -d
```

This starts Debezium, Kafka, Zookeeper, PostgreSQL, Spark, MinIO, Redis, Airflow, and Cassandra (with its dedicated Debezium Server).

### Verify Debezium is running

```bash
curl http://localhost:8083/
# Expected: {"version":"3.4.0.Final","commit":"..."}
```

### Internal PostgreSQL — already configured

The Pulse internal PostgreSQL is pre-configured with:
- `wal_level = logical` — required for logical replication
- `debezium_user` role with `REPLICATION` privilege and `SELECT` on all tables

This happens automatically on first container start via the initialization scripts in `sql/`.

### Connector plugins — what the Dockerfile installs

The Debezium container is built from `.docker/debezium/Dockerfile` (extends `quay.io/debezium/connect:3.4`):

- **Pre-installed in base image** — PostgreSQL, MySQL, MongoDB, SQL Server, Oracle (JARs), Db2
- **Downloaded during image build** — MariaDB, Vitess, Spanner, Informix, and the Debezium Scripting extension
- **Cassandra** — cannot run inside Kafka Connect; requires a separate `quay.io/debezium/server:3.4` container (see section 11 below)

After running `docker-compose up -d`, all connector plugins above are ready. The databases that require **additional manual steps on the Pulse system** are:

| Database | What you must do |
|----------|-----------------|
| Oracle | Place `ojdbc8.jar` at `./jars/ojdbc8.jar` **before** `docker-compose up` (see section 6) |
| Google Spanner | Place GCP credentials JSON at `./jars/gcp-credentials.json` **before** `docker-compose up` (see section 9) |
| Cassandra | Edit `conf/debezium-cassandra.properties` (set keyspace and password), then enable CDC per table with CQL after containers start (see section 11) |

---

## Auto-Configured vs Manual-Configuration Databases

The table below summarises, from the **Pulse system side**, which databases are fully ready after `docker-compose up -d` and which require additional manual steps.

| Database | Connector ready after `docker-compose up`? | Manual action required on Pulse side |
|----------|--------------------------------------------|---------------------------------------|
| PostgreSQL | ✅ **Yes — fully automatic** | None |
| MySQL | ✅ **Yes — fully automatic** | None |
| MariaDB | ✅ **Yes — fully automatic** | None |
| MongoDB | ✅ **Yes — fully automatic** | None |
| SQL Server | ✅ **Yes — fully automatic** | None |
| IBM Db2 | ✅ **Yes — fully automatic** | None |
| Vitess | ✅ **Yes — fully automatic** | None |
| Informix | ✅ **Yes — fully automatic** | None |
| Oracle | ⚠️ **No — requires manual file** | Download `ojdbc8.jar` from Oracle and place at `./jars/ojdbc8.jar` |
| Google Spanner | ⚠️ **No — requires manual file** | Place GCP service account JSON at `./jars/gcp-credentials.json` |
| Cassandra | ✅ **Yes — included in docker-compose** | Edit `conf/debezium-cassandra.properties` (keyspace and password), then enable CDC per table (see section 11) |

> **Note:** "Connector ready" means the plugin is installed and Kafka Connect can load it. You still need to configure the remote database itself (WAL level, binlog, etc.) as described in the sections below.

---

## Upgrade Notes — Debezium 2.x → 3.4

If you are upgrading an existing Pulse deployment from Debezium 2.5 to 3.4, be aware of the following:

### What changes automatically
- All community connectors (MariaDB, Vitess, Spanner, Informix) are updated to 3.4.0.Final when the image is rebuilt.
- MariaDB now uses its own dedicated connector (`debezium-connector-mariadb`) instead of the MySQL connector. Existing connectors deployed with `io.debezium.connector.mariadb.MariaDbConnector` continue to work. If you had any old connectors deployed with `io.debezium.connector.mysql.MySqlConnector` for MariaDB, delete and redeploy them.
- The scripting extension is now placed in its own `/kafka/connect/debezium-scripting/` plugin directory (instead of inside the Oracle connector directory). No action needed if you have not customised scripting transforms.

### Breaking change — Vitess tablet type
The `vitess.tablet.type` connector property value changed from `MASTER` to `PRIMARY` in Debezium 3.x. This is already updated in `debezium_connector_manager.py`. If you have any manually deployed Vitess connectors, update their configuration before or after the upgrade.

### Risk areas
| Area | Risk | Action |
|------|------|--------|
| Existing connector offsets | Low — 3.x can continue reading offsets written by 2.x | None; test after upgrade |
| Schema history topics | Low — backward-compatible format | None; monitor for errors |
| Vitess connectors | Medium — `MASTER` value rejected in 3.x | Delete and redeploy Vitess connectors with `PRIMARY` |
| MariaDB connectors using MySQL class | Medium — old class still works but new class is preferred | Redeploy with `io.debezium.connector.mariadb.MariaDbConnector` |
| Kafka version | Low — Confluent 7.7.0 (Kafka 3.7.x) is compatible with Debezium 3.4 | None |
| Java version | Debezium 3.x base image uses Java 21 (up from 11 in 2.x) | No action; handled by the container image |

---

## Database Setup — Remote Side

### 1. PostgreSQL

**Minimum version:** PostgreSQL 10+

#### Step 1 — Edit `postgresql.conf`

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after editing:

```bash
sudo systemctl restart postgresql
# or for Docker:
docker restart <postgres-container>
```

#### Step 2 — Create `debezium_user`

Connect as a superuser and run:

```sql
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'your_strong_password';

GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;
```

#### Step 3 — Verify

```sql
SHOW wal_level;             -- Must be 'logical'
SHOW max_replication_slots; -- Must be >= 1
```

#### URI to enter in Pulse

```
postgresql://debezium_user:your_strong_password@your-host:5432/your_database
```

---

### 2. MySQL

**Minimum version:** MySQL 5.7+

#### Step 1 — Edit `my.cnf` (or `my.ini` on Windows)

```conf
[mysqld]
server-id         = 1
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
expire_logs_days  = 3

# Recommended: GTID-based replication for more reliable CDC
gtid_mode                = ON
enforce_gtid_consistency = ON
```

Restart MySQL:

```bash
sudo systemctl restart mysql
```

#### Step 2 — Create `debezium_user`

```sql
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_strong_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;
```

#### Step 3 — Verify

```sql
SHOW VARIABLES LIKE 'log_bin';          -- Must be ON
SHOW VARIABLES LIKE 'binlog_format';    -- Must be ROW
SHOW VARIABLES LIKE 'binlog_row_image'; -- Must be FULL
```

#### URI to enter in Pulse

```
mysql://debezium_user:your_strong_password@your-host:3306/your_database
```

---

### 3. MariaDB

**Minimum version:** MariaDB 10.5+

> ℹ️ **Pulse system note:** Debezium 3.4 ships with a dedicated `debezium-connector-mariadb` plugin (available since Debezium 2.7). The Dockerfile installs it automatically. Pulse uses `io.debezium.connector.mariadb.MariaDbConnector` for MariaDB connections, which correctly handles MariaDB-specific GTID format and protocol differences from MySQL.

#### Step 1 — Edit `my.cnf`

```conf
[mysqld]
server-id         = 1
log_bin           = mariadb-bin
binlog_format     = ROW
binlog_row_image  = FULL
```

Restart MariaDB:

```bash
sudo systemctl restart mariadb
```

#### Step 2 — Create `debezium_user`

```sql
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_strong_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;
```

#### Step 3 — Verify

```sql
SHOW VARIABLES LIKE 'log_bin';          -- Must be ON
SHOW VARIABLES LIKE 'binlog_format';    -- Must be ROW
```

#### URI to enter in Pulse

```
mariadb://debezium_user:your_strong_password@your-host:3306/your_database
```

---

### 4. MongoDB

**Minimum version:** MongoDB 4.0+

#### Step 1 — Run as a replica set

MongoDB change streams require a replica set. Even a single-node deployment must be initialized as a replica set.

Connect to `mongosh` and run:

```javascript
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
})
```

For an existing standalone instance, add `--replSet rs0` to the `mongod` startup arguments and restart before running `rs.initiate()`.

#### Step 2 — Create `debezium_user`

```javascript
use admin;
db.createUser({
  user: "debezium_user",
  pwd: "your_strong_password",
  roles: [
    { role: "read", db: "your_database" },
    { role: "read", db: "local" },
    { role: "read", db: "config" },
    { role: "readAnyDatabase", db: "admin" }
  ]
});
```

#### Step 3 — Verify

```javascript
rs.status()  // All members must show stateStr: "PRIMARY" or "SECONDARY"
```

#### URI to enter in Pulse

```
mongodb://debezium_user:your_strong_password@your-host:27017/your_database?replicaSet=rs0&authSource=admin
```

---

### 5. SQL Server

**Minimum version:** SQL Server 2016+, with SQL Server Agent running

#### Step 1 — Enable CDC on the database

Connect as `sysadmin` and run:

```sql
USE your_database;
EXEC sys.sp_cdc_enable_db;
```

#### Step 2 — Enable CDC on each table to capture

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'orders',
  @role_name     = NULL;

-- Repeat for every table you want to stream
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'payments',
  @role_name     = NULL;
```

#### Step 3 — Ensure SQL Server Agent is running

CDC capture and cleanup jobs require SQL Server Agent. Verify it is running:

```sql
EXEC xp_servicecontrol N'QUERYSTATE', N'SQLServerAGENT';
-- Must return: Running
```

#### Step 4 — Create `debezium_user`

```sql
CREATE LOGIN debezium_user WITH PASSWORD = 'your_strong_password';

USE your_database;
CREATE USER debezium_user FOR LOGIN debezium_user;
ALTER ROLE db_datareader ADD MEMBER debezium_user;
GRANT VIEW DATABASE STATE TO debezium_user;
```

#### Step 5 — Verify

```sql
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'your_database';
-- is_cdc_enabled must be 1
```

#### URI to enter in Pulse

```
mssql://debezium_user:your_strong_password@your-host:1433/your_database
```

---

### 6. Oracle

**Minimum version:** Oracle 11g R2+

> ⚠️ **Pulse system — extra file required:** The Oracle Debezium connector JARs are pre-installed in the base image, but the Oracle JDBC driver (`ojdbc8.jar`) **cannot be redistributed** due to Oracle's license. You must supply it manually:
> 1. Download `ojdbc8.jar` from [Oracle JDBC Downloads](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html).
> 2. Place it at `./jars/ojdbc8.jar` (next to `docker-compose.yml`).
>
> The `debezium` service mounts it automatically via the volume in `docker-compose.yml`:
> ```yaml
> volumes:
>   - ./jars/ojdbc8.jar:/kafka/connect/debezium-connector-oracle/ojdbc8.jar
> ```
> **The Oracle connector will fail to start without this file.**

#### Step 1 — Enable archive log mode

Connect as `SYSDBA`:

```sql
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

#### Step 2 — Enable supplemental logging

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Enable per-table supplemental logging for each table to capture:

```sql
ALTER TABLE schema_name.orders   ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE schema_name.payments ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

#### Step 3 — Create `debezium_user`

For a **Container Database (CDB/PDB)** architecture:

```sql
ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER c##debezium_user IDENTIFIED BY your_strong_password
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;

GRANT CREATE SESSION        TO c##debezium_user;
GRANT SELECT ON V_$DATABASE TO c##debezium_user;
GRANT SELECT ON V_$LOG      TO c##debezium_user;
GRANT SELECT ON V_$LOGFILE  TO c##debezium_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS  TO c##debezium_user;
GRANT SELECT ON V_$ARCHIVED_LOG     TO c##debezium_user;
GRANT SELECT ON V_$TRANSACTION      TO c##debezium_user;
GRANT LOGMINING             TO c##debezium_user;
GRANT SELECT_CATALOG_ROLE   TO c##debezium_user;
GRANT EXECUTE ON DBMS_LOGMNR TO c##debezium_user;

-- Grant SELECT on each table to capture
GRANT SELECT ON schema_name.orders   TO c##debezium_user;
GRANT SELECT ON schema_name.payments TO c##debezium_user;
```

For a **non-CDB** architecture, omit the `c##` prefix:

```sql
CREATE USER debezium_user IDENTIFIED BY your_strong_password;
-- (same GRANT statements as above without c## prefix)
```

#### URI to enter in Pulse

```
oracle://debezium_user:your_strong_password@your-host:1521/service_name
```

> **Note:** For CDB, use `c##debezium_user` as the username in the URI.

---

### 7. IBM Db2

**Minimum version:** Db2 11.1+ with SQL Replication feature licensed

#### Step 1 — Enable CDC capture on tables

```sql
CALL ASNCDC.ADDTABLE('SCHEMA_NAME', 'ORDERS');
CALL ASNCDC.ADDTABLE('SCHEMA_NAME', 'PAYMENTS');

-- Start the capture agent
CALL ASNCDC.REINIT();
```

#### Step 2 — Create `debezium_user`

```sql
CREATE USER debezium_user IDENTIFIED BY your_strong_password;
GRANT CONNECT ON DATABASE TO debezium_user;
GRANT SELECT ON TABLE schema_name.orders   TO debezium_user;
GRANT SELECT ON TABLE schema_name.payments TO debezium_user;

-- Required: read access to ASN catalog tables
GRANT SELECT ON ASNCDC.IBMSNAP_REGISTER TO debezium_user;
GRANT SELECT ON ASNCDC.IBMSNAP_SIGNAL   TO debezium_user;
```

#### URI to enter in Pulse

```
db2://debezium_user:your_strong_password@your-host:50000/your_database
```

---

### 8. Vitess

**Minimum version:** Vitess 14+, VStream enabled

#### Requirements

- VTGate must be accessible from the Pulse network on the gRPC port (default 15991).
- VStream must be enabled on the Vitess cluster.
- No special user creation steps are required beyond the normal Vitess authentication configured for your cluster.

#### Verify VStream is accessible

```bash
curl -s http://<vtgate-host>:15000/debug/status | grep -i "ok"
```

#### URI to enter in Pulse

```
vitess://vtgate-host:15991/keyspace_name
```

---

### 9. Google Cloud Spanner

**Requirement:** A GCP service account with `roles/spanner.databaseReader` and `roles/spanner.viewer`.

> ⚠️ **Pulse system — extra file required:** The Spanner connector plugin is downloaded and installed automatically during the Docker image build. However, it requires a **GCP service account credentials JSON file at runtime**. Place the key file at `./jars/gcp-credentials.json` (next to `docker-compose.yml`) before running `docker-compose up`. The `debezium` service mounts it automatically:
> ```yaml
> volumes:
>   - ./jars/gcp-credentials.json:/etc/gcp/credentials.json
> environment:
>   - GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/credentials.json
> ```
> **The Spanner connector will fail to authenticate without this file.**

#### Step 1 — Create a change stream on the database

Using the Google Cloud Console or `gcloud`:

```sql
CREATE CHANGE STREAM pulse_change_stream
  FOR orders, payments, inventory
  OPTIONS (
    retention_period = '7d',
    value_capture_type = 'NEW_AND_OLD_VALUES'
  );
```

#### Step 2 — Create a service account

1. In Google Cloud Console, go to **IAM & Admin → Service Accounts**.
2. Create a service account, e.g., `pulse-debezium`.
3. Assign roles:
   - `Cloud Spanner Database Reader`
   - `Cloud Spanner Viewer`
4. Download the JSON key file.

#### Step 3 — Configure credentials in Pulse

Place the JSON key file at `./jars/gcp-credentials.json` (relative to `docker-compose.yml`). The `debezium` service mounts it automatically:

```yaml
volumes:
  - ./jars/gcp-credentials.json:/etc/gcp/credentials.json
```

#### URI to enter in Pulse

```
spanner://your-gcp-project/your-instance/your-database
```

---

### 10. Informix

**Minimum version:** Informix 12.10+ with CDC option

#### Step 1 — Enable CDC on the database and tables

Connect as `informix` (the database owner) and run:

```sql
EXECUTE FUNCTION task('cdc add database', 'your_database');

EXECUTE FUNCTION task('cdc add table', 'your_database:informix.orders');
EXECUTE FUNCTION task('cdc add table', 'your_database:informix.payments');
```

#### Step 2 — Create `debezium_user`

```sql
CREATE USER debezium_user WITH PASSWORD 'your_strong_password';
GRANT SELECT ON orders   TO debezium_user;
GRANT SELECT ON payments TO debezium_user;
```

#### URI to enter in Pulse

```
informix://debezium_user:your_strong_password@your-host:9088/your_database
```

---

### 11. Cassandra

**Minimum version:** Cassandra 4.0+

> ℹ️ **Pulse system — already included in docker-compose:** The Cassandra connector **cannot run inside Kafka Connect**. Instead, a separate `debezium-cassandra` service using `quay.io/debezium/server:3.4` is included in `docker-compose.yml`. It shares the `cassandra_commitlog` volume with the `cassandra` service and reads commit log files directly. The `cassandra` service itself is also included, built from `.docker/cassandra/Dockerfile` which enables CDC in `cassandra.yaml` automatically. This is different from all other supported databases, which run inside the `debezium` Kafka Connect container.

#### Step 1 — Enable CDC in `cassandra.yaml`

CDC is already enabled in the `cassandra` container via the Dockerfile (`.docker/cassandra/Dockerfile`). No manual `cassandra.yaml` editing is required when using the provided Docker setup.

If you are using an **external** Cassandra instance, add the following to its `cassandra.yaml` and restart:

```yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_total_space_in_mb: 4096
```

Restart Cassandra:

```bash
sudo systemctl restart cassandra
```

#### Step 2 — Enable CDC per table

After the containers start, connect to Cassandra and run:

```cql
ALTER TABLE keyspace_name.orders   WITH cdc = true;
ALTER TABLE keyspace_name.payments WITH cdc = true;
```

Using the bundled container:

```bash
docker exec -it cassandra cqlsh
```

#### Step 3 — Create `debezium_user`

```cql
CREATE ROLE debezium_user WITH PASSWORD = 'your_strong_password' AND LOGIN = true;
GRANT SELECT ON KEYSPACE keyspace_name TO debezium_user;
```

#### Pulse system — configure `conf/debezium-cassandra.properties`

The `cassandra` and `debezium-cassandra` services are **already included** in `docker-compose.yml`. Before running `docker-compose up -d`, update `conf/debezium-cassandra.properties` with your keyspace name and password:

```properties
debezium.source.cassandra.keyspace=your_keyspace
debezium.source.cassandra.password=your_strong_password
```

The full properties file is at `conf/debezium-cassandra.properties`. The `cassandra.hosts` is pre-set to `10.5.0.55` (the cassandra container's IP on the spark-network).

#### URI to enter in Pulse

```
cassandra://debezium_user:your_strong_password@10.5.0.55:9042/keyspace_name
```

---

## Quick-Reference Checklist

### Remote database administrator

- [ ] Choose the database section above and follow its steps in order.
- [ ] Configure the database for CDC (WAL level, binlog, replica set, archive log, etc.).
- [ ] Create the `debezium_user` with the exact grants listed for your database.
- [ ] Run the verification commands to confirm CDC is active.
- [ ] Build the connection URI using the format shown and hand it to the Pulse user.

### Pulse onboarding user

- [ ] In the Pulse web UI, navigate to **Onboarding → Connect**.
- [ ] Select **"Database (Real-time CDC)"** as the ingestion type.
- [ ] Enter the connection URI provided by your database administrator.
- [ ] Enter the comma-separated list of table names to capture (e.g., `orders,payments,inventory`).
- [ ] Click **Connect**. Pulse validates the connection and begins the initial snapshot.
- [ ] Review and confirm the field mappings, then click **Confirm Mapping**.
- [ ] Pulse starts the continuous streaming pipeline. Data appears on the dashboard within minutes.

### Pulse system administrator (one-time)

- [ ] Clone the repository and copy `.env.example` to `.env`, then fill in all values.
- [ ] For Oracle: download `ojdbc8.jar` from Oracle and place it at `./jars/ojdbc8.jar` on the host running Docker. Ensure `./jars/ojdbc8.jar` is listed in `.gitignore` and is **not** committed to version control. The `debezium` service mounts it automatically — no extra steps needed inside the container.
- [ ] For Google Spanner: create or obtain a service account JSON key with the required permissions and save it as `./jars/gcp-credentials.json` on the host running Docker. Ensure this file is listed in `.gitignore` and is **not** committed to version control. The `debezium` service mounts it automatically — no extra steps needed inside the container.
- [ ] For Cassandra: edit `conf/debezium-cassandra.properties` and replace `CHANGE_ME` with your keyspace name and `debezium_user` password.
- [ ] Run `docker-compose up -d` to start all services.
- [ ] Verify Debezium is healthy: `curl http://localhost:8083/`
- [ ] Verify Kafka is healthy: `docker logs kafka | tail -20`
- [ ] For Cassandra: after containers are up, connect with `docker exec -it cassandra cqlsh` and enable CDC per table (see section 11).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| "Authentication failed" on connect | Wrong username or password in URI | Re-check the `debezium_user` password; re-create if needed |
| "Cannot connect" on connect | Wrong host, port, or firewall | Confirm the host and port are reachable from the Pulse server |
| Connector status is `FAILED` | Database not configured for CDC | Follow the setup steps above for your database type |
| No Kafka topics created | Connector not deployed | Check `curl localhost:8083/connectors` |
| Topics exist but are empty | No changes since the initial snapshot | Insert/update a row in the source database |
| `wal_level must be logical` | PostgreSQL `wal_level` not set | Edit `postgresql.conf` and restart |
| `REPLICATION permission denied` | User missing REPLICATION role | Re-run the `CREATE USER … WITH REPLICATION` statement |
| Snapshot is very slow | Large tables during initial load | Wait; the snapshot is a one-time full read before CDC starts |
| Oracle connector fails immediately | `ojdbc8.jar` is missing | Download from Oracle and place at `./jars/ojdbc8.jar`, then restart the `debezium` container |
| Spanner connector authentication error | GCP credentials file not mounted | Place service account JSON at `./jars/gcp-credentials.json` and restart the `debezium` container |
| Cassandra — no events received | `debezium-cassandra` container not running, or CDC not enabled on tables | Run `docker logs debezium-cassandra --tail 50`; enable CDC per table with `ALTER TABLE ... WITH cdc = true` (see section 11) |

For Debezium logs:

```bash
docker logs debezium --tail 100 -f
```

For Kafka Connect connector status:

```bash
curl http://localhost:8083/connectors/pulse-cdc-connector/status | python3 -m json.tool
```
