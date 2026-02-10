# CDC Setup Guide — Pulse Real-Time Database Ingestion

This guide covers setup for **both sides**:

- **User side (Part 1):** The database administrator configures their source database for CDC and provides a connection URI to Pulse.
- **Our side (Part 2):** The Pulse infrastructure (Debezium, Kafka, Spark) that consumes change events.

---

## How It Works

```
Source Database (user's)
      │
      │  Transaction log (WAL / binlog / oplog / etc.)
      ▼
┌──────────────┐        ┌───────┐        ┌────────────────┐        ┌───────┐
│   Debezium   │──CDC──▶│ Kafka │──────▶│ Spark Streaming │──────▶│ MinIO │
│  (container) │        │       │        │  (mapping)      │        │mapped/│
└──────────────┘        └───────┘        └────────────────┘        └───────┘
```

Debezium reads the database's native change stream (not polling queries) and produces one Kafka event per row-level change. Spark Streaming consumes those events, runs them through the mapping pipeline, and writes the results to MinIO.

---

## Supported Databases

| Database | URI Scheme | Default Port | CDC Mechanism |
|---|---|---|---|
| PostgreSQL | `postgresql://` | 5432 | Logical replication (WAL) |
| MySQL | `mysql://` | 3306 | Binary log (binlog) |
| MariaDB | `mariadb://` | 3306 | Binary log (binlog) |
| MongoDB | `mongodb://` | 27017 | Change streams (oplog) |
| SQL Server | `mssql://` | 1433 | SQL Server CDC |
| Oracle | `oracle://` | 1521 | LogMiner / XStream |
| IBM Db2 | `db2://` | 50000 | SQL Replication |
| Vitess | `vitess://` | 15991 | VStream gRPC |
| Google Spanner | `spanner://` | N/A | Change streams |
| Informix | `informix://` | 9088 | Change Data Capture API |
| Cassandra | `cassandra://` | 9042 | Commit log CDC |

---

# Part 1 — User Side: Database Setup

The user must configure their source database before providing the connection URI to Pulse. Each database has different requirements.

## URI Format

```
scheme://username:password@host:port/database
```

Examples:
```
postgresql://debezium_user:secret@10.0.0.50:5432/ecommerce
mysql://debezium_user:secret@db.example.com:3306/shop
mongodb://debezium_user:secret@mongo-rs1:27017/orders?replicaSet=rs0
mssql://debezium_user:secret@sqlserver.local:1433/sales
oracle://debezium_user:secret@oracle-host:1521/ORCL
db2://debezium_user:secret@db2-host:50000/ECOMDB
vitess://vtgate-host:15991/commerce
spanner://my-gcp-project/my-instance/my-database
informix://debezium_user:secret@informix-host:9088/stores
cassandra://debezium_user:secret@cassandra-host:9042/ecommerce
```

---

## 1. PostgreSQL

### 1.1 Configure `postgresql.conf`

```conf
# Required for logical replication
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after changing these settings:
```bash
sudo systemctl restart postgresql
```

### 1.2 Create Debezium User

```sql
-- Create user with replication privilege
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'your_password';

-- Grant read access on tables to capture
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

-- Allow future tables to be readable
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;
```

### 1.3 Verify

```sql
SHOW wal_level;           -- Must be 'logical'
SHOW max_replication_slots; -- Must be >= 1
```

### URI

```
postgresql://debezium_user:your_password@host:5432/dbname
```

---

## 2. MySQL

### 2.1 Configure `my.cnf` (or `my.ini`)

```conf
[mysqld]
server-id         = 1
log_bin           = mysql-bin
binlog_format     = ROW
binlog_row_image  = FULL
expire_logs_days  = 3

# Required for Debezium to read GTID-based positions
gtid_mode                = ON
enforce_gtid_consistency = ON
```

Restart MySQL:
```bash
sudo systemctl restart mysql
```

### 2.2 Create Debezium User

```sql
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;
```

### 2.3 Verify

```sql
SHOW VARIABLES LIKE 'binlog_format';    -- Must be ROW
SHOW VARIABLES LIKE 'binlog_row_image'; -- Must be FULL
SHOW VARIABLES LIKE 'log_bin';          -- Must be ON
```

### URI

```
mysql://debezium_user:your_password@host:3306/dbname
```

---

## 3. MariaDB

### 3.1 Configure `my.cnf`

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

### 3.2 Create Debezium User

```sql
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;
```

### URI

```
mariadb://debezium_user:your_password@host:3306/dbname
```

---

## 4. MongoDB

### 4.1 Requirements

- MongoDB must be running as a **replica set** (even a single-node replica set works).
- MongoDB version 4.0+ is required for change streams.

To initialize a single-node replica set:
```javascript
// Connect to mongosh
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
})
```

### 4.2 Create Debezium User

```javascript
use admin;
db.createUser({
  user: "debezium_user",
  pwd: "your_password",
  roles: [
    { role: "read", db: "your_database" },
    { role: "read", db: "local" },        // Required to read oplog
    { role: "read", db: "config" },        // Required for sharded clusters
    { role: "readAnyDatabase", db: "admin" }
  ]
});
```

### 4.3 Verify

```javascript
rs.status()  // Must show replica set members
```

### URI

```
mongodb://debezium_user:your_password@host:27017/dbname?replicaSet=rs0&authSource=admin
```

---

## 5. SQL Server

### 5.1 Enable CDC on the Database

```sql
-- Enable CDC on the database (requires sysadmin)
USE your_database;
EXEC sys.sp_cdc_enable_db;
```

### 5.2 Enable CDC on Each Table

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'orders',
  @role_name     = NULL;

-- Repeat for each table you want to capture
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'payments',
  @role_name     = NULL;
```

### 5.3 Ensure SQL Server Agent is Running

```sql
-- SQL Server Agent must be running for CDC to capture changes
EXEC xp_servicecontrol N'QUERYSTATE', N'SQLServerAGENT';
```

### 5.4 Create Debezium User

```sql
CREATE LOGIN debezium_user WITH PASSWORD = 'your_password';
USE your_database;
CREATE USER debezium_user FOR LOGIN debezium_user;
ALTER ROLE db_datareader ADD MEMBER debezium_user;

-- Grant CDC access
GRANT VIEW DATABASE STATE TO debezium_user;
```

### URI

```
mssql://debezium_user:your_password@host:1433/dbname
```

---

## 6. Oracle

### 6.1 Enable Archive Log Mode

```sql
-- Connect as SYSDBA
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

### 6.2 Enable Supplemental Logging

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### 6.3 Create Debezium User

```sql
-- For CDB (Container Database) / PDB architecture:
ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER c##debezium_user IDENTIFIED BY your_password
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;

GRANT CREATE SESSION TO c##debezium_user;
GRANT SELECT ON V_$DATABASE TO c##debezium_user;
GRANT SELECT ON V_$LOG TO c##debezium_user;
GRANT SELECT ON V_$LOGFILE TO c##debezium_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##debezium_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##debezium_user;
GRANT SELECT ON V_$TRANSACTION TO c##debezium_user;
GRANT LOGMINING TO c##debezium_user;
GRANT SELECT_CATALOG_ROLE TO c##debezium_user;
GRANT EXECUTE ON DBMS_LOGMNR TO c##debezium_user;

-- For non-CDB architecture, omit the c## prefix:
-- CREATE USER debezium_user IDENTIFIED BY your_password ...

-- Grant SELECT on tables to capture
GRANT SELECT ON schema_name.table_name TO c##debezium_user;
```

### 6.4 Enable Supplemental Logging on Tables

```sql
ALTER TABLE schema_name.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE schema_name.payments ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### URI

```
oracle://debezium_user:your_password@host:1521/service_name
```

---

## 7. IBM Db2

### 7.1 Enable CDC (ASN Capture)

Tables must be placed in capture mode by a DBA. This requires Db2's SQL Replication feature.

```sql
-- Enable capture for a table
CALL ASNCDC.ADDTABLE('SCHEMA_NAME', 'TABLE_NAME');

-- Start capture agent
CALL ASNCDC.REINIT();
```

### 7.2 Create Debezium User

```sql
CREATE USER debezium_user IDENTIFIED BY your_password;
GRANT CONNECT ON DATABASE TO debezium_user;
GRANT SELECT ON TABLE schema_name.orders TO debezium_user;
GRANT SELECT ON TABLE schema_name.payments TO debezium_user;
-- Grant on ASN tables
GRANT SELECT ON ASNCDC.IBMSNAP_REGISTER TO debezium_user;
GRANT SELECT ON ASNCDC.IBMSNAP_SIGNAL TO debezium_user;
```

### URI

```
db2://debezium_user:your_password@host:50000/dbname
```

---

## 8. Vitess

### 8.1 Requirements

- VTGate must be accessible via gRPC (default port 15991).
- VStream must be enabled on the Vitess cluster.
- No special user creation is needed beyond normal Vitess authentication.

### 8.2 Verify VStream

```bash
# Check VStream is accessible
vtctlclient VDiff -- --tablet_types MASTER keyspace.workflow
```

### URI

```
vitess://vtgate-host:15991/keyspace
```

---

## 9. Google Cloud Spanner

### 9.1 Create a Change Stream

```sql
CREATE CHANGE STREAM pulse_change_stream
  FOR orders, payments, inventory
  OPTIONS (
    retention_period = '7d',
    value_capture_type = 'NEW_AND_OLD_VALUES'
  );
```

### 9.2 Service Account Credentials

Create a GCP service account with the following roles:
- `roles/spanner.databaseReader`
- `roles/spanner.viewer`

Download the JSON key file and either:
- Set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` environment variable, OR
- Mount the key file into the Debezium container

### URI

```
spanner://project-id/instance-id/database-id
```

---

## 10. Informix

### 10.1 Enable CDC

```sql
-- Enable CDC on the database
EXECUTE FUNCTION task('cdc add database', 'your_database');

-- Enable CDC on each table
EXECUTE FUNCTION task('cdc add table', 'your_database:informix.orders');
EXECUTE FUNCTION task('cdc add table', 'your_database:informix.payments');
```

### 10.2 Create Debezium User

```sql
CREATE USER debezium_user WITH PASSWORD 'your_password';
GRANT SELECT ON orders TO debezium_user;
GRANT SELECT ON payments TO debezium_user;
```

### URI

```
informix://debezium_user:your_password@host:9088/dbname
```

---

## 11. Cassandra

### 11.1 Enable CDC in `cassandra.yaml`

```yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_total_space_in_mb: 4096
```

Restart Cassandra:
```bash
sudo systemctl restart cassandra
```

### 11.2 Enable CDC Per Table

```sql
ALTER TABLE keyspace_name.orders WITH cdc = true;
ALTER TABLE keyspace_name.payments WITH cdc = true;
ALTER TABLE keyspace_name.inventory WITH cdc = true;
```

### 11.3 Create Debezium User

```sql
CREATE ROLE debezium_user WITH PASSWORD = 'your_password' AND LOGIN = true;
GRANT SELECT ON KEYSPACE keyspace_name TO debezium_user;
```

### 11.4 Important Note

The Cassandra Debezium connector runs as a **standalone JVM agent** deployed on each Cassandra node (not inside Kafka Connect). For Kafka Connect-based deployment, use Debezium Server as a bridge. See the Debezium documentation for Cassandra-specific deployment.

### URI

```
cassandra://debezium_user:your_password@host:9042/keyspace
```

---

# Part 2 — Our Side: Pulse Infrastructure Setup

This section is for the Pulse development team to set up and maintain the CDC pipeline.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  docker-compose services                                        │
│                                                                 │
│  ┌──────────┐  ┌─────────┐  ┌──────────┐  ┌───────┐  ┌──────┐│
│  │Debezium  │  │  Kafka   │  │  Spark   │  │ MinIO │  │ ...  ││
│  │10.5.0.10 │  │10.5.0.7 │  │10.5.0.3  │  │10.5.0.4│  │      ││
│  │:8083     │  │:9092    │  │:7077     │  │:9000  │  │      ││
│  └──────────┘  └─────────┘  └──────────┘  └───────┘  └──────┘│
│       spark-network (10.5.0.0/16)                               │
└─────────────────────────────────────────────────────────────────┘
```

## 2.1 Docker Compose — Debezium Service

The Debezium container is already defined in `docker-compose.yml`:

```yaml
debezium:
  image: debezium/connect:2.5
  container_name: debezium
  depends_on:
    - kafka
    - zookeeper
    - postgresql
  ports:
    - "8083:8083"
  environment:
    BOOTSTRAP_SERVERS: 10.5.0.7:9092
    GROUP_ID: debezium-connect
    CONFIG_STORAGE_TOPIC: debezium_configs
    OFFSET_STORAGE_TOPIC: debezium_offsets
    STATUS_STORAGE_TOPIC: debezium_status
    CONFIG_STORAGE_REPLICATION_FACTOR: 1
    OFFSET_STORAGE_REPLICATION_FACTOR: 1
    STATUS_STORAGE_REPLICATION_FACTOR: 1
    KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    KEY_CONVERTER_SCHEMAS_ENABLE: "false"
    VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
    CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
    CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
  networks:
    spark-network:
      ipv4_address: 10.5.0.10
  restart: always
```

Start it:
```bash
docker-compose up -d debezium
```

### Installing Additional Connector Plugins

The default `debezium/connect:2.5` image ships with connectors for **PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, Db2**. For other databases, you need to add plugins.

To add connectors for Vitess, Spanner, Informix, MariaDB, or Cassandra, create a custom Dockerfile:

```dockerfile
FROM debezium/connect:2.5

# Example: add Vitess connector
RUN cd /kafka/connect && \
    curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-vitess/2.5.0.Final/debezium-connector-vitess-2.5.0.Final-plugin.tar.gz | tar xz

# Example: add Spanner connector
RUN cd /kafka/connect && \
    curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-spanner/2.5.0.Final/debezium-connector-spanner-2.5.0.Final-plugin.tar.gz | tar xz
```

Then update `docker-compose.yml` to build from it:
```yaml
debezium:
  build:
    context: .
    dockerfile: .docker/debezium/Dockerfile
  # ... rest stays the same
```

## 2.2 How the Code Works

When the user sets `mode = "db"` in `run_mapping.py`:

1. **`run_mapping.py`** calls `run_db_mode(CONFIG)`.
2. **`run_db_mode()`** imports `DebeziumConnectorManager` and calls `create_connector_config(db_uri, tables)`.
3. **`debezium_connector_manager.py`** parses the URI, auto-detects the database type, and builds the correct Debezium connector configuration.
4. The connector is deployed to Kafka Connect via REST API (`POST /connectors`).
5. **`spark_streaming.py`** consumes `ecom.*` Kafka topics. The `normalize_message_row()` function handles both canonical messages and Debezium's native format (which has `op`, `before`, `after`, `source` fields).
6. Normalized data flows through `map.py` and lands in MinIO `mapped/`.

### Key Files

| File | Purpose |
|---|---|
| `mapping/run_mapping.py` | Entry point — CONFIG has `db_uri` and `db_tables` |
| `mapping/streaming/ingestion/debezium_connector_manager.py` | URI parsing, connector config generation, REST API deployment |
| `mapping/streaming/spark_streaming.py` | Kafka consumer, Debezium format normalization |
| `mapping/streaming/canonical_message.py` | `from_debezium()` / `is_debezium_format()` helpers |
| `mapping/streaming/ingestion/db_ingest_service.py` | DEPRECATED — old polling-based approach |

## 2.3 Verifying a Deployment

### Check Kafka Connect is Running

```bash
curl http://localhost:8083/
# {"version":"3.6.0","commit":"...","kafka_cluster_id":"..."}
```

### List Deployed Connectors

```bash
curl http://localhost:8083/connectors
# ["pulse-cdc-connector"]
```

### Check Connector Status

```bash
curl http://localhost:8083/connectors/pulse-cdc-connector/status
```

A healthy response looks like:
```json
{
  "name": "pulse-cdc-connector",
  "connector": { "state": "RUNNING", "worker_id": "10.5.0.10:8083" },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "10.5.0.10:8083" }]
}
```

### Check Kafka Topics

```bash
docker exec kafka kafka-topics --bootstrap-server 10.5.0.7:9092 --list | grep ecom
```

### Read Raw CDC Events

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server 10.5.0.7:9092 \
  --topic ecom.public.orders \
  --from-beginning \
  --max-messages 5
```

## 2.4 Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Connector status `FAILED` | Database not configured for CDC | Follow Part 1 for the specific database |
| No topics created | Connector not deployed | Check `curl localhost:8083/connectors` |
| Topics exist but empty | No changes in source DB since snapshot | Insert/update a row in the source database |
| `REPLICATION` permission denied | User missing replication role | Re-run the `GRANT` statements from Part 1 |
| `wal_level` must be `logical` | PostgreSQL not configured | Edit `postgresql.conf` and restart |
| Snapshot stuck | Large tables during initial load | Wait for snapshot to complete; check connector task logs |
| Debezium container not starting | Kafka not ready | Ensure Kafka is healthy: `docker logs kafka` |

### Viewing Debezium Logs

```bash
docker logs debezium --tail 100 -f
```

## 2.5 Deleting a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/pulse-cdc-connector
```

Or via Python:
```python
from streaming.ingestion.debezium_connector_manager import DebeziumConnectorManager
manager = DebeziumConnectorManager()
manager.delete_connector("pulse-cdc-connector")
```

---

## Quick Start Checklist

### User (Database Admin)

1. [ ] Configure database for CDC (see Part 1 for your database type)
2. [ ] Create `debezium_user` with appropriate permissions
3. [ ] Verify CDC configuration (run the verify commands)
4. [ ] Provide connection URI to Pulse: `scheme://debezium_user:password@host:port/database`

### Pulse Team

1. [ ] Ensure `docker-compose up -d debezium kafka zookeeper` services are running
2. [ ] If using a non-standard database, install the connector plugin (see 2.1)
3. [ ] Set `CONFIG["mode"] = "db"` and `CONFIG["db_uri"]` in `run_mapping.py`
4. [ ] Set `CONFIG["db_tables"]` to the list of tables to capture
5. [ ] Run `python run_mapping.py`
6. [ ] Verify connector status: `curl localhost:8083/connectors/pulse-cdc-connector/status`
7. [ ] Check Kafka topics are receiving events
