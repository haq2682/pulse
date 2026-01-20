# Pulse Mapping Module - Unified Entry Point

## Quick Start

The mapping module provides a unified entry point (`run_mapping.py`) with 3 modes for data processing. Configuration is done by editing the `CONFIG` dictionary in the file.

### 1. Batch Mode
Load data from MinIO `bucket_name/ingested` folder, process through the mapping pipeline, and save results to `bucket_name/mapped` folder.

**Configuration:**
```python
CONFIG = {
    "mode": "batch",
    "bucket_name": "pulse-bucket-1",
}
```

**Run:**
```bash
python run_mapping.py
```

### 2. DB Mode
Ingest data from a database URI, process through the mapping pipeline, and save results to `bucket_name/mapped` folder.

**Configuration:**
```python
CONFIG = {
    "mode": "db",
    "bucket_name": "pulse-bucket-1",
    "db_uri": "postgresql://user:pass@host:5432/database",
    "db_poll_interval": 10,
}
```

**Run:**
```bash
python run_mapping.py
```

**Note:** Before using DB mode, ensure the database administrator has completed the prerequisites outlined in the [Database Administrator Prerequisites](#database-administrator-prerequisites) section below.

### 3. API Mode
Ingest data from an API endpoint, process through the mapping pipeline, and save results to `bucket_name/mapped` folder.

**Configuration:**
```python
CONFIG = {
    "mode": "api",
    "bucket_name": "pulse-bucket-1",
    "api_url": "http://localhost:5000/api/data",
    "api_poll_interval": 10,
}
```

**Run:**
```bash
python run_mapping.py
```

### Configuration Reference

Edit the `CONFIG` dictionary in `run_mapping.py`:

```python
CONFIG = {
    # Mode: "batch", "db", or "api"
    "mode": "batch",
    
    # Common settings
    "bucket_name": "pulse-bucket-1",  # MinIO bucket name
    
    # DB mode settings (only used when mode="db")
    "db_uri": "postgresql://user:pass@localhost:5432/ecommerce",
    "db_poll_interval": 10,  # Polling interval in seconds
    
    # API mode settings (only used when mode="api")
    "api_url": "http://localhost:5000/api/data",
    "api_poll_interval": 10,  # Polling interval in seconds
    
    # Optional: Kafka bootstrap servers (defaults to env var if None)
    "kafka_bootstrap": None,
}
```

**Note:** In production, these configuration values will be provided by the React frontend.

### Architecture

- **Batch Mode**: Direct processing using PySpark
  - MinIO ingested folder → PySpark mapping → MinIO mapped folder
  
- **DB Mode**: Streaming pipeline with Change Data Capture (CDC)
  - Database → Kafka (via db_ingest_service.py) → PySpark Streaming (spark_streaming.py) → MinIO mapped folder
  
- **API Mode**: Streaming pipeline with API polling
  - API Endpoint → Kafka (via api_ingest_service.py) → PySpark Streaming (spark_streaming.py) → MinIO mapped folder

---

# Database Streaming Documentation

## Overview

The mapping module provides database streaming capabilities that ingest data from external databases into Kafka topics using Debezium CDC (Change Data Capture). This document outlines the prerequisites that database administrators must complete before providing database URIs for streaming.

**Supported Databases:**
- PostgreSQL (with logical replication)
- MySQL (with binlog)
- MongoDB (with replica sets)
- Microsoft SQL Server (with CDC)
- Oracle Database (with LogMiner/XStream)
- IBM Db2 (with SQL Replication)
- Apache Cassandra (with CDC)
- Vitess (MySQL-compatible with VStream)
- Google Cloud Spanner (with Change Streams)

Each database has specific prerequisites and configuration requirements detailed in this documentation.

## Table of Contents

1. [Database Administrator Prerequisites](#database-administrator-prerequisites)
2. [Credential Management](#credential-management)
3. [Database-Specific Setup](#database-specific-setup)
   - [PostgreSQL](#postgresql)
   - [MySQL](#mysql)
   - [MongoDB](#mongodb)
   - [Microsoft SQL Server](#microsoft-sql-server)
   - [Oracle Database](#oracle-database)
   - [IBM Db2](#ibm-db2)
   - [Apache Cassandra](#apache-cassandra)
   - [Vitess](#vitess-mysql-compatible)
   - [Google Cloud Spanner](#spanner-google-cloud)
4. [Security Best Practices](#security-best-practices)
5. [Connection URI Format](#connection-uri-format)
6. [Usage in Code](#usage-in-code)
7. [Environment Variables](#environment-variables)
8. [Troubleshooting](#troubleshooting)
9. [Additional Resources](#additional-resources)

---

## Database Administrator Prerequisites

Before providing a database URI for streaming, the database administrator must:

1. **Create a dedicated streaming user** with appropriate permissions
2. **Grant specific roles** required for Change Data Capture (CDC)
3. **Configure database settings** to enable logical replication (if applicable)
4. **Provide secure credentials** for the streaming user

### Why These Steps Are Necessary

Debezium and our streaming infrastructure require:
- **Read access** to all tables that need to be streamed
- **Replication permissions** to capture database changes in real-time
- **Schema access** to discover tables and their structures
- **Connection slots** for maintaining persistent connections

**Important:** Regular application users typically do not have these permissions. A dedicated user with elevated privileges is required.

---

## Credential Management

### Current Implementation

The current implementation manages database credentials in the following way:

1. **Database URI Storage**: The database URI (containing credentials) is provided by the frontend/API and passed directly to the ingestion service
2. **Environment Variables**: Infrastructure database credentials are stored in `.env` file (see `.env.example`)
3. **Connection Security**: Credentials are embedded in the connection URI format

### Credential Components

A streaming database user requires:
- **Username**: Dedicated user account (e.g., `debezium_user`, `cdc_reader`)
- **Password**: Strong password following your organization's security policy
- **Host/Port**: Network-accessible database endpoint
- **Database Name**: Target database to stream from
- **SSL/TLS**: Optional but recommended for production environments

### How Credentials Flow Through the System

```
Frontend/API Input (DB URI)
    ↓
db_ingest_service.py (ingest_from_uri function)
    ↓
db_connector.py (get_connection function)
    ↓
Database Driver (psycopg2, mysql.connector, pymongo, pyodbc)
    ↓
Remote Database
```

**Security Note:** Credentials in the URI are used for the connection session only and are not persisted in logs or files by default.

---

## Database-Specific Setup

### PostgreSQL

PostgreSQL requires the most configuration for Debezium CDC because of its logical replication requirements.

#### 1. Prerequisites

The DBA must configure the database for logical replication:

```sql
-- Edit postgresql.conf
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

After changing `postgresql.conf`, restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

#### 2. Create Streaming User

```sql
-- Create the user
CREATE USER debezium_user WITH PASSWORD 'secure_password_here';

-- Grant connection permissions
GRANT CONNECT ON DATABASE your_database TO debezium_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO debezium_user;

-- Grant SELECT on all tables in the schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

-- Grant SELECT on future tables (optional but recommended)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;

-- Grant replication permissions (required for Debezium CDC)
ALTER USER debezium_user WITH REPLICATION;
```

#### 3. Create Publication (for Logical Replication)

```sql
-- Create a publication for all tables
CREATE PUBLICATION debezium_publication FOR ALL TABLES;

-- OR create for specific tables only
CREATE PUBLICATION debezium_publication FOR TABLE customers, orders, products;
```

#### 4. Verify Setup

```sql
-- Check if user has replication role
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'debezium_user';

-- Check publications
SELECT * FROM pg_publication;

-- Check replication slots (after connection)
SELECT * FROM pg_replication_slots;
```

#### 5. Connection URI Format

```
postgresql://debezium_user:secure_password_here@hostname:5432/your_database
```

**Reference:** [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)

---

### MySQL

MySQL requires binlog to be enabled for CDC.

#### 1. Prerequisites

The DBA must enable binary logging:

```sql
-- Edit my.cnf or my.ini
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
expire_logs_days = 10
```

Restart MySQL after configuration changes:
```bash
sudo systemctl restart mysql
```

#### 2. Create Streaming User

```sql
-- Create user
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'secure_password_here';

-- Grant necessary permissions
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT 
ON *.* TO 'debezium_user'@'%';

-- Grant SELECT on specific database
GRANT SELECT ON your_database.* TO 'debezium_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;
```

#### 3. Verify Setup

```sql
-- Check user privileges
SHOW GRANTS FOR 'debezium_user'@'%';

-- Verify binlog is enabled
SHOW VARIABLES LIKE 'log_bin';

-- Check binlog format
SHOW VARIABLES LIKE 'binlog_format';
```

#### 4. Connection URI Format

```
mysql://debezium_user:secure_password_here@hostname:3306/your_database
```

**Reference:** [Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)

---

### MongoDB

MongoDB requires replica set configuration for Change Streams.

#### 1. Prerequisites

MongoDB must be running as a replica set (even single-node deployments):

```javascript
// Initialize replica set (if not already done)
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
})
```

#### 2. Create Streaming User

```javascript
// Connect to MongoDB
use admin

// Create user with read and change stream permissions
db.createUser({
  user: "debezium_user",
  pwd: "secure_password_here",
  roles: [
    { role: "read", db: "your_database" },
    { role: "readAnyDatabase", db: "admin" }
  ]
})

// For additional databases, grant read access
db.grantRolesToUser("debezium_user", [
  { role: "read", db: "another_database" }
])
```

#### 3. Verify Setup

```javascript
// Check replica set status
rs.status()

// Check user permissions
use admin
db.getUser("debezium_user")

// Test authentication
db.auth("debezium_user", "secure_password_here")
```

#### 4. Connection URI Format

```
mongodb://debezium_user:secure_password_here@hostname:27017/your_database?replicaSet=rs0
```

**Reference:** [Debezium MongoDB Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mongodb.html)

---

### Microsoft SQL Server

SQL Server requires SQL Server Agent to be running and database in FULL recovery mode.

#### 1. Prerequisites

The DBA must enable CDC on the database:

```sql
-- Enable CDC on database
USE your_database;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on specific tables
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'customers',
  @role_name = NULL;

-- Check CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'your_database';
```

#### 2. Create Streaming User

```sql
-- Create login
CREATE LOGIN debezium_user WITH PASSWORD = 'secure_password_here';

-- Create user in database
USE your_database;
CREATE USER debezium_user FOR LOGIN debezium_user;

-- Grant necessary permissions
GRANT SELECT ON SCHEMA::dbo TO debezium_user;
EXEC sp_addrolemember 'db_datareader', 'debezium_user';

-- Grant CDC permissions
EXEC sp_addrolemember 'db_owner', 'debezium_user'; -- OR specific CDC permissions
```

#### 3. Verify Setup

```sql
-- Check if CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'your_database';

-- Check user permissions
SELECT * FROM sys.database_permissions WHERE grantee_principal_id = USER_ID('debezium_user');
```

#### 4. Connection URI Format

```
mssql://debezium_user:secure_password_here@hostname:1433/your_database
```

**Reference:** [Debezium SQL Server Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)

---

### Oracle Database

Oracle requires archive log mode and supplemental logging for CDC.

#### 1. Prerequisites

The DBA must enable archive log mode and supplemental logging:

```sql
-- Check if archive log mode is enabled
SELECT log_mode FROM v$database;

-- Enable archive log mode (requires database restart)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- Enable supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable supplemental logging for specific tables
ALTER TABLE schema.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

#### 2. Create Streaming User

```sql
-- Create user
CREATE USER debezium_user IDENTIFIED BY secure_password_here
  DEFAULT TABLESPACE users
  TEMPORARY TABLESPACE temp
  QUOTA UNLIMITED ON users;

-- Grant necessary privileges
GRANT CREATE SESSION TO debezium_user;
GRANT SELECT ANY TABLE TO debezium_user;
GRANT SELECT_CATALOG_ROLE TO debezium_user;

-- Grant LogMiner privileges (required for CDC)
GRANT SELECT ON v_$database TO debezium_user;
GRANT SELECT ON v_$logfile TO debezium_user;
GRANT SELECT ON v_$log TO debezium_user;
GRANT SELECT ON v_$archived_log TO debezium_user;
GRANT SELECT ON v_$archive_dest_status TO debezium_user;
GRANT SELECT ON v_$transaction TO debezium_user;

-- Grant access to specific schema
GRANT SELECT ON schema.* TO debezium_user;

-- For Oracle 12c+ with multitenant (CDB/PDB)
ALTER SESSION SET CONTAINER = your_pdb;
-- Then grant privileges as above
```

#### 3. Configure LogMiner

```sql
-- Check LogMiner configuration
SELECT supplemental_log_data_min, supplemental_log_data_pk, 
       supplemental_log_data_ui FROM v$database;

-- Enable minimum supplemental logging (if not already enabled)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Verify redo log configuration
SELECT group#, bytes/1024/1024 as size_mb, members, status 
FROM v$log 
ORDER BY group#;
```

#### 4. Verify Setup

```sql
-- Check archive log mode
SELECT log_mode FROM v$database;

-- Check supplemental logging
SELECT supplemental_log_data_min FROM v$database;

-- Check user privileges
SELECT * FROM dba_sys_privs WHERE grantee = 'DEBEZIUM_USER';
SELECT * FROM dba_tab_privs WHERE grantee = 'DEBEZIUM_USER';
```

#### 5. Connection URI Format

```
oracle://debezium_user:secure_password_here@hostname:1521/SERVICE_NAME

-- For RAC (Real Application Clusters)
oracle://debezium_user:secure_password_here@//host1:1521,host2:1521/SERVICE_NAME

-- Using SID instead of Service Name
oracle://debezium_user:secure_password_here@hostname:1521:SID
```

**Important Notes:**
- Oracle connector requires Oracle LogMiner or Oracle XStream API
- Supplemental logging increases redo log generation
- Monitor disk space for archive logs
- Consider archive log retention policies

**Reference:** [Debezium Oracle Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/oracle.html)

---

### IBM Db2

IBM Db2 requires SQL replication to be enabled for CDC.

#### 1. Prerequisites

The DBA must enable SQL replication:

```sql
-- Enable database for SQL replication
UPDATE DATABASE CONFIGURATION FOR your_database USING logarchmeth1 LOGRETAIN;

-- Create ASNCDC schema (if not exists)
-- This schema is required for Db2 CDC

-- Activate database
ACTIVATE DATABASE your_database;

-- Start ASN Capture
-- This requires Db2 Replication tools to be installed
```

#### 2. Create Streaming User

```sql
-- Connect as SYSADM or DBADM
CONNECT TO your_database;

-- Create user (on Linux/Unix)
-- Note: User must exist at OS level first

-- Grant database access
GRANT CONNECT ON DATABASE TO USER debezium_user;

-- Grant schema access
GRANT USAGE ON SCHEMA schema_name TO USER debezium_user;

-- Grant table privileges
GRANT SELECT ON TABLE schema_name.table_name TO USER debezium_user;

-- Grant access to ASNCDC schema (required for CDC)
GRANT USAGE ON SCHEMA ASNCDC TO USER debezium_user;
GRANT SELECT ON SCHEMA ASNCDC TO USER debezium_user;

-- Grant access to capture control tables
GRANT SELECT ON ASNCDC.IBMSNAP_REGISTER TO USER debezium_user;
GRANT SELECT ON ASNCDC.IBMSNAP_PRUNE_SET TO USER debezium_user;
```

#### 3. Configure Tables for Capture

```sql
-- Register tables for capture
-- This must be done for each table you want to stream
CALL ASNCDC.ADDTABLE(
  'SCHEMA_NAME',
  'TABLE_NAME'
);

-- Verify registered tables
SELECT * FROM ASNCDC.IBMSNAP_REGISTER 
WHERE SOURCE_OWNER = 'SCHEMA_NAME';
```

#### 4. Start ASN Capture Service

```bash
# Start the Capture program (requires Db2 Replication installation)
# Linux/Unix:
asnccmd capture_server=your_server capture_schema=ASNCDC start

# Or use Db2 command:
db2 "CALL ASNCDC.ASNCCMD('capture_server=your_server','capture_schema=ASNCDC','start')"
```

#### 5. Verify Setup

```sql
-- Check if logging is enabled
SELECT LOGARCHMETH1 FROM SYSIBMADM.DBCFG WHERE NAME = 'logarchmeth1';

-- Check ASN Capture status
SELECT * FROM ASNCDC.IBMSNAP_CAPTRACE ORDER BY TRACE_TIME DESC FETCH FIRST 10 ROWS ONLY;

-- Verify registered tables
SELECT * FROM ASNCDC.IBMSNAP_REGISTER;
```

#### 6. Connection URI Format

```
db2://debezium_user:secure_password_here@hostname:50000/your_database

-- For SSL connection
db2://debezium_user:secure_password_here@hostname:50001/your_database:sslConnection=true;
```

**Important Notes:**
- Db2 CDC requires IBM Db2 Replication tools
- ASN Capture must be running continuously
- Monitor log space and archive retention
- Consider performance impact of logging

**Reference:** [Debezium Db2 Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/db2.html)

---

### Apache Cassandra

Cassandra uses commit log and CDC (Change Data Capture) directory for change tracking.

#### 1. Prerequisites

The DBA must enable CDC in cassandra.yaml:

```yaml
# Edit cassandra.yaml
cdc_enabled: true
cdc_raw_directory: /var/lib/cassandra/cdc_raw
cdc_free_space_check_interval_ms: 250

# Commit log settings (ensure adequate space)
commitlog_total_space_in_mb: 8192
```

Restart Cassandra after configuration changes:
```bash
sudo systemctl restart cassandra
```

#### 2. Create Streaming User

```sql
-- Connect to Cassandra (cqlsh)
CREATE ROLE IF NOT EXISTS debezium_user WITH PASSWORD = 'secure_password_here' 
  AND LOGIN = true;

-- Grant keyspace access
GRANT SELECT ON KEYSPACE your_keyspace TO debezium_user;

-- Grant access to system tables
GRANT SELECT ON KEYSPACE system TO debezium_user;
GRANT SELECT ON KEYSPACE system_schema TO debezium_user;

-- For specific tables only
GRANT SELECT ON your_keyspace.table_name TO debezium_user;
```

#### 3. Enable CDC on Tables

```sql
-- Enable CDC on specific tables
ALTER TABLE your_keyspace.table_name WITH cdc = true;

-- Verify CDC is enabled
SELECT keyspace_name, table_name, cdc 
FROM system_schema.tables 
WHERE keyspace_name = 'your_keyspace' AND cdc = true;
```

#### 4. Configure CDC Directory

```bash
# Ensure CDC directory exists and has proper permissions
sudo mkdir -p /var/lib/cassandra/cdc_raw
sudo chown cassandra:cassandra /var/lib/cassandra/cdc_raw
sudo chmod 755 /var/lib/cassandra/cdc_raw

# Monitor CDC directory size
du -sh /var/lib/cassandra/cdc_raw
```

#### 5. Verify Setup

```sql
-- Check CDC enabled tables
SELECT keyspace_name, table_name, cdc 
FROM system_schema.tables 
WHERE cdc = true;

-- Check user permissions
LIST ALL PERMISSIONS OF debezium_user;
```

#### 6. Connection Format

For Cassandra, Debezium uses a different approach (DSE CDC or Cassandra CDC Agent):

```
# Cassandra connector configuration (typically in connector config)
cassandra.hosts=hostname:9042
cassandra.username=debezium_user
cassandra.password=secure_password_here
cassandra.keyspace=your_keyspace
```

**Important Notes:**
- Cassandra CDC requires adequate disk space for commit logs
- CDC files must be cleaned up after processing
- Monitor CDC directory to prevent disk exhaustion
- Consider using DataStax Enterprise for enhanced CDC features
- Standard Debezium Cassandra connector has limitations; consider alternatives

**Reference:** [Debezium Cassandra Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/cassandra.html)

---

### Vitess (MySQL-compatible)

Vitess uses VStream API for change streaming, providing MySQL compatibility with CDC capabilities.

#### 1. Prerequisites

Vitess must be configured with VReplication enabled:

```yaml
# Vitess configuration
# VStream must be enabled in vttablet
--enable_vstream=true
--vstream_packet_size=250000
```

#### 2. Create Streaming User

```sql
-- Connect to Vitess (uses MySQL protocol)
-- Create user with appropriate privileges
CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'secure_password_here';

-- Grant privileges for CDC
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';

-- Grant keyspace-specific access
GRANT SELECT ON your_keyspace.* TO 'debezium_user'@'%';

FLUSH PRIVILEGES;
```

#### 3. Configure VStream

VStream is automatically available once enabled. Verify configuration:

```bash
# Check vttablet configuration
vtctldclient GetTablets --keyspace your_keyspace

# Verify VStream is available
vtctldclient VStream --keyspace your_keyspace
```

#### 4. Verify Setup

```sql
-- Verify user privileges
SHOW GRANTS FOR 'debezium_user'@'%';

-- Check if user can access tables
USE your_keyspace;
SHOW TABLES;
```

#### 5. Connection URI Format

```
# Vitess connection (MySQL protocol)
mysql://debezium_user:secure_password_here@vtgate-host:15306/your_keyspace

# Or using Vitess-specific parameters
vitess://debezium_user:secure_password_here@vtgate-host:15999/your_keyspace
```

**Important Notes:**
- Vitess provides horizontal sharding for MySQL
- VStream API provides efficient change streaming
- Compatible with MySQL Debezium connector
- Consider shard topology when setting up CDC

**Reference:** [Debezium Vitess Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/vitess.html)

---

### Spanner (Google Cloud)

Google Cloud Spanner uses change streams for CDC capabilities.

#### 1. Prerequisites

The DBA must create change streams in Spanner:

```sql
-- Create a change stream for specific tables
CREATE CHANGE STREAM change_stream_name
  FOR customers, orders, products;

-- Or create for all tables in database
CREATE CHANGE STREAM change_stream_all
  FOR ALL;
```

#### 2. Create Streaming User (Service Account)

For Google Cloud Spanner, use IAM service accounts:

```bash
# Create service account
gcloud iam service-accounts create debezium-reader \
  --display-name="Debezium CDC Reader"

# Grant necessary roles
gcloud spanner databases add-iam-policy-binding your-database \
  --instance=your-instance \
  --member="serviceAccount:debezium-reader@your-project.iam.gserviceaccount.com" \
  --role="roles/spanner.databaseReader"

# Additional permission for change streams
gcloud spanner databases add-iam-policy-binding your-database \
  --instance=your-instance \
  --member="serviceAccount:debezium-reader@your-project.iam.gserviceaccount.com" \
  --role="roles/spanner.changeStreamReader"

# Create and download service account key
gcloud iam service-accounts keys create debezium-key.json \
  --iam-account=debezium-reader@your-project.iam.gserviceaccount.com
```

#### 3. Configure Change Stream Retention

```sql
-- Set retention period for change stream (1-7 days)
ALTER CHANGE STREAM change_stream_name
  SET OPTIONS (retention_period = '7d');

-- Verify change stream configuration
SELECT * FROM INFORMATION_SCHEMA.CHANGE_STREAMS;
```

#### 4. Verify Setup

```sql
-- Check change streams
SELECT 
  change_stream_name,
  table_name,
  all_columns,
  start_timestamp
FROM INFORMATION_SCHEMA.CHANGE_STREAMS;

-- Test query access
SELECT * FROM customers LIMIT 1;
```

#### 5. Connection Configuration

For Spanner, use connector configuration with service account:

```json
{
  "connector.class": "io.debezium.connector.spanner.SpannerConnector",
  "gcp.spanner.project.id": "your-project-id",
  "gcp.spanner.instance.id": "your-instance-id",
  "gcp.spanner.database.id": "your-database-id",
  "gcp.spanner.credentials.json": "/path/to/debezium-key.json",
  "gcp.spanner.change.stream": "change_stream_name"
}
```

**Important Notes:**
- Spanner change streams have retention limits (1-7 days)
- Service account keys should be stored securely
- Consider costs for change stream storage
- Change streams impact database performance

**Reference:** [Debezium Spanner Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/spanner.html)

---

## Security Best Practices

### 1. Credential Security

- ✅ **Use strong passwords**: Minimum 16 characters with mixed case, numbers, and symbols
- ✅ **Rotate credentials regularly**: Change passwords every 90 days
- ✅ **Use read-only permissions**: Grant only SELECT and replication permissions
- ✅ **Network security**: Use SSL/TLS for database connections
- ✅ **IP whitelisting**: Restrict database access to known IP addresses

### 2. Credential Storage

- ✅ **Never commit credentials**: Add `.env` to `.gitignore`
- ✅ **Use environment variables**: Store credentials in `.env` file (see `.env.example`)
- ✅ **Use secrets management**: Consider HashiCorp Vault, AWS Secrets Manager, or Azure Key Vault for production
- ✅ **Encrypt at rest**: Ensure credentials are encrypted when stored

### 3. Access Control

- ✅ **Dedicated user per environment**: Use different users for dev, staging, and production
- ✅ **Least privilege principle**: Grant only necessary permissions
- ✅ **Audit logging**: Enable database audit logs to track user activity
- ✅ **Monitor connections**: Set up alerts for unusual connection patterns

### 4. Database Configuration

- ✅ **Connection pooling**: Configure appropriate connection limits
- ✅ **Timeout settings**: Set reasonable connection and query timeouts
- ✅ **SSL/TLS enforcement**: Require encrypted connections in production
- ✅ **Backup before enabling CDC**: Ensure you have recent backups

---

## Connection URI Format

### General Format

```
<protocol>://<username>:<password>@<host>:<port>/<database>[?options]
```

### Protocol Mapping

| Database | Protocol | Default Port | Notes |
|----------|----------|--------------|-------|
| PostgreSQL | `postgresql://` or `postgres://` | 5432 | Supports logical replication |
| MySQL | `mysql://` | 3306 | Requires binlog enabled |
| MongoDB | `mongodb://` or `mongodb+srv://` | 27017 | Requires replica set |
| SQL Server | `mssql://` or `sqlserver://` | 1433 | Requires CDC enabled |
| Oracle | `oracle://` | 1521 | Requires LogMiner or XStream |
| IBM Db2 | `db2://` | 50000 | Requires ASN Capture |
| Cassandra | N/A | 9042 | Uses connector config, not URI |
| Vitess | `mysql://` or `vitess://` | 15306/15999 | MySQL-compatible with VStream |
| Spanner | N/A | N/A | Uses GCP service account config |

**Note:** Some databases (Cassandra, Spanner) use connector configuration instead of traditional connection URIs.

### Connection String Examples

#### PostgreSQL
```
postgresql://debezium_user:password@10.5.0.5:5432/ecommerce
```

#### PostgreSQL with SSL
```
postgresql://debezium_user:password@prod-db.example.com:5432/ecommerce?sslmode=require
```

#### MySQL
```
mysql://debezium_user:password@10.5.0.11:3306/ecommerce
```

#### MongoDB Replica Set
```
mongodb://debezium_user:password@10.5.0.12:27017/ecommerce?replicaSet=rs0&authSource=admin
```

#### MongoDB Atlas (Cloud)
```
mongodb+srv://debezium_user:password@cluster0.abc123.mongodb.net/ecommerce
```

#### SQL Server
```
mssql://debezium_user:password@10.5.0.13:1433/ecommerce
```

#### Oracle
```
oracle://debezium_user:password@prod-oracle.example.com:1521/ORCL
```

#### Oracle with Service Name
```
oracle://debezium_user:password@prod-oracle.example.com:1521/SERVICE_NAME
```

#### IBM Db2
```
db2://debezium_user:password@db2-host.example.com:50000/your_database
```

#### Db2 with SSL
```
db2://debezium_user:password@db2-host.example.com:50001/your_database:sslConnection=true;
```

#### Vitess (MySQL-compatible)
```
mysql://debezium_user:password@vtgate-host:15306/commerce
```

#### Cassandra (Configuration-based)
```json
{
  "cassandra.hosts": "cassandra-host:9042",
  "cassandra.username": "debezium_user",
  "cassandra.password": "password",
  "cassandra.keyspace": "ecommerce"
}
```

#### Google Cloud Spanner (Configuration-based)
```json
{
  "gcp.spanner.project.id": "your-project",
  "gcp.spanner.instance.id": "your-instance",
  "gcp.spanner.database.id": "ecommerce",
  "gcp.spanner.credentials.json": "/path/to/key.json",
  "gcp.spanner.change.stream": "ecommerce_changes"
}
```

---

## Usage in Code

### Using the Database Ingestion Service

```python
from mapping.streaming.ingestion.db_ingest_service import ingest_from_uri

# Database URI (provided by DBA after setup)
db_uri = "postgresql://debezium_user:secure_password@host:5432/database"

# Kafka configuration
kafka_bootstrap = "10.5.0.7:9092"
poll_interval = 10  # seconds

# Start ingestion
ingest_from_uri(
    db_uri=db_uri,
    poll_interval=poll_interval,
    kafka_bootstrap=kafka_bootstrap
)
```

### Connection Details

The system will:
1. Auto-detect database type from URI
2. Connect using provided credentials
3. Discover all tables in the database
4. Map tables to canonical schema
5. Stream changes to Kafka topics

---

## Environment Variables

Update your `.env` file with streaming database credentials:

```bash
# Streaming Database Configuration (for external data sources)
STREAMING_DB_URI="postgresql://debezium_user:password@external-host:5432/external_db"

# Kafka Configuration
KAFKA_BOOTSTRAP="10.5.0.7:9092"
KAFKA_BOOTSTRAP_EXTERNAL="localhost:9092"

# Internal PostgreSQL (for canonical data storage)
POSTGRES_USER="your_postgres_user"
POSTGRES_PASSWORD="your_postgres_password"
POSTGRES_DATABASE_NAME="your_database_name"
POSTGRES_SERVER="10.5.0.5"
```

---

## Troubleshooting

### PostgreSQL

**Problem:** `permission denied for table`  
**Solution:** Grant SELECT permissions: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;`

**Problem:** `must be superuser or replication role`  
**Solution:** Grant replication: `ALTER USER debezium_user WITH REPLICATION;`

**Problem:** `could not create replication slot`  
**Solution:** Increase `max_replication_slots` in `postgresql.conf`

### MySQL

**Problem:** `Access denied; you need the REPLICATION SLAVE privilege`  
**Solution:** `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';`

**Problem:** `binary log is not enabled`  
**Solution:** Enable binlog in `my.cnf` and restart MySQL

### MongoDB

**Problem:** `not authorized on admin to execute command`  
**Solution:** Grant `readAnyDatabase` role on admin database

**Problem:** `The $changeStream stage is only supported on replica sets`  
**Solution:** Initialize replica set with `rs.initiate()`

### SQL Server

**Problem:** `CDC is not enabled for database`  
**Solution:** Enable CDC: `EXEC sys.sp_cdc_enable_db;`

**Problem:** `SQL Server Agent is not running`  
**Solution:** Start SQL Server Agent service (required for CDC)

### Oracle

**Problem:** `ORA-01031: insufficient privileges`  
**Solution:** Grant LogMiner privileges and SELECT on v$database views

**Problem:** `Database is not in ARCHIVELOG mode`  
**Solution:** Enable archive log mode and restart database

**Problem:** `ORA-01327: failed to exclusively lock system dictionary`  
**Solution:** Ensure supplemental logging is enabled: `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;`

### IBM Db2

**Problem:** `ASN Capture is not running`  
**Solution:** Start ASN Capture: `asnccmd capture_server=your_server capture_schema=ASNCDC start`

**Problem:** `Table not registered for capture`  
**Solution:** Register table: `CALL ASNCDC.ADDTABLE('SCHEMA_NAME', 'TABLE_NAME');`

**Problem:** `ASNCDC schema does not exist`  
**Solution:** Install Db2 Replication tools and create ASNCDC schema

### Cassandra

**Problem:** `CDC is not enabled`  
**Solution:** Set `cdc_enabled: true` in cassandra.yaml and restart

**Problem:** `CDC directory is full`  
**Solution:** Clean up processed CDC files and monitor disk space

**Problem:** `Table does not have CDC enabled`  
**Solution:** Enable CDC on table: `ALTER TABLE keyspace.table WITH cdc = true;`

### Vitess

**Problem:** `VStream not available`  
**Solution:** Enable VStream in vttablet: `--enable_vstream=true`

**Problem:** `Cannot connect to vtgate`  
**Solution:** Verify vtgate is running and accessible on port 15306

### Google Cloud Spanner

**Problem:** `Change stream does not exist`  
**Solution:** Create change stream: `CREATE CHANGE STREAM stream_name FOR ALL;`

**Problem:** `Permission denied on change stream`  
**Solution:** Grant role: `roles/spanner.changeStreamReader` to service account

**Problem:** `Service account key invalid`  
**Solution:** Regenerate service account key and update configuration

### General

**Problem:** `Connection refused`  
**Solution:** Check firewall rules, ensure database is accessible from application host

**Problem:** `Authentication failed`  
**Solution:** Verify username, password, and ensure user has been created

**Problem:** `SSL/TLS handshake failed`  
**Solution:** Verify SSL certificates and connection string SSL parameters

**Problem:** `Too many connections`  
**Solution:** Increase max_connections or close idle connections

---

## Additional Resources

### Official Debezium Documentation
- [Debezium Documentation](https://debezium.io/documentation/)
- [Debezium Connector List](https://debezium.io/documentation/reference/stable/connectors/index.html)

### Database-Specific Documentation

#### Core Databases
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [MySQL Binary Log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
- [MongoDB Change Streams](https://docs.mongodb.com/manual/changeStreams/)
- [SQL Server CDC](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

#### Extended Databases
- [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
- [IBM Db2 SQL Replication](https://www.ibm.com/docs/en/db2/11.5?topic=replication-sql)
- [Apache Cassandra CDC](https://cassandra.apache.org/doc/latest/cassandra/operating/cdc.html)
- [Vitess VStream](https://vitess.io/docs/concepts/vstream/)
- [Google Cloud Spanner Change Streams](https://cloud.google.com/spanner/docs/change-streams)

### Security and Best Practices
- [Database Security Best Practices](https://owasp.org/www-project-database-security/)
- [Secrets Management Guide](https://www.hashicorp.com/resources/what-is-secrets-management)
- [SSL/TLS Configuration](https://www.ssl.com/guide/ssl-best-practices/)

---

## Support

For questions or issues related to database streaming setup, please:
1. Review this documentation thoroughly
2. Check the official Debezium documentation for your database type
3. Consult with your database administrator
4. Open an issue in the repository with detailed error messages and configuration details
