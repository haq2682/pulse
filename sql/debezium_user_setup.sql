-- =============================================================================
-- Debezium CDC User Setup Scripts
-- =============================================================================
-- Run the section matching your database type on your source database BEFORE
-- entering the connection URI in the Pulse onboarding page.
--
-- The debezium_user account is used by Pulse to read your database's change
-- stream (WAL / binlog / oplog / CDC tables) without modifying any data.
--
-- After running the appropriate section below, enter this URI in Pulse:
--   postgresql://debezium_user:<password>@<host>:<port>/<database>
--   mysql://debezium_user:<password>@<host>:<port>/<database>
--   mariadb://debezium_user:<password>@<host>:<port>/<database>
--   mongodb://debezium_user:<password>@<host>:<port>/<database>?replicaSet=rs0&authSource=admin
--   mssql://debezium_user:<password>@<host>:<port>/<database>
--   oracle://debezium_user:<password>@<host>:<port>/<service_name>
--   vitess://debezium_user:<password>@<vtgate-host>:<port>/<keyspace>
--   cassandra://debezium_user:<password>@<host>:<port>/<keyspace>
-- =============================================================================


-- =============================================================================
-- 1. PostgreSQL
-- =============================================================================
-- Prerequisites: set wal_level = logical in postgresql.conf and restart.
--   wal_level = logical
--   max_replication_slots = 4
--   max_wal_senders = 4

CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'your_password';

GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium_user;


-- =============================================================================
-- 2. MySQL
-- =============================================================================
-- Prerequisites: set in my.cnf and restart MySQL:
--   server-id         = 1
--   log_bin           = mysql-bin
--   binlog_format     = ROW
--   binlog_row_image  = FULL
--   gtid_mode                = ON
--   enforce_gtid_consistency = ON

CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;


-- =============================================================================
-- 3. MariaDB
-- =============================================================================
-- Prerequisites: set in my.cnf and restart MariaDB:
--   server-id         = 1
--   log_bin           = mariadb-bin
--   binlog_format     = ROW
--   binlog_row_image  = FULL

CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;


-- =============================================================================
-- 4. MongoDB  (run in mongosh)
-- =============================================================================
-- Prerequisites: MongoDB must run as a replica set (even single-node).
--   rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "localhost:27017" }] })

-- use admin;
-- db.createUser({
--   user: "debezium_user",
--   pwd: "your_password",
--   roles: [
--     { role: "read",            db: "your_database" },
--     { role: "read",            db: "local"         },
--     { role: "read",            db: "config"        },
--     { role: "readAnyDatabase", db: "admin"         }
--   ]
-- });


-- =============================================================================
-- 5. SQL Server
-- =============================================================================
-- Step 1: Enable CDC on the database (requires sysadmin)
USE your_database;
EXEC sys.sp_cdc_enable_db;

-- Step 2: Enable CDC on each table you want to capture
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'orders',
  @role_name     = NULL;

-- Step 3: Create debezium_user login and grant permissions
CREATE LOGIN debezium_user WITH PASSWORD = 'your_password';
USE your_database;
CREATE USER debezium_user FOR LOGIN debezium_user;
ALTER ROLE db_datareader ADD MEMBER debezium_user;
GRANT VIEW DATABASE STATE TO debezium_user;


-- =============================================================================
-- 6. Oracle
-- =============================================================================
-- Prerequisites:
--   1. Enable Archive Log Mode (as SYSDBA):
--        SHUTDOWN IMMEDIATE;
--        STARTUP MOUNT;
--        ALTER DATABASE ARCHIVELOG;
--        ALTER DATABASE OPEN;
--   2. Enable supplemental logging:
--        ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
--        ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- For CDB (Container Database) architecture:
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

-- Grant SELECT on each table to capture (repeat for each table)
GRANT SELECT ON schema_name.orders TO c##debezium_user;
GRANT SELECT ON schema_name.payments TO c##debezium_user;

-- Enable supplemental logging on each captured table
ALTER TABLE schema_name.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE schema_name.payments ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- For non-CDB architecture, omit the c## prefix and CDB$ROOT step.


-- =============================================================================
-- 7. Vitess  (run against VTGate / underlying MySQL shards)
-- =============================================================================
-- Vitess uses MySQL protocol. Run on each underlying MySQL shard or via VTGate:
--   server-id         = 1            (set per shard in my.cnf)
--   log_bin           = mysql-bin
--   binlog_format     = ROW
--   binlog_row_image  = FULL

CREATE USER 'debezium_user'@'%' IDENTIFIED BY 'your_password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';
FLUSH PRIVILEGES;


-- =============================================================================
-- 8. Cassandra  (run via cqlsh)
-- =============================================================================
-- Prerequisites:
--   1. Enable CDC in cassandra.yaml:
--        cdc_enabled: true
--        cdc_raw_directory: /var/lib/cassandra/cdc_raw
--   2. Enable CDC on each table you want to capture:
--        ALTER TABLE your_keyspace.orders WITH cdc = true;
--        ALTER TABLE your_keyspace.payments WITH cdc = true;

CREATE ROLE debezium_user WITH PASSWORD = 'your_password' AND LOGIN = true;
GRANT SELECT ON ALL TABLES IN KEYSPACE your_keyspace TO debezium_user;
