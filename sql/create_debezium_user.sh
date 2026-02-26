#!/bin/bash
# create_debezium_user.sh
#
# Create the debezium_user role for Change Data Capture (CDC).
#
# Debezium connects to the source database as this user to read the
# transaction log (WAL/binlog/oplog) and stream changes into Kafka.
#
# This script runs automatically when the PostgreSQL container starts
# for the first time (via /docker-entrypoint-initdb.d/).
#
# Set DEBEZIUM_PASSWORD in your .env file to override the default.
# See docs/DATABASE_SETUP_GUIDE.md for per-database instructions.

set -e

DEBEZIUM_PWD="${DEBEZIUM_PASSWORD:-debezium_changeme}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_NAME="${POSTGRES_DB:-postgres}"

psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "$DB_NAME" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = 'debezium_user'
        ) THEN
            CREATE USER debezium_user WITH
                REPLICATION
                LOGIN
                PASSWORD '$DEBEZIUM_PWD';
        END IF;
    END
    \$\$;

    -- Grant SELECT on all existing tables in the public schema
    GRANT USAGE ON SCHEMA public TO debezium_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

    -- Automatically grant SELECT on any tables created in the future
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO debezium_user;
EOSQL

