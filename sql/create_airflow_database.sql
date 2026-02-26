-- Create the Airflow metadata database.
-- This script runs automatically during PostgreSQL container initialisation.
-- The 'airflow' DB is separate from the main 'pulse' application DB so that
-- Airflow metadata does not interfere with application data.

-- SELECT 'CREATE DATABASE airflow'
-- WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- ./sql/create_airflow_db.sql
CREATE DATABASE airflow;