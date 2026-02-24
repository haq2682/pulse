-- Migration: add api_url column to onboarding table
-- Run this against existing databases that were created before this column was added.

ALTER TABLE onboarding
ADD COLUMN IF NOT EXISTS api_url TEXT NULL;

COMMENT ON COLUMN onboarding.api_url IS
    'User-provided external REST API endpoint (api ingestion mode only). '
    'Stored during start-mapping and used to trigger the api_streaming Airflow DAG '
    'when the user confirms their column mappings.';
