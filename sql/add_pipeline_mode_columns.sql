-- Add columns for pipeline mode tracking (batch vs streaming)
-- Run this migration to support streaming pipeline integration

-- Add pipeline_mode column (batch, db, api)
ALTER TABLE pipeline_status 
ADD COLUMN IF NOT EXISTS pipeline_mode VARCHAR(20) DEFAULT 'batch';

-- Add pipeline_type column (batch or streaming)
ALTER TABLE pipeline_status 
ADD COLUMN IF NOT EXISTS pipeline_type VARCHAR(20) DEFAULT 'batch';

-- Add comment for documentation
COMMENT ON COLUMN pipeline_status.pipeline_mode IS 'Ingestion mode: batch (file upload), db (CDC), or api';
COMMENT ON COLUMN pipeline_status.pipeline_type IS 'Pipeline execution type: batch or streaming';
