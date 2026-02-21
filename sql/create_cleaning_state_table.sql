-- State Tracking Table for Incremental Cleaning
-- This table tracks which files have been processed by the cleaning pipeline
-- to enable incremental processing and avoid reprocessing the same files

CREATE TABLE IF NOT EXISTS cleaning_state (
    file_path VARCHAR(500) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    file_size BIGINT,
    record_count BIGINT,
    checksum VARCHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries on processed_at
CREATE INDEX IF NOT EXISTS idx_cleaning_state_processed_at 
ON cleaning_state(processed_at DESC);

-- Index for faster queries on file paths
CREATE INDEX IF NOT EXISTS idx_cleaning_state_file_path 
ON cleaning_state(file_path);

-- Comment on table
COMMENT ON TABLE cleaning_state IS 
'Tracks processed files for incremental cleaning pipeline';

COMMENT ON COLUMN cleaning_state.file_path IS 
'Full path to the file in MinIO (e.g., mapped/orders.csv)';

COMMENT ON COLUMN cleaning_state.processed_at IS 
'Timestamp when the file was last processed';

COMMENT ON COLUMN cleaning_state.file_size IS 
'Size of the file in bytes';

COMMENT ON COLUMN cleaning_state.record_count IS 
'Number of records in the file';

COMMENT ON COLUMN cleaning_state.checksum IS 
'MD5 checksum of the file for change detection';
