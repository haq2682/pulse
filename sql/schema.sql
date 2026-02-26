-- User table
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NULL,
    reset_token TEXT NULL,
    reset_token_expires TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Business table
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_name VARCHAR(255) NOT NULL,
    business_region VARCHAR(100),
    business_currency VARCHAR(50),
    ingestion_type VARCHAR(50) CHECK (ingestion_type IN ('batch', 'db', 'api')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Admins table
CREATE TABLE admins (
    admin_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    reset_token VARCHAR(500),
    reset_token_expires TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Onboarding table
CREATE TABLE onboarding (
    onboarding_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_id VARCHAR(50) NULL,
    current_step VARCHAR(100) NOT NULL DEFAULT 'business' CHECK (current_step IN ('business', 'data-type', 'connect', 'mapping', 'mapping-in-progress')),
    ingestion_type VARCHAR(50) NULL CHECK (ingestion_type IN ('batch', 'db', 'api')),
    api_url TEXT NULL,                   -- User-provided external API endpoint (api mode only)
    db_uri TEXT NULL CHECK (LENGTH(db_uri) <= 2048),        -- User-provided database URI (db mode only)
    db_tables TEXT NULL CHECK (LENGTH(db_tables) <= 4096),  -- Comma-separated list of tables to capture (db mode only)
    is_completed BOOLEAN DEFAULT FALSE,
    mapping_status VARCHAR(50) NULL CHECK (mapping_status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    mapping_error TEXT NULL,
    mapping_started_at TIMESTAMP NULL,
    mapping_completed_at TIMESTAMP NULL,
    mapping_results JSONB NULL,
    manual_mappings JSONB NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id)
);

-- Uploaded files table
CREATE TABLE uploaded_files (
    file_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(100),
    s3_key VARCHAR(1000) NOT NULL,
    upload_status VARCHAR(50) DEFAULT 'uploading' CHECK (upload_status IN ('uploading', 'completed', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS nifi_schemas (
    schema_name VARCHAR(255) PRIMARY KEY,
    schema_text TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default schema for pulse data
INSERT INTO nifi_schemas (schema_name, schema_text) VALUES
('pulse_schema', '{
  "type": "record",
  "name": "PulseData",
  "fields": [
    {"name": "id", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": ["null", "long"], "default": null}
  ]
}')
ON CONFLICT (schema_name) DO NOTHING;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_businesses_updated_at
BEFORE UPDATE ON businesses
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_admins_updated_at
BEFORE UPDATE ON admins
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_onboarding_updated_at
BEFORE UPDATE ON onboarding
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_uploaded_files_updated_at
BEFORE UPDATE ON uploaded_files
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Pipeline Status table for tracking data processing pipeline execution
CREATE TABLE pipeline_status (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    current_step VARCHAR(100),
    progress_percentage INTEGER DEFAULT 0 CHECK (progress_percentage >= 0 AND progress_percentage <= 100),
    failed_phase VARCHAR(50) NULL,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    process_ids JSONB NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
);

CREATE TRIGGER update_pipeline_status_updated_at
BEFORE UPDATE ON pipeline_status
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Analytics Exports table for tracking generated PDF reports
CREATE TABLE analytics_exports (
    export_id VARCHAR(50) PRIMARY KEY,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    sections_exported JSONB NOT NULL DEFAULT '[]',
    total_sections INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- XAI Chat Conversations table
CREATE TABLE xai_conversations (
    conversation_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_id VARCHAR(50) NOT NULL,
    title VARCHAR(500) DEFAULT 'New Chat',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE
);

CREATE TRIGGER update_xai_conversations_updated_at
BEFORE UPDATE ON xai_conversations
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- XAI Chat Messages table
CREATE TABLE xai_messages (
    message_id VARCHAR(50) PRIMARY KEY,
    conversation_id VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'notification')),
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    severity VARCHAR(20) DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'error', 'success')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES xai_conversations(conversation_id) ON DELETE CASCADE
);