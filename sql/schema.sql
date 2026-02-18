-- User table
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NULL,
    reset_token TEXT NULL,
    reset_token_expires TIMESTAMP NULL
);

-- Business table
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_name VARCHAR(255) NOT NULL,
    business_region VARCHAR(100),
    business_currency VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Admins table
CREATE TABLE admins (
    admin_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    reset_token VARCHAR(500),
    reset_token_expires TIMESTAMP
);

-- Onboarding table
CREATE TABLE onboarding (
    onboarding_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    business_id VARCHAR(50) NULL,
    current_step VARCHAR(100) NOT NULL DEFAULT 'business' CHECK (current_step IN ('business', 'data-type', 'connect', 'mapping')),
    ingestion_type VARCHAR(50) NULL CHECK (ingestion_type IN ('batch', 'db', 'api')),
    is_completed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id)
);