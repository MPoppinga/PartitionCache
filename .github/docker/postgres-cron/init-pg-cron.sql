-- Initialize pg_cron and roaringbitmap extensions for integration testing
-- This script runs automatically when the PostgreSQL container starts

-- Create the pg_cron extension in the test database
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create the roaringbitmap extension in the test database
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- Verify the extensions are properly installed
SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_cron', 'roaringbitmap');

-- Grant necessary permissions for testing
-- Allow the integration user to schedule and manage cron jobs
GRANT USAGE ON SCHEMA cron TO integration_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO integration_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO integration_user;

-- Create a simple test table for validation
CREATE TABLE IF NOT EXISTS test_cron_results (
    id SERIAL PRIMARY KEY,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions on test table
GRANT ALL PRIVILEGES ON test_cron_results TO integration_user;
GRANT USAGE, SELECT ON SEQUENCE test_cron_results_id_seq TO integration_user;