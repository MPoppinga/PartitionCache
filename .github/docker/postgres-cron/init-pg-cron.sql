-- Initialize pg_cron and roaringbitmap extensions for integration testing
-- This script runs automatically when the PostgreSQL container starts
-- Note: Individual test databases will be created dynamically with all extensions

-- Create the pg_cron extension in the default postgres database
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create the roaringbitmap extension in the default postgres database  
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- Verify the extensions are properly installed
SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_cron', 'roaringbitmap');

-- Grant necessary permissions for the integration user
-- Allow the integration user to create databases and manage extensions
GRANT CREATE ON DATABASE postgres TO integration_user;
GRANT USAGE ON SCHEMA cron TO integration_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO integration_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO integration_user;