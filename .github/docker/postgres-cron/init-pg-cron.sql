-- Initialize pg_cron and roaringbitmap extensions for integration testing
-- This script runs automatically when the PostgreSQL container starts

-- Create the partitioncache_integration database for pg_cron
CREATE DATABASE partitioncache_integration;

-- Grant database creation permissions to integration user
GRANT CREATE ON DATABASE postgres TO integration_user;

-- Connect to the partitioncache_integration database to set up extensions
\c partitioncache_integration;

-- Create the pg_cron extension in the partitioncache_integration database
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create the roaringbitmap extension in the partitioncache_integration database  
CREATE EXTENSION IF NOT EXISTS roaringbitmap;

-- Verify the extensions are properly installed
SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_cron', 'roaringbitmap');

-- Grant necessary permissions for the integration user
GRANT USAGE ON SCHEMA cron TO integration_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO integration_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO integration_user;

-- Create test tables for pg_cron testing
CREATE TABLE test_cron_results (
    id SERIAL PRIMARY KEY,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_locations (
    id SERIAL PRIMARY KEY,
    zipcode INTEGER,
    region TEXT,
    name TEXT,
    population INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_businesses (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    business_type TEXT NOT NULL,
    region_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    zipcode TEXT NOT NULL,
    x DECIMAL(10,6) NOT NULL,
    y DECIMAL(10,6) NOT NULL,  
    rating DECIMAL(2,1) DEFAULT 3.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_spatial_points (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    point_type TEXT NOT NULL,
    region_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    zipcode TEXT NOT NULL,
    x DECIMAL(10,6) NOT NULL,
    y DECIMAL(10,6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions on test tables
GRANT ALL PRIVILEGES ON test_cron_results TO integration_user;
GRANT USAGE, SELECT ON SEQUENCE test_cron_results_id_seq TO integration_user;
GRANT ALL PRIVILEGES ON test_locations TO integration_user;
GRANT USAGE, SELECT ON SEQUENCE test_locations_id_seq TO integration_user;
GRANT ALL PRIVILEGES ON test_businesses TO integration_user;
GRANT USAGE, SELECT ON SEQUENCE test_businesses_id_seq TO integration_user;
GRANT ALL PRIVILEGES ON test_spatial_points TO integration_user;
GRANT USAGE, SELECT ON SEQUENCE test_spatial_points_id_seq TO integration_user;

-- Note: PartitionCache queue processor functions are loaded at test runtime
-- via the setup_postgresql_queue_processor.py CLI tool