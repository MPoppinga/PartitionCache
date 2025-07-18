-- PartitionCache PostgreSQL Cache Eviction - Cache Database Components
-- This file contains components that must be installed in the cache/work database
-- These components handle the actual eviction processing

-- Function to initialize eviction log table in cache database
CREATE OR REPLACE FUNCTION partitioncache_initialize_eviction_cache_log_table(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_log_table TEXT;
BEGIN
    v_log_table := p_table_prefix || '_eviction_log';

    -- Log table for eviction history (lives in cache database)
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id SERIAL PRIMARY KEY,
            job_name TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            queries_removed_count INTEGER,
            status TEXT NOT NULL CHECK (status IN (''success'', ''failed'')),
            message TEXT,
            execution_time_ms NUMERIC(10,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_log_table);
    
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_created_at ON %I(created_at DESC)',
        replace(v_log_table, '.', '_'), v_log_table);
END;
$$ LANGUAGE plpgsql;

-- Helper function to get the queries table name
CREATE OR REPLACE FUNCTION partitioncache_get_queries_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'queries';
    ELSE
        RETURN p_table_prefix || '_queries';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper function to get the cache table name for a partition
CREATE OR REPLACE FUNCTION partitioncache_get_cache_table_name(p_table_prefix TEXT, p_partition_key TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'cache_' || p_partition_key;
    ELSE
        RETURN p_table_prefix || '_cache_' || p_partition_key;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Helper function to get the metadata table name
CREATE OR REPLACE FUNCTION partitioncache_get_metadata_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'partition_metadata';
    ELSE
        RETURN p_table_prefix || '_partition_metadata';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Eviction strategy: Remove the oldest entries if count exceeds threshold
CREATE OR REPLACE FUNCTION _partitioncache_evict_oldest_from_partition(
    p_job_name TEXT, p_table_prefix TEXT, p_partition_key TEXT, p_threshold INTEGER
)
RETURNS INTEGER AS $$
DECLARE
    v_cache_table TEXT;
    v_queries_table TEXT;
    v_log_table TEXT;
    v_current_count INTEGER;
    v_to_remove_count INTEGER;
    v_removed_hashes TEXT[];
BEGIN
    v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    v_log_table := p_table_prefix || '_eviction_log';

    EXECUTE format('SELECT count(*)::INTEGER FROM %I', v_cache_table) INTO v_current_count;

    IF v_current_count <= p_threshold THEN
        RETURN 0;
    END IF;

    v_to_remove_count := v_current_count - p_threshold;

    EXECUTE format('
        WITH victims AS (
            SELECT q.query_hash
            FROM %I q
            JOIN %I c ON q.query_hash = c.query_hash
            WHERE q.partition_key = %L AND q.status = ''ok''
            ORDER BY q.last_seen ASC
            LIMIT %s
        ), deleted AS (
            DELETE FROM %I
            WHERE query_hash IN (SELECT query_hash FROM victims)
            RETURNING query_hash
        )
        SELECT array_agg(query_hash) FROM deleted',
        v_queries_table, v_cache_table, p_partition_key, v_to_remove_count, v_cache_table)
    INTO v_removed_hashes;
    
    IF array_length(v_removed_hashes, 1) > 0 THEN
        EXECUTE format('DELETE FROM %I WHERE partition_key = %L AND query_hash = ANY(%L)', 
            v_queries_table, p_partition_key, v_removed_hashes);

        EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, %L, %L, ''success'', ''Evicted oldest queries.'')',
            v_log_table, p_job_name, p_partition_key, array_length(v_removed_hashes, 1));
        
        RETURN array_length(v_removed_hashes, 1);
    END IF;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- Eviction strategy: Remove the largest entries if count exceeds threshold
CREATE OR REPLACE FUNCTION _partitioncache_evict_largest_from_partition(
    p_job_name TEXT, p_table_prefix TEXT, p_partition_key TEXT, p_threshold INTEGER
)
RETURNS INTEGER AS $$
DECLARE
    v_cache_table TEXT;
    v_queries_table TEXT;
    v_log_table TEXT;
    v_current_count INTEGER;
    v_to_remove_count INTEGER;
    v_removed_hashes TEXT[];
    v_count_column_exists BOOLEAN;
BEGIN
    v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    v_log_table := p_table_prefix || '_eviction_log';

    EXECUTE format('SELECT count(*)::INTEGER FROM %I', v_cache_table) INTO v_current_count;

    IF v_current_count <= p_threshold THEN
        RETURN 0;
    END IF;

    -- Check if a count column exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = v_cache_table AND column_name LIKE '%_count'
    ) INTO v_count_column_exists;

    IF NOT v_count_column_exists THEN
        EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, %L, 0, ''failed'', ''Largest eviction strategy failed: No count column found in cache table %s.'')',
            v_log_table, p_job_name, p_partition_key, v_cache_table);
        RETURN 0;
    END IF;

    v_to_remove_count := v_current_count - p_threshold;

    EXECUTE format('
        WITH victims AS (
            SELECT c.query_hash FROM %I c
            JOIN %I q ON c.query_hash = q.query_hash
            WHERE q.partition_key = %L AND q.status = ''ok''
            ORDER BY c.partition_keys_count DESC
            LIMIT %s
        ), deleted AS (
            DELETE FROM %I
            WHERE query_hash IN (SELECT query_hash FROM victims)
            RETURNING query_hash
        )
        SELECT array_agg(query_hash) FROM deleted',
        v_cache_table, v_queries_table, p_partition_key, v_to_remove_count, v_cache_table)
    INTO v_removed_hashes;

    IF array_length(v_removed_hashes, 1) > 0 THEN
        EXECUTE format('DELETE FROM %I WHERE partition_key = %L AND query_hash = ANY(%L)', 
            v_queries_table, p_partition_key, v_removed_hashes);

        EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, %L, %L, ''success'', ''Evicted largest queries.'')',
            v_log_table, p_job_name, p_partition_key, array_length(v_removed_hashes, 1));
        
        RETURN array_length(v_removed_hashes, 1);
    END IF;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- Main eviction function called by cron with parameters (executed in cache database)
CREATE OR REPLACE FUNCTION partitioncache_run_eviction_job_with_params(
    p_job_name TEXT, 
    p_table_prefix TEXT,
    p_strategy TEXT,
    p_threshold INTEGER
)
RETURNS VOID AS $$
DECLARE
    v_metadata_table TEXT;
    v_partition RECORD;
    v_start_time TIMESTAMP;
    v_log_table TEXT;
BEGIN
    v_start_time := clock_timestamp();
    v_log_table := p_table_prefix || '_eviction_log';
    
    -- No configuration lookup needed - all parameters passed directly

    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);

    -- Check if metadata table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = v_metadata_table) THEN
        EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, ''N/A'', 0, ''success'', ''No cache metadata table found. Cache may be empty or not initialized.'')',
            v_log_table, p_job_name);
        RETURN;
    END IF;

    -- Loop through all partitions
    FOR v_partition IN EXECUTE format('SELECT partition_key FROM %I', v_metadata_table)
    LOOP
        BEGIN
            IF p_strategy = 'oldest' THEN
                PERFORM _partitioncache_evict_oldest_from_partition(p_job_name, p_table_prefix, v_partition.partition_key, p_threshold);
            ELSIF p_strategy = 'largest' THEN
                PERFORM _partitioncache_evict_largest_from_partition(p_job_name, p_table_prefix, v_partition.partition_key, p_threshold);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, %L, 0, ''failed'', %L)',
                v_log_table, p_job_name, v_partition.partition_key, SQLERRM);
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Note: No backward compatibility wrapper needed since manual commands 
-- read config from cron database and call parameter-based function directly

-- Function to remove all eviction-related objects from cache database
CREATE OR REPLACE FUNCTION partitioncache_remove_eviction_cache_objects(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_log_table TEXT;
BEGIN
    v_log_table := p_table_prefix || '_eviction_log';

    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', v_log_table);

    -- Drop cache-specific functions
    DROP FUNCTION IF EXISTS partitioncache_run_eviction_job_with_params(TEXT, TEXT, TEXT, INTEGER);
    DROP FUNCTION IF EXISTS _partitioncache_evict_oldest_from_partition(TEXT, TEXT, TEXT, INTEGER);
    DROP FUNCTION IF EXISTS _partitioncache_evict_largest_from_partition(TEXT, TEXT, TEXT, INTEGER);
    DROP FUNCTION IF EXISTS partitioncache_initialize_eviction_cache_log_table(TEXT);
END;
$$ LANGUAGE plpgsql;