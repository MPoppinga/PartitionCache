-- PartitionCache PostgreSQL Cache Eviction Processor
-- This file contains all the necessary SQL objects to evict cache entries directly within PostgreSQL
-- using pg_cron for scheduling.
--
-- Architecture Overview:
-- A single cron job runs periodically, triggering the main eviction function.
-- This function reads from a configuration table to determine the eviction strategy
-- and then applies it to all relevant cache partitions.
--
-- Main Components:
-- - Eviction Config Table: Stores settings like enabled, frequency, strategy, and threshold.
-- - Eviction Log Table: Records all eviction actions for monitoring.
-- - partitioncache_run_eviction_job(): The main function called by cron to orchestrate eviction.
-- - _partitioncache_evict_..._from_partition(): Internal functions that implement the specific eviction strategies.
-- - Cron Sync Trigger: Automatically manages the pg_cron job based on changes to the config table.

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


-- Function to initialize eviction processor tables
CREATE OR REPLACE FUNCTION partitioncache_initialize_eviction_tables(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_log_table TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';
    v_log_table := p_table_prefix || '_eviction_log';

    -- Configuration table for the eviction processor
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            job_name TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT false,
            frequency_minutes INTEGER NOT NULL DEFAULT 60 CHECK (frequency_minutes > 0),
            strategy TEXT NOT NULL DEFAULT ''oldest'' CHECK (strategy IN (''oldest'', ''largest'')),
            threshold INTEGER NOT NULL DEFAULT 1000 CHECK (threshold > 0),
            table_prefix TEXT NOT NULL,
            job_id BIGINT DEFAULT NULL, -- Store pg_cron job ID for cross-database management
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);

    -- Log table for eviction history
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


-- Trigger function to synchronize config with pg_cron using API functions and cross-database scheduling
CREATE OR REPLACE FUNCTION partitioncache_sync_eviction_cron_job()
RETURNS TRIGGER AS $$
DECLARE
    v_command TEXT;
    v_work_database TEXT;
    v_job_id BIGINT;
BEGIN
    -- Get work database name (current database where this trigger is running)
    SELECT current_database() INTO v_work_database;
    
    IF (TG_OP = 'DELETE') THEN
        -- Unschedule job using stored job_id
        IF OLD.job_id IS NOT NULL THEN
            BEGIN
                PERFORM cron.unschedule(OLD.job_id);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
            END;
        END IF;
        RETURN OLD;
    END IF;

    -- Build the command to be executed by cron
    v_command := format('SELECT partitioncache_run_eviction_job(%L, %L)', NEW.job_name, NEW.table_prefix);

    -- First, unschedule existing job if this is an UPDATE
    IF (TG_OP = 'UPDATE' AND OLD.job_id IS NOT NULL) THEN
        BEGIN
            PERFORM cron.unschedule(OLD.job_id);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
        END;
    END IF;

    -- Schedule new job using pg_cron API with cross-database execution
    BEGIN
        SELECT cron.schedule_in_database(
            NEW.job_name,
            CONCAT(NEW.frequency_minutes, ' minutes'),
            v_command,
            v_work_database,
            current_user,
            NEW.enabled
        ) INTO v_job_id;
        
        -- Store the job ID for later management
        NEW.job_id := v_job_id;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to schedule eviction job %: %', NEW.job_name, SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to attach the trigger to the eviction config table
CREATE OR REPLACE FUNCTION partitioncache_create_eviction_config_trigger(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_trigger_name TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';
    v_trigger_name := 'trg_sync_eviction_cron_job_' || replace(v_config_table, '.', '_');

    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, v_config_table);

    EXECUTE format('
        CREATE TRIGGER %I
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION partitioncache_sync_eviction_cron_job()
    ', v_trigger_name, v_config_table);
END;
$$ LANGUAGE plpgsql;

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


-- Main eviction function called by cron
CREATE OR REPLACE FUNCTION partitioncache_run_eviction_job(p_job_name TEXT, p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config RECORD;
    v_metadata_table TEXT;
    v_partition RECORD;
    v_start_time TIMESTAMP;
    v_config_table TEXT;
    v_log_table TEXT;
BEGIN
    v_start_time := clock_timestamp();
    v_config_table := p_table_prefix || '_eviction_config';
    v_log_table := p_table_prefix || '_eviction_log';

    -- Get configuration for this job
    EXECUTE format('SELECT * FROM %I WHERE job_name = %L', v_config_table, p_job_name) INTO v_config;
    IF v_config IS NULL THEN
        RAISE WARNING 'No configuration found for eviction job %', p_job_name;
        RETURN;
    END IF;

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
            IF v_config.strategy = 'oldest' THEN
                PERFORM _partitioncache_evict_oldest_from_partition(p_job_name, p_table_prefix, v_partition.partition_key, v_config.threshold);
            ELSIF v_config.strategy = 'largest' THEN
                PERFORM _partitioncache_evict_largest_from_partition(p_job_name, p_table_prefix, v_partition.partition_key, v_config.threshold);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            EXECUTE format('INSERT INTO %I (job_name, partition_key, queries_removed_count, status, message) VALUES (%L, %L, 0, ''failed'', %L)',
                v_log_table, p_job_name, v_partition.partition_key, SQLERRM);
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Function to remove all eviction-related objects
CREATE OR REPLACE FUNCTION partitioncache_remove_eviction_objects(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_log_table TEXT;
    v_job_name TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';
    v_log_table := p_table_prefix || '_eviction_log';

    -- Get job name to remove from cron.job
    EXECUTE format('SELECT job_name FROM %I LIMIT 1', v_config_table) INTO v_job_name;
    IF v_job_name IS NOT NULL THEN
        DELETE FROM cron.job WHERE jobname = v_job_name;
    END IF;

    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', v_config_table);
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', v_log_table);

    -- Drop functions, be careful if they are shared
    -- For now, this is kept self-contained
    DROP FUNCTION IF EXISTS partitioncache_run_eviction_job(TEXT, TEXT);
    DROP FUNCTION IF EXISTS _partitioncache_evict_oldest_from_partition(TEXT, TEXT, TEXT, INTEGER);
    DROP FUNCTION IF EXISTS _partitioncache_evict_largest_from_partition(TEXT, TEXT, TEXT, INTEGER);
    DROP FUNCTION IF EXISTS partitioncache_sync_eviction_cron_job();
    DROP FUNCTION IF EXISTS partitioncache_create_eviction_config_trigger(TEXT);
    DROP FUNCTION IF EXISTS partitioncache_initialize_eviction_tables(TEXT);
END;
$$ LANGUAGE plpgsql; 