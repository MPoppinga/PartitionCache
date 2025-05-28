-- PartitionCache Direct PostgreSQL Queue Processor
-- This file contains all the necessary SQL objects to process queries directly within PostgreSQL
-- using pg_cron for scheduling

-- Configuration table to control the processor
CREATE TABLE IF NOT EXISTS partitioncache_processor_config (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1), -- Ensure only one row
    enabled BOOLEAN NOT NULL DEFAULT false,
    max_parallel_jobs INTEGER NOT NULL DEFAULT 5 CHECK (max_parallel_jobs > 0 AND max_parallel_jobs <= 20),
    frequency_seconds INTEGER NOT NULL DEFAULT 1 CHECK (frequency_seconds > 0),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default configuration if not exists
INSERT INTO partitioncache_processor_config (enabled, max_parallel_jobs, frequency_seconds)
VALUES (false, 5, 1)
ON CONFLICT (id) DO NOTHING;

-- Log table for job execution history
CREATE TABLE IF NOT EXISTS partitioncache_processor_log (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started', 'success', 'failed', 'timeout')),
    error_message TEXT,
    rows_affected INTEGER,
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for log queries
CREATE INDEX IF NOT EXISTS idx_processor_log_created_at ON partitioncache_processor_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processor_log_status ON partitioncache_processor_log(status, created_at DESC);

-- Active jobs tracking table to prevent concurrent execution of same query
CREATE TABLE IF NOT EXISTS partitioncache_active_jobs (
    query_hash TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_hash, partition_key)
);

-- Function to get the target cache table name for a partition
CREATE OR REPLACE FUNCTION get_cache_table_name(p_table_prefix TEXT, p_partition_key TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN p_table_prefix || '_cache_' || p_partition_key;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to check if we can process more jobs
CREATE OR REPLACE FUNCTION can_process_more_jobs()
RETURNS BOOLEAN AS $$
DECLARE
    v_config RECORD;
    v_active_count INTEGER;
BEGIN
    -- Get configuration
    SELECT enabled, max_parallel_jobs INTO v_config
    FROM partitioncache_processor_config
    LIMIT 1;
    
    -- Check if processing is enabled
    IF NOT v_config.enabled THEN
        RETURN false;
    END IF;
    
    -- Count active jobs
    SELECT COUNT(*) INTO v_active_count
    FROM partitioncache_active_jobs;
    
    RETURN v_active_count < v_config.max_parallel_jobs;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up stale active jobs (older than 5 minutes)
CREATE OR REPLACE FUNCTION cleanup_stale_jobs()
RETURNS INTEGER AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM partitioncache_active_jobs
    WHERE started_at < NOW() - INTERVAL '5 minutes';
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    -- Log stale job cleanup
    IF v_deleted > 0 THEN
        INSERT INTO partitioncache_processor_log (job_id, query_hash, partition_key, status, error_message)
        SELECT job_id, query_hash, partition_key, 'timeout', 'Job timed out after 5 minutes'
        FROM partitioncache_active_jobs
        WHERE started_at < NOW() - INTERVAL '5 minutes';
    END IF;
    
    RETURN v_deleted;
END;
$$ LANGUAGE plpgsql;

-- Main function to process a single query from the queue
CREATE OR REPLACE FUNCTION process_queue_item(p_table_prefix TEXT)
RETURNS TABLE(processed BOOLEAN, message TEXT) AS $$
DECLARE
    v_queue_item RECORD;
    v_job_id TEXT;
    v_start_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_cache_table TEXT;
    v_insert_query TEXT;
    v_datatype TEXT;
    v_metadata_table TEXT;
    v_queries_table TEXT;
    v_cache_backend TEXT;
BEGIN
    -- Clean up stale jobs first
    PERFORM cleanup_stale_jobs();
    
    -- Check if we can process more jobs
    IF NOT can_process_more_jobs() THEN
        RETURN QUERY SELECT false, 'Max parallel jobs reached or processing disabled';
        RETURN;
    END IF;
    
    -- Try to get a query fragment from the queue
    -- Using SELECT FOR UPDATE SKIP LOCKED to handle concurrent access
    SELECT id, query, hash, partition_key, partition_datatype
    INTO v_queue_item
    FROM query_fragment_queue
    WHERE NOT EXISTS (
        SELECT 1 FROM partitioncache_active_jobs aj
        WHERE aj.query_hash = query_fragment_queue.hash
        AND aj.partition_key = query_fragment_queue.partition_key
    )
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;
    
    -- No item found
    IF NOT FOUND THEN
        RETURN QUERY SELECT false, 'No items in queue';
        RETURN;
    END IF;
    
    -- Generate job ID
    v_job_id := 'job_' || extract(epoch from now())::text || '_' || v_queue_item.id::text;
    v_start_time := now();
    
    -- Register as active job
    INSERT INTO partitioncache_active_jobs (query_hash, partition_key, job_id)
    VALUES (v_queue_item.hash, v_queue_item.partition_key, v_job_id);
    
    -- Log job start
    INSERT INTO partitioncache_processor_log (job_id, query_hash, partition_key, status)
    VALUES (v_job_id, v_queue_item.hash, v_queue_item.partition_key, 'started');
    
    BEGIN
        -- Get the cache table name
        v_cache_table := get_cache_table_name(p_table_prefix, v_queue_item.partition_key);
        v_metadata_table := p_table_prefix || '_partition_metadata';
        v_queries_table := p_table_prefix || '_queries';
        
        -- Get datatype from metadata if not provided
        IF v_queue_item.partition_datatype IS NULL THEN
            EXECUTE format('SELECT datatype FROM %I WHERE partition_key = %L', v_metadata_table, v_queue_item.partition_key)
            INTO v_datatype;
        ELSE
            v_datatype := v_queue_item.partition_datatype;
        END IF;
        
        -- Determine cache backend type (array vs bit)
        -- Check if the cache table has a 'value' column (array) or 'partition_keys' column (bit)
        SELECT CASE 
            WHEN EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = v_cache_table 
                AND column_name = 'partition_keys'
            ) THEN 'bit'
            ELSE 'array'
        END INTO v_cache_backend;
        
        -- Build the INSERT query based on cache backend type
        IF v_cache_backend = 'bit' THEN
            -- For bit cache, we need to convert the result set to a bit string
            -- This is more complex and would need the bitsize from metadata
            RAISE EXCEPTION 'Bit cache backend processing not yet implemented in direct processor';
        ELSE
            -- For array cache, wrap the query result in an array
            v_insert_query := format(
                'INSERT INTO %I (query_hash, value) 
                 SELECT %L, ARRAY(
                     %s
                 )
                 ON CONFLICT (query_hash) DO UPDATE SET value = EXCLUDED.value',
                v_cache_table,
                v_queue_item.hash,
                v_queue_item.query
            );
        END IF;
        
        -- Execute the insert query
        EXECUTE v_insert_query;
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        
        -- Also update the queries table
        EXECUTE format(
            'INSERT INTO %I (query_hash, partition_key, query, last_seen)
             VALUES (%L, %L, %L, now())
             ON CONFLICT (query_hash, partition_key) DO UPDATE SET last_seen = now()',
            v_queries_table,
            v_queue_item.hash,
            v_queue_item.partition_key,
            v_queue_item.query
        );
        
        -- Delete from queue
        DELETE FROM query_fragment_queue WHERE id = v_queue_item.id;
        
        -- Remove from active jobs
        DELETE FROM partitioncache_active_jobs 
        WHERE query_hash = v_queue_item.hash AND partition_key = v_queue_item.partition_key;
        
        -- Log success
        INSERT INTO partitioncache_processor_log (job_id, query_hash, partition_key, status, rows_affected, execution_time_ms)
        VALUES (
            v_job_id, 
            v_queue_item.hash, 
            v_queue_item.partition_key, 
            'success', 
            v_rows_affected,
            extract(milliseconds from (now() - v_start_time))::integer
        );
        
        RETURN QUERY SELECT true, format('Processed query %s for partition %s', v_queue_item.hash, v_queue_item.partition_key);
        
    EXCEPTION WHEN OTHERS THEN
        -- Remove from active jobs
        DELETE FROM partitioncache_active_jobs 
        WHERE query_hash = v_queue_item.hash AND partition_key = v_queue_item.partition_key;
        
        -- Log failure
        INSERT INTO partitioncache_processor_log (job_id, query_hash, partition_key, status, error_message, execution_time_ms)
        VALUES (
            v_job_id, 
            v_queue_item.hash, 
            v_queue_item.partition_key, 
            'failed', 
            SQLERRM,
            extract(milliseconds from (now() - v_start_time))::integer
        );
        
        -- Re-raise the error
        RAISE EXCEPTION 'Failed to process query %: %', v_queue_item.hash, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;

-- Main processor function that will be called by pg_cron
CREATE OR REPLACE FUNCTION partitioncache_process_queue(p_table_prefix TEXT DEFAULT 'partitioncache')
RETURNS TABLE(processed_count INTEGER, message TEXT) AS $$
DECLARE
    v_processed_count INTEGER := 0;
    v_result RECORD;
    v_max_iterations INTEGER;
BEGIN
    -- Get max parallel jobs for iteration limit
    SELECT max_parallel_jobs INTO v_max_iterations
    FROM partitioncache_processor_config
    LIMIT 1;
    
    -- Process up to max_parallel_jobs items
    FOR i IN 1..COALESCE(v_max_iterations, 5) LOOP
        SELECT * INTO v_result FROM process_queue_item(p_table_prefix);
        
        IF v_result.processed THEN
            v_processed_count := v_processed_count + 1;
        ELSE
            -- No more items to process or can't process more
            EXIT;
        END IF;
    END LOOP;
    
    RETURN QUERY SELECT v_processed_count, format('Processed %s queue items', v_processed_count);
END;
$$ LANGUAGE plpgsql;

-- Helper function to get processor status
CREATE OR REPLACE FUNCTION get_processor_status()
RETURNS TABLE(
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    active_jobs INTEGER,
    queue_length INTEGER,
    recent_successes INTEGER,
    recent_failures INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.enabled,
        c.max_parallel_jobs,
        c.frequency_seconds,
        (SELECT COUNT(*)::INTEGER FROM partitioncache_active_jobs) as active_jobs,
        (SELECT COUNT(*)::INTEGER FROM query_fragment_queue) as queue_length,
        (SELECT COUNT(*)::INTEGER FROM partitioncache_processor_log 
         WHERE status = 'success' AND created_at > NOW() - INTERVAL '5 minutes') as recent_successes,
        (SELECT COUNT(*)::INTEGER FROM partitioncache_processor_log 
         WHERE status = 'failed' AND created_at > NOW() - INTERVAL '5 minutes') as recent_failures
    FROM partitioncache_processor_config c
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to enable/disable the processor
CREATE OR REPLACE FUNCTION set_processor_enabled(p_enabled BOOLEAN)
RETURNS VOID AS $$
BEGIN
    UPDATE partitioncache_processor_config 
    SET enabled = p_enabled, updated_at = NOW()
    WHERE id = 1;
END;
$$ LANGUAGE plpgsql;

-- Function to update processor configuration
CREATE OR REPLACE FUNCTION update_processor_config(
    p_enabled BOOLEAN DEFAULT NULL,
    p_max_parallel_jobs INTEGER DEFAULT NULL,
    p_frequency_seconds INTEGER DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE partitioncache_processor_config 
    SET 
        enabled = COALESCE(p_enabled, enabled),
        max_parallel_jobs = COALESCE(p_max_parallel_jobs, max_parallel_jobs),
        frequency_seconds = COALESCE(p_frequency_seconds, frequency_seconds),
        updated_at = NOW()
    WHERE id = 1;
END;
$$ LANGUAGE plpgsql;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_query_fragment_queue_processing 
ON query_fragment_queue(priority DESC, created_at ASC);

-- Helper function to manually trigger processing (useful for testing)
CREATE OR REPLACE FUNCTION manual_process_queue(p_count INTEGER DEFAULT 1, p_table_prefix TEXT DEFAULT 'partitioncache')
RETURNS TABLE(total_processed INTEGER, messages TEXT[]) AS $$
DECLARE
    v_total INTEGER := 0;
    v_messages TEXT[] := ARRAY[]::TEXT[];
    v_result RECORD;
BEGIN
    FOR i IN 1..p_count LOOP
        SELECT * INTO v_result FROM process_queue_item(p_table_prefix);
        
        IF v_result.processed THEN
            v_total := v_total + 1;
            v_messages := array_append(v_messages, v_result.message);
        ELSE
            v_messages := array_append(v_messages, format('[%s] %s', i, v_result.message));
            EXIT WHEN v_result.message = 'No items in queue';
        END IF;
    END LOOP;
    
    RETURN QUERY SELECT v_total, v_messages;
END;
$$ LANGUAGE plpgsql; 