-- PartitionCache PostgreSQL Queue Processor - Cache Database Components
-- This file contains components that must be installed in the cache/work database
-- These components handle the actual queue processing and cache operations

-- Function to initialize processor tables in cache database
CREATE OR REPLACE FUNCTION partitioncache_initialize_cache_processor_tables(p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS VOID AS $$
DECLARE
    v_log_table TEXT;
    v_active_jobs_table TEXT;
BEGIN
    v_log_table := p_queue_prefix || '_processor_log';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    
    -- Log table for job execution history
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN (''started'', ''success'', ''failed'', ''timeout'', ''skipped'')),
            error_message TEXT,
            rows_affected INTEGER,
            execution_time_ms NUMERIC(10,3),
            execution_source TEXT NOT NULL CHECK (execution_source IN (''cron'', ''manual'', ''unknown'')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_log_table);
    
    -- Index for log queries
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(created_at DESC)', 
        'idx_' || replace(v_log_table, '.', '_') || '_created_at', v_log_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(status, created_at DESC)', 
        'idx_' || replace(v_log_table, '.', '_') || '_status', v_log_table);
    
    -- Active jobs tracking table to prevent concurrent execution of same query
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            job_id TEXT NOT NULL,
            pid INTEGER,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            -- Store the original queue item data for background processing
            query TEXT NOT NULL,
            partition_datatype TEXT,
            PRIMARY KEY (query_hash, partition_key)
        )', v_active_jobs_table);
END;
$$ LANGUAGE plpgsql;

-- Function to get the target cache table name for a partition
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

-- Function to get the metadata table name
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

-- Function to get the queries table name
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

-- Function to ensure metadata and queries tables exist with correct naming
CREATE OR REPLACE FUNCTION partitioncache_ensure_metadata_tables(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_metadata_table TEXT;
    v_queries_table TEXT;
    v_cache_backend TEXT;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Determine cache backend type from table prefix
    IF p_table_prefix LIKE '%_roaringbit%' THEN
        v_cache_backend := 'roaringbit';
    ELSIF p_table_prefix LIKE '%_bit%' THEN
        v_cache_backend := 'bit';
    ELSE
        v_cache_backend := 'array';
    END IF;
    
    -- Create metadata table
    IF v_cache_backend = 'bit' THEN
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype = ''integer''),
                bitsize INTEGER DEFAULT NULL,
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    ELSIF v_cache_backend = 'roaringbit' THEN
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype = ''integer''),
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    ELSE
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype IN (''integer'', ''float'', ''text'', ''timestamp'')),
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    END IF;
    
    -- Create queries table with status
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            query TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT ''ok'' CHECK (status IN (''ok'', ''timeout'', ''failed'', ''limit'')),
            last_seen TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (query_hash, partition_key)
        )', v_queries_table);
END;
$$ LANGUAGE plpgsql;

-- Function to bootstrap a partition (create metadata entry and cache table)
CREATE OR REPLACE FUNCTION partitioncache_bootstrap_partition(
    p_table_prefix TEXT, 
    p_partition_key TEXT, 
    p_datatype TEXT,
    p_cache_backend TEXT,
    p_bitsize INTEGER DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    v_metadata_table TEXT;
    v_cache_table TEXT;
    v_partition_exists BOOLEAN;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
    
    -- Check if partition already exists
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE partition_key = %L)', v_metadata_table, p_partition_key)
    INTO v_partition_exists;
    
    IF v_partition_exists THEN
        RETURN true; -- Already exists, nothing to do
    END IF;
    
    -- Create metadata entry based on cache backend
    IF p_cache_backend = 'bit' THEN
        -- For bit cache, include bitsize
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype, bitsize) VALUES (%L, %L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype, p_bitsize
        );
        
        -- Create bit cache table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys BIT VARYING,
                partition_keys_count INTEGER GENERATED ALWAYS AS (length(replace(partition_keys::text, ''0'', ''''))) STORED
            )', v_cache_table);
    ELSIF p_cache_backend = 'roaringbit' THEN
        -- For roaring bit cache
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype) VALUES (%L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype
        );
        
        -- Create roaring bitmap cache table (enable extension first)
        EXECUTE 'CREATE EXTENSION IF NOT EXISTS roaringbitmap';
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys roaringbitmap,
                partition_keys_count INTEGER GENERATED ALWAYS AS (rb_cardinality(partition_keys)) STORED
            )', v_cache_table);
    ELSE -- 'array'
        -- For array cache
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype) VALUES (%L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype
        );
        
        -- Create array cache table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys %s[],
                partition_keys_count integer GENERATED ALWAYS AS (array_length(partition_keys, 1)) STORED
            )', v_cache_table, 
            CASE p_datatype 
                WHEN 'integer' THEN 'INTEGER'
                WHEN 'float' THEN 'REAL'
                WHEN 'text' THEN 'TEXT'
                WHEN 'timestamp' THEN 'TIMESTAMP'
                ELSE 'TEXT'
            END);
    END IF;
    
    RETURN true;
END;
$$ LANGUAGE plpgsql;

-- Main dispatcher function that will be called by pg_cron with parameters
CREATE OR REPLACE FUNCTION partitioncache_run_single_job_with_params(
    p_job_name TEXT,
    p_table_prefix TEXT,
    p_queue_prefix TEXT,
    p_cache_backend TEXT,
    p_timeout_seconds INTEGER DEFAULT 1800,
    p_result_limit INTEGER DEFAULT NULL
)
RETURNS TABLE(dispatched_job_id TEXT, status TEXT) AS $$
DECLARE
    v_queue_table TEXT;
    v_active_jobs_table TEXT;
    v_log_table TEXT;
    v_queue_item RECORD;
BEGIN
    -- No configuration lookup needed - all parameters passed directly
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    v_log_table := p_queue_prefix || '_processor_log';

    -- Dequeue the next uncached item, skipping locked items and already processed items
    DECLARE
        v_queries_table TEXT;
    BEGIN
        v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
        
        EXECUTE format(
            'DELETE FROM %I
                WHERE id = (
                    SELECT q.id 
                    FROM %I q
                    WHERE NOT EXISTS (
                        SELECT 1 FROM %I qr 
                        WHERE qr.query_hash = q.hash 
                        AND qr.partition_key = q.partition_key
                    )  -- Only uncached items
                    ORDER BY q.priority DESC, q.id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, query, hash, partition_key, partition_datatype',
            v_queue_table, v_queue_table, v_queries_table
        ) INTO v_queue_item;
    END;

    -- If no item was dequeued, use idle time for cleanup
    IF v_queue_item.id IS NULL THEN
        -- Try to clean up cached items during idle time
        DECLARE
            v_cleanup_count INTEGER;
        BEGIN
            v_cleanup_count := partitioncache_cleanup_cached_queue_items(
                p_queue_prefix, 
                p_table_prefix, 
                50  -- Clean up to 50 cached items
            );
            
            IF v_cleanup_count > 0 THEN
                RAISE NOTICE 'Cleaned up % cached items from queue during idle time', v_cleanup_count;
                dispatched_job_id := 'cleanup_' || v_cleanup_count;
                status := 'cleanup_completed';
            ELSE
                dispatched_job_id := NULL;
                status := 'no_jobs_available';
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            -- Don't let cleanup failures affect main processing
            RAISE NOTICE 'Queue cleanup failed: %', SQLERRM;
            dispatched_job_id := NULL;
            status := 'no_jobs_available';
        END;
        
        RETURN NEXT;
        RETURN;
    END IF;

    -- Try to acquire a lock for this job by inserting into active jobs
    BEGIN
        EXECUTE format('INSERT INTO %I (query_hash, partition_key, job_id, started_at, query, partition_datatype, pid) VALUES (%L, %L, %L, NOW(), %L, %L, %s)', 
            v_active_jobs_table, v_queue_item.hash, v_queue_item.partition_key, 'job_' || v_queue_item.id, v_queue_item.query, v_queue_item.partition_datatype, pg_backend_pid());
    EXCEPTION WHEN unique_violation THEN
        -- This query/partition combo is already running, also skipping the job
        EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_source) VALUES (%L, %L, %L, %L, %L, %L)', 
                v_log_table, 'job_' || v_queue_item.id, v_queue_item.hash, v_queue_item.partition_key, 'skipped', 'Query in processing', 'cron');
      
        dispatched_job_id := 'job_' || v_queue_item.id;
        RAISE NOTICE 'Skipping job %', dispatched_job_id;
        status := 'skipped';
        RETURN NEXT;
        RETURN;
    END;

    -- Log job start
    EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, execution_source) VALUES (%L, %L, %L, %L, %L)', 
        v_log_table, 'job_' || v_queue_item.id, v_queue_item.hash, v_queue_item.partition_key, 'started', 'cron');
    RAISE NOTICE 'Job started %', dispatched_job_id;
    
    -- Process the item directly
    DECLARE
        is_success BOOLEAN;
    BEGIN
        is_success := _partitioncache_execute_job(
            v_queue_item.id::INTEGER,
            v_queue_item.query,
            v_queue_item.hash,
            v_queue_item.partition_key,
            v_queue_item.partition_datatype,
            p_queue_prefix,
            p_table_prefix,
            p_cache_backend,
            'cron'
        );
        RAISE NOTICE 'Job processed %', dispatched_job_id;
    END;
    dispatched_job_id := 'job_' || v_queue_item.id;
    status := 'processed';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Include the complete _partitioncache_execute_job function
CREATE OR REPLACE FUNCTION _partitioncache_execute_job(
    p_item_id INTEGER,
    p_query TEXT,
    p_query_hash TEXT,
    p_partition_key TEXT,
    p_partition_datatype TEXT,
    p_queue_prefix TEXT,
    p_table_prefix TEXT,
    p_cache_backend TEXT,
    p_execution_source TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    v_job_id TEXT;
    v_start_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_cache_table TEXT;
    v_insert_query TEXT;
    v_datatype TEXT;
    v_metadata_table TEXT;
    v_queries_table TEXT;
    v_active_jobs_table TEXT;
    v_log_table TEXT;
    v_config_table TEXT;
    v_bitsize INTEGER;
    v_bit_query TEXT;
    v_already_processed BOOLEAN;
    v_timeout_seconds INTEGER;
    v_result_limit INTEGER;
BEGIN
    v_job_id := 'job_' || p_item_id;
    v_start_time := clock_timestamp();
    
    -- Set table names
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    v_log_table := p_queue_prefix || '_processor_log';
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Default timeout and result_limit (config comes from cron database)
    v_timeout_seconds := 1800;
    v_result_limit := NULL;
    
    BEGIN
        -- Process the item using the same logic as the background processor
        PERFORM partitioncache_ensure_metadata_tables(p_table_prefix);
        
        v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
        v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
        
        -- Datatype detection logic
        IF p_partition_datatype IS NOT NULL AND p_partition_datatype != '' THEN
            v_datatype := p_partition_datatype;
        ELSE
            -- Check if partition already exists in metadata
            EXECUTE format('SELECT datatype FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key)
            INTO v_datatype;
            
            IF v_datatype IS NULL THEN
                -- Try to get actual column datatype from information_schema
                BEGIN
                    EXECUTE format('
                        SELECT data_type 
                        FROM information_schema.columns 
                        WHERE column_name = %L 
                        AND table_name IN (
                            SELECT DISTINCT table_name 
                            FROM information_schema.tables 
                            WHERE table_type = ''BASE TABLE''
                            AND table_schema = ''public''
                        )
                        LIMIT 1
                    ', p_partition_key) INTO v_datatype;
                    
                    -- Map PostgreSQL data types to our cache types
                    IF v_datatype IS NOT NULL THEN
                        v_datatype := CASE v_datatype
                            WHEN 'integer' THEN 'integer'
                            WHEN 'bigint' THEN 'integer'
                            WHEN 'smallint' THEN 'integer'
                            WHEN 'real' THEN 'float'
                            WHEN 'double precision' THEN 'float'
                            WHEN 'numeric' THEN 'float'
                            WHEN 'timestamp without time zone' THEN 'timestamp'
                            WHEN 'timestamp with time zone' THEN 'timestamp'
                            WHEN 'date' THEN 'timestamp'
                            ELSE 'text'
                        END;
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_datatype := NULL;
                END;
                
                -- Fall back to regex-based detection if still unknown
                IF v_datatype IS NULL THEN
                    BEGIN
                        EXECUTE format('
                            WITH sample_data AS (SELECT %s::text as sample_value FROM (%s) query_result WHERE %s IS NOT NULL LIMIT 10)
                            SELECT CASE WHEN COUNT(*) = 0 THEN ''text'' WHEN COUNT(CASE WHEN sample_value ~ ''^[-+]?[0-9]+$'' THEN 1 END) = COUNT(*) THEN ''integer'' WHEN COUNT(CASE WHEN sample_value ~ ''^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$'' THEN 1 END) = COUNT(*) THEN ''float'' WHEN COUNT(CASE WHEN sample_value ~ ''^\\d{4}-\\d{2}-\\d{2}'' THEN 1 END) = COUNT(*) THEN ''timestamp'' ELSE ''text'' END FROM sample_data
                        ', p_partition_key, p_query, p_partition_key) INTO v_datatype;
                    EXCEPTION WHEN OTHERS THEN
                        v_datatype := 'text';
                    END;
                END IF;
            END IF;
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = v_cache_table) THEN
            DECLARE v_bitsize_for_bootstrap INTEGER;
            BEGIN
                IF p_cache_backend = 'bit' THEN
                    EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key) INTO v_bitsize_for_bootstrap;
                    IF v_bitsize_for_bootstrap IS NULL THEN v_bitsize_for_bootstrap := 1000; END IF;
                END IF;
                PERFORM partitioncache_bootstrap_partition(p_table_prefix, p_partition_key, v_datatype, p_cache_backend, v_bitsize_for_bootstrap);
            END;
        END IF;
        
        -- Build cache insertion query based on backend type
        IF p_cache_backend = 'bit' THEN
            EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key) INTO v_bitsize;
            v_bit_query := format(
                'WITH bit_positions AS (
                    SELECT %s::INTEGER AS position
                    FROM (%s) AS query_result
                    WHERE %s::INTEGER >= 0
                ),
                max_position AS (
                    SELECT COALESCE(MAX(position), 0) AS max_pos FROM bit_positions
                ),
                bit_array AS (
                    SELECT generate_series(0, max_pos) AS bit_index
                    FROM max_position
                ),
                bit_string AS (
                    SELECT string_agg(
                        CASE WHEN bit_array.bit_index IN (SELECT position FROM bit_positions) 
                             THEN ''1'' 
                             ELSE ''0'' 
                        END,
                        ''''
                        ORDER BY bit_array.bit_index
                    ) AS bit_value
                    FROM bit_array
                )
                SELECT bit_value::BIT VARYING FROM bit_string',
                p_partition_key, p_query, p_partition_key
            );
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, (%s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, v_bit_query);
        ELSIF p_cache_backend = 'roaringbit' THEN
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, rb_build_agg(%s::INTEGER) FROM (%s) AS query_result ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, p_partition_key, p_query);
        ELSIF p_cache_backend = 'array' THEN
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, ARRAY(SELECT %s FROM (%s) AS query_result) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, p_partition_key, p_query);
        ELSE
            RAISE EXCEPTION 'Unsupported cache_backend type: %', p_cache_backend;
        END IF;
        
        -- Security check
        IF p_query LIKE '%DELETE %' OR p_query LIKE '%DROP %' THEN
            RAISE EXCEPTION 'Query contains DELETE or DROP';
        END IF;

        EXECUTE v_insert_query;
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        
        -- Check result limit if configured
        IF v_result_limit IS NOT NULL THEN
            DECLARE
                v_partition_count INTEGER;
            BEGIN
                EXECUTE format('SELECT partition_keys_count FROM %I WHERE query_hash = %L', v_cache_table, p_query_hash) 
                INTO v_partition_count;
                
                IF v_partition_count IS NOT NULL AND v_partition_count >= v_result_limit THEN
                    -- Limit exceeded: remove from cache and set limit status
                    EXECUTE format('DELETE FROM %I WHERE query_hash = %L', v_cache_table, p_query_hash);
                    EXECUTE format('INSERT INTO %I (query_hash, partition_key, query, status, last_seen) VALUES (%L, %L, %L, ''limit'', now()) ON CONFLICT (query_hash, partition_key) DO UPDATE SET status = ''limit'', last_seen = now()', v_queries_table, p_query_hash, p_partition_key, p_query);
                    
                    -- Clean up active job
                    EXECUTE format('DELETE FROM %I WHERE job_id = %L', v_active_jobs_table, v_job_id);
                    
                    -- Log as success with limit exceeded
                    EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_time_ms, execution_source) VALUES (%L, %L, %L, %L, %L, %L, %L)', 
                        v_log_table, v_job_id, p_query_hash, p_partition_key, 'success', 'Result limit exceeded: ' || v_partition_count || ' >= ' || v_result_limit, 
                        ROUND(extract(epoch from (clock_timestamp() - v_start_time)) * 1000, 3), p_execution_source);
                    
                    RETURN true;
                END IF;
            END;
        END IF;
        
        -- Insert/update queries table with 'ok' status
        EXECUTE format('INSERT INTO %I (query_hash, partition_key, query, status, last_seen) VALUES (%L, %L, %L, ''ok'', now()) ON CONFLICT (query_hash, partition_key) DO UPDATE SET status = ''ok'', last_seen = now()', v_queries_table, p_query_hash, p_partition_key, p_query);
        
        -- Clean up active job
        EXECUTE format('DELETE FROM %I WHERE job_id = %L', v_active_jobs_table, v_job_id);
        
        -- Log success
        EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, rows_affected, execution_time_ms, execution_source) VALUES (%L, %L, %L, %L, %L, %L, %L)', 
            v_log_table, v_job_id, p_query_hash, p_partition_key, 'success', v_rows_affected, 
            ROUND(extract(epoch from (clock_timestamp() - v_start_time)) * 1000, 3), p_execution_source);
        
        RETURN true;
        
    EXCEPTION 
        WHEN query_canceled THEN
            -- Clean up active job
            EXECUTE format('DELETE FROM %I WHERE job_id = %L', v_active_jobs_table, v_job_id);

            -- Mark query as timed out to prevent re-processing
            EXECUTE format('INSERT INTO %I (query_hash, partition_key, query, status, last_seen) VALUES (%L, %L, %L, ''timeout'', now()) ON CONFLICT (query_hash, partition_key) DO UPDATE SET status = ''timeout'', last_seen = now()', v_queries_table, p_query_hash, p_partition_key, p_query);

            -- Log timeout
            EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_time_ms, execution_source) VALUES (%L, %L, %L, %L, %L, %L, %L)', 
                v_log_table, v_job_id, p_query_hash, p_partition_key, 'timeout', 'Query timed out after ' || v_timeout_seconds || ' seconds.', 
                ROUND(extract(epoch from (clock_timestamp() - v_start_time)) * 1000, 3), p_execution_source);
        
            RETURN false;
        WHEN OTHERS THEN
            -- Clean up active job
            EXECUTE format('DELETE FROM %I WHERE job_id = %L', v_active_jobs_table, v_job_id);
            
            -- Mark query as failed to prevent re-processing
            EXECUTE format('INSERT INTO %I (query_hash, partition_key, query, status, last_seen) VALUES (%L, %L, %L, ''failed'', now()) ON CONFLICT (query_hash, partition_key) DO UPDATE SET status = ''failed'', last_seen = now()', v_queries_table, p_query_hash, p_partition_key, p_query);

            -- Log failure
            EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_time_ms, execution_source) VALUES (%L, %L, %L, %L, %L, %L, %L)', 
                v_log_table, v_job_id, p_query_hash, p_partition_key, 'failed', SQLERRM, 
                ROUND(extract(epoch from (clock_timestamp() - v_start_time)) * 1000, 3), p_execution_source);
        
            RETURN false;
    END;
END;
$$ LANGUAGE plpgsql;

-- Note: No backward compatibility wrapper needed since manual commands 
-- read config from cron database and call parameter-based function directly

-- Manual processing function for testing - reads config and calls parameter-based function
CREATE OR REPLACE FUNCTION partitioncache_manual_process_queue(p_count INTEGER DEFAULT 100)
RETURNS TABLE(processed_count INTEGER, message TEXT) AS $$
DECLARE
    v_total INTEGER := 0;
    v_result RECORD;
    v_job_name TEXT := 'partitioncache_process_queue';
    v_config RECORD;
    v_config_table TEXT;
BEGIN
    -- Try to get configuration from local config table first
    v_config_table := COALESCE(current_setting('partitioncache.queue_prefix', true), 'partitioncache_queue') || '_processor_config';
    
    BEGIN
        EXECUTE format('SELECT job_name, enabled, max_parallel_jobs, frequency_seconds, timeout_seconds, table_prefix, queue_prefix, cache_backend, target_database, result_limit FROM %I WHERE job_name = %L', 
                      v_config_table, v_job_name) INTO v_config;
    EXCEPTION WHEN OTHERS THEN
        v_config := NULL;
    END;
    
    -- If no local config found, try to find any config table
    IF v_config IS NULL THEN
        SELECT tablename INTO v_config_table
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '%_processor_config'
        AND EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = tablename 
            AND column_name = 'job_name'
        )
        LIMIT 1;
        
        IF v_config_table IS NOT NULL THEN
            EXECUTE format('SELECT job_name, enabled, max_parallel_jobs, frequency_seconds, timeout_seconds, table_prefix, queue_prefix, cache_backend, target_database, result_limit FROM %I WHERE job_name = %L', 
                          v_config_table, v_job_name) INTO v_config;
        END IF;
        
        IF v_config IS NULL THEN
            RETURN QUERY SELECT 0, 'No configuration found for job. Please run setup first.';
            RETURN;
        END IF;
    END IF;

    FOR i IN 1..p_count LOOP
        -- Call the parameter-based function with config values
        SELECT * FROM partitioncache_run_single_job_with_params(
            v_job_name,
            v_config.table_prefix,
            v_config.queue_prefix, 
            v_config.cache_backend,
            v_config.timeout_seconds,
            v_config.result_limit
        ) INTO v_result;

        -- If the queue is empty, exit the loop.
        IF v_result.status = 'no_jobs_available' THEN
            EXIT;
        END IF;

        -- We count anything that wasn't a "no_jobs" as a processed attempt.
        IF v_result.dispatched_job_id IS NOT NULL THEN
            v_total := v_total + 1;
        END IF;
    END LOOP;
    
    RETURN QUERY SELECT v_total, 
        CASE 
            WHEN v_total = 0 THEN 'No items processed or queue was empty'
            ELSE format('Attempted to process %s items', v_total)
        END;
END;
$$ LANGUAGE plpgsql;

-- Helper function to clean up cached items from queue during idle time
CREATE OR REPLACE FUNCTION partitioncache_cleanup_cached_queue_items(
    p_queue_prefix TEXT DEFAULT 'partitioncache_queue',
    p_table_prefix TEXT DEFAULT 'partitioncache_bit', 
    p_limit INTEGER DEFAULT 50
)
RETURNS INTEGER AS $$
DECLARE
    v_queue_table TEXT;
    v_queries_table TEXT;
    v_cleaned_count INTEGER;
BEGIN
    -- Build table names using existing helper functions
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Single atomic operation: remove cached items and update last_seen
    EXECUTE format('
        WITH cleaned AS (
            DELETE FROM %I q
            WHERE id IN (
                SELECT q.id 
                FROM %I q
                INNER JOIN %I qr ON qr.query_hash = q.hash 
                    AND qr.partition_key = q.partition_key
                ORDER BY q.priority DESC, q.id
                FOR UPDATE SKIP LOCKED  -- Avoid conflicts with other workers
                LIMIT %L
            )
            RETURNING q.hash, q.partition_key
        )
        UPDATE %I qr
        SET last_seen = now()
        FROM cleaned c
        WHERE qr.query_hash = c.hash 
        AND qr.partition_key = c.partition_key',
        v_queue_table,
        v_queue_table,
        v_queries_table,
        p_limit,
        v_queries_table
    );
    
    GET DIAGNOSTICS v_cleaned_count = ROW_COUNT;
    RETURN v_cleaned_count;
END;
$$ LANGUAGE plpgsql;