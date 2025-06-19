-- PartitionCache Direct PostgreSQL Queue Processor
-- This file contains all the necessary SQL objects to process queries directly within PostgreSQL
-- using pg_cron for scheduling
--
-- Main Functions:
-- - partitioncache_initialize_processor_tables(): Initialize required tables with correct prefixes
-- - partitioncache_process_queue(): Main processor function called by pg_cron
-- - partitioncache_process_queue_item(): Process a single queue item
-- - partitioncache_get_processor_status(): Get basic processor status
-- - partitioncache_get_processor_status_detailed(): Get detailed status with cache info
-- - partitioncache_get_queue_and_cache_info(): Get detailed queue and cache architecture info
-- - partitioncache_set_processor_enabled(): Enable/disable processor
-- - partitioncache_update_processor_config(): Update processor configuration
-- - partitioncache_manual_process_queue(): Manually trigger processing (for testing)
--
-- Helper Functions:
-- - partitioncache_get_cache_table_name(): Get cache table name for a partition
-- - partitioncache_can_process_more_jobs(): Check if more jobs can be processed
-- - partitioncache_cleanup_stale_jobs(): Clean up stale active jobs

-- Function to initialize processor tables with correct prefixes
CREATE OR REPLACE FUNCTION partitioncache_initialize_processor_tables(p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_log_table TEXT;
    v_active_jobs_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    v_log_table := p_queue_prefix || '_processor_log';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    
    -- Configuration table to control the processor
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1), -- Ensure only one row
            enabled BOOLEAN NOT NULL DEFAULT false,
            max_parallel_jobs INTEGER NOT NULL DEFAULT 5 CHECK (max_parallel_jobs > 0 AND max_parallel_jobs <= 20),
            frequency_seconds INTEGER NOT NULL DEFAULT 1 CHECK (frequency_seconds > 0),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);
    
    -- Insert default configuration if not exists
    EXECUTE format('
        INSERT INTO %I (enabled, max_parallel_jobs, frequency_seconds)
        VALUES (false, 5, 1)
        ON CONFLICT (id) DO NOTHING', v_config_table);
    
    -- Log table for job execution history
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL,
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN (''started'', ''success'', ''failed'', ''timeout'')),
            error_message TEXT,
            rows_affected INTEGER,
            execution_time_ms INTEGER,
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
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (query_hash, partition_key)
        )', v_active_jobs_table);
END;
$$ LANGUAGE plpgsql;

-- Function to get the target cache table name for a partition
CREATE OR REPLACE FUNCTION partitioncache_get_cache_table_name(p_table_prefix TEXT, p_partition_key TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Generate single underscore table names consistently
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'cache_' || p_partition_key;
    ELSIF right(p_table_prefix, 1) = '_' THEN
        -- Prefix already has trailing underscore, use it directly
        RETURN p_table_prefix || 'cache_' || p_partition_key;
    ELSE
        -- Prefix doesn't have trailing underscore, add one
        RETURN p_table_prefix || '_cache_' || p_partition_key;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get the metadata table name
CREATE OR REPLACE FUNCTION partitioncache_get_metadata_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Generate single underscore table names consistently
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'partition_metadata';
    ELSIF right(p_table_prefix, 1) = '_' THEN
        -- Prefix already has trailing underscore, use it directly
        RETURN p_table_prefix || 'partition_metadata';
    ELSE
        -- Prefix doesn't have trailing underscore, add one
        RETURN p_table_prefix || '_partition_metadata';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get the queries table name
CREATE OR REPLACE FUNCTION partitioncache_get_queries_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Generate single underscore table names consistently
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'queries';
    ELSIF right(p_table_prefix, 1) = '_' THEN
        -- Prefix already has trailing underscore, use it directly
        RETURN p_table_prefix || 'queries';
    ELSE
        -- Prefix doesn't have trailing underscore, add one
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
    IF p_table_prefix LIKE '%_bit%' THEN
        v_cache_backend := 'bit';
    ELSE
        v_cache_backend := 'array';
    END IF;
    
    -- Create metadata table
    IF v_cache_backend = 'bit' THEN
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype IN (''integer'', ''float'', ''text'', ''timestamp'')),
                bitsize INTEGER NOT NULL DEFAULT 1000,
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
    
    -- Create queries table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            query TEXT NOT NULL,
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
    p_cache_backend TEXT DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    v_metadata_table TEXT;
    v_cache_table TEXT;
    v_partition_exists BOOLEAN;
    v_bitsize INTEGER := 1000; -- Default bitsize for bit caches
    v_backend TEXT;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
    
    -- Check if partition already exists
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE partition_key = %L)', v_metadata_table, p_partition_key)
    INTO v_partition_exists;
    
    IF v_partition_exists THEN
        RETURN true; -- Already exists, nothing to do
    END IF;
    
    -- Determine cache backend from environment or parameter
    v_backend := COALESCE(p_cache_backend, 'array'); -- Default to array if not specified
    
    -- Create metadata entry
    IF v_backend = 'bit' THEN
        -- For bit cache, include bitsize
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype, bitsize) VALUES (%L, %L, %L)',
            v_metadata_table, p_partition_key, p_datatype, v_bitsize
        );
        
        -- Create bit cache table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys BIT VARYING,
                partition_keys_count INTEGER NOT NULL GENERATED ALWAYS AS (length(replace(partition_keys::text, ''0'', ''''))) STORED
            )', v_cache_table);
    ELSE
        -- For array cache
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype) VALUES (%L, %L)',
            v_metadata_table, p_partition_key, p_datatype
        );
        
        -- Create array cache table
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys %s[]
                partition_keys_count integer NOT NULL GENERATED ALWAYS AS length(partition_keys) STORED
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

-- Function to check if we can process more jobs
CREATE OR REPLACE FUNCTION partitioncache_can_process_more_jobs(p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS BOOLEAN AS $$
DECLARE
    v_config RECORD;
    v_active_count INTEGER;
    v_config_table TEXT;
    v_active_jobs_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    
    -- Get configuration
    EXECUTE format('SELECT enabled, max_parallel_jobs FROM %I LIMIT 1', v_config_table)
    INTO v_config;
    
    -- Check if processing is enabled
    IF NOT v_config.enabled THEN
        RETURN false;
    END IF;
    
    -- Count active jobs
    EXECUTE format('SELECT COUNT(*) FROM %I', v_active_jobs_table)
    INTO v_active_count;
    
    RETURN v_active_count < v_config.max_parallel_jobs;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up stale active jobs (older than 5 minutes)
CREATE OR REPLACE FUNCTION partitioncache_cleanup_stale_jobs(p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS INTEGER AS $$
DECLARE
    v_deleted INTEGER;
    v_active_jobs_table TEXT;
    v_processor_log_table TEXT;
BEGIN
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    v_processor_log_table := p_queue_prefix || '_processor_log';
    
    -- Log stale job cleanup
    EXECUTE format('
        INSERT INTO %I (job_id, query_hash, partition_key, status, error_message)
        SELECT job_id, query_hash, partition_key, ''timeout'', ''Job timed out after 5 minutes''
        FROM %I
        WHERE started_at < NOW() - INTERVAL ''5 minutes''
    ', v_processor_log_table, v_active_jobs_table);
    
    -- Delete stale jobs
    EXECUTE format('DELETE FROM %I WHERE started_at < NOW() - INTERVAL ''5 minutes''', v_active_jobs_table);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    RETURN v_deleted;
END;
$$ LANGUAGE plpgsql;

-- Main function to process a single query from the queue
CREATE OR REPLACE FUNCTION partitioncache_process_queue_item(p_table_prefix TEXT, p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
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
    v_queue_table TEXT;
    v_cache_backend TEXT;
    v_bitsize INTEGER;
    v_bit_query TEXT;
BEGIN
    -- Clean up stale jobs first
    PERFORM partitioncache_cleanup_stale_jobs(p_queue_prefix);
    
    -- Check if we can process more jobs
    IF NOT partitioncache_can_process_more_jobs(p_queue_prefix) THEN
        RETURN QUERY SELECT false, 'Max parallel jobs reached or processing disabled';
        RETURN;
    END IF;
    
    -- Set table names with prefixes
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    
    -- Try to get a query fragment from the queue
    -- Using SELECT FOR UPDATE SKIP LOCKED to handle concurrent access
    EXECUTE format('
        SELECT id, query, hash, partition_key, partition_datatype
        FROM %I
        WHERE NOT EXISTS (
            SELECT 1 FROM %I aj
            WHERE aj.query_hash = %I.hash
            AND aj.partition_key = %I.partition_key
        )
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ', v_queue_table, p_queue_prefix || '_active_jobs', v_queue_table, v_queue_table)
    INTO v_queue_item;
    
    -- No item found - check if queue_item is NULL instead of relying on FOUND
    IF v_queue_item.id IS NULL THEN
        RETURN QUERY SELECT false, 'No items in queue';
        RETURN;
    END IF;

    -- TODO also check if the query is already in the queries table ()
    
    -- Generate job ID
    v_job_id := 'job_' || extract(epoch from now())::text || '_' || v_queue_item.id::text;
    v_start_time := now();
    
    -- Register as active job
    EXECUTE format('INSERT INTO %I (query_hash, partition_key, job_id) VALUES (%L, %L, %L)',
        p_queue_prefix || '_active_jobs', v_queue_item.hash, v_queue_item.partition_key, v_job_id);
    
    -- Log job start
    EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status) VALUES (%L, %L, %L, %L)',
        p_queue_prefix || '_processor_log', v_job_id, v_queue_item.hash, v_queue_item.partition_key, 'started');
    
    BEGIN
        -- Ensure metadata and queries tables exist with correct naming
        PERFORM partitioncache_ensure_metadata_tables(p_table_prefix);
        
        -- Get the cache table name
        v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, v_queue_item.partition_key);
        v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
        v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
        
        -- Get datatype from metadata if not provided
        IF v_queue_item.partition_datatype IS NULL THEN
            EXECUTE format('SELECT datatype FROM %I WHERE partition_key = %L', v_metadata_table, v_queue_item.partition_key)
            INTO v_datatype;
        ELSE
            v_datatype := v_queue_item.partition_datatype;
        END IF;
        
        -- If datatype is still null, try to infer it from the query results
        IF v_datatype IS NULL THEN
            -- Try to detect datatype by examining a sample of the query results
            BEGIN
                EXECUTE format('
                    WITH sample_data AS (
                        SELECT %s as sample_value 
                        FROM (%s) query_result 
                        WHERE %s IS NOT NULL 
                        LIMIT 10
                    ),
                    type_analysis AS (
                        SELECT 
                            COUNT(*) as total_samples,
                            COUNT(CASE WHEN sample_value ~ ''^[0-9]+$'' THEN 1 END) as integer_count,
                            COUNT(CASE WHEN sample_value ~ ''^[0-9]+\.[0-9]+$'' THEN 1 END) as float_count,
                            COUNT(CASE WHEN sample_value ~ ''^[0-9]{4}-[0-9]{2}-[0-9]{2}'' THEN 1 END) as date_count
                        FROM sample_data
                    )
                    SELECT 
                        CASE 
                            WHEN total_samples = 0 THEN ''text''
                            WHEN integer_count = total_samples THEN ''integer''
                            WHEN float_count + integer_count = total_samples THEN ''float''
                            WHEN date_count = total_samples THEN ''timestamp''
                            ELSE ''text''
                        END as detected_type
                    FROM type_analysis
                ', v_queue_item.partition_key, v_queue_item.query, v_queue_item.partition_key)
                INTO v_datatype;
                
                -- Log the detection for debugging
                RAISE NOTICE 'Auto-detected datatype for partition %: %', v_queue_item.partition_key, v_datatype;
                
            EXCEPTION WHEN OTHERS THEN
                -- If detection fails, fall back to heuristics based on partition key name
                CASE 
                    WHEN v_queue_item.partition_key ILIKE '%zip%' OR v_queue_item.partition_key ILIKE '%code%' THEN
                        v_datatype := 'integer';
                    WHEN v_queue_item.partition_key ILIKE '%id%' THEN
                        v_datatype := 'integer';
                    WHEN v_queue_item.partition_key ILIKE '%date%' OR v_queue_item.partition_key ILIKE '%time%' THEN
                        v_datatype := 'timestamp';
                    ELSE
                        v_datatype := 'text';
                END CASE;
                
                RAISE NOTICE 'Datatype detection failed, using heuristic for partition %: %', v_queue_item.partition_key, v_datatype;
            END;
        END IF;
        
        -- Bootstrap partition if it doesn't exist yet
        -- Determine cache backend type from environment variables or existing table structure
        SELECT CASE 
            WHEN EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = v_cache_table 
                AND column_name = 'partition_keys'
                AND data_type IN ('bit', 'bit varying')
            ) THEN 'bit'
            WHEN EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = v_cache_table 
                AND column_name = 'partition_keys'
                AND (data_type LIKE '%[]' OR data_type = 'ARRAY')
            ) THEN 'array'
            WHEN EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = v_cache_table
            ) THEN 'unknown'
            ELSE NULL -- Table doesn't exist yet
        END INTO v_cache_backend;
        
        -- If cache table doesn't exist, bootstrap it %TODO: Make this more robust
        IF v_cache_backend IS NULL THEN
            -- Determine backend type from table prefix or default to array
            IF p_table_prefix LIKE '%_bit%' THEN
                v_cache_backend := 'bit';
            ELSE
                v_cache_backend := 'array';
            END IF;
            
            -- Bootstrap the partition
            PERFORM partitioncache_bootstrap_partition(p_table_prefix, v_queue_item.partition_key, v_datatype, v_cache_backend);
        END IF;
        
        -- Build the INSERT query based on cache backend type
        IF v_cache_backend = 'bit' THEN
            -- For bit cache, we need to convert the result set to a bit string
            -- Get bitsize from metadata
            EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, v_queue_item.partition_key)
            INTO v_bitsize;
            
            -- If bitsize not found, use default of 1000
            IF v_bitsize IS NULL THEN
                v_bitsize := 1000;
            END IF;
            
            -- Build bit conversion query
            v_bit_query := format(
                'WITH bit_positions AS (
                    SELECT %s::INTEGER AS position
                    FROM (%s) AS query_result
                    WHERE %s::INTEGER >= 0 AND %s::INTEGER < %s
                ),
                bit_array AS (
                    SELECT generate_series(0, %s - 1) AS bit_index
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
                v_queue_item.partition_key,
                v_queue_item.query,
                v_queue_item.partition_key,
                v_queue_item.partition_key,
                v_bitsize,
                v_bitsize
            );
            
            v_insert_query := format(
                'INSERT INTO %I (query_hash, partition_keys) 
                 SELECT %L, (%s)
                 ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys',
                v_cache_table,
                v_queue_item.hash,
                v_bit_query
            );
        ELSIF v_cache_backend = 'array' THEN
            -- For array cache with partition_keys column
            v_insert_query := format(
                'INSERT INTO %I (query_hash, partition_keys) 
                 SELECT %L, ARRAY(
                     %s
                 )
                 ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys',
                v_cache_table,
                v_queue_item.hash,
                v_queue_item.query
            );
        ELSE
            RAISE EXCEPTION 'Unsupported cache table structure for table %. Expected partition_keys column with bit or array datatype.', v_cache_table;
        END IF;
        
        -- Execute the insert query
        EXECUTE v_insert_query;
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        
        -- Also update the queries table
        EXECUTE format(
            'INSERT INTO %I (query_hash, partition_key, query, last_seen)
             VALUES (%L, %L, %L, now())
             ON CONFLICT (query_hash, partition_key) DO UPDATE SET last_seen = now()',
              -- TODO also set runtime of query here
              -- TODO also set status {ok, timeout, too_many_results}
              -- TODO also set highest_partition_key_in_db (geht nur für ints.) Alternativ einen anderen indetifier der abbildet wie aktuell der eintrag ist. (anzahl partitionen keys etc)
            v_queries_table,
            v_queue_item.hash,
            v_queue_item.partition_key,
            v_queue_item.query
        );
        
        -- Delete from queue
        EXECUTE format('DELETE FROM %I WHERE id = %L', v_queue_table, v_queue_item.id);
        
        -- Remove from active jobs
        EXECUTE format('DELETE FROM %I WHERE query_hash = %L AND partition_key = %L',
            p_queue_prefix || '_active_jobs', v_queue_item.hash, v_queue_item.partition_key);
        
        -- Log success
        EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, rows_affected, execution_time_ms) VALUES (%L, %L, %L, %L, %L, %L)',
            p_queue_prefix || '_processor_log', v_job_id, v_queue_item.hash, v_queue_item.partition_key, 'success', v_rows_affected, extract(milliseconds from (now() - v_start_time))::integer);
        
        RETURN QUERY SELECT true, format('Processed query %s for partition %s', v_queue_item.hash, v_queue_item.partition_key);
        
    EXCEPTION WHEN OTHERS THEN  -- TODO also if timeout? Then we need to set the status to timeout to prevent infinite retries
        -- Remove from active jobs
        EXECUTE format('DELETE FROM %I WHERE query_hash = %L AND partition_key = %L',
            p_queue_prefix || '_active_jobs', v_queue_item.hash, v_queue_item.partition_key);
        
        -- Log failure
        EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_time_ms) VALUES (%L, %L, %L, %L, %L, %L)',
            p_queue_prefix || '_processor_log', v_job_id, v_queue_item.hash, v_queue_item.partition_key, 'failed', SQLERRM, extract(milliseconds from (now() - v_start_time))::integer);
        
        -- Re-raise the error
        RAISE EXCEPTION 'Failed to process query %: %', v_queue_item.hash, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;

-- Main processor function that will be called by pg_cron
CREATE OR REPLACE FUNCTION partitioncache_process_queue(p_table_prefix TEXT DEFAULT 'partitioncache', p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS TABLE(processed_count INTEGER, message TEXT) AS $$
DECLARE
    v_processed_count INTEGER := 0;
    v_result RECORD;
    v_max_iterations INTEGER;
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    
    -- Get max parallel jobs for iteration limit
    EXECUTE format('SELECT max_parallel_jobs FROM %I LIMIT 1', v_config_table)
    INTO v_max_iterations;
    
    -- Process up to max_parallel_jobs items
    FOR i IN 1..COALESCE(v_max_iterations, 5) LOOP
        SELECT * INTO v_result FROM partitioncache_process_queue_item(p_table_prefix, p_queue_prefix);
        
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



-- Function to enable/disable the processor
CREATE OR REPLACE FUNCTION partitioncache_set_processor_enabled(p_enabled BOOLEAN, p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    
    EXECUTE format('UPDATE %I SET enabled = %L, updated_at = NOW() WHERE id = 1', v_config_table, p_enabled);
END;
$$ LANGUAGE plpgsql;

-- Function to update processor configuration
CREATE OR REPLACE FUNCTION partitioncache_update_processor_config(
    p_enabled BOOLEAN DEFAULT NULL,
    p_max_parallel_jobs INTEGER DEFAULT NULL,
    p_frequency_seconds INTEGER DEFAULT NULL,
    p_queue_prefix TEXT DEFAULT 'partitioncache_queue'
)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    
    EXECUTE format('
        UPDATE %I 
        SET 
            enabled = COALESCE(%L, enabled),
            max_parallel_jobs = COALESCE(%L, max_parallel_jobs),
            frequency_seconds = COALESCE(%L, frequency_seconds),
            updated_at = NOW()
        WHERE id = 1',
        v_config_table, p_enabled, p_max_parallel_jobs, p_frequency_seconds);
END;
$$ LANGUAGE plpgsql;

-- Helper function to manually trigger processing (useful for testing)
CREATE OR REPLACE FUNCTION partitioncache_manual_process_queue(p_count INTEGER DEFAULT 1, p_table_prefix TEXT DEFAULT 'partitioncache', p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS TABLE(total_processed INTEGER, messages TEXT[]) AS $$
DECLARE
    v_total INTEGER := 0;
    v_messages TEXT[] := ARRAY[]::TEXT[];
    v_result RECORD;
BEGIN
    FOR i IN 1..p_count LOOP
        SELECT * INTO v_result FROM partitioncache_process_queue_item(p_table_prefix, p_queue_prefix);
        
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