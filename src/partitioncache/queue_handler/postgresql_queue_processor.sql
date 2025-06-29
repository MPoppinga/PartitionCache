-- PartitionCache Direct PostgreSQL Queue Processor
-- This file contains all the necessary SQL objects to process queries directly within PostgreSQL
-- using pg_cron for scheduling
--
-- Architecture Overview:
-- ┌─────────────────────┐
-- │  pg_cron Scheduler  │
-- │  ┌────────────────┐ │
-- │  │ N cron jobs    │ │
-- │  │ run every      │ │
-- │  │ X seconds      │ │
-- │  └────────┬───────┘ │
-- └───────────┼─────────┘
--             │triggers
--             ▼
-- ┌─────────────────────────────────────┐
-- │     Worker Function                 │
-- │  ┌──────────────────────────────┐  │
-- │  │ partitioncache_run_single_job│  │
-- │  └──────────┬───────────────────┘  │
-- │             ▼                      │
-- │  ┌──────────────────────────────┐  │
-- │  │ Dequeue 1 item with          │  │
-- │  │ FOR UPDATE SKIP LOCKED       │  │
-- │  └──────────┬───────────────────┘  │
-- │             ▼                      │
-- │  ┌──────────────────────────────┐  │
-- │  │ _partitioncache_execute_job  │───> Process item
-- │  └──────────────────────────────┘  │
-- └────────────────────────────────────┘
--
-- Main Functions:
-- - partitioncache_initialize_processor_tables(): Initialize required tables with correct prefixes
-- - partitioncache_run_single_job(): Entrypoint for one cron worker; processes one queue item.
-- - _partitioncache_execute_job(): The core internal logic for processing a single job.
-- - partitioncache_set_processor_enabled(): Enable/disable processor
-- - partitioncache_update_processor_config(): Update processor configuration
-- - partitioncache_manual_process_queue(): Manually trigger processing for testing.
--
-- Helper Functions:
-- - partitioncache_sync_cron_job(): Trigger to create/update/delete cron jobs based on config.
-- - partitioncache_handle_timeouts(): Dedicated function (called by its own cron job) to handle timeouts.
-- - partitioncache_cleanup_stale_jobs(): Dedicated function (called by its own cron job) for stale job recovery.

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
            job_name TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT false,
            max_parallel_jobs INTEGER NOT NULL DEFAULT 5 CHECK (max_parallel_jobs > 0 AND max_parallel_jobs <= 20),
            frequency_seconds INTEGER NOT NULL DEFAULT 1 CHECK (frequency_seconds > 0),
            timeout_seconds INTEGER NOT NULL DEFAULT 1800 CHECK (timeout_seconds > 0), -- Default 30 minutes
            stale_after_seconds INTEGER NOT NULL DEFAULT 3600 CHECK (stale_after_seconds > 0),
            table_prefix TEXT NOT NULL,
            queue_prefix TEXT NOT NULL,
            cache_backend TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);
    
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

-- Trigger function to synchronize config with cron.job
CREATE OR REPLACE FUNCTION partitioncache_sync_cron_job()
RETURNS TRIGGER AS $$
DECLARE
    v_command TEXT;
    v_job_base TEXT;
    v_timeout_seconds INTEGER;
    v_timeout_statement TEXT;
BEGIN
    v_job_base := NEW.job_name; -- base name, e.g. partitioncache_process_queue

    IF (TG_OP = 'DELETE') THEN
        -- Remove all jobs that start with the base name
        DELETE FROM cron.job WHERE jobname LIKE v_job_base || '%';
        RETURN OLD;
    END IF;

    -- Get timeout from the config row being inserted/updated
    v_timeout_seconds := NEW.timeout_seconds;
    IF v_timeout_seconds IS NULL THEN v_timeout_seconds := 1800; END IF;

    -- Set a local timeout for this specific transaction
    -- The timeout is specified in milliseconds.
    v_timeout_statement := 'SET LOCAL statement_timeout = ' || (v_timeout_seconds * 1000)::TEXT;
    

    -- Build the command string executed by every worker , with the timeout set in transaction
    v_command := format('BEGIN; %s; SELECT * FROM partitioncache_run_single_job(%L); COMMIT;', v_timeout_statement, v_job_base);

    -- Remove existing worker jobs and recreate to match max_parallel_jobs
    DELETE FROM cron.job WHERE jobname LIKE v_job_base || '_%';
    


    FOR i IN 1..NEW.max_parallel_jobs LOOP
        INSERT INTO cron.job (jobname, schedule, command, active)
        VALUES (
            v_job_base || '_' || i,
            CONCAT(NEW.frequency_seconds, ' seconds'),
            v_command,
            NEW.enabled
        );
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to attach the trigger to the config table
CREATE OR REPLACE FUNCTION partitioncache_create_config_trigger(p_queue_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_trigger_name TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    v_trigger_name := 'trg_sync_cron_job_' || replace(v_config_table, '.', '_');

    -- Drop existing trigger to ensure idempotency
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, v_config_table);

    -- Create the trigger
    EXECUTE format('
        CREATE TRIGGER %I
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION partitioncache_sync_cron_job()
    ', v_trigger_name, v_config_table);
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
                datatype TEXT NOT NULL CHECK (datatype IN (''integer'', ''float'', ''text'', ''timestamp'')),
                bitsize INTEGER NOT NULL DEFAULT 1000,
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
            status TEXT NOT NULL DEFAULT ''ok'' CHECK (status IN (''ok'', ''timeout'', ''failed'')),
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

-- Main dispatcher function that will be called by pg_cron
CREATE OR REPLACE FUNCTION partitioncache_run_single_job(p_job_name TEXT)
RETURNS TABLE(dispatched_job_id TEXT, status TEXT) AS $$
DECLARE
    v_config RECORD;
    v_queue_table TEXT;
    v_active_jobs_table TEXT;
    v_log_table TEXT;
    v_config_table TEXT;
    v_queue_item RECORD;
BEGIN
    -- For job name 'partitioncache_process_queue', we need to find the processor config table
    -- Try to find it by looking for tables ending with '_processor_config'
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
    
    IF v_config_table IS NULL THEN
        -- Use default if not found
        v_config_table := 'partitioncache_queue_processor_config';
    END IF;
    
    -- Get configuration for this job
    BEGIN
        EXECUTE format('SELECT * FROM %I WHERE job_name = %L', v_config_table, p_job_name) INTO STRICT v_config;
        -- Log to console
        RAISE NOTICE 'Job name: %', p_job_name;
        RAISE NOTICE 'Config: %', v_config;

    EXCEPTION 
        WHEN NO_DATA_FOUND THEN
            RAISE NOTICE 'No data found for job % in table %', p_job_name, v_config_table;
            -- This can happen if the trigger creates jobs before config is committed. Exit gracefully.
            RETURN;
        WHEN TOO_MANY_ROWS THEN
            -- This indicates a configuration error.
            RAISE WARNING 'Multiple configurations found for job % in table %', p_job_name, v_config_table;
            RETURN;
    END;
    
    IF NOT v_config.enabled THEN
        RAISE WARNING 'Job % is executed even though it is disabled', p_job_name;
    END IF;

    v_queue_table := v_config.queue_prefix || '_query_fragment_queue';
    v_active_jobs_table := v_config.queue_prefix || '_active_jobs';
    v_log_table := v_config.queue_prefix || '_processor_log';

    -- Dequeue the next item, skipping locked items
    EXECUTE format(
        'DELETE FROM %I
            WHERE id = (
                SELECT id FROM %I
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, query, hash, partition_key, partition_datatype',
        v_queue_table, v_queue_table
    ) INTO v_queue_item;

    -- If no item was dequeued, exit gracefully
    IF v_queue_item.id IS NULL THEN
        dispatched_job_id := NULL;
        status := 'no_jobs_available';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Check if this query has already been processed
    DECLARE
        v_queries_table TEXT;
        v_already_processed BOOLEAN;
    BEGIN
        v_queries_table := partitioncache_get_queries_table_name(v_config.table_prefix);
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE query_hash = %L AND partition_key = %L)', 
            v_queries_table, v_queue_item.hash, v_queue_item.partition_key) INTO v_already_processed;
        
        IF v_already_processed THEN
            EXECUTE format('INSERT INTO %I (job_id, query_hash, partition_key, status, error_message, execution_source) VALUES (%L, %L, %L, %L, %L, %L)', 
                v_log_table, 'job_' || v_queue_item.id, v_queue_item.hash, v_queue_item.partition_key, 'skipped', 'Query already processed', 'cron');
            
            dispatched_job_id := 'job_' || v_queue_item.id;
            RAISE NOTICE 'Skipping job %', dispatched_job_id;
            status := 'skipped';
            RETURN NEXT;
            RETURN;
        END IF;
    END;

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
            v_config.queue_prefix,
            v_config.table_prefix,
            v_config.cache_backend,
            'cron'
        );
        RAISE NOTICE 'Job processed %', dispatched_job_id;
    END;
    dispatched_job_id := 'job_' || v_queue_item.id;
    status := 'processed';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Core processing function for cron workers and manual execution
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
BEGIN
    v_job_id := 'job_' || p_item_id;
    v_start_time := clock_timestamp(); -- Use clock_timestamp() for better precision
    
    -- Set table names
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    v_log_table := p_queue_prefix || '_processor_log';
    v_config_table := p_queue_prefix || '_processor_config';
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Get timeout from config
    EXECUTE format('SELECT timeout_seconds FROM %I LIMIT 1', v_config_table) INTO v_timeout_seconds;
    
    BEGIN
        -- Process the item using the same logic as the background processor
        PERFORM partitioncache_ensure_metadata_tables(p_table_prefix);
        
        v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
        v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
        
        -- Priority order for datatype detection (same as background processor):
        -- 1. Check provided datatype parameter
        -- 2. Check existing metadata table entry
        -- 3. Use PostgreSQL information_schema to get actual column datatype
        -- 4. Fall back to regex-based detection on sample data
        
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
                            SELECT CASE WHEN COUNT(*) = 0 THEN ''text'' WHEN COUNT(CASE WHEN sample_value ~ ''^[-+]?[0-9]+$'' THEN 1 END) = COUNT(*) THEN ''integer'' WHEN COUNT(CASE WHEN sample_value ~ ''^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'' THEN 1 END) = COUNT(*) THEN ''float'' WHEN COUNT(CASE WHEN sample_value ~ ''^\d{4}-\d{2}-\d{2}'' THEN 1 END) = COUNT(*) THEN ''timestamp'' ELSE ''text'' END FROM sample_data
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
            IF v_bitsize IS NULL THEN v_bitsize := 1000; END IF;
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
                p_partition_key, p_query, p_partition_key, p_partition_key, v_bitsize, v_bitsize
            );
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, (%s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, v_bit_query);
        ELSIF p_cache_backend = 'roaringbit' THEN
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, rb_build_agg(%s::INTEGER) FROM (%s) AS query_result ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, p_partition_key, p_query);
        ELSIF p_cache_backend = 'array' THEN
            -- For array cache, we need to aggregate the results into an array
            v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, ARRAY(SELECT %s FROM (%s) AS query_result) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, p_partition_key, p_query);
        ELSE
            RAISE EXCEPTION 'Unsupported cache_backend type: %', p_cache_backend;
        END IF;
        
        -- check what statement_timeout is set to
        RAISE NOTICE 'Statement timeout: %', current_setting('statement_timeout');

        -- check that query does not contain DELETE OR DROP
        IF p_query LIKE '%DELETE %' OR p_query LIKE '%DROP %' THEN
            RAISE EXCEPTION 'Query contains DELETE or DROP';
        END IF;

        EXECUTE v_insert_query;
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        
        RAISE NOTICE 'Query executed';
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
            RAISE NOTICE 'Query canceled';
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
            RAISE NOTICE 'Query failed: %', SQLERRM;
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

-- This function is for manual testing and debugging. It processes a specified number of items
-- from the queue immediately, bypassing the cron scheduler.
CREATE OR REPLACE FUNCTION partitioncache_manual_process_queue(p_count INTEGER DEFAULT 100)
RETURNS TABLE(processed_count INTEGER, message TEXT) AS $$
DECLARE
    v_total INTEGER := 0;
    v_result RECORD;
    v_job_name TEXT := 'partitioncache_process_queue'; -- Assuming the standard job name
BEGIN
    FOR i IN 1..p_count LOOP
        -- Call the same function as the cron worker.
        SELECT * FROM partitioncache_run_single_job(v_job_name) INTO v_result;

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

-- Note: For timeout testing, use transaction-level statement_timeout:
-- BEGIN;
-- SET LOCAL statement_timeout = 1000;  -- 1 second
-- SELECT * FROM partitioncache_manual_process_queue(1);
-- COMMIT;

-- Manual processing function for partitioncache_manual_process_queue with manual execution source
-- DEPRECATED: This function has been consolidated into _partitioncache_execute_job
DROP FUNCTION IF EXISTS partitioncache_process_queue_item_manual(INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);

-- Function to enable or disable the queue processor
CREATE OR REPLACE FUNCTION partitioncache_set_processor_enabled(p_enabled BOOLEAN, p_queue_prefix TEXT DEFAULT 'partitioncache_queue', p_job_name TEXT DEFAULT 'partitioncache_process_queue')
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    EXECUTE format(
        'UPDATE %I SET enabled = %L, updated_at = NOW() WHERE job_name = %L',
        v_config_table, p_enabled, p_job_name
    );
END;
$$ LANGUAGE plpgsql;

-- Function to update the processor configuration
CREATE OR REPLACE FUNCTION partitioncache_update_processor_config(
    p_job_name TEXT,
    p_enabled BOOLEAN DEFAULT NULL, 
    p_max_parallel_jobs INTEGER DEFAULT NULL,
    p_frequency_seconds INTEGER DEFAULT NULL,
    p_timeout_seconds INTEGER DEFAULT NULL,
    p_table_prefix TEXT DEFAULT NULL,
    p_queue_prefix TEXT DEFAULT 'partitioncache_queue'
)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_update_sql TEXT;
    v_set_clauses TEXT[] := '{}';
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';

    -- Build the SET clauses dynamically
    IF p_enabled IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('enabled = %L', p_enabled));
    END IF;
    IF p_max_parallel_jobs IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('max_parallel_jobs = %L', p_max_parallel_jobs));
    END IF;
    IF p_frequency_seconds IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('frequency_seconds = %L', p_frequency_seconds));
    END IF;
    IF p_timeout_seconds IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('timeout_seconds = %L', p_timeout_seconds));
    END IF;
    IF p_table_prefix IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('table_prefix = %L', p_table_prefix));
    END IF;

    -- Only execute if there's something to update
    IF array_length(v_set_clauses, 1) > 0 THEN
        v_update_sql := format(
            'UPDATE %I SET %s, updated_at = NOW() WHERE job_name = %L',
            v_config_table,
            array_to_string(v_set_clauses, ', '),
            p_job_name
        );
        EXECUTE v_update_sql;
    END IF;
END;
$$ LANGUAGE plpgsql;