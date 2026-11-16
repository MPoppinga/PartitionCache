-- PartitionCache PostgreSQL Queue Processor - Cache Database Components
-- This file contains components that must be installed in the cache/work database
-- These components handle the actual queue processing and cache operations

-- Note: Global cache handler functions must be loaded separately by Python
-- The file postgresql_cache_handlers.sql contains required functions

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

-- Note: Table naming and metadata functions are loaded from postgresql_cache_handlers.sql

-- Main dispatcher function that will be called by pg_cron with parameters
CREATE OR REPLACE FUNCTION partitioncache_run_single_job_with_params(
    p_job_name TEXT,
    p_table_prefix TEXT,
    p_queue_prefix TEXT,
    p_cache_backend TEXT,
    p_timeout_seconds INTEGER DEFAULT 1800,
    p_result_limit INTEGER DEFAULT NULL,
    p_default_bitsize INTEGER DEFAULT NULL
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
                RETURNING id, query, hash, partition_key, partition_datatype, cache_backend',
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
            COALESCE(v_queue_item.cache_backend, p_cache_backend),
            'cron',
            p_timeout_seconds,
            p_default_bitsize,
            p_result_limit
        );
        RAISE NOTICE 'Job processed %', dispatched_job_id;
    END;
    dispatched_job_id := 'job_' || v_queue_item.id;
    status := 'processed';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Include the complete _partitioncache_execute_job function. The version without p_result_limit is dropped:
-- next to the new overload, a call with eleven arguments would be ambiguous.
DROP FUNCTION IF EXISTS _partitioncache_execute_job(INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER, INTEGER);
CREATE OR REPLACE FUNCTION _partitioncache_execute_job(
    p_item_id INTEGER,
    p_query TEXT,
    p_query_hash TEXT,
    p_partition_key TEXT,
    p_partition_datatype TEXT,
    p_queue_prefix TEXT,
    p_table_prefix TEXT,
    p_cache_backend TEXT,
    p_execution_source TEXT,
    p_timeout_seconds INTEGER DEFAULT 1800,
    p_default_bitsize INTEGER DEFAULT NULL,
    -- Results with at least this many partition keys are not stored; the query gets the status 'limit'
    p_result_limit INTEGER DEFAULT NULL
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
    -- array/bit/roaringbit: the variant runs as this plain SELECT and returns its keys as a typed array,
    -- which reaches the INSERT as parameter $1 (one variable per array column type)
    v_select_query TEXT;
    v_int_keys INTEGER[];
    v_numeric_keys NUMERIC[];
    v_text_keys TEXT[];
    v_timestamp_keys TIMESTAMP[];
    v_min_key BIGINT;
    v_max_key BIGINT;
    v_grown_bitsize INTEGER;
    v_bitsize_buffer INTEGER;
    -- Bit array from the key array ($1) and the bitsize (%s, twice); keys must lie in 0 .. bitsize - 1
    v_bit_template TEXT := 'WITH bit_positions AS (
            SELECT position FROM unnest($1) AS u(position)
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
        SELECT bit_value::BIT(%s) FROM bit_string';
    v_already_processed BOOLEAN;
    v_timeout_seconds INTEGER;
    v_result_limit INTEGER;
    v_default_bitsize INTEGER := p_default_bitsize;
    v_geometry_column TEXT;
    v_h3_resolution INTEGER;
    v_srid INTEGER;
    v_centroid_expr TEXT;
BEGIN
    v_job_id := 'job_' || p_item_id;
    v_start_time := clock_timestamp();

    -- Queue items carry the full handler name (postgresql_array, postgis_bbox), the processor
    -- config the short form (array, bbox). The branches below compare against the short form.
    p_cache_backend := regexp_replace(p_cache_backend, '^(postgresql|postgis)_', '');
    
    -- Set table names
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    v_log_table := p_queue_prefix || '_processor_log';
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Use passed timeout parameter
    v_timeout_seconds := COALESCE(p_timeout_seconds, 1800);
    v_result_limit := p_result_limit;
    
    BEGIN
        -- postgis_bbox entries are written by the Python handler only (raw geometries, multi-alias fragments flattened);
        -- the item is recorded as failed below instead of being stored in another format
        IF p_cache_backend = 'bbox' THEN
            RAISE EXCEPTION 'The PostgreSQL queue processor does not support the postgis_bbox cache backend; populate it with pcache-monitor';
        END IF;

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
        
        -- Bootstrap partition with existing or default bitsize (to be expanded later if needed)
        IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = v_cache_table) THEN
            DECLARE v_bitsize_for_bootstrap INTEGER;
            DECLARE v_existing_bitsize INTEGER;
            BEGIN
                IF p_cache_backend = 'bit' THEN
                    -- Get existing bitsize if partition already exists
                    EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key) INTO v_existing_bitsize;
                    
                    -- Use existing bitsize or default (will expand after query execution if needed)
                    IF v_existing_bitsize IS NOT NULL THEN
                        v_bitsize_for_bootstrap := v_existing_bitsize;
                    ELSE
                        v_bitsize_for_bootstrap := COALESCE(v_default_bitsize, 1000);
                    END IF;
                END IF;
                PERFORM partitioncache_bootstrap_partition(p_table_prefix, p_partition_key, v_datatype, p_cache_backend, v_bitsize_for_bootstrap);
            END;
        END IF;
        
        -- Build cache insertion query based on backend type
        IF p_cache_backend = 'bit' THEN
            EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key) INTO v_bitsize;
            
            -- Handle potential bitsize expansion after main query execution
            -- Note: Bitsize validation/expansion will be handled by the bootstrap function's GREATEST logic
            
            -- The INSERT is built after the SELECT has run (see the execution block)
            v_select_query := format('SELECT ARRAY(SELECT %s FROM (%s) AS query_result WHERE %s IS NOT NULL)', p_partition_key, p_query, p_partition_key);
        ELSIF p_cache_backend IN ('roaringbit', 'array') THEN
            v_select_query := format('SELECT ARRAY(SELECT %s FROM (%s) AS query_result WHERE %s IS NOT NULL)', p_partition_key, p_query, p_partition_key);
        ELSIF p_cache_backend = 'h3' THEN
            -- Read spatial config from config table
            v_config_table := p_queue_prefix || '_processor_config';
            BEGIN
                EXECUTE format('SELECT COALESCE(geometry_column, ''geom''), COALESCE(h3_resolution, 9), COALESCE(bbox_srid, 4326) FROM %I LIMIT 1', v_config_table)
                INTO v_geometry_column, v_h3_resolution, v_srid;
            EXCEPTION WHEN OTHERS THEN
                v_geometry_column := 'geom';
                v_h3_resolution := 9;
                v_srid := 4326;
            END;

            -- Build centroid expression - transform to WGS84 if SRID != 4326
            IF v_srid != 4326 THEN
                v_centroid_expr := format('ST_Transform(ST_Centroid(sub.%I), 4326)::point', v_geometry_column);
            ELSE
                v_centroid_expr := format('ST_Centroid(sub.%I)::point', v_geometry_column);
            END IF;

            v_insert_query := format(
                'INSERT INTO %I (query_hash, partition_keys, partition_keys_count) '
                'SELECT %L, '
                'ARRAY(SELECT DISTINCT h3_lat_lng_to_cell(%s, %s)::bigint FROM (%s) AS sub), '
                '(SELECT COUNT(DISTINCT h3_lat_lng_to_cell(%s, %s)::bigint) FROM (%s) AS sub) '
                'ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys, '
                'partition_keys_count = EXCLUDED.partition_keys_count',
                v_cache_table, p_query_hash,
                v_centroid_expr, v_h3_resolution, p_query,
                v_centroid_expr, v_h3_resolution, p_query
            );
        ELSE
            RAISE EXCEPTION 'Unsupported cache_backend type: %', p_cache_backend;
        END IF;
        
        -- Security check
        IF p_query LIKE '%DELETE %' OR p_query LIKE '%DROP %' THEN
            RAISE EXCEPTION 'Query contains DELETE or DROP';
        END IF;

        -- Execute main query
        BEGIN
            IF v_select_query IS NOT NULL THEN
                -- SELECT and INSERT are separate statements: PostgreSQL never runs a statement that writes
                -- with parallel workers, so the variant inside an INSERT ... SELECT ran single-threaded.
                -- The keys stay a typed array on the server. ARRAY(subquery) is '{}' for zero rows, never
                -- NULL, so an empty result is stored as an empty array / all-zero bits / empty bitmap and
                -- stays distinct from the NULL marker.
                IF p_cache_backend = 'bit' THEN
                    EXECUTE v_select_query INTO v_int_keys;
                    IF v_int_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                    -- Keys a bit array cannot hold are not dropped: a negative key fails the job, a key
                    -- beyond the bitsize grows the partition's bit arrays to max key + 1 + buffer, where
                    -- the buffer is the setting partitioncache.bitsize_buffer (ALTER DATABASE ... SET), default 0
                    SELECT min(k), max(k) INTO v_min_key, v_max_key FROM unnest(v_int_keys) AS k;
                    IF v_min_key < 0 THEN
                        RAISE EXCEPTION 'Partition key % is negative; bit arrays cannot store it', v_min_key;
                    END IF;
                    IF v_max_key >= v_bitsize THEN
                        v_bitsize_buffer := COALESCE(NULLIF(current_setting('partitioncache.bitsize_buffer', true), '')::INTEGER, 0);
                        IF v_bitsize_buffer < 0 THEN
                            RAISE EXCEPTION 'partitioncache.bitsize_buffer must not be negative (is %)', v_bitsize_buffer;
                        END IF;
                        PERFORM partitioncache_bootstrap_partition(p_table_prefix, p_partition_key, v_datatype, p_cache_backend, (v_max_key + 1 + v_bitsize_buffer)::INTEGER);
                    END IF;
                    -- The bitsize was read before the variant ran; another job may have grown the bit arrays
                    -- since. Lock the cache table first: a job that grows it holds it until commit, so the
                    -- bitsize read afterwards matches the column type (INSERT takes this lock anyway).
                    EXECUTE format('LOCK TABLE %I IN ROW EXCLUSIVE MODE', v_cache_table);
                    EXECUTE format('SELECT bitsize FROM %I WHERE partition_key = %L', v_metadata_table, p_partition_key) INTO v_grown_bitsize;
                    IF v_max_key >= v_grown_bitsize THEN
                        RAISE EXCEPTION 'Partition key % exceeds bitsize % of partition %', v_max_key, v_grown_bitsize, p_partition_key;
                    END IF;
                    IF v_max_key >= v_bitsize THEN
                        RAISE WARNING 'Partition key % exceeds bitsize % of partition %: bit arrays grown to %', v_max_key, v_bitsize, p_partition_key, v_grown_bitsize;
                    END IF;
                    v_bitsize := v_grown_bitsize;
                    EXECUTE format('INSERT INTO %I (query_hash, partition_keys) SELECT %L, (%s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash, format(v_bit_template, v_bitsize, v_bitsize))
                        USING v_int_keys;
                ELSIF p_cache_backend = 'roaringbit' THEN
                    EXECUTE v_select_query INTO v_int_keys;
                    IF v_int_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                    EXECUTE format('INSERT INTO %I (query_hash, partition_keys) VALUES (%L, rb_build($1)) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash)
                        USING v_int_keys;
                ELSE
                    v_insert_query := format('INSERT INTO %I (query_hash, partition_keys) VALUES (%L, $1) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys', v_cache_table, p_query_hash);
                    IF v_datatype = 'integer' THEN
                        EXECUTE v_select_query INTO v_int_keys;
                        IF v_int_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                        EXECUTE v_insert_query USING v_int_keys;
                    ELSIF v_datatype = 'float' THEN
                        EXECUTE v_select_query INTO v_numeric_keys;
                        IF v_numeric_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                        EXECUTE v_insert_query USING v_numeric_keys;
                    ELSIF v_datatype = 'timestamp' THEN
                        EXECUTE v_select_query INTO v_timestamp_keys;
                        IF v_timestamp_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                        EXECUTE v_insert_query USING v_timestamp_keys;
                    ELSE
                        EXECUTE v_select_query INTO v_text_keys;
                        IF v_text_keys IS NULL THEN RAISE EXCEPTION 'Variant query returned no key array'; END IF;
                        EXECUTE v_insert_query USING v_text_keys;
                    END IF;
                END IF;
            ELSE
                EXECUTE v_insert_query;
            END IF;
            GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        END;
        
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

-- Manual processing: runs up to p_count items in this session as one statement, so the configured timeout_seconds
-- does not apply; the session's statement_timeout (if any) bounds the whole batch. The configuration table is
-- {p_queue_prefix}_processor_config, else that of the setting partitioncache.queue_prefix, else the only one in the
-- schema; the row is p_job_name, else the only processor row for the current database. Ambiguity is an error.
-- The version without p_queue_prefix is dropped: next to the new one, a call with one or two arguments would be ambiguous.
DROP FUNCTION IF EXISTS partitioncache_manual_process_queue(INTEGER, TEXT);
CREATE OR REPLACE FUNCTION partitioncache_manual_process_queue(
    p_count INTEGER DEFAULT 100,
    p_job_name TEXT DEFAULT NULL,
    p_queue_prefix TEXT DEFAULT NULL
)
RETURNS TABLE(processed_count INTEGER, message TEXT) AS $$
DECLARE
    v_total INTEGER := 0;
    v_result RECORD;
    v_config RECORD;
    v_config_table TEXT;
    v_queue_prefix TEXT;
    v_candidates TEXT[];
BEGIN
    v_queue_prefix := COALESCE(p_queue_prefix, NULLIF(current_setting('partitioncache.queue_prefix', true), ''));

    IF v_queue_prefix IS NOT NULL THEN
        v_config_table := v_queue_prefix || '_processor_config';
        IF to_regclass(quote_ident(v_config_table)) IS NULL THEN
            RETURN QUERY SELECT 0, format('No configuration table %s for queue prefix %s. Please run setup first.', v_config_table, v_queue_prefix);
            RETURN;
        END IF;
    ELSE
        SELECT array_agg(t.tablename::TEXT ORDER BY t.tablename) INTO v_candidates
        FROM pg_tables t
        WHERE t.schemaname = current_schema()
        AND t.tablename LIKE '%\_processor\_config'
        AND EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = t.schemaname AND c.table_name = t.tablename AND c.column_name = 'job_name'
        );

        IF v_candidates IS NULL THEN
            RETURN QUERY SELECT 0, 'No configuration found for job. Please run setup first.';
            RETURN;
        ELSIF array_length(v_candidates, 1) > 1 THEN
            RAISE EXCEPTION 'More than one processor configuration table in this database (%); pass the queue prefix: partitioncache_manual_process_queue(count, job_name, queue_prefix), or pcache-postgresql-queue-processor manual-process with PG_QUEUE_TABLE_PREFIX set',
                array_to_string(v_candidates, ', ');
        END IF;
        v_config_table := v_candidates[1];
    END IF;

    IF p_job_name IS NOT NULL THEN
        EXECUTE format('SELECT array_agg(job_name) FROM %I WHERE job_name = %L', v_config_table, p_job_name) INTO v_candidates;
        IF v_candidates IS NULL THEN
            RETURN QUERY SELECT 0, format('No configuration %s in %s. Please run setup first.', p_job_name, v_config_table);
            RETURN;
        END IF;
    ELSE
        EXECUTE format('SELECT array_agg(job_name ORDER BY job_name) FROM %I WHERE job_name LIKE %L AND target_database = current_database()',
                       v_config_table, 'partitioncache_process_queue_' || current_database() || '%') INTO v_candidates;
        IF v_candidates IS NULL THEN
            RETURN QUERY SELECT 0, 'No configuration found for job. Please run setup first.';
            RETURN;
        ELSIF array_length(v_candidates, 1) > 1 THEN
            RAISE EXCEPTION 'More than one processor configuration for database % in % (%); pass the job name: partitioncache_manual_process_queue(count, job_name, queue_prefix), or pcache-postgresql-queue-processor manual-process --table-prefix',
                current_database(), v_config_table, array_to_string(v_candidates, ', ');
        END IF;
    END IF;

    EXECUTE format('SELECT job_name, table_prefix, queue_prefix, cache_backend, timeout_seconds, result_limit, default_bitsize FROM %I WHERE job_name = %L',
                   v_config_table, v_candidates[1]) INTO v_config;

    FOR i IN 1..p_count LOOP
        -- Call the parameter-based function with config values
        SELECT * FROM partitioncache_run_single_job_with_params(
            v_config.job_name,  -- Use the actual job_name from config
            v_config.table_prefix,
            v_config.queue_prefix, 
            v_config.cache_backend,
            v_config.timeout_seconds,
            v_config.result_limit,
            v_config.default_bitsize
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

-- Function to clean up old processor log entries.
CREATE OR REPLACE FUNCTION partitioncache_cleanup_processor_logs(
    p_queue_prefix TEXT DEFAULT 'partitioncache_queue',
    p_retention_days INTEGER DEFAULT 30
)
RETURNS INTEGER AS $$
DECLARE
    v_log_table TEXT;
    v_deleted_count INTEGER;
BEGIN
    v_log_table := p_queue_prefix || '_processor_log';

    EXECUTE format(
        'DELETE FROM %I WHERE created_at < NOW() - INTERVAL ''%s days''',
        v_log_table,
        p_retention_days
    );

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;
