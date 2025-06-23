-- Helper function to get detailed queue and cache architecture information
CREATE OR REPLACE FUNCTION partitioncache_get_queue_and_cache_info(p_table_prefix TEXT DEFAULT 'partitioncache', p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS TABLE(
    partition_key TEXT,
    cache_table_name TEXT,
    cache_architecture TEXT,
    cache_column_type TEXT,
    bitsize INTEGER,
    partition_datatype TEXT,
    queue_items INTEGER,
    last_cache_update TIMESTAMP,
    cache_exists BOOLEAN
) AS $$
DECLARE
    v_metadata_table TEXT;
    v_queries_table TEXT;
    v_queue_table TEXT;
    v_metadata_exists BOOLEAN := false;
    v_queries_exists BOOLEAN := false;
    v_queue_exists BOOLEAN := false;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    
    -- Check if tables exist
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = v_metadata_table
    ) INTO v_metadata_exists;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = v_queries_table
    ) INTO v_queries_exists;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = v_queue_table
    ) INTO v_queue_exists;
    
    -- If none of the key tables exist, return empty result
    IF NOT v_metadata_exists AND NOT v_queue_exists THEN
        RETURN;
    END IF;
    
    -- Build dynamic query based on what tables exist
    IF v_metadata_exists AND v_queue_exists THEN
        -- Full query with both metadata and queue - Check if bitsize column exists
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = v_metadata_table 
            AND column_name = 'bitsize'
        ) THEN
            -- Metadata table has bitsize column (bit cache)
            RETURN QUERY
            EXECUTE format('
                WITH partition_info AS (
                    SELECT DISTINCT 
                        COALESCE(pm.partition_key, qfq.partition_key) as partition_key,
                        pm.datatype as partition_datatype,
                        pm.bitsize,
                        COUNT(qfq.id) as queue_items
                    FROM %I pm
                    FULL OUTER JOIN %I qfq ON pm.partition_key = qfq.partition_key
                    GROUP BY pm.partition_key, qfq.partition_key, pm.datatype, pm.bitsize
                ),
                cache_info AS (
                    SELECT 
                        pi.partition_key,
                        partitioncache_get_cache_table_name(%L, pi.partition_key) as cache_table_name,
                        pi.partition_datatype,
                        pi.bitsize,
                        pi.queue_items,
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                            ) THEN true
                            ELSE false
                        END as cache_exists,
                        (SELECT CASE 
                                    WHEN data_type = ''USER-DEFINED'' THEN udt_name
                                    ELSE data_type
                                END
                         FROM information_schema.columns 
                         WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                         AND column_name = ''partition_keys''
                        )::TEXT as cache_column_type,
                        %s as last_cache_update
                    FROM partition_info pi
                )
                SELECT 
                    ci.partition_key,
                    ci.cache_table_name,
                    CASE 
                        WHEN NOT ci.cache_exists THEN ''not_created''
                        WHEN ci.cache_column_type LIKE ''bit%%'' THEN ''bit''
                        WHEN ci.cache_column_type = ''roaringbitmap'' THEN ''roaringbit''
                        WHEN ci.cache_column_type LIKE ''%%[]'' OR ci.cache_column_type = ''ARRAY'' THEN ''array''
                        ELSE ''unknown''
                    END as cache_architecture,
                    ci.cache_column_type,
                    ci.bitsize,
                    ci.partition_datatype,
                    ci.queue_items::INTEGER,
                    ci.last_cache_update,
                    ci.cache_exists
                FROM cache_info ci
                ORDER BY ci.partition_key
            ', v_metadata_table, v_queue_table, p_table_prefix, p_table_prefix, p_table_prefix, 
               CASE WHEN v_queries_exists THEN 
                   format('(SELECT MAX(q.last_seen) FROM %I q WHERE q.partition_key = pi.partition_key)', v_queries_table)
               ELSE 'NULL::TIMESTAMP' END);
        ELSE
            -- Metadata table doesn't have bitsize column (array cache)
            RETURN QUERY
            EXECUTE format('
                WITH partition_info AS (
                    SELECT DISTINCT 
                        COALESCE(pm.partition_key, qfq.partition_key) as partition_key,
                        pm.datatype as partition_datatype,
                        NULL::INTEGER as bitsize,
                        COUNT(qfq.id) as queue_items
                    FROM %I pm
                    FULL OUTER JOIN %I qfq ON pm.partition_key = qfq.partition_key
                    GROUP BY pm.partition_key, qfq.partition_key, pm.datatype
                ),
                cache_info AS (
                    SELECT 
                        pi.partition_key,
                        partitioncache_get_cache_table_name(%L, pi.partition_key) as cache_table_name,
                        pi.partition_datatype,
                        pi.bitsize,
                        pi.queue_items,
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                            ) THEN true
                            ELSE false
                        END as cache_exists,
                        (SELECT CASE 
                                    WHEN data_type = ''USER-DEFINED'' THEN udt_name
                                    ELSE data_type
                                END
                         FROM information_schema.columns 
                         WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                         AND column_name = ''partition_keys''
                        )::TEXT as cache_column_type,
                        %s as last_cache_update
                    FROM partition_info pi
                )
                SELECT 
                    ci.partition_key,
                    ci.cache_table_name,
                    CASE 
                        WHEN NOT ci.cache_exists THEN ''not_created''
                        WHEN ci.cache_column_type LIKE ''bit%%'' THEN ''bit''
                        WHEN ci.cache_column_type = ''roaringbitmap'' THEN ''roaringbit''
                        WHEN ci.cache_column_type LIKE ''%%[]'' OR ci.cache_column_type = ''ARRAY'' THEN ''array''
                        ELSE ''unknown''
                    END as cache_architecture,
                    ci.cache_column_type,
                    ci.bitsize,
                    ci.partition_datatype,
                    ci.queue_items::INTEGER,
                    ci.last_cache_update,
                    ci.cache_exists
                FROM cache_info ci
                ORDER BY ci.partition_key
            ', v_metadata_table, v_queue_table, p_table_prefix, p_table_prefix, p_table_prefix, 
               CASE WHEN v_queries_exists THEN 
                   format('(SELECT MAX(q.last_seen) FROM %I q WHERE q.partition_key = pi.partition_key)', v_queries_table)
               ELSE 'NULL::TIMESTAMP' END);
        END IF;
    ELSIF v_queue_exists THEN
        -- Queue only (no metadata)
        RETURN QUERY
        EXECUTE format('
            WITH partition_info AS (
                SELECT DISTINCT 
                    qfq.partition_key,
                    NULL::TEXT as partition_datatype,
                    NULL::INTEGER as bitsize,
                    COUNT(qfq.id) as queue_items
                FROM %I qfq
                GROUP BY qfq.partition_key
            ),
            cache_info AS (
                SELECT 
                    pi.partition_key,
                    partitioncache_get_cache_table_name(%L, pi.partition_key) as cache_table_name,
                    pi.partition_datatype,
                    pi.bitsize,
                    pi.queue_items,
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                        ) THEN true
                        ELSE false
                    END as cache_exists,
                    (SELECT CASE 
                                WHEN data_type = ''USER-DEFINED'' THEN udt_name
                                ELSE data_type
                            END
                     FROM information_schema.columns 
                     WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                     AND column_name = ''partition_keys''
                    )::TEXT as cache_column_type,
                    %s as last_cache_update
                FROM partition_info pi
            )
            SELECT 
                ci.partition_key,
                ci.cache_table_name,
                CASE 
                    WHEN NOT ci.cache_exists THEN ''not_created''
                    WHEN ci.cache_column_type LIKE ''bit%%'' THEN ''bit''
                    WHEN ci.cache_column_type = ''roaringbitmap'' THEN ''roaringbit''
                    WHEN ci.cache_column_type LIKE ''%%[]'' OR ci.cache_column_type = ''ARRAY'' THEN ''array''
                    ELSE ''unknown''
                END as cache_architecture,
                ci.cache_column_type,
                ci.bitsize,
                ci.partition_datatype,
                ci.queue_items::INTEGER,
                ci.last_cache_update,
                ci.cache_exists
            FROM cache_info ci
            ORDER BY ci.partition_key
        ', v_queue_table, p_table_prefix, p_table_prefix, p_table_prefix,
           CASE WHEN v_queries_exists THEN 
               format('(SELECT MAX(q.last_seen) FROM %I q WHERE q.partition_key = pi.partition_key)', v_queries_table)
           ELSE 'NULL::TIMESTAMP' END);
    
    ELSIF v_metadata_exists THEN
        -- Metadata only (no queue) - Check if bitsize column exists
        -- For array caches, bitsize column might not exist
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = v_metadata_table 
            AND column_name = 'bitsize'
        ) THEN
            -- Metadata table has bitsize column (bit cache)
            RETURN QUERY
            EXECUTE format('
                WITH partition_info AS (
                    SELECT DISTINCT 
                        pm.partition_key,
                        pm.datatype as partition_datatype,
                        pm.bitsize,
                        0 as queue_items
                    FROM %I pm
                ),
                cache_info AS (
                    SELECT 
                        pi.partition_key,
                        partitioncache_get_cache_table_name(%L, pi.partition_key) as cache_table_name,
                        pi.partition_datatype,
                        pi.bitsize,
                        pi.queue_items,
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                            ) THEN true
                            ELSE false
                        END as cache_exists,
                        (SELECT CASE 
                                    WHEN data_type = ''USER-DEFINED'' THEN udt_name
                                    ELSE data_type
                                END
                         FROM information_schema.columns 
                         WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                         AND column_name = ''partition_keys''
                        )::TEXT as cache_column_type,
                        %s as last_cache_update
                    FROM partition_info pi
                )
                SELECT 
                    ci.partition_key,
                    ci.cache_table_name,
                    CASE 
                        WHEN NOT ci.cache_exists THEN ''not_created''
                        WHEN ci.cache_column_type LIKE ''bit%%'' THEN ''bit''
                        WHEN ci.cache_column_type = ''roaringbitmap'' THEN ''roaringbit''
                        WHEN ci.cache_column_type LIKE ''%%[]'' OR ci.cache_column_type = ''ARRAY'' THEN ''array''
                        ELSE ''unknown''
                    END as cache_architecture,
                    ci.cache_column_type,
                    ci.bitsize,
                    ci.partition_datatype,
                    ci.queue_items::INTEGER,
                    ci.last_cache_update,
                    ci.cache_exists
                FROM cache_info ci
                ORDER BY ci.partition_key
            ', v_metadata_table, p_table_prefix, p_table_prefix, p_table_prefix,
               CASE WHEN v_queries_exists THEN 
                   format('(SELECT MAX(q.last_seen) FROM %I q WHERE q.partition_key = pi.partition_key)', v_queries_table)
               ELSE 'NULL::TIMESTAMP' END);
        ELSE
            -- Metadata table doesn't have bitsize column (array cache)
            RETURN QUERY
            EXECUTE format('
                WITH partition_info AS (
                    SELECT DISTINCT 
                        pm.partition_key,
                        pm.datatype as partition_datatype,
                        NULL::INTEGER as bitsize,
                        0 as queue_items
                    FROM %I pm
                ),
                cache_info AS (
                    SELECT 
                        pi.partition_key,
                        partitioncache_get_cache_table_name(%L, pi.partition_key) as cache_table_name,
                        pi.partition_datatype,
                        pi.bitsize,
                        pi.queue_items,
                        CASE 
                            WHEN EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                            ) THEN true
                            ELSE false
                        END as cache_exists,
                        (SELECT CASE 
                                    WHEN data_type = ''USER-DEFINED'' THEN udt_name
                                    ELSE data_type
                                END
                         FROM information_schema.columns 
                         WHERE table_name = partitioncache_get_cache_table_name(%L, pi.partition_key)
                         AND column_name = ''partition_keys''
                        )::TEXT as cache_column_type,
                        %s as last_cache_update
                    FROM partition_info pi
                )
                SELECT 
                    ci.partition_key,
                    ci.cache_table_name,
                    CASE 
                        WHEN NOT ci.cache_exists THEN ''not_created''
                        WHEN ci.cache_column_type LIKE ''bit%%'' THEN ''bit''
                        WHEN ci.cache_column_type = ''roaringbitmap'' THEN ''roaringbit''
                        WHEN ci.cache_column_type LIKE ''%%[]'' OR ci.cache_column_type = ''ARRAY'' THEN ''array''
                        ELSE ''unknown''
                    END as cache_architecture,
                    ci.cache_column_type,
                    ci.bitsize,
                    ci.partition_datatype,
                    ci.queue_items::INTEGER,
                    ci.last_cache_update,
                    ci.cache_exists
                FROM cache_info ci
                ORDER BY ci.partition_key
            ', v_metadata_table, p_table_prefix, p_table_prefix, p_table_prefix,
               CASE WHEN v_queries_exists THEN 
                   format('(SELECT MAX(q.last_seen) FROM %I q WHERE q.partition_key = pi.partition_key)', v_queries_table)
               ELSE 'NULL::TIMESTAMP' END);
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get basic processor status
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status(p_queue_prefix TEXT, p_job_name TEXT DEFAULT 'partitioncache_process_queue')
RETURNS TABLE(
    job_name TEXT,
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    timeout_seconds INTEGER,
    table_prefix TEXT,
    queue_prefix TEXT,
    cache_backend TEXT,
    updated_at TIMESTAMP,
    job_is_active BOOLEAN,
    job_schedule TEXT,
    job_command TEXT,
    active_jobs_count INTEGER
) AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';

    RETURN QUERY
    EXECUTE format(
        'WITH cron_jobs AS (
            SELECT
                active,
                schedule,
                command
            FROM cron.job
            WHERE jobname LIKE %L || %L
        )
        SELECT
            conf.job_name,
            conf.enabled,
            conf.max_parallel_jobs,
            conf.frequency_seconds,
            conf.timeout_seconds,
            conf.table_prefix,
            conf.queue_prefix,
            conf.cache_backend,
            conf.updated_at,
            (SELECT bool_or(active) FROM cron_jobs) as job_is_active,
            (SELECT schedule FROM cron_jobs LIMIT 1) as job_schedule,
            (SELECT command FROM cron_jobs LIMIT 1) as job_command,
            (SELECT COUNT(*)::INTEGER FROM partitioncache_get_active_jobs_info(%L))
        FROM
            %I conf
        WHERE
            conf.job_name = %L',
        p_job_name, '_%', p_queue_prefix, v_config_table, p_job_name
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced processor status function with queue and cache information
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status_detailed(p_table_prefix TEXT, p_queue_prefix TEXT, p_job_name TEXT DEFAULT 'partitioncache_process_queue')
RETURNS TABLE(
    -- Basic processor status
    job_name TEXT,
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    job_is_active BOOLEAN,
    job_schedule TEXT,
    -- Queue stats
    active_jobs_count INTEGER,
    queue_length INTEGER,
    recent_successes_5m INTEGER,
    recent_failures_5m INTEGER,
    -- Cache architecture summary
    total_partitions INTEGER,
    partitions_with_cache INTEGER,
    roaringbit_partitions INTEGER,
    bit_cache_partitions INTEGER,
    array_cache_partitions INTEGER,
    uncreated_cache_partitions INTEGER,
    -- Details
    recent_logs JSON,
    active_job_details JSON
) AS $$
DECLARE
    v_queue_table TEXT;
    v_config_table TEXT;
BEGIN
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_config_table := p_queue_prefix || '_processor_config';

    RETURN QUERY
    EXECUTE format(
        'SELECT
            s.job_name,
            s.enabled,
            s.max_parallel_jobs,
            s.frequency_seconds,
            s.job_is_active,
            s.job_schedule,
            s.active_jobs_count,
            (SELECT COUNT(*)::INTEGER FROM %I) as queue_length,
            (SELECT COUNT(*)::INTEGER FROM partitioncache_get_log_info(%L) WHERE status = %L AND created_at > NOW() - INTERVAL %L) as recent_successes_5m,
            (SELECT COUNT(*)::INTEGER FROM partitioncache_get_log_info(%L) WHERE status = %L AND created_at > NOW() - INTERVAL %L) as recent_failures_5m,
            COALESCE(ca.total_partitions, 0),
            COALESCE(ca.partitions_with_cache, 0),
            COALESCE(ca.roaringbit_partitions, 0),
            COALESCE(ca.bit_partitions, 0),
            COALESCE(ca.array_partitions, 0),
            COALESCE(ca.uncreated_partitions, 0),
            (SELECT json_agg(log_info) FROM (SELECT * FROM partitioncache_get_log_info(%L) LIMIT 10) log_info) as recent_logs,
            (SELECT json_agg(active_jobs_info) FROM (SELECT * FROM partitioncache_get_active_jobs_info(%L)) active_jobs_info) as active_job_details
        FROM
            partitioncache_get_processor_status(%L, %L) s,
            LATERAL (
                SELECT
                    COUNT(*)::INTEGER AS total_partitions,
                    COUNT(*) FILTER (WHERE cache_exists)::INTEGER AS partitions_with_cache,
                    COUNT(*) FILTER (WHERE cache_architecture = %L)::INTEGER AS roaringbit_partitions,
                    COUNT(*) FILTER (WHERE cache_architecture = %L)::INTEGER AS bit_partitions,
                    COUNT(*) FILTER (WHERE cache_architecture = %L)::INTEGER AS array_partitions,
                    COUNT(*) FILTER (WHERE cache_architecture = %L)::INTEGER AS uncreated_partitions
                FROM partitioncache_get_queue_and_cache_info(%L, %L)
            ) ca',
        v_queue_table, 
        p_queue_prefix, 'success', '5 minutes',
        p_queue_prefix, 'failed', '5 minutes',
        p_queue_prefix, 
        p_queue_prefix, 
        p_queue_prefix, p_job_name, 
        'roaringbit', 'bit', 'array', 'not_created',
        p_table_prefix, p_queue_prefix
    );
END;

$$ LANGUAGE plpgsql;

-- Function to get active jobs (for status checks)
CREATE OR REPLACE FUNCTION partitioncache_get_active_jobs_info(p_queue_prefix TEXT)
RETURNS TABLE(job_id TEXT, query_hash TEXT, partition_key TEXT, started_at TIMESTAMP) AS $$
DECLARE
    v_active_jobs_table TEXT;
BEGIN
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    RETURN QUERY EXECUTE format('SELECT job_id, query_hash, partition_key, started_at FROM %I', v_active_jobs_table);
END;
$$ LANGUAGE plpgsql;

-- Function to get log info (for status checks)
CREATE OR REPLACE FUNCTION partitioncache_get_log_info(p_queue_prefix TEXT)
RETURNS TABLE(job_id TEXT, query_hash TEXT, partition_key TEXT, status TEXT, error_message TEXT, rows_affected INTEGER, execution_time_ms NUMERIC, execution_source TEXT, created_at TIMESTAMP) AS $$
DECLARE
    v_log_table TEXT;
BEGIN
    v_log_table := p_queue_prefix || '_processor_log';
    RETURN QUERY EXECUTE format('SELECT job_id, query_hash, partition_key, status, error_message, rows_affected, execution_time_ms, execution_source, created_at FROM %I ORDER BY created_at DESC', v_log_table);
END;
$$ LANGUAGE plpgsql;

