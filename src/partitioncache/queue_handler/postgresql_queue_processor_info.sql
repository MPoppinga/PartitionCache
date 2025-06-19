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
                        (SELECT data_type 
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
                        (SELECT data_type 
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
                    (SELECT data_type 
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
                        (SELECT data_type 
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
                        (SELECT data_type 
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

-- Enhanced processor status function with queue and cache information
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status_detailed(p_table_prefix TEXT DEFAULT 'partitioncache', p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS TABLE(
    -- Basic processor status
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    active_jobs INTEGER,
    queue_length INTEGER,
    recent_successes INTEGER,
    recent_failures INTEGER,
    -- Queue and cache summary
    total_partitions INTEGER,
    partitions_with_cache INTEGER,
    bit_cache_partitions INTEGER,
    array_cache_partitions INTEGER,
    uncreated_cache_partitions INTEGER
) AS $$
DECLARE
    v_queue_table TEXT;
    v_queue_length INTEGER;
    v_processor_config_table TEXT;
    v_processor_log_table TEXT;
    v_active_jobs_table TEXT;
    v_config_enabled BOOLEAN := false;
    v_config_max_jobs INTEGER := 5;
    v_config_frequency INTEGER := 1;
    v_active_jobs_count INTEGER := 0;
    v_recent_successes INTEGER := 0;
    v_recent_failures INTEGER := 0;
    v_total_partitions INTEGER := 0;
    v_partitions_with_cache INTEGER := 0;
    v_bit_cache_partitions INTEGER := 0;
    v_array_cache_partitions INTEGER := 0;
    v_uncreated_cache_partitions INTEGER := 0;
BEGIN
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_processor_config_table := p_queue_prefix || '_processor_config';
    v_processor_log_table := p_queue_prefix || '_processor_log';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    
    -- Get queue length dynamically
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I', v_queue_table) INTO v_queue_length;
    EXCEPTION WHEN OTHERS THEN
        v_queue_length := 0;
    END;
    
    -- Get processor config
    BEGIN
        EXECUTE format('SELECT enabled, max_parallel_jobs, frequency_seconds FROM %I LIMIT 1', v_processor_config_table)
        INTO v_config_enabled, v_config_max_jobs, v_config_frequency;
    EXCEPTION WHEN OTHERS THEN
        -- Use defaults if table doesn't exist
        v_config_enabled := false;
        v_config_max_jobs := 5;
        v_config_frequency := 1;
    END;
    
    -- Get active jobs count
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I', v_active_jobs_table) INTO v_active_jobs_count;
    EXCEPTION WHEN OTHERS THEN
        v_active_jobs_count := 0;
    END;
    
    -- Get recent successes and failures
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE status = ''success'' AND created_at > NOW() - INTERVAL ''5 minutes''', v_processor_log_table)
        INTO v_recent_successes;
    EXCEPTION WHEN OTHERS THEN
        v_recent_successes := 0;
    END;
    
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE status = ''failed'' AND created_at > NOW() - INTERVAL ''5 minutes''', v_processor_log_table)
        INTO v_recent_failures;
    EXCEPTION WHEN OTHERS THEN
        v_recent_failures := 0;
    END;
    
    -- Get cache summary using simpler approach
    BEGIN
        SELECT 
            COALESCE(COUNT(*), 0),
            COALESCE(COUNT(*) FILTER (WHERE cache_exists), 0),
            COALESCE(COUNT(*) FILTER (WHERE cache_architecture = 'bit'), 0),
            COALESCE(COUNT(*) FILTER (WHERE cache_architecture = 'array'), 0),
            COALESCE(COUNT(*) FILTER (WHERE cache_architecture = 'not_created'), 0)
        INTO v_total_partitions, v_partitions_with_cache, v_bit_cache_partitions, v_array_cache_partitions, v_uncreated_cache_partitions
        FROM partitioncache_get_queue_and_cache_info(p_table_prefix, p_queue_prefix);
    EXCEPTION WHEN OTHERS THEN
        -- Use zero values when function fails
        v_total_partitions := 0;
        v_partitions_with_cache := 0;
        v_bit_cache_partitions := 0;
        v_array_cache_partitions := 0;
        v_uncreated_cache_partitions := 0;
    END;
    
    RETURN QUERY
    SELECT 
        v_config_enabled,
        v_config_max_jobs,
        v_config_frequency,
        v_active_jobs_count,
        v_queue_length,
        v_recent_successes,
        v_recent_failures,
        v_total_partitions,
        v_partitions_with_cache,
        v_bit_cache_partitions,
        v_array_cache_partitions,
        v_uncreated_cache_partitions;
END;
$$ LANGUAGE plpgsql;

-- Helper function to get processor status
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status(p_table_prefix TEXT DEFAULT 'partitioncache', p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS TABLE(
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    active_jobs INTEGER,
    queue_length INTEGER,
    recent_successes INTEGER,
    recent_failures INTEGER
) AS $$
DECLARE
    v_queue_table TEXT;
    v_queue_length INTEGER;
    v_processor_config_table TEXT;
    v_processor_log_table TEXT;
    v_active_jobs_table TEXT;
    v_config_enabled BOOLEAN := false;
    v_config_max_jobs INTEGER := 5;
    v_config_frequency INTEGER := 1;
    v_active_jobs_count INTEGER := 0;
    v_recent_successes INTEGER := 0;
    v_recent_failures INTEGER := 0;
BEGIN
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_processor_config_table := p_queue_prefix || '_processor_config';
    v_processor_log_table := p_queue_prefix || '_processor_log';
    v_active_jobs_table := p_queue_prefix || '_active_jobs';
    
    -- Get queue length dynamically
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I', v_queue_table) INTO v_queue_length;
    EXCEPTION WHEN OTHERS THEN
        v_queue_length := 0;
    END;
    
    -- Get processor config
    BEGIN
        EXECUTE format('SELECT enabled, max_parallel_jobs, frequency_seconds FROM %I LIMIT 1', v_processor_config_table)
        INTO v_config_enabled, v_config_max_jobs, v_config_frequency;
    EXCEPTION WHEN OTHERS THEN
        -- Use defaults if table doesn't exist
        v_config_enabled := false;
        v_config_max_jobs := 5;
        v_config_frequency := 1;
    END;
    
    -- Get active jobs count
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I', v_active_jobs_table) INTO v_active_jobs_count;
    EXCEPTION WHEN OTHERS THEN
        v_active_jobs_count := 0;
    END;
    
    -- Get recent successes and failures
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE status = ''success'' AND created_at > NOW() - INTERVAL ''5 minutes''', v_processor_log_table)
        INTO v_recent_successes;
    EXCEPTION WHEN OTHERS THEN
        v_recent_successes := 0;
    END;
    
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM %I WHERE status = ''failed'' AND created_at > NOW() - INTERVAL ''5 minutes''', v_processor_log_table)
        INTO v_recent_failures;
    EXCEPTION WHEN OTHERS THEN
        v_recent_failures := 0;
    END;
    
    RETURN QUERY
    SELECT 
        v_config_enabled,
        v_config_max_jobs,
        v_config_frequency,
        v_active_jobs_count,
        v_queue_length,
        v_recent_successes,
        v_recent_failures;
END;
$$ LANGUAGE plpgsql;