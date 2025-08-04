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