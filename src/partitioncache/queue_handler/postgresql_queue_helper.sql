-- Function for fragment queue (hash, partition_key constraint)
CREATE OR REPLACE FUNCTION non_blocking_fragment_queue_upsert(
    p_hash TEXT,
    p_partition_key TEXT,
    p_partition_datatype TEXT,
    p_query TEXT,
    p_priority INT DEFAULT 1,
    p_queue_table TEXT DEFAULT 'partitioncache_fragmentqueue',
    p_cache_backend TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_id INT;
    v_sql TEXT;
BEGIN
    -- Try to find and lock the row that would cause a conflict
    -- If it's locked (being processed), NOWAIT will throw exception immediately
    v_sql := format('SELECT id FROM %I WHERE hash = $1 AND partition_key = $2 FOR UPDATE NOWAIT', p_queue_table);
    EXECUTE v_sql INTO v_id USING p_hash, p_partition_key;

    -- Check if row was found
    IF v_id IS NULL THEN
        RAISE no_data_found;
    END IF;

    -- If SELECT succeeded, row exists and we have the lock - safe to UPDATE
    v_sql := format(
        'UPDATE %I SET priority = priority + $1, updated_at = CURRENT_TIMESTAMP WHERE hash = $2 AND partition_key = $3',
        p_queue_table
    );
    EXECUTE v_sql USING p_priority, p_hash, p_partition_key;

    RETURN 'updated';

EXCEPTION
    -- Row doesn't exist - attempt INSERT with conflict handling
    WHEN no_data_found THEN
        BEGIN
            v_sql := format(
                'INSERT INTO %I (hash, partition_key, partition_datatype, query, priority, cache_backend, updated_at, created_at) ' ||
                'VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ' ||
                'ON CONFLICT (hash, partition_key) DO UPDATE SET ' ||
                'priority = %I.priority + $5, updated_at = CURRENT_TIMESTAMP',
                p_queue_table, p_queue_table
            );
            EXECUTE v_sql USING p_hash, p_partition_key, p_partition_datatype, p_query, p_priority, p_cache_backend;
            RETURN 'inserted';
        EXCEPTION
            -- Handle concurrent INSERT race conditions
            WHEN serialization_failure OR deadlock_detected OR unique_violation THEN
                RETURN 'skipped_concurrent';
        END;

    -- Row is locked (being processed) - skip operation to avoid blocking
    WHEN lock_not_available THEN
        RETURN 'skipped_locked';

    -- Handle concurrent update/insert errors that can occur during heavy load
    WHEN serialization_failure OR deadlock_detected THEN
        RETURN 'skipped_concurrent';

    -- Handle any other errors gracefully
    WHEN OTHERS THEN
        RAISE NOTICE 'Unexpected error in non_blocking_fragment_queue_upsert: %', SQLERRM;
        RETURN 'error';
END;
$$ LANGUAGE plpgsql;

-- Function for original query queue (query, partition_key constraint)  
CREATE OR REPLACE FUNCTION non_blocking_original_queue_upsert(
    p_query TEXT,
    p_partition_key TEXT,
    p_partition_datatype TEXT,
    p_priority INT DEFAULT 1,
    p_queue_table TEXT DEFAULT 'partitioncache_originalqueue'
) RETURNS TEXT AS $$
DECLARE
    v_id INT;
    v_sql TEXT;
BEGIN
    -- Try to find and lock the row that would cause a conflict
    -- If it's locked (being processed), NOWAIT will throw exception immediately
    v_sql := format('SELECT id FROM %I WHERE query = $1 AND partition_key = $2 FOR UPDATE NOWAIT', p_queue_table);
    EXECUTE v_sql INTO v_id USING p_query, p_partition_key;

    -- Check if row was found
    IF v_id IS NULL THEN
        RAISE no_data_found;
    END IF;

    -- If SELECT succeeded, row exists and we have the lock - safe to UPDATE
    v_sql := format(
        'UPDATE %I SET priority = priority + $1, updated_at = CURRENT_TIMESTAMP WHERE query = $2 AND partition_key = $3',
        p_queue_table
    );
    EXECUTE v_sql USING p_priority, p_query, p_partition_key;
    
    RETURN 'updated';

EXCEPTION
    -- Row doesn't exist - attempt INSERT with conflict handling
    WHEN no_data_found THEN
        BEGIN
            v_sql := format(
                'INSERT INTO %I (query, partition_key, partition_datatype, priority, updated_at, created_at) ' ||
                'VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ' ||
                'ON CONFLICT (query, partition_key) DO UPDATE SET ' ||
                'priority = %I.priority + $4, updated_at = CURRENT_TIMESTAMP',
                p_queue_table, p_queue_table
            );
            EXECUTE v_sql USING p_query, p_partition_key, p_partition_datatype, p_priority;
            RETURN 'inserted';
        EXCEPTION
            -- Handle concurrent INSERT race conditions
            WHEN serialization_failure OR deadlock_detected OR unique_violation THEN
                RETURN 'skipped_concurrent';
        END;
        
    -- Row is locked (being processed) - skip operation to avoid blocking
    WHEN lock_not_available THEN
        RETURN 'skipped_locked';
        
    -- Handle concurrent update/insert errors that can occur during heavy load
    WHEN serialization_failure OR deadlock_detected THEN
        RETURN 'skipped_concurrent';
        
    -- Handle any other errors gracefully
    WHEN OTHERS THEN
        RAISE NOTICE 'Unexpected error in non_blocking_original_queue_upsert: %', SQLERRM;
        RETURN 'error';
END;
$$ LANGUAGE plpgsql;


-- Batch function for fragment queue (wrapper for non_blocking_fragment_queue_upsert)
CREATE OR REPLACE FUNCTION non_blocking_fragment_queue_batch_upsert(
    p_hashes TEXT[],
    p_partition_keys TEXT[],
    p_partition_datatypes TEXT[],
    p_queries TEXT[],
    p_priorities INT[] DEFAULT NULL,
    p_queue_table TEXT DEFAULT 'partitioncache_fragmentqueue',
    p_cache_backends TEXT[] DEFAULT NULL
) RETURNS TABLE(idx INT, hash TEXT, partition_key TEXT, status TEXT) AS $$
DECLARE
    v_array_length INT;
    v_idx INT;
    v_status TEXT;
BEGIN
    -- Validate array lengths
    v_array_length := array_length(p_hashes, 1);

    IF v_array_length IS NULL OR v_array_length = 0 THEN
        RETURN;
    END IF;

    IF array_length(p_partition_keys, 1) != v_array_length OR
       array_length(p_partition_datatypes, 1) != v_array_length OR
       array_length(p_queries, 1) != v_array_length OR
       (p_priorities IS NOT NULL AND array_length(p_priorities, 1) != v_array_length) OR
       (p_cache_backends IS NOT NULL AND array_length(p_cache_backends, 1) != v_array_length) THEN
        RAISE EXCEPTION 'All input arrays must have the same length';
    END IF;

    -- Process each item using the existing function
    FOR v_idx IN 1..v_array_length LOOP
        v_status := non_blocking_fragment_queue_upsert(
            p_hashes[v_idx],
            p_partition_keys[v_idx],
            p_partition_datatypes[v_idx],
            p_queries[v_idx],
            COALESCE(p_priorities[v_idx], 1),
            p_queue_table,
            CASE WHEN p_cache_backends IS NOT NULL THEN p_cache_backends[v_idx] ELSE NULL END
        );

        -- Return result for this item
        RETURN QUERY SELECT v_idx, p_hashes[v_idx], p_partition_keys[v_idx], v_status;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Overloaded version with default queue table and priorities
CREATE OR REPLACE FUNCTION non_blocking_fragment_queue_batch_upsert(
    p_hashes TEXT[],
    p_partition_keys TEXT[],
    p_partition_datatypes TEXT[],
    p_queries TEXT[]
) RETURNS TABLE(idx INT, hash TEXT, partition_key TEXT, status TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM non_blocking_fragment_queue_batch_upsert(
        p_hashes,
        p_partition_keys,
        p_partition_datatypes,
        p_queries,
        NULL::INT[],
        'partitioncache_fragmentqueue',
        NULL::TEXT[]
    );
END;
$$ LANGUAGE plpgsql;

-- Example usage:

-- Batch usage example:
-- SELECT * FROM non_blocking_fragment_queue_batch_upsert(
--     ARRAY['hash1', 'hash2', 'hash3'],
--     ARRAY['city_id', 'city_id', 'city_id'],
--     ARRAY['integer', 'integer', 'integer'],
--     ARRAY['SELECT * FROM table1', 'SELECT * FROM table2', 'SELECT * FROM table3'],
--     ARRAY[1, 2, 1]
-- );