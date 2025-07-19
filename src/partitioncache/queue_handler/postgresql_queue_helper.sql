-- Non-blocking queue upsert functions for PartitionCache
-- Solves concurrency issues with ON CONFLICT + FOR UPDATE SKIP LOCKED
-- Uses FOR UPDATE NOWAIT to avoid blocking when rows are being processed

-- Function for fragment queue (hash, partition_key constraint)
CREATE OR REPLACE FUNCTION non_blocking_fragment_queue_upsert(
    p_hash TEXT,
    p_partition_key TEXT, 
    p_partition_datatype TEXT,
    p_query TEXT,
    p_priority INT DEFAULT 1,
    p_queue_table TEXT DEFAULT 'partitioncache_fragmentqueue'
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
                'INSERT INTO %I (hash, partition_key, partition_datatype, query, priority, updated_at, created_at) ' ||
                'VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ' ||
                'ON CONFLICT (hash, partition_key) DO UPDATE SET ' ||
                'priority = %I.priority + $5, updated_at = CURRENT_TIMESTAMP',
                p_queue_table, p_queue_table
            );
            EXECUTE v_sql USING p_hash, p_partition_key, p_partition_datatype, p_query, p_priority;
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

-- Helper function to create the functions with custom table names
CREATE OR REPLACE FUNCTION create_non_blocking_queue_functions(
    p_original_queue_table TEXT,
    p_fragment_queue_table TEXT
) RETURNS VOID AS $$
BEGIN
    -- This function allows creating the upsert functions with custom table names
    -- for different PartitionCache configurations
    
    EXECUTE format($func$
        CREATE OR REPLACE FUNCTION non_blocking_fragment_queue_upsert_%s(
            p_hash TEXT, p_partition_key TEXT, p_partition_datatype TEXT,
            p_query TEXT, p_priority INT DEFAULT 1
        ) RETURNS TEXT AS $inner$
        BEGIN
            RETURN non_blocking_fragment_queue_upsert(p_hash, p_partition_key, p_partition_datatype, p_query, p_priority, %L);
        END;
        $inner$ LANGUAGE plpgsql;
    $func$, replace(p_fragment_queue_table, 'partitioncache_', ''), p_fragment_queue_table);
    
    EXECUTE format($func$
        CREATE OR REPLACE FUNCTION non_blocking_original_queue_upsert_%s(
            p_query TEXT, p_partition_key TEXT, p_partition_datatype TEXT, 
            p_priority INT DEFAULT 1
        ) RETURNS TEXT AS $inner$
        BEGIN
            RETURN non_blocking_original_queue_upsert(p_query, p_partition_key, p_partition_datatype, p_priority, %L);
        END;
        $inner$ LANGUAGE plpgsql;
    $func$, replace(p_original_queue_table, 'partitioncache_', ''), p_original_queue_table);
    
    RAISE NOTICE 'Created non-blocking queue functions for tables: % and %', p_original_queue_table, p_fragment_queue_table;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT create_non_blocking_queue_functions('partitioncache_originalqueue', 'partitioncache_fragmentqueue');