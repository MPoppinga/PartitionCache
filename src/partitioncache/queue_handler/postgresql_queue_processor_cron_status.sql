-- Helper function to resolve job name from parameters
CREATE OR REPLACE FUNCTION partitioncache_resolve_job_name(
    p_queue_prefix TEXT,
    p_target_database TEXT DEFAULT NULL,
    p_job_name TEXT DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_config_table TEXT;
    v_job_name TEXT;
    v_target_db TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    
    -- Determine job name to search for
    IF p_job_name IS NOT NULL THEN
        RETURN p_job_name;
    ELSIF p_target_database IS NOT NULL THEN
        -- Try to find a config entry for this target database to get table_prefix
        DECLARE
            v_table_prefix TEXT;
            v_found_configs INTEGER := 0;
        BEGIN
            EXECUTE format('SELECT COUNT(*), MAX(table_prefix) FROM %I WHERE target_database = %L', 
                          v_config_table, p_target_database) INTO v_found_configs, v_table_prefix;
            
            IF v_found_configs = 1 THEN
                -- Single config found - use table_prefix for job name construction
                RETURN partitioncache_construct_job_name(p_target_database, v_table_prefix);
            ELSIF v_found_configs > 1 THEN
                -- Multiple configs - ambiguous, require explicit job_name
                RAISE EXCEPTION 'Multiple processor configurations found for database %. Please specify explicit job_name parameter.', p_target_database;
            ELSE
                -- No configs found - use simple naming
                RETURN partitioncache_construct_job_name(p_target_database, NULL);
            END IF;
        END;
    ELSE
        -- Try to get from config if neither provided
        BEGIN
            EXECUTE format('SELECT target_database FROM %I LIMIT 1', v_config_table) INTO v_target_db;
            RETURN partitioncache_construct_job_name(v_target_db, NULL);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'Either target_database or job_name must be provided when config is empty';
        END;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get basic processor status
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status(
    p_queue_prefix TEXT, 
    p_target_database TEXT DEFAULT NULL,
    p_job_name TEXT DEFAULT NULL
)
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
    v_job_name TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    v_job_name := partitioncache_resolve_job_name(p_queue_prefix, p_target_database, p_job_name);

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
            0::INTEGER
        FROM
            %I conf
        WHERE
            conf.job_name = %L',
        v_job_name, '_%', v_config_table, v_job_name
    );
END;
$$ LANGUAGE plpgsql;

-- Enhanced processor status function with queue information
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status_detailed(
    p_table_prefix TEXT, 
    p_queue_prefix TEXT, 
    p_target_database TEXT DEFAULT NULL,
    p_job_name TEXT DEFAULT NULL
)
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
    -- Details
    recent_logs JSON,
    active_job_details JSON
) AS $$
DECLARE
    v_queue_table TEXT;
    v_config_table TEXT;
    v_job_name TEXT;
BEGIN
    v_queue_table := p_queue_prefix || '_query_fragment_queue';
    v_config_table := p_queue_prefix || '_processor_config';
    v_job_name := partitioncache_resolve_job_name(p_queue_prefix, p_target_database, p_job_name);

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
            0::INTEGER as queue_length,
            0::INTEGER as recent_successes_5m,
            0::INTEGER as recent_failures_5m,
            NULL::JSON as recent_logs,
            NULL::JSON as active_job_details
        FROM
            partitioncache_get_processor_status(%L, %L, %L) s',
        p_queue_prefix, p_target_database, v_job_name
    );
END;

$$ LANGUAGE plpgsql;