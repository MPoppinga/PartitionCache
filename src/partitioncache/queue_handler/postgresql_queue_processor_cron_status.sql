-- Function to get basic processor status
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status(p_queue_prefix TEXT, p_job_name TEXT DEFAULT NULL)
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
    
    -- Dynamic job name construction if not provided
    v_job_name := COALESCE(p_job_name, 'partitioncache_process_queue_' || current_database());

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
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status_detailed(p_table_prefix TEXT, p_queue_prefix TEXT, p_job_name TEXT DEFAULT NULL)
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
    
    -- Dynamic job name construction if not provided
    v_job_name := COALESCE(p_job_name, 'partitioncache_process_queue_' || current_database());

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
            partitioncache_get_processor_status(%L, %L) s',
        p_queue_prefix, v_job_name
    );
END;

$$ LANGUAGE plpgsql;