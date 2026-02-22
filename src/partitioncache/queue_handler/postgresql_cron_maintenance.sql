-- PartitionCache PostgreSQL Cron Maintenance
-- Optional helpers to keep pg_cron metadata bounded.

-- Function to clean up old cron.job_run_details entries.
CREATE OR REPLACE FUNCTION partitioncache_cleanup_cron_run_details(
    p_retention_days INTEGER DEFAULT 30
)
RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM cron.job_run_details
    WHERE start_time < NOW() - (p_retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Schedule recurring cleanup in the cron database.
CREATE OR REPLACE FUNCTION partitioncache_schedule_cron_run_details_cleanup(
    p_database_name TEXT,
    p_retention_days INTEGER DEFAULT 30,
    p_schedule TEXT DEFAULT '0 3 * * *'
)
RETURNS BIGINT AS $$
DECLARE
    v_job_name TEXT := 'partitioncache_cron_maintenance_cleanup_details';
    v_command TEXT;
    v_job_id BIGINT;
BEGIN
    BEGIN
        PERFORM cron.unschedule(v_job_name);
    EXCEPTION WHEN OTHERS THEN
        -- Ignore if missing.
    END;

    v_command := format('SELECT partitioncache_cleanup_cron_run_details(%L)', p_retention_days);

    SELECT cron.schedule_in_database(
        v_job_name,
        p_schedule,
        v_command,
        p_database_name
    ) INTO v_job_id;

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;
