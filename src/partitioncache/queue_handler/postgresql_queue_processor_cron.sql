-- PartitionCache PostgreSQL Queue Processor - Cron Database Components
-- This file contains components that must be installed in the pg_cron database
-- These components handle job scheduling via pg_cron and cross-database execution

-- Function to initialize processor configuration table in cron database
CREATE OR REPLACE FUNCTION partitioncache_initialize_cron_config_table(p_queue_prefix TEXT DEFAULT 'partitioncache_queue')
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_queue_prefix || '_processor_config';
    
    -- Configuration table to control the processor (lives in cron database)
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
            target_database TEXT NOT NULL, -- Database where work should be executed
            result_limit INTEGER DEFAULT NULL CHECK (result_limit IS NULL OR result_limit > 0), -- Limit number of partition keys, NULL = disabled
            job_ids BIGINT[] DEFAULT ARRAY[]::BIGINT[], -- Store pg_cron job IDs for management
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);
END;
$$ LANGUAGE plpgsql;

-- Trigger function to synchronize config with pg_cron using cross-database scheduling
CREATE OR REPLACE FUNCTION partitioncache_sync_cron_job()
RETURNS TRIGGER AS $$
DECLARE
    v_command TEXT;
    v_job_base TEXT;
    v_timeout_seconds INTEGER;
    v_timeout_statement TEXT;
    v_target_database TEXT;
    v_job_name TEXT;
    v_job_id BIGINT;
    v_new_job_ids BIGINT[] := ARRAY[]::BIGINT[];
    v_old_job_id BIGINT;
    v_schedule TEXT;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        -- Unschedule all jobs stored in job_ids array
        IF OLD.job_ids IS NOT NULL THEN
            FOREACH v_old_job_id IN ARRAY OLD.job_ids LOOP
                BEGIN
                    PERFORM cron.unschedule(v_old_job_id);
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Could not unschedule job ID %: %', v_old_job_id, SQLERRM;
                END;
            END LOOP;
        END IF;
        RETURN OLD;
    END IF;

    v_job_base := NEW.job_name;
    v_target_database := NEW.target_database; -- Use target database from config
    v_timeout_seconds := COALESCE(NEW.timeout_seconds, 1800);

    -- Set timeout for the target database session
    v_timeout_statement := 'SET LOCAL statement_timeout = ' || (v_timeout_seconds * 1000)::TEXT;

    -- Build command to execute in target database with proper timeout and parameters
    v_command := format('BEGIN; %s; SELECT * FROM partitioncache_run_single_job_with_params(%L, %L, %L, %L, %L, %L); COMMIT;', 
                       v_timeout_statement, NEW.job_name, NEW.table_prefix, NEW.queue_prefix, 
                       NEW.cache_backend, NEW.timeout_seconds, NEW.result_limit);

    -- First, unschedule existing jobs if this is an UPDATE
    IF (TG_OP = 'UPDATE' AND OLD.job_ids IS NOT NULL) THEN
        FOREACH v_old_job_id IN ARRAY OLD.job_ids LOOP
            BEGIN
                PERFORM cron.unschedule(v_old_job_id);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not unschedule job ID %: %', v_old_job_id, SQLERRM;
            END;
        END LOOP;
    END IF;

    -- Create new jobs using pg_cron API with cross-database execution
    FOR i IN 1..NEW.max_parallel_jobs LOOP
        v_job_name := v_job_base || '_' || i;
        
        BEGIN
            -- Use schedule_in_database to run jobs in the target database
            -- Convert frequency to proper cron format
            IF NEW.frequency_seconds < 60 THEN
                v_schedule := CONCAT(NEW.frequency_seconds, ' seconds');
            ELSE
                -- Convert to cron format for intervals ≥60 seconds
                IF NEW.frequency_seconds % 60 = 0 THEN
                    -- Even minutes: use minute interval
                    v_schedule := CONCAT('*/', NEW.frequency_seconds / 60, ' * * * *');
                ELSE
                    -- Use second-level cron format: "*/X * * * * *"
                    v_schedule := CONCAT('*/', NEW.frequency_seconds, ' * * * * *');
                END IF;
            END IF;
            
            SELECT cron.schedule_in_database(
                v_job_name,
                v_schedule,
                v_command,
                v_target_database,
                current_user,
                NEW.enabled
            ) INTO v_job_id;
            
            v_new_job_ids := array_append(v_new_job_ids, v_job_id);
            
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to schedule job %: %', v_job_name, SQLERRM;
        END;
    END LOOP;

    -- Update the job_ids array in the config table
    NEW.job_ids := v_new_job_ids;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to attach the trigger to the config table
CREATE OR REPLACE FUNCTION partitioncache_create_cron_config_trigger(p_queue_prefix TEXT)
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
        BEFORE INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION partitioncache_sync_cron_job()
    ', v_trigger_name, v_config_table);
END;
$$ LANGUAGE plpgsql;

-- Function to enable or disable the queue processor (from cron database)
CREATE OR REPLACE FUNCTION partitioncache_set_processor_enabled_cron(
    p_enabled BOOLEAN, 
    p_queue_prefix TEXT DEFAULT 'partitioncache_queue', 
    p_job_name TEXT DEFAULT 'partitioncache_process_queue'
)
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

-- Function to update processor configuration (from cron database)
CREATE OR REPLACE FUNCTION partitioncache_update_processor_config_cron(
    p_job_name TEXT,
    p_enabled BOOLEAN DEFAULT NULL, 
    p_max_parallel_jobs INTEGER DEFAULT NULL,
    p_frequency_seconds INTEGER DEFAULT NULL,
    p_timeout_seconds INTEGER DEFAULT NULL,
    p_table_prefix TEXT DEFAULT NULL,
    p_target_database TEXT DEFAULT NULL,
    p_result_limit INTEGER DEFAULT NULL,
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
    IF p_target_database IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('target_database = %L', p_target_database));
    END IF;
    IF p_result_limit IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('result_limit = %L', p_result_limit));
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

-- Function to get processor status from cron database
CREATE OR REPLACE FUNCTION partitioncache_get_processor_status_cron(p_queue_prefix TEXT, p_job_name TEXT DEFAULT 'partitioncache_process_queue')
RETURNS TABLE(
    job_name TEXT,
    enabled BOOLEAN,
    max_parallel_jobs INTEGER,
    frequency_seconds INTEGER,
    timeout_seconds INTEGER,
    table_prefix TEXT,
    queue_prefix TEXT,
    cache_backend TEXT,
    target_database TEXT,
    updated_at TIMESTAMP,
    job_is_active BOOLEAN,
    job_schedule TEXT,
    job_command TEXT
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
            conf.target_database,
            conf.updated_at,
            (SELECT bool_or(active) FROM cron_jobs) as job_is_active,
            (SELECT schedule FROM cron_jobs LIMIT 1) as job_schedule,
            (SELECT command FROM cron_jobs LIMIT 1) as job_command
        FROM
            %I conf
        WHERE
            conf.job_name = %L',
        p_job_name, '_%', v_config_table, p_job_name
    );
END;
$$ LANGUAGE plpgsql;