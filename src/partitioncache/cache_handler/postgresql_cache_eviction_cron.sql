-- PartitionCache PostgreSQL Cache Eviction - Cron Database Components
-- This file contains components that must be installed in the pg_cron database
-- These components handle eviction job scheduling via pg_cron

-- Function to initialize eviction configuration table in cron database
CREATE OR REPLACE FUNCTION partitioncache_initialize_eviction_cron_config_table(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';

    -- Configuration table for the eviction processor (lives in cron database)
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            job_name TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT false,
            frequency_minutes INTEGER NOT NULL DEFAULT 60 CHECK (frequency_minutes > 0),
            strategy TEXT NOT NULL DEFAULT ''oldest'' CHECK (strategy IN (''oldest'', ''largest'')),
            threshold INTEGER NOT NULL DEFAULT 1000 CHECK (threshold > 0),
            table_prefix TEXT NOT NULL,
            target_database TEXT NOT NULL, -- Database where eviction work should be executed
            job_id BIGINT DEFAULT NULL, -- Store pg_cron job ID for cross-database management
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);
END;
$$ LANGUAGE plpgsql;

-- Trigger function to synchronize config with pg_cron using cross-database scheduling
CREATE OR REPLACE FUNCTION partitioncache_sync_eviction_cron_job()
RETURNS TRIGGER AS $$
DECLARE
    v_command TEXT;
    v_target_database TEXT;
    v_job_id BIGINT;
    v_schedule TEXT;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        -- Unschedule job using stored job_id
        IF OLD.job_id IS NOT NULL THEN
            BEGIN
                PERFORM cron.unschedule(OLD.job_id);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
            END;
        END IF;
        RETURN OLD;
    END IF;

    v_target_database := NEW.target_database; -- Use target database from config

    -- Build the command to be executed by cron in target database with parameters
    v_command := format('SELECT partitioncache_run_eviction_job_with_params(%L, %L, %L, %L)', 
                       NEW.job_name, NEW.table_prefix, NEW.strategy, NEW.threshold);

    -- First, unschedule existing job if this is an UPDATE
    IF (TG_OP = 'UPDATE' AND OLD.job_id IS NOT NULL) THEN
        BEGIN
            PERFORM cron.unschedule(OLD.job_id);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
        END;
    END IF;

    -- Schedule new job using pg_cron API with cross-database execution
    BEGIN
        -- Convert frequency to proper cron format
        -- pg_cron accepts "X minutes" only for X < 60, otherwise needs cron format
        IF NEW.frequency_minutes < 60 THEN
            v_schedule := CONCAT(NEW.frequency_minutes, ' minutes');
        ELSE
            -- Convert to cron format: "0 */X * * *" for every X hours
            -- For non-hour multiples, use minute intervals: "*/X * * * *" 
            IF NEW.frequency_minutes % 60 = 0 THEN
                v_schedule := CONCAT('0 */', NEW.frequency_minutes / 60, ' * * *');
            ELSE
                v_schedule := CONCAT('*/', NEW.frequency_minutes, ' * * * *');
            END IF;
        END IF;
        
        SELECT cron.schedule_in_database(
            NEW.job_name,
            v_schedule,
            v_command,
            v_target_database,
            current_user,
            NEW.enabled
        ) INTO v_job_id;
        
        -- Store the job ID for later management
        NEW.job_id := v_job_id;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Failed to schedule eviction job %: %', NEW.job_name, SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to attach the trigger to the eviction config table
CREATE OR REPLACE FUNCTION partitioncache_create_eviction_cron_config_trigger(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_trigger_name TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';
    v_trigger_name := 'trg_sync_eviction_cron_job_' || replace(v_config_table, '.', '_');

    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, v_config_table);

    EXECUTE format('
        CREATE TRIGGER %I
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION partitioncache_sync_eviction_cron_job()
    ', v_trigger_name, v_config_table);
END;
$$ LANGUAGE plpgsql;

-- Function to enable or disable eviction (from cron database)
CREATE OR REPLACE FUNCTION partitioncache_set_eviction_enabled_cron(
    p_enabled BOOLEAN, 
    p_table_prefix TEXT,
    p_job_name TEXT
)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';
    EXECUTE format(
        'UPDATE %I SET enabled = %L, updated_at = NOW() WHERE job_name = %L',
        v_config_table, p_enabled, p_job_name
    );
END;
$$ LANGUAGE plpgsql;

-- Function to update eviction configuration (from cron database)
CREATE OR REPLACE FUNCTION partitioncache_update_eviction_config_cron(
    p_job_name TEXT,
    p_table_prefix TEXT,
    p_enabled BOOLEAN DEFAULT NULL,
    p_frequency_minutes INTEGER DEFAULT NULL,
    p_strategy TEXT DEFAULT NULL,
    p_threshold INTEGER DEFAULT NULL,
    p_target_database TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_update_sql TEXT;
    v_set_clauses TEXT[] := '{}';
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';

    -- Build the SET clauses dynamically
    IF p_enabled IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('enabled = %L', p_enabled));
    END IF;
    IF p_frequency_minutes IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('frequency_minutes = %L', p_frequency_minutes));
    END IF;
    IF p_strategy IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('strategy = %L', p_strategy));
    END IF;
    IF p_threshold IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('threshold = %L', p_threshold));
    END IF;
    IF p_target_database IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('target_database = %L', p_target_database));
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

-- Function to get eviction status from cron database
CREATE OR REPLACE FUNCTION partitioncache_get_eviction_status_cron(p_table_prefix TEXT, p_job_name TEXT)
RETURNS TABLE(
    job_name TEXT,
    enabled BOOLEAN,
    frequency_minutes INTEGER,
    strategy TEXT,
    threshold INTEGER,
    table_prefix TEXT,
    target_database TEXT,
    updated_at TIMESTAMP,
    job_is_active BOOLEAN,
    job_schedule TEXT,
    job_command TEXT
) AS $$
DECLARE
    v_config_table TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';

    RETURN QUERY
    EXECUTE format(
        'WITH cron_jobs AS (
            SELECT
                active,
                schedule,
                command
            FROM cron.job
            WHERE jobname = %L
        )
        SELECT
            conf.job_name,
            conf.enabled,
            conf.frequency_minutes,
            conf.strategy,
            conf.threshold,
            conf.table_prefix,
            conf.target_database,
            conf.updated_at,
            (SELECT active FROM cron_jobs) as job_is_active,
            (SELECT schedule FROM cron_jobs) as job_schedule,
            (SELECT command FROM cron_jobs) as job_command
        FROM
            %I conf
        WHERE
            conf.job_name = %L',
        p_job_name, v_config_table, p_job_name
    );
END;
$$ LANGUAGE plpgsql;