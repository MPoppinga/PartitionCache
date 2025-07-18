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
        -- Try to unschedule by job_id first
        IF OLD.job_id IS NOT NULL THEN
            BEGIN
                PERFORM cron.unschedule(OLD.job_id);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
            END;
        END IF;
        
        -- Also try to unschedule by job name to ensure cleanup
        BEGIN
            PERFORM cron.unschedule(OLD.job_name);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not unschedule job by name %: %', OLD.job_name, SQLERRM;
        END;
        
        RETURN OLD;
    END IF;

    v_target_database := NEW.target_database; -- Use target database from config

    -- Build the command to be executed by cron in target database with parameters
    v_command := format('SELECT partitioncache_run_eviction_job_with_params(%L, %L, %L, %L)', 
                       NEW.job_name, NEW.table_prefix, NEW.strategy, NEW.threshold);

    -- Always remove existing job on UPDATE to ensure sync (handles external changes)
    IF (TG_OP = 'UPDATE') THEN
        -- Try to unschedule by job_id first
        IF OLD.job_id IS NOT NULL THEN
            BEGIN
                PERFORM cron.unschedule(OLD.job_id);
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not unschedule job ID %: %', OLD.job_id, SQLERRM;
            END;
        END IF;
        
        -- Also try to unschedule by job name to catch externally modified jobs
        BEGIN
            PERFORM cron.unschedule(OLD.job_name);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not unschedule job by name %: %', OLD.job_name, SQLERRM;
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
            v_target_database
        ) INTO v_job_id;
        
        -- Enable/disable the job after creation
        IF NOT NEW.enabled THEN
            PERFORM cron.alter_job(v_job_id, active => false);
        END IF;
        
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

-- Note: Enable/disable is handled by direct UPDATE to config table
-- The trigger partitioncache_sync_eviction_cron_job() handles cron job management

-- Note: Configuration updates are handled by direct UPDATE to config table
-- The trigger partitioncache_sync_eviction_cron_job() handles cron job management

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