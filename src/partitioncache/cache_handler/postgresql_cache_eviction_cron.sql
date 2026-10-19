-- PartitionCache PostgreSQL Cache Eviction - Cron Database Components
-- This file contains components that must be installed in the pg_cron database
-- These components handle eviction job scheduling via pg_cron

-- Function to construct a unique eviction job name based on database and table prefix
-- This ensures multiple eviction managers can coexist for different databases and table prefixes
CREATE OR REPLACE FUNCTION partitioncache_construct_eviction_job_name(
    p_target_database TEXT,
    p_table_prefix TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_job_name TEXT;
    v_table_suffix TEXT;
BEGIN
    -- Base name includes the database
    v_job_name := 'partitioncache_evict_' || p_target_database;

    IF p_table_prefix IS NOT NULL AND p_table_prefix != '' THEN
        -- Extract suffix from table prefix (e.g., 'partitioncache_cache1' -> 'cache1')
        -- Special case: if table_prefix is just underscores or doesn't contain 'partitioncache'
        IF REPLACE(p_table_prefix, '_', '') = '' OR POSITION('partitioncache' IN p_table_prefix) = 0 THEN
            -- This is a custom prefix (not standard partitioncache pattern)
            v_table_suffix := REPLACE(p_table_prefix, '_', '');
            IF v_table_suffix = '' THEN
                v_table_suffix := 'custom';
            END IF;
        ELSE
            -- Remove 'partitioncache' prefix and any underscores
            v_table_suffix := REPLACE(REPLACE(p_table_prefix, 'partitioncache', ''), '_', '');
            IF v_table_suffix = '' THEN
                v_table_suffix := 'default';
            END IF;
        END IF;
        v_job_name := v_job_name || '_' || v_table_suffix;
    END IF;


    RETURN v_job_name;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

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
            job_owner TEXT DEFAULT current_user, -- Role used to run the pg_cron job
            job_id BIGINT DEFAULT NULL, -- Store pg_cron job ID for cross-database management
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )', v_config_table);

    -- Migration: add job_owner column for existing installations
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS job_owner TEXT DEFAULT current_user', v_config_table);
END;
$$ LANGUAGE plpgsql;

-- Function to schedule eviction log cleanup in the target database.
CREATE OR REPLACE FUNCTION partitioncache_schedule_eviction_log_cleanup(
    p_table_prefix TEXT,
    p_target_database TEXT,
    p_retention_days INTEGER DEFAULT 30,
    p_schedule TEXT DEFAULT '0 3 * * *'
)
RETURNS BIGINT AS $$
DECLARE
    v_job_name TEXT;
    v_command TEXT;
    v_job_id BIGINT;
BEGIN
    v_job_name := 'partitioncache_eviction_log_cleanup_' || p_table_prefix;

    BEGIN
        PERFORM cron.unschedule(v_job_name);
    EXCEPTION WHEN OTHERS THEN
        -- Ignore if missing.
    END;

    v_command := format('SELECT partitioncache_cleanup_eviction_logs(%L, %L)', p_table_prefix, p_retention_days);

    SELECT cron.schedule_in_database(
        v_job_name,
        p_schedule,
        v_command,
        p_target_database
    ) INTO v_job_id;

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to unschedule eviction log cleanup.
CREATE OR REPLACE FUNCTION partitioncache_unschedule_eviction_log_cleanup(
    p_table_prefix TEXT
)
RETURNS VOID AS $$
DECLARE
    v_job_name TEXT;
BEGIN
    v_job_name := 'partitioncache_eviction_log_cleanup_' || p_table_prefix;

    BEGIN
        PERFORM cron.unschedule(v_job_name);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not unschedule eviction log cleanup job %: %', v_job_name, SQLERRM;
    END;
END;
$$ LANGUAGE plpgsql;

-- Trigger function to synchronize config with pg_cron using cross-database scheduling
CREATE OR REPLACE FUNCTION partitioncache_sync_eviction_cron_job()
RETURNS TRIGGER AS $$
DECLARE
    v_command TEXT;
    v_target_database TEXT;
    v_job_owner TEXT;
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
    v_job_owner := COALESCE(NEW.job_owner, current_user);

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
        -- pg_cron schedule_in_database only accepts "[1-59] seconds" or standard cron format
        -- For minute intervals, always use cron format: "*/X * * * *"
        IF NEW.frequency_minutes = 1 THEN
            v_schedule := '* * * * *';
        ELSIF NEW.frequency_minutes < 60 THEN
            v_schedule := CONCAT('*/', NEW.frequency_minutes, ' * * * *');
        ELSE
            -- Convert to cron format: "0 */X * * *" for every X hours
            -- For non-hour multiples, use minute intervals: "*/X * * * *"
            IF NEW.frequency_minutes % 60 = 0 THEN
                v_schedule := CONCAT('0 */', NEW.frequency_minutes / 60, ' * * *');
            ELSE
                v_schedule := CONCAT('*/', NEW.frequency_minutes, ' * * * *');
            END IF;
        END IF;

        -- Use 6-arg form: job_name, schedule, command, database, username, active
        SELECT cron.schedule_in_database(
            NEW.job_name,
            v_schedule,
            v_command,
            v_target_database,
            v_job_owner,
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
        BEFORE INSERT OR UPDATE OR DELETE ON %I
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

-- Function to update eviction processor configuration (from cron database)
CREATE OR REPLACE FUNCTION partitioncache_update_eviction_config_cron(
    p_table_prefix TEXT,
    p_job_name TEXT DEFAULT NULL,
    p_enabled BOOLEAN DEFAULT NULL,
    p_frequency_minutes INTEGER DEFAULT NULL,
    p_strategy TEXT DEFAULT NULL,
    p_threshold INTEGER DEFAULT NULL,
    p_job_owner TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_config_table TEXT;
    v_update_sql TEXT;
    v_set_clauses TEXT[] := '{}';
    v_job_name TEXT;
BEGIN
    v_config_table := p_table_prefix || '_eviction_config';

    -- Determine job name
    IF p_job_name IS NOT NULL THEN
        v_job_name := p_job_name;
    ELSE
        -- Try to find a single config entry
        EXECUTE format('SELECT job_name FROM %I LIMIT 1', v_config_table) INTO v_job_name;
        IF v_job_name IS NULL THEN
            RAISE EXCEPTION 'No eviction configuration found in table %', v_config_table;
        END IF;
    END IF;

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
    IF p_job_owner IS NOT NULL THEN
        v_set_clauses := array_append(v_set_clauses, format('job_owner = %L', p_job_owner));
    END IF;

    -- Only execute if there's something to update
    IF array_length(v_set_clauses, 1) > 0 THEN
        v_update_sql := format(
            'UPDATE %I SET %s, updated_at = NOW() WHERE job_name = %L',
            v_config_table,
            array_to_string(v_set_clauses, ', '),
            v_job_name
        );
        EXECUTE v_update_sql;
    END IF;
END;
$$ LANGUAGE plpgsql;
