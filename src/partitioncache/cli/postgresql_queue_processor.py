#!/usr/bin/env python3
"""
Setup and manage PostgreSQL queue processor for PartitionCache.

This script sets up the necessary database objects and pg_cron jobs to process
queries directly within PostgreSQL, eliminating the need for external observer scripts.

Usage Examples:
    # Basic setup
    pcache-postgresql-queue-processor setup

    # Show basic processor status
    pcache-postgresql-queue-processor status

    # Show detailed status with cache architecture overview
    pcache-postgresql-queue-processor status-detailed

    # Show detailed queue and cache information for each partition
    pcache-postgresql-queue-processor queue-info

    # Show detailed queue info with custom table prefix
    pcache-postgresql-queue-processor queue-info --table-prefix my_cache

"""

import argparse
import os
import sys
from logging import getLogger
from pathlib import Path

import psycopg
from psycopg import sql

from partitioncache.cli.common_args import add_environment_args, add_verbosity_args, configure_logging, load_environment_with_validation

logger = getLogger("PartitionCache.PostgreSQLQueueProcessor")

# SQL file locations
SQL_HANDLERS_FILE = Path(__file__).parent.parent / "cache_handler" / "postgresql_cache_handlers.sql"
SQL_CRON_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor_cron.sql"
SQL_CACHE_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor_cache.sql"
SQL_CRON_STATUS_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor_cron_status.sql"
SQL_CACHE_INFO_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor_cache_info.sql"


def get_cache_backend_from_env() -> str:
    """
    Get the cache backend type from environment variables.
    """
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
    supported_backends = {"postgresql_array", "postgresql_bit", "postgresql_roaringbit"}
    if cache_backend not in supported_backends:
        raise ValueError(f"Unsupported CACHE_BACKEND for direct processor: {cache_backend}")

    # Return the simple name for the backend (e.g., 'array', 'bit')
    return cache_backend.replace("postgresql_", "")


def get_table_prefix_from_env() -> str:
    """
    Get the table prefix from environment variables based on the CACHE_BACKEND.
    This is used to determine which cache tables the direct processor should interact with.
    """
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")

    supported_backends = {
        "postgresql_array": "PG_ARRAY_CACHE_TABLE_PREFIX",
        "postgresql_bit": "PG_BIT_CACHE_TABLE_PREFIX",
        "postgresql_roaringbit": "PG_ROARINGBIT_CACHE_TABLE_PREFIX",
    }

    if cache_backend not in supported_backends:
        raise ValueError(f"Unsupported CACHE_BACKEND for direct processor: {cache_backend}")

    table_prefix_env = supported_backends[cache_backend]
    table_prefix = os.getenv(table_prefix_env)

    if not table_prefix:
        raise ValueError(f"Environment variable {table_prefix_env} is not set for CACHE_BACKEND {cache_backend}")

    return table_prefix


def get_queue_table_prefix_from_env() -> str:
    """
    Get the queue table prefix from environment variables.

    Returns the queue table prefix, defaulting to partitioncache_queue.
    """
    return os.getenv("PG_QUEUE_TABLE_PREFIX", "partitioncache_queue")


def get_cache_database_name() -> str:
    """Get the cache database name from environment."""
    return os.getenv("DB_NAME", "postgres")


def get_cron_database_name() -> str:
    """Get the cron database name from environment."""
    return os.getenv("PG_CRON_DATABASE", "postgres")


def construct_processor_job_name(target_database: str, table_prefix: str | None) -> str:
    """
    Construct a processor job name from database and table prefix.

    This mirrors the SQL partitioncache_construct_job_name() function logic
    to ensure consistency between Python and SQL job naming.

    Args:
        target_database: The target database name
        table_prefix: Optional table prefix for multiple processors per database

    Returns:
        The constructed job name following the pattern:
        partitioncache_process_queue_<database>[_<suffix>]
    """
    base_name = f"partitioncache_process_queue_{target_database}"

    if not table_prefix or table_prefix.strip() == "":
        # No suffix for null/empty prefix
        job_name = base_name
    else:
        # Extract and clean suffix based on prefix pattern
        if table_prefix == "partitioncache":
            suffix = "default"
        elif table_prefix.startswith("partitioncache_"):
            suffix = table_prefix[len("partitioncache_") :].replace("_", "") or "default"
        else:
            suffix = table_prefix.replace("_", "") or "custom"

        job_name = f"{base_name}_{suffix}"


    return job_name


def validate_environment() -> tuple[bool, str]:
    """Validate that all required environment variables are set and consistent."""
    required_vars = {
        "queue": ["PG_QUEUE_HOST", "PG_QUEUE_PORT", "PG_QUEUE_USER", "PG_QUEUE_PASSWORD", "PG_QUEUE_DB"],
        "cache": ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"],
    }

    errors = []

    # Check required variables
    for component, vars in required_vars.items():
        for var in vars:
            if not os.getenv(var):
                errors.append(f"{var} not set (required for {component})")

    if errors:
        return False, "Missing environment variables:\n" + "\n".join(errors)

    # Check if queue and cache are the same database instance
    # Note: pg_cron database (PG_CRON_*) can be different for cross-database setup
    queue_config = {
        "host": os.getenv("PG_QUEUE_HOST"),
        "port": os.getenv("PG_QUEUE_PORT"),
        "db": os.getenv("PG_QUEUE_DB"),
    }

    cache_config = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "db": os.getenv("DB_NAME"),
    }

    if queue_config != cache_config:
        return False, (
            f"Queue and cache databases must be the same instance for direct processing.\n"
            f"Queue: {queue_config['host']}:{queue_config['port']}/{queue_config['db']}\n"
            f"Cache: {cache_config['host']}:{cache_config['port']}/{cache_config['db']}\n"
            f"Note: pg_cron database (PG_CRON_DATABASE) can be different for cross-database setup."
        )

    try:
        get_table_prefix_from_env()
    except ValueError as e:
        return False, str(e)

    return True, "Environment validated successfully"


def get_db_connection():
    """Get database connection using environment variables."""
    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME"),
    )


def get_pg_cron_connection():
    """Get pg_cron database connection using environment variables."""
    return psycopg.connect(
        host=os.getenv("PG_CRON_HOST", os.getenv("DB_HOST")),
        port=int(os.getenv("PG_CRON_PORT", os.getenv("DB_PORT", "5432"))),
        user=os.getenv("PG_CRON_USER", os.getenv("DB_USER")),
        password=os.getenv("PG_CRON_PASSWORD", os.getenv("DB_PASSWORD")),
        dbname=os.getenv("PG_CRON_DATABASE", "postgres"),
    )


def check_pg_cron_installed() -> bool:
    """Check if pg_cron extension is installed in the pg_cron database."""
    try:
        with get_pg_cron_connection() as conn:
            with conn.cursor() as cur:
                # First check if it exists without trying to create it
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
                if cur.fetchone() is not None:
                    return True

                # Try to create it if not exists
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
                conn.commit()
                return True
    except Exception as e:
        # pg_cron not available - this is OK for manual processing
        logger.debug(f"pg_cron extension not available: {e}")
        return False


def check_and_grant_pg_cron_permissions() -> tuple[bool, str]:
    """Check and optionally grant pg_cron permissions to current user."""
    try:
        with get_pg_cron_connection() as conn:
            with conn.cursor() as cur:
                # Get current user from work database
                work_user = os.getenv("DB_USER")

                # Check if user has required permissions
                cur.execute(
                    """
                    SELECT
                        has_schema_privilege(%s, 'cron', 'USAGE') as schema_usage,
                        has_function_privilege(%s, 'cron.schedule_in_database(text,text,text,text,text,boolean)', 'EXECUTE') as schedule_exec,
                        has_function_privilege(%s, 'cron.unschedule(bigint)', 'EXECUTE') as unschedule_exec
                """,
                    (work_user, work_user, work_user),
                )

                perms = cur.fetchone()
                if not perms:
                    return False, "Could not check permissions"

                schema_usage, schedule_exec, unschedule_exec = perms

                if schema_usage and schedule_exec and unschedule_exec:
                    return True, "All required permissions are already granted"

                # Try to grant missing permissions (requires superuser privileges)
                missing_perms = []
                try:
                    if not schema_usage:
                        cur.execute("GRANT USAGE ON SCHEMA cron TO %s", (work_user,))
                        missing_perms.append("SCHEMA cron USAGE")

                    if not schedule_exec:
                        cur.execute("GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO %s", (work_user,))
                        missing_perms.append("cron.schedule_in_database")

                    if not unschedule_exec:
                        cur.execute("GRANT EXECUTE ON FUNCTION cron.unschedule TO %s", (work_user,))
                        cur.execute("GRANT EXECUTE ON FUNCTION cron.alter_job TO %s", (work_user,))
                        missing_perms.append("cron.unschedule and cron.alter_job")

                    # Also grant SELECT on cron.job for monitoring (optional)
                    cur.execute("GRANT SELECT ON cron.job TO %s", (work_user,))

                    conn.commit()
                    return True, f"Successfully granted permissions: {', '.join(missing_perms)}"

                except Exception as grant_error:
                    return False, f"Failed to grant permissions (need superuser): {grant_error}"

    except Exception as e:
        return False, f"Error checking pg_cron permissions: {e}"


def setup_cron_database_objects(cron_conn):
    """Set up cron database components (configuration tables and scheduling functions)."""
    logger.info("Setting up cron database objects...")

    # Read and execute cron SQL file
    sql_cron_content = SQL_CRON_FILE.read_text()
    with cron_conn.cursor() as cur:
        cur.execute(sql_cron_content)

    # Read and execute cron status SQL file (status functions need access to cron.job table)
    sql_cron_status_content = SQL_CRON_STATUS_FILE.read_text()
    with cron_conn.cursor() as cur:
        cur.execute(sql_cron_status_content)

    # Initialize cron config table
    queue_prefix = get_queue_table_prefix_from_env()
    with cron_conn.cursor() as cur:
        cur.execute("SELECT partitioncache_initialize_cron_config_table(%s)", [queue_prefix])
        cur.execute("SELECT partitioncache_create_cron_config_trigger(%s)", [queue_prefix])

    cron_conn.commit()
    logger.info("Cron database objects created successfully")


def setup_cache_database_objects(cache_conn):
    """Set up cache database components (worker functions and processing logic)."""
    logger.info("Setting up cache database objects...")

    # First, ensure queue tables are set up using the queue handler
    from partitioncache.queue_handler import get_queue_handler

    try:
        logger.info("Initializing queue handler to ensure queue tables exist...")
        queue_handler = get_queue_handler()
        queue_handler.close()
        logger.info("Queue tables verified/created successfully")
    except Exception as e:
        logger.error(f"Failed to setup queue tables via queue handler: {e}")
        raise

    # Ensure cache metadata tables are set up using cache handler
    from partitioncache.cache_handler import get_cache_handler

    try:
        cache_backend_env = os.getenv("CACHE_BACKEND", "postgresql_array")
        logger.info(f"Initializing cache handler ({cache_backend_env}) to ensure metadata tables exist...")
        cache_handler = get_cache_handler(cache_backend_env)
        cache_handler.close()
        logger.info("Cache metadata tables verified/created successfully")
    except Exception as e:
        logger.error(f"Failed to setup cache metadata tables via cache handler: {e}")
        raise

    # Load global cache handler functions first
    sql_handlers_content = SQL_HANDLERS_FILE.read_text()
    with cache_conn.cursor() as cur:
        cur.execute(sql_handlers_content)

    # Read and execute cache SQL file
    sql_cache_content = SQL_CACHE_FILE.read_text()
    with cache_conn.cursor() as cur:
        cur.execute(sql_cache_content)

    # Read and execute cache info SQL file (cache and queue info functions)
    sql_cache_info_content = SQL_CACHE_INFO_FILE.read_text()
    with cache_conn.cursor() as cur:
        cur.execute(sql_cache_info_content)

    # Initialize cache processor tables
    queue_prefix = get_queue_table_prefix_from_env()
    with cache_conn.cursor() as cur:
        cur.execute("SELECT partitioncache_initialize_cache_processor_tables(%s)", [queue_prefix])

    cache_conn.commit()
    logger.info("Cache database objects created successfully")


def setup_database_objects(cache_conn, cron_conn=None):
    """Execute the SQL files to create all necessary database objects in both databases.

    Args:
        cache_conn: Cache database connection
        cron_conn: Cron database connection (optional, if None uses cache_conn)
    """
    logger.info("Setting up cross-database objects...")

    if cron_conn is None:
        cron_conn = cache_conn
        logger.info("Using same database for cron and cache operations")
    else:
        logger.info("Using separate databases for cron and cache operations")

    # Setup cron database components
    setup_cron_database_objects(cron_conn)

    # Setup cache database components
    setup_cache_database_objects(cache_conn)

    logger.info("Cross-database setup completed successfully")


def insert_initial_config(
    cron_conn,
    job_name: str,
    table_prefix: str,
    queue_prefix: str,
    cache_backend: str,
    frequency: int,
    enabled: bool,
    timeout: int,
    target_database: str,
    default_bitsize: int | None,
):
    """Insert the initial configuration row in cron database which will trigger the cron job creation."""
    logger.info(f"Inserting initial config for job '{job_name}' in cron database")
    config_table = f"{queue_prefix}_processor_config"

    with cron_conn.cursor() as cur:
        # This will either insert a new config or do nothing if it already exists.
        # The trigger will handle creating/updating the cron.job.
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, queue_prefix, cache_backend, frequency_seconds, enabled, timeout_seconds, target_database, default_bitsize)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO NOTHING
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout, target_database, default_bitsize),
        )
    cron_conn.commit()
    logger.info("Initial configuration inserted successfully in cron database. Cron job should be synced.")


def insert_cache_config(
    cache_conn,
    job_name: str,
    table_prefix: str,
    queue_prefix: str,
    cache_backend: str,
    frequency: int,
    enabled: bool,
    timeout: int,
    target_database: str,
    default_bitsize: int | None,
):
    """Insert configuration in cache database for worker function access (eliminates need for dblink)."""
    logger.info(f"Inserting config for job '{job_name}' in cache database for worker functions")
    config_table = f"{queue_prefix}_processor_config"

    with cache_conn.cursor() as cur:
        # Create the config table if it doesn't exist (without trigger)
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    job_name TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT false,
                    max_parallel_jobs INTEGER NOT NULL DEFAULT 2,
                    frequency_seconds INTEGER NOT NULL DEFAULT 1,
                    timeout_seconds INTEGER NOT NULL DEFAULT 1800,
                    table_prefix TEXT NOT NULL,
                    queue_prefix TEXT NOT NULL,
                    cache_backend TEXT NOT NULL,
                    target_database TEXT NOT NULL,
                    result_limit INTEGER DEFAULT NULL,
                    default_bitsize INTEGER DEFAULT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            ).format(sql.Identifier(config_table))
        )

        # Insert configuration for worker functions to access
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, queue_prefix, cache_backend, frequency_seconds, enabled, timeout_seconds, target_database, default_bitsize)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO UPDATE SET
                    table_prefix = EXCLUDED.table_prefix,
                    queue_prefix = EXCLUDED.queue_prefix,
                    cache_backend = EXCLUDED.cache_backend,
                    frequency_seconds = EXCLUDED.frequency_seconds,
                    enabled = EXCLUDED.enabled,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    target_database = EXCLUDED.target_database,
                    default_bitsize = EXCLUDED.default_bitsize,
                    updated_at = CURRENT_TIMESTAMP
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout, target_database, default_bitsize),
        )
    cache_conn.commit()
    logger.info("Configuration inserted successfully in cache database for worker function access.")


def remove_all_processor_objects(conn, queue_prefix: str):
    """Remove all processor-related tables, cron job and PartitionCache functions."""
    logger.info("Removing all PostgreSQL queue processor objects and functions...")
    config_table = f"{queue_prefix}_processor_config"
    log_table = f"{queue_prefix}_processor_log"
    active_jobs_table = f"{queue_prefix}_active_jobs"

    # Get the database name to construct the job name pattern
    target_database = get_cache_database_name()
    # Match all processor jobs for this database
    job_name_pattern = f"partitioncache_process_queue_{target_database}%"

    with conn.cursor() as cur:
        # Drop processor-specific tables first. The DELETE trigger on the config
        # table will unschedule the cron job automatically.
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(config_table)))
        logger.info(f"Dropped table '{config_table}' (cron job should be removed by trigger if present).")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(log_table)))
        logger.info(f"Dropped table '{log_table}'")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(active_jobs_table)))
        logger.info(f"Dropped table '{active_jobs_table}'")

    # Explicitly remove any remaining cron.job entries for this database (in case trigger wasn't present)
    # Use the pg_cron database connection when it's different from the cache database
    if get_cron_database_name() == get_cache_database_name():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cron.job WHERE jobname LIKE %s", [job_name_pattern])  # best-effort
    else:
        cron_conn = get_pg_cron_connection()
        try:
            with cron_conn.cursor() as cron_cur:
                cron_cur.execute("DELETE FROM cron.job WHERE jobname LIKE %s", [job_name_pattern])
            cron_conn.commit()
        finally:
            cron_conn.close()

    with conn.cursor() as cur:
        # Dynamically drop all functions beginning with 'partitioncache_'
        logger.info("Dropping all functions with prefix 'partitioncache_' …")
        cur.execute(
            """
            DO $$
            DECLARE
                rec RECORD;
            BEGIN
                FOR rec IN
                    SELECT p.oid::regprocedure AS func_ident
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = 'public' AND p.proname LIKE 'partitioncache_%'
                LOOP
                    EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE', rec.func_ident);
                END LOOP;
            END;$$;
            """
        )
        logger.info("All PartitionCache functions dropped.")

    conn.commit()
    logger.info("All processor objects and functions removed.")


def enable_processor(queue_prefix: str):
    """Enable the queue processor job."""
    logger.info("Enabling queue processor...")
    conn = get_pg_cron_connection()
    target_database = get_cache_database_name()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT partitioncache_set_processor_enabled_cron(true, %s, %s)", [queue_prefix, target_database])
        conn.commit()
        logger.info("Queue processor enabled.")
    finally:
        conn.close()


def disable_processor(queue_prefix: str):
    """Disable the queue processor job."""
    logger.info("Disabling queue processor...")
    conn = get_pg_cron_connection()
    target_database = get_cache_database_name()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT partitioncache_set_processor_enabled_cron(false, %s, %s)", [queue_prefix, target_database])
        conn.commit()
        logger.info("Queue processor disabled.")
    finally:
        conn.close()


def update_processor_config(
    conn,
    enabled: bool | None = None,
    max_parallel_jobs: int | None = None,
    frequency: int | None = None,
    timeout: int | None = None,
    table_prefix: str | None = None,
    result_limit: int | None = None,
    queue_prefix: str | None = None,
):
    """Update processor configuration."""
    if all(arg is None for arg in [enabled, max_parallel_jobs, frequency, timeout, table_prefix, result_limit]):
        logger.warning("No configuration options provided to update.")
        return

    if queue_prefix is None:
        queue_prefix = get_queue_table_prefix_from_env()

    logger.info("Updating processor configuration...")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partitioncache_update_processor_config_cron(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (None, enabled, max_parallel_jobs, frequency, timeout, table_prefix, None, result_limit, None, queue_prefix),
        )
    conn.commit()
    logger.info("Processor configuration updated successfully.")


def handle_setup(table_prefix: str, queue_prefix: str, cache_backend: str, frequency: int, enabled: bool, timeout: int, default_bitsize: int | None):
    """Handle the main setup logic."""
    logger.info("Starting PostgreSQL queue processor cross-database setup...")

    # Get database connections
    cache_conn = get_db_connection()
    cron_conn = get_pg_cron_connection()
    target_database = get_cache_database_name()

    # Check if pg_cron is available (but don't require it)
    pg_cron_available = check_pg_cron_installed()

    if enabled and not pg_cron_available:
        logger.error("Cannot enable processor with pg_cron: extension is not installed.")
        logger.info("You can still set up the processor infrastructure and use manual processing.")
        sys.exit(1)

    # Check and grant pg_cron permissions if needed
    if enabled and pg_cron_available:
        perms_ok, perms_msg = check_and_grant_pg_cron_permissions()
        if perms_ok:
            logger.info(f"pg_cron permissions: {perms_msg}")
        else:
            logger.warning(f"pg_cron permissions issue: {perms_msg}")
            logger.warning("You may need to manually grant permissions as superuser:")
            work_user = os.getenv("DB_USER")
            logger.warning(f"  GRANT USAGE ON SCHEMA cron TO {work_user};")
            logger.warning(f"  GRANT EXECUTE ON FUNCTION cron.schedule_in_database TO {work_user};")
            logger.warning(f"  GRANT EXECUTE ON FUNCTION cron.unschedule TO {work_user};")

    # Setup all database objects in both databases
    if get_cron_database_name() == get_cache_database_name():
        logger.info("Setting up components in single database (cron and cache are same)")
        setup_database_objects(cache_conn, cron_conn=None)
        cron_conn.close()  # Same as cache_conn, close duplicate
    else:
        logger.info(f"Setting up components in cross-database mode: cron={get_cron_database_name()}, cache={get_cache_database_name()}")
        setup_database_objects(cache_conn, cron_conn)

    # Construct job name using simplified logic when we have full context
    # The SQL helper function handles discovery scenarios where table_prefix is unknown
    job_name = construct_processor_job_name(target_database, table_prefix)

    # Insert configuration in cron database for pg_cron job management
    actual_cron_conn = cache_conn if get_cron_database_name() == get_cache_database_name() else cron_conn
    insert_initial_config(actual_cron_conn, job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout, target_database, default_bitsize)

    # Also create minimal config in cache database for manual commands (without triggers)
    if get_cron_database_name() != get_cache_database_name():
        insert_cache_config(cache_conn, job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout, target_database, default_bitsize)

    if enabled and pg_cron_available:
        logger.info(f"Setup complete. The processor job is created and scheduled via pg_cron to run in {target_database}.")
    else:
        logger.info("Setup complete. The processor infrastructure is ready for manual processing.")
        if not pg_cron_available:
            logger.info("Note: pg_cron is not available. Use 'manual-process' command or enable pg_cron for automated processing.")

    # Close connections
    cache_conn.close()
    if get_cron_database_name() != get_cache_database_name():
        cron_conn.close()


def get_processor_status(queue_prefix: str):
    """Get basic processor status from cron database."""
    logger.info("Fetching processor status...")
    conn = get_pg_cron_connection()
    target_database = get_cache_database_name()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM partitioncache_get_processor_status(%s, %s)", [queue_prefix, target_database])
            status = cur.fetchone()  # type: ignore[assignment]
            if status:
                columns = [desc[0] for desc in cur.description]  # type: ignore[attr-defined]
                return dict(zip(columns, status, strict=False))
    finally:
        conn.close()
    return None


def get_processor_status_detailed(table_prefix: str, queue_prefix: str):
    """Get detailed processor status from cron database."""
    logger.info("Fetching detailed processor status...")
    conn = get_pg_cron_connection()
    target_database = get_cache_database_name()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM partitioncache_get_processor_status_detailed(%s, %s, %s)", [table_prefix, queue_prefix, target_database])
            status = cur.fetchone()
            if status:
                columns = [desc[0] for desc in cur.description]  # type: ignore[attr-defined]
                return dict(zip(columns, status, strict=False))
    finally:
        conn.close()
    return None


def get_queue_and_cache_info(table_prefix: str, queue_prefix: str):
    """Get detailed queue and cache info for all partitions from cache database."""
    logger.info("Fetching queue and cache info...")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM partitioncache_get_queue_and_cache_info(%s, %s)", [table_prefix, queue_prefix])
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # type: ignore[attr-defined]
            return [dict(zip(columns, row, strict=False)) for row in results]
    finally:
        conn.close()


def print_status(status):
    """Print basic processor status."""
    if not status:
        logger.warning("Processor status not available. Is it set up?")
        return

    # Status output goes to stdout via print since this IS the requested data
    print("--- PostgreSQL Processor Status ---")
    print(f"  Job Name:           {status.get('job_name', 'N/A')}")
    print(f"  Enabled:            {status.get('enabled', 'N/A')}")
    print(f"  Cron Job Active:    {status.get('job_is_active', 'N/A')}")
    print(f"  Cron Schedule:      {status.get('job_schedule', 'N/A')}")
    print(f"  Max Parallel Jobs:  {status.get('max_parallel_jobs', 'N/A')}")
    print(f"  Frequency (sec):    {status.get('frequency_seconds', 'N/A')}")
    print(f"  Timeout (sec):      {status.get('timeout_seconds', 'N/A')}")
    print(f"  Active Jobs:        {status.get('active_jobs_count', 'N/A')}")
    print(f"  Cache Backend:      {status.get('cache_backend', 'N/A')}")
    print(f"  Table Prefix:       {status.get('table_prefix', 'N/A')}")
    print(f"  Queue Prefix:       {status.get('queue_prefix', 'N/A')}")
    print(f"  Last Config Update: {status.get('updated_at', 'N/A')}")
    print(f"  Command:            {status.get('job_command', 'N/A')}")
    print("-----------------------------------")


def print_detailed_status(status):
    """Print detailed processor status."""
    if not status:
        print("Detailed processor status not available. Is it set up?")
        return

    print("--- PostgreSQL Processor Status (Detailed) ---")
    print("\n[Processor Configuration]")
    print(f"  Job Name:           {status.get('job_name', 'N/A')}")
    print(f"  Enabled (in DB):    {status.get('enabled', 'N/A')}")
    print(f"  Cron Job Active:    {status.get('job_is_active', 'N/A')}")
    print(f"  Max Parallel Jobs:  {status.get('max_parallel_jobs', 'N/A')}")
    print(f"  Frequency (sec):    {status.get('frequency_seconds', 'N/A')}")
    print(f"  Cron Schedule:      {status.get('job_schedule', 'N/A')}")

    print("\n[Live Status]")
    print(f"  Active Jobs:        {status.get('active_jobs_count', 'N/A')}")
    print(f"  Queue Length:       {status.get('queue_length', 'N/A')}")
    print(f"  Success (last 5m):  {status.get('recent_successes_5m', 'N/A')}")
    print(f"  Failures (last 5m): {status.get('recent_failures_5m', 'N/A')}")

    print("\n[Recent Logs (limit 10)]")
    recent_logs = status.get("recent_logs")
    if recent_logs:
        for log in recent_logs:
            execution_source = log.get("execution_source", "unknown")
            execution_time = log.get("execution_time_ms", "N/A")
            print(f"  - Job: {log.get('job_id')}, Status: {log.get('status')}, Source: {execution_source}, Time: {execution_time}ms")
            print(f"    Created: {log.get('created_at')}")
            if log.get("error_message"):
                print(f"    Error: {log.get('error_message')}")
    else:
        print("  No recent logs found.")

    print("\n[Active Job Details]")
    active_jobs = status.get("active_job_details")
    if active_jobs:
        for job in active_jobs:
            print(f"  - Job: {job.get('job_id')}, Hash: {job.get('query_hash')}, Partition: {job.get('partition_key')}, Started: {job.get('started_at')}")
    else:
        print("  No jobs currently active.")
    print("--------------------------------------------")


def print_queue_and_cache_info(queue_info):
    """Print detailed queue and cache info in a structured table."""
    if not queue_info:
        print("No partition information found.")
        return

    # Determine column widths
    max_pk = max(len(r["partition_key"]) for r in queue_info) if queue_info else 13
    max_ct = max(len(r["cache_table_name"]) for r in queue_info) if queue_info else 16

    # Header
    print(
        f"{'Partition Key':<{max_pk}} | {'Queue Items':>12} | {'Cache Table':<{max_ct}} | {'Architecture':>14} | {'Datatype':>10} | {'Bitsize':>7} | {'Last Update':<26} | {'Exists'}"
    )
    print(f"{'-' * max_pk}-+-{'-' * 12}-+-{'-' * max_ct}-+-{'-' * 14}-+-{'-' * 10}-+-{'-' * 7}-+-{'-' * 26}-+--------")

    # Rows
    for r in queue_info:
        pk = r.get("partition_key", "N/A")
        qi = r.get("queue_items", "N/A")
        ct = r.get("cache_table_name", "N/A")
        ca = r.get("cache_architecture", "N/A")
        dt = r.get("partition_datatype", "N/A")
        bs = r.get("bitsize", "N/A") or "-"
        lu = r.get("last_cache_update", "N/A") or "N/A"
        ce = "Yes" if r.get("cache_exists") else "No"
        print(f"{pk:<{max_pk}} | {qi:>12} | {ct:<{max_ct}} | {ca:>14} | {dt:>10} | {bs:>7} | {str(lu):<26} | {ce}")


def view_logs(conn, limit: int = 20, status_filter: str | None = None, queue_prefix: str | None = None):
    """View processor logs."""
    if queue_prefix is None:
        queue_prefix = get_queue_table_prefix_from_env()

    log_table = f"{queue_prefix}_processor_log"
    logger.info(f"Fetching logs from {log_table} (limit: {limit}, status: {status_filter or 'any'})")

    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(log_table))
        params: list = []  # type: ignore[type-arg]
        if status_filter:
            query += sql.SQL(" WHERE status = %s")
            params.append(status_filter)
        query += sql.SQL(" ORDER BY created_at DESC LIMIT %s")
        params.append(limit)

        try:
            cur.execute(query, params)
            logs = cur.fetchall()
            if not logs:
                print("No logs found.")
                return

            columns = [desc[0] for desc in cur.description]
            for log in logs:
                log_dict = dict(zip(columns, log, strict=False))
                execution_source = log_dict.get("execution_source", "unknown")
                execution_time = log_dict.get("execution_time_ms", "N/A")
                print(
                    f"[{log_dict['created_at']}] Job: {log_dict['job_id']} | Status: {log_dict['status']} | Source: {execution_source} | Time: {execution_time}ms | Partition: {log_dict.get('partition_key', 'N/A')}"
                )
                if log_dict.get("error_message"):
                    print(f"  Error: {log_dict['error_message']}")

        except psycopg.errors.UndefinedTable:
            print(f"Error: Log table '{log_table}' not found. Please run setup first.")


def manual_process_queue(conn, count: int):
    """Manually process a number of items from the queue."""
    logger.info(f"Manually processing up to {count} items from the queue...")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM partitioncache_manual_process_queue(%s)",
            (count,),
        )
        result = cur.fetchone()
        if result:
            print(f"Result: {result[1]} (Attempted to process: {result[0]})")
        else:
            print("No result from manual processing.")
    conn.commit()


def main():
    """Main function to handle CLI commands."""
    # Create main parser
    parser = argparse.ArgumentParser(description="Manage PostgreSQL queue processor for PartitionCache.")

    # Add common environment arguments
    add_environment_args(parser)
    add_verbosity_args(parser)

    # Add subparsers BEFORE parse_known_args so help can see them
    subparsers = parser.add_subparsers(
        title="commands",
        description="Available commands",
        dest="command",
        help="Additional help for each command"
    )

    # setup command
    setup_parser = subparsers.add_parser("setup", help="Setup database objects and pg_cron job")
    setup_parser.add_argument("--frequency", type=int, default=1, help="Frequency in seconds to run the processor job (default: 1)")
    setup_parser.add_argument("--timeout", type=int, default=1800, help="Timeout in seconds for processing jobs (default: 1800 = 30 minutes)")
    setup_parser.add_argument(
        "--enable-after-setup",
        action="store_true",
        help="Enable the processor immediately after setup (by default it is disabled)",
    )
    setup_parser.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: based on env vars)")
    setup_parser.add_argument("--bitsize", type=int, help="Global default bitsize for postgresql_bit backend (optional)")
    setup_parser.set_defaults(
        func=lambda args: handle_setup(
            args.table_prefix or get_table_prefix_from_env(),
            get_queue_table_prefix_from_env(),
            get_cache_backend_from_env(),
            args.frequency,
            args.enable_after_setup,
            args.timeout,
            args.bitsize,
        )
    )

    # remove command
    parser_remove = subparsers.add_parser("remove", help="Remove the pg_cron job and all processor tables")
    parser_remove.set_defaults(func=lambda args: remove_all_processor_objects(get_db_connection(), get_queue_table_prefix_from_env()))

    # enable command
    parser_enable = subparsers.add_parser("enable", help="Enable the queue processor job")
    parser_enable.set_defaults(func=lambda args: enable_processor(get_queue_table_prefix_from_env()))

    # disable command
    parser_disable = subparsers.add_parser("disable", help="Disable the queue processor job")
    parser_disable.set_defaults(func=lambda args: disable_processor(get_queue_table_prefix_from_env()))

    # update-config command
    parser_update = subparsers.add_parser("update-config", help="Update processor configuration")
    parser_update.add_argument("--enable", action=argparse.BooleanOptionalAction, help="Enable or disable the processor")
    parser_update.add_argument("--max-parallel-jobs", type=int, help="New max parallel jobs limit")
    parser_update.add_argument("--frequency", type=int, help="New frequency in seconds for the processor job")
    parser_update.add_argument("--timeout", type=int, help="New timeout in seconds for processing jobs")
    parser_update.add_argument("--table-prefix", type=str, help="New table prefix for the cache tables")
    parser_update.add_argument("--result-limit", type=int, help="New result limit for queries (NULL to disable)")
    parser_update.set_defaults(
        func=lambda args: update_processor_config(
            get_pg_cron_connection(),
            enabled=args.enable,
            max_parallel_jobs=args.max_parallel_jobs,
            frequency=args.frequency,
            timeout=args.timeout,
            table_prefix=args.table_prefix,
            result_limit=args.result_limit,
            queue_prefix=get_queue_table_prefix_from_env(),
        )
    )

    # status command
    parser_status = subparsers.add_parser("status", help="Show basic processor status")
    parser_status.set_defaults(
        func=lambda args: print_status(
            get_processor_status(
                get_queue_table_prefix_from_env(),
            )
        )
    )

    # status-detailed command
    parser_status_detailed = subparsers.add_parser("status-detailed", help="Show detailed processor status")
    parser_status_detailed.set_defaults(
        func=lambda args: print_detailed_status(
            get_processor_status_detailed(
                get_table_prefix_from_env(),
                get_queue_table_prefix_from_env(),
            )
        )
    )

    # queue-info command
    parser_queue_info = subparsers.add_parser("queue-info", help="Show detailed queue and cache info for each partition")
    parser_queue_info.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: based on env vars)")
    parser_queue_info.set_defaults(
        func=lambda args: print_queue_and_cache_info(
            get_queue_and_cache_info(
                args.table_prefix or get_table_prefix_from_env(),
                get_queue_table_prefix_from_env(),
            )
        )
    )

    # logs command
    parser_logs = subparsers.add_parser("logs", help="View processor logs")
    parser_logs.add_argument("-n", "--limit", type=int, default=20, help="Number of log entries to show")
    parser_logs.add_argument("--status", type=str, help="Filter logs by status (e.g., success, failed)")
    parser_logs.set_defaults(
        func=lambda args: view_logs(
            get_db_connection(),
            limit=args.limit,
            status_filter=args.status,
            queue_prefix=get_queue_table_prefix_from_env(),
        )
    )

    # check-permissions command
    parser_perms = subparsers.add_parser("check-permissions", help="Check and optionally grant pg_cron permissions")

    def check_permissions_command(args):
        print("Checking pg_cron permissions...")
        result = check_and_grant_pg_cron_permissions()
        print(f"Result: {result[1]}")

    parser_perms.set_defaults(func=check_permissions_command)

    # manual-process command
    parser_manual = subparsers.add_parser("manual-process", help="Manually trigger processing of queue items for testing")
    parser_manual.add_argument("count", type=int, help="Number of items to process")
    parser_manual.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: based on env vars)")
    parser_manual.set_defaults(
        func=lambda args: manual_process_queue(
            get_db_connection(),
            args.count,
        )
    )

    try:
        # Parse all arguments and execute the corresponding function
        args = parser.parse_args()

        # Configure logging based on verbosity
        configure_logging(args)

        # Load environment variables
        load_environment_with_validation(args.env_file)

        # Only validate environment for non-help commands
        if hasattr(args, "func"):
            # Validate environment after loading .env
            is_valid, message = validate_environment()
            if not is_valid:
                logger.error(f"Environment validation failed: {message}")
                sys.exit(1)
            args.func(args)
        else:
            parser.print_help()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
