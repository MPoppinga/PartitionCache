#!/usr/bin/env python3
"""
Setup and manage PostgreSQL cache eviction for PartitionCache.

This script sets up the necessary database objects and pg_cron jobs to run cache
eviction strategies directly within PostgreSQL.

Usage Examples:
    # Basic setup for a cache with table prefix 'my_cache'
    pcache-eviction-manager setup --table-prefix my_cache

    # Show basic processor status
    pcache-eviction-manager status --table-prefix my_cache

    # Enable the eviction job
    pcache-eviction-manager enable --table-prefix my_cache

    # Update the eviction strategy
    pcache-eviction-manager update-config --strategy largest --threshold 500 --table-prefix my_cache

    # View eviction logs
    pcache-eviction-manager logs --table-prefix my_cache
"""

import argparse
import os
import sys
from logging import getLogger
from pathlib import Path

import psycopg
from psycopg import sql

from partitioncache.cli.common_args import add_environment_args, add_verbosity_args, configure_logging, load_environment_with_validation

logger = getLogger("PartitionCache.PostgreSQLCacheEviction")

# SQL file locations
SQL_CRON_FILE = Path(__file__).parent.parent / "cache_handler" / "postgresql_cache_eviction_cron.sql"
SQL_CACHE_FILE = Path(__file__).parent.parent / "cache_handler" / "postgresql_cache_eviction_cache.sql"


def construct_eviction_job_name(target_database: str, table_prefix: str | None) -> str:
    """
    Construct a unique job name for the eviction manager based on database and table prefix.

    This ensures multiple eviction managers can coexist for different databases and table prefixes.
    The job name follows the pattern: partitioncache_evict_<database>[_<suffix>]

    Args:
        target_database: The target database name
        table_prefix: The table prefix (e.g., 'partitioncache_cache1')

    Returns:
        A unique job name for the eviction configuration

    Examples:
        >>> construct_eviction_job_name("mydb", "partitioncache_cache1")
        'partitioncache_evict_mydb_cache1'
        >>> construct_eviction_job_name("mydb", "partitioncache")
        'partitioncache_evict_mydb_default'
        >>> construct_eviction_job_name("mydb", None)
        'partitioncache_evict_mydb'
    """
    # Base name includes the database
    base_name = f"partitioncache_evict_{target_database}"

    if not table_prefix:
        job_name = base_name
    else:
        # Extract suffix from table prefix (e.g., 'partitioncache_cache1' -> 'cache1')
        # Special case: if table_prefix is just underscores or doesn't contain 'partitioncache'
        if not table_prefix.replace('_', '') or 'partitioncache' not in table_prefix:
            # This is a custom prefix (not standard partitioncache pattern)
            table_suffix = table_prefix.replace('_', '') or 'custom'
        else:
            # Remove 'partitioncache' prefix and any underscores
            table_suffix = table_prefix.replace('partitioncache', '').replace('_', '') or 'default'

        job_name = f"{base_name}_{table_suffix}"

    # PostgreSQL identifiers have a 63-character limit
    # If we exceed this, truncate intelligently to preserve uniqueness
    if len(job_name) > 63:
        # For eviction jobs, preserve the suffix which differentiates table prefixes
        # Check if there's a suffix (underscore in the last part)
        if "_" in job_name[-25:]:  # Check last 25 chars for a suffix
            # Try to keep first 40 chars + "..." + last 20 chars
            truncated_name = job_name[:40] + "..." + job_name[-20:]
            logger.warning(
                f"Job name '{job_name}' exceeds PostgreSQL 63-character limit. "
                f"Truncating to '{truncated_name}'"
            )
            job_name = truncated_name
        else:
            # No clear suffix, just truncate
            truncated_name = job_name[:63]
            logger.warning(
                f"Job name '{job_name}' exceeds PostgreSQL 63-character limit. "
                f"Truncating to '{truncated_name}'"
            )
            job_name = truncated_name

    return job_name


def get_table_prefix(args) -> str:
    """Get the table prefix from args or environment."""
    if args.table_prefix:
        return str(args.table_prefix)

    # Check cache backend and use corresponding table prefix
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")

    if cache_backend == "postgresql_roaringbit":
        table_prefix = os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX")
    elif cache_backend == "postgresql_bit":
        table_prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX")
    else:  # default to postgresql_array
        table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX")

    if table_prefix:
        return table_prefix

    # Fallback: try all possible table prefix env vars
    table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX") or os.getenv("PG_BIT_CACHE_TABLE_PREFIX") or os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX")
    if table_prefix:
        return table_prefix

    raise ValueError(
        f"Could not determine table_prefix from environment variables for cache backend '{cache_backend}'. "
        f"Please set the appropriate table prefix environment variable or provide --table-prefix."
    )


def validate_environment() -> tuple[bool, str]:
    """Validate that all required environment variables are set."""
    required_vars = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    errors = []

    for var in required_vars:
        if not os.getenv(var):
            errors.append(f"{var} not set")

    if errors:
        return False, "Missing environment variables:\n" + "\n".join(errors)

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


def get_cache_database_name() -> str:
    """Get the cache database name from environment."""
    return os.getenv("DB_NAME", "geominedb2_pdb")


def get_cron_database_name() -> str:
    """Get the cron database name from environment."""
    return os.getenv("PG_CRON_DATABASE", "postgres")


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
        logger.error(f"Failed to check/create pg_cron extension: {e}")
        return False


def setup_cron_eviction_objects(cron_conn, table_prefix: str):
    """Set up eviction cron database components (configuration tables and scheduling functions)."""
    logger.info("Setting up eviction cron database objects...")

    # Read and execute cron SQL file
    sql_cron_content = SQL_CRON_FILE.read_text()
    with cron_conn.cursor() as cur:
        cur.execute(sql_cron_content)

    # Initialize cron config table
    with cron_conn.cursor() as cur:
        cur.execute("SELECT partitioncache_initialize_eviction_cron_config_table(%s)", [table_prefix])
        cur.execute("SELECT partitioncache_create_eviction_cron_config_trigger(%s)", [table_prefix])

    cron_conn.commit()
    logger.info("Eviction cron database objects created successfully")


def setup_cache_eviction_objects(cache_conn, table_prefix: str):
    """Set up eviction cache database components (eviction logic and log tables)."""
    logger.info("Setting up eviction cache database objects...")

    # Read and execute cache SQL file
    sql_cache_content = SQL_CACHE_FILE.read_text()
    with cache_conn.cursor() as cur:
        cur.execute(sql_cache_content)

    # Initialize cache eviction tables
    with cache_conn.cursor() as cur:
        cur.execute("SELECT partitioncache_initialize_eviction_cache_log_table(%s)", [table_prefix])

    cache_conn.commit()
    logger.info("Eviction cache database objects created successfully")


def setup_database_objects(cache_conn, cron_conn, table_prefix: str):
    """Execute the SQL files to create all necessary database objects in both databases."""
    logger.info("Setting up cross-database eviction objects...")

    if get_cron_database_name() == get_cache_database_name():
        logger.info("Using same database for cron and cache eviction operations")
        # Setup both components in the same database
        setup_cron_eviction_objects(cache_conn, table_prefix)
        setup_cache_eviction_objects(cache_conn, table_prefix)
    else:
        logger.info("Using separate databases for cron and cache eviction operations")
        # Setup cron components in cron database
        setup_cron_eviction_objects(cron_conn, table_prefix)
        # Setup cache components in cache database
        setup_cache_eviction_objects(cache_conn, table_prefix)

    logger.info("Cross-database eviction setup completed successfully")


def insert_initial_config(cron_conn, job_name: str, table_prefix: str, frequency: int, enabled: bool, strategy: str, threshold: int, target_database: str):
    """Insert the initial configuration row in cron database which will trigger the cron job creation."""
    logger.info(f"Inserting initial config for job '{job_name}' in cron database")
    config_table = f"{table_prefix}_eviction_config"

    with cron_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, frequency_minutes, enabled, strategy, threshold, target_database)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO NOTHING
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, frequency, enabled, strategy, threshold, target_database),
        )
    cron_conn.commit()
    logger.info("Initial configuration inserted in cron database. Cron job should be synced.")


def insert_cache_eviction_config(cache_conn, job_name: str, table_prefix: str, frequency: int, enabled: bool, strategy: str, threshold: int, target_database: str):
    """Insert configuration in cache database for eviction function access (eliminates need for dblink)."""
    logger.info(f"Inserting eviction config for job '{job_name}' in cache database for worker functions")
    config_table = f"{table_prefix}_eviction_config"

    with cache_conn.cursor() as cur:
        # Create the config table if it doesn't exist (without trigger)
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    job_name TEXT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT false,
                    frequency_minutes INTEGER NOT NULL DEFAULT 60 CHECK (frequency_minutes > 0),
                    strategy TEXT NOT NULL DEFAULT 'oldest' CHECK (strategy IN ('oldest', 'largest')),
                    threshold INTEGER NOT NULL DEFAULT 1000 CHECK (threshold > 0),
                    table_prefix TEXT NOT NULL,
                    target_database TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            ).format(sql.Identifier(config_table))
        )

        # Insert configuration for eviction functions to access
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, frequency_minutes, enabled, strategy, threshold, target_database)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO UPDATE SET
                    table_prefix = EXCLUDED.table_prefix,
                    frequency_minutes = EXCLUDED.frequency_minutes,
                    enabled = EXCLUDED.enabled,
                    strategy = EXCLUDED.strategy,
                    threshold = EXCLUDED.threshold,
                    target_database = EXCLUDED.target_database,
                    updated_at = CURRENT_TIMESTAMP
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, frequency, enabled, strategy, threshold, target_database),
        )
    cache_conn.commit()
    logger.info("Eviction configuration inserted successfully in cache database for worker function access.")


def check_eviction_job_exists() -> bool:
    """Check if any eviction job exists in the database."""
    try:
        conn = psycopg.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME"),
        )

        # Get the table prefix from environment
        cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
        if cache_backend == "postgresql_array":
            table_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "partitioncache")
        elif cache_backend == "postgresql_bit":
            table_prefix = os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "partitioncache")
        else:
            table_prefix = os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "partitioncache")

        target_database = os.getenv("DB_NAME")
        job_name = construct_eviction_job_name(target_database, table_prefix)
        config_table = f"{table_prefix}_eviction_config"

        with conn.cursor() as cur:
            # Check if config table exists and has an enabled job
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                [config_table],
            )
            table_exists = cur.fetchone()
            if table_exists:
                table_exists = table_exists[0]

            if not table_exists:
                conn.close()
                return False

            # Check if job is enabled
            cur.execute(sql.SQL("SELECT enabled FROM {} WHERE job_name = %s").format(sql.Identifier(config_table)), [job_name])
            result = cur.fetchone()

        conn.close()
        return bool(result and result[0])

    except Exception:
        return False


def remove_all_eviction_objects(conn, table_prefix: str):
    """Remove all eviction-related tables, cron job and functions."""
    logger.info("Removing all PostgreSQL cache eviction objects and functions...")
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_remove_eviction_objects(%s)", [table_prefix])
    conn.commit()
    logger.info("All cache eviction objects and functions removed.")


def set_processor_enabled(conn, table_prefix: str, job_name: str, enabled: bool):
    """Enable or disable the eviction job by updating config table (trigger handles cron job)."""
    status = "Enabling" if enabled else "Disabling"
    logger.info(f"{status} cache eviction job...")

    # Update config table in cron database - trigger will handle the cron job
    cron_conn = get_pg_cron_connection()
    config_table = f"{table_prefix}_eviction_config"

    with cron_conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE {} SET enabled = %s, updated_at = NOW() WHERE job_name = %s").format(sql.Identifier(config_table)),
            (enabled, job_name)
        )
    cron_conn.commit()
    cron_conn.close()
    logger.info(f"Cache eviction job {status.lower()}d successfully.")


def update_processor_config(conn, table_prefix: str, job_name: str, **kwargs):
    """Update processor configuration in cron database (trigger handles cron job)."""
    config_table = f"{table_prefix}_eviction_config"
    updates = {k: v for k, v in kwargs.items() if v is not None}

    if not updates:
        logger.warning("No configuration options provided to update.")
        return

    # Always add updated_at
    set_clauses = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Literal(v)) for k, v in updates.items())
    set_clauses = sql.SQL("{}, updated_at = NOW()").format(set_clauses)

    logger.info(f"Updating eviction processor configuration for job '{job_name}'...")

    # Update config table in cron database - trigger will handle the cron job
    cron_conn = get_pg_cron_connection()
    with cron_conn.cursor() as cur:
        query = sql.SQL("UPDATE {} SET {} WHERE job_name = {}").format(sql.Identifier(config_table), set_clauses, sql.Literal(job_name))
        cur.execute(query)

    cron_conn.commit()
    cron_conn.close()
    logger.info("Processor configuration updated successfully.")


def handle_setup(table_prefix: str, frequency: int, enabled: bool, strategy: str, threshold: int):
    """Handle the main setup logic."""
    logger.info("Starting PostgreSQL cache eviction cross-database setup...")

    # Get database connections
    cache_conn = get_db_connection()
    cron_conn = get_pg_cron_connection()
    target_database = get_cache_database_name()

    if not check_pg_cron_installed():
        logger.error("pg_cron extension is not installed. Please install it first.")
        cache_conn.close()
        cron_conn.close()
        sys.exit(1)

    # Setup database objects in both databases
    if get_cron_database_name() == get_cache_database_name():
        logger.info("Setting up eviction components in single database (cron and cache are same)")
        setup_database_objects(cache_conn, cron_conn, table_prefix)
        cron_conn.close()  # Same as cache_conn, close duplicate
    else:
        logger.info(f"Setting up eviction components in cross-database mode: cron={get_cron_database_name()}, cache={get_cache_database_name()}")
        setup_database_objects(cache_conn, cron_conn, table_prefix)

    job_name = construct_eviction_job_name(target_database, table_prefix)

    # Insert configuration in cron database for pg_cron job management only
    actual_cron_conn = cache_conn if get_cron_database_name() == get_cache_database_name() else cron_conn
    insert_initial_config(actual_cron_conn, job_name, table_prefix, frequency, enabled, strategy, threshold, target_database)

    logger.info("Setup complete. The eviction job is created and configured via the config table.")

    # Close connections
    cache_conn.close()
    if get_cron_database_name() != get_cache_database_name():
        cron_conn.close()


def get_processor_status(conn, table_prefix: str):
    """Get basic processor status."""
    logger.info("Fetching eviction processor status...")
    config_table = f"{table_prefix}_eviction_config"
    target_database = get_cache_database_name()
    job_name = construct_eviction_job_name(target_database, table_prefix)

    with conn.cursor() as cur:
        try:
            cur.execute(
                sql.SQL("""
                    SELECT
                        conf.*,
                        cron.active as job_is_active,
                        cron.schedule as job_schedule
                    FROM {} conf
                    LEFT JOIN cron.job cron ON conf.job_name = cron.jobname
                    WHERE conf.job_name = %s
                """).format(sql.Identifier(config_table)),
                [job_name],
            )

            status = cur.fetchone()
            if status:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, status, strict=False))
        except psycopg.errors.UndefinedTable:
            logger.error(f"Config table '{config_table}' not found. Please run setup first.")
            return None
    return None


def print_status(status):
    """Print basic processor status."""
    if not status:
        print("Eviction processor status not available. Is it set up?")
        return

    print("--- PostgreSQL Eviction Processor Status ---")
    print(f"  Job Name:           {status.get('job_name', 'N/A')}")
    print(f"  Enabled:            {status.get('enabled', 'N/A')}")
    print(f"  Cron Job Active:    {status.get('job_is_active', 'N/A')}")
    print(f"  Cron Schedule:      {status.get('job_schedule', 'N/A')}")
    print(f"  Strategy:           {status.get('strategy', 'N/A')}")
    print(f"  Threshold:          {status.get('threshold', 'N/A')}")
    print(f"  Frequency (min):    {status.get('frequency_minutes', 'N/A')}")
    print(f"  Table Prefix:       {status.get('table_prefix', 'N/A')}")
    print(f"  Last Config Update: {status.get('updated_at', 'N/A')}")
    print("------------------------------------------")


def view_logs(conn, table_prefix: str, limit: int = 20):
    """View processor logs."""
    log_table = f"{table_prefix}_eviction_log"
    logger.info(f"Fetching logs from {log_table} (limit: {limit})")

    with conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("SELECT * FROM {} ORDER BY created_at DESC LIMIT %s").format(sql.Identifier(log_table)), [limit])
            logs = cur.fetchall()
            if not logs:
                print("No logs found.")
                return

            columns = [desc[0] for desc in cur.description]
            for log in logs:
                log_dict = dict(zip(columns, log, strict=False))
                print(
                    f"[{log_dict['created_at']}] Job: {log_dict['job_name']} | "
                    f"Partition: {log_dict.get('partition_key', 'N/A')} | "
                    f"Status: {log_dict['status']} | "
                    f"Removed: {log_dict.get('queries_removed_count', 'N/A')} | "
                    f"Message: {log_dict.get('message', '')}"
                )

        except psycopg.errors.UndefinedTable:
            print(f"Error: Log table '{log_table}' not found. Please run setup first.")


def manual_run(table_prefix):
    """Manually run the eviction job - reads config from cron database."""
    target_database = get_cache_database_name()
    job_name = construct_eviction_job_name(target_database, table_prefix)
    logger.info(f"Manually running eviction job '{job_name}'...")

    # Get connections to read config from cron database and execute in cache database
    cache_conn = get_db_connection()
    cron_conn = get_pg_cron_connection()

    try:
        # Read configuration from cron database
        config_table = f"{table_prefix}_eviction_config"
        actual_cron_conn = cache_conn if get_cron_database_name() == get_cache_database_name() else cron_conn

        with actual_cron_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT strategy, threshold FROM {} WHERE job_name = %s").format(sql.Identifier(config_table)),
                [job_name]
            )
            config = cur.fetchone()

            if not config:
                logger.error(f"No configuration found for job '{job_name}' in cron database. Please run setup first.")
                return

            strategy, threshold = config

        # Execute eviction in cache database using parameter-based function
        with cache_conn.cursor() as cur:
            cur.execute("SELECT partitioncache_run_eviction_job_with_params(%s, %s, %s, %s)",
                       (job_name, table_prefix, strategy, threshold))
        cache_conn.commit()
        logger.info("Manual eviction run complete. Check logs for details.")

    finally:
        cache_conn.close()
        if get_cron_database_name() != get_cache_database_name():
            cron_conn.close()


def main():
    """Main function to handle CLI commands."""

    parser = argparse.ArgumentParser(description="Manage PostgreSQL cache eviction for PartitionCache.", formatter_class=argparse.RawTextHelpFormatter)

    add_environment_args(parser)
    add_verbosity_args(parser)
    # Table prefix is required for most commands
    parser.add_argument("--table-prefix", help="Table prefix for cache tables (e.g., 'my_cache'). Can also be set via env vars.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # setup command
    setup_parser = subparsers.add_parser("setup", help="Setup database objects and pg_cron job for eviction")
    setup_parser.add_argument("--frequency", type=int, default=60, help="Frequency in minutes to run the eviction job (default: 60)")
    setup_parser.add_argument("--threshold", type=int, default=1000, help="Cache size threshold to trigger eviction (default: 1000)")
    setup_parser.add_argument("--strategy", choices=["oldest", "largest"], default="oldest", help="Eviction strategy (default: oldest)")
    setup_parser.add_argument("--enable-after-setup", action="store_true", help="Enable the job immediately after setup (default: disabled)")

    # remove command
    subparsers.add_parser("remove", help="Remove the pg_cron job and all eviction tables")

    # enable command
    subparsers.add_parser("enable", help="Enable the eviction job")

    # disable command
    subparsers.add_parser("disable", help="Disable the eviction job")

    # update-config command
    update_parser = subparsers.add_parser("update-config", help="Update eviction processor configuration")
    update_parser.add_argument("--frequency", type=int, help="New frequency in minutes")
    update_parser.add_argument("--threshold", type=int, help="New cache size threshold")
    update_parser.add_argument("--strategy", choices=["oldest", "largest"], help="New eviction strategy")

    # status command
    subparsers.add_parser("status", help="Show eviction processor status")

    # logs command
    logs_parser = subparsers.add_parser("logs", help="View eviction logs")
    logs_parser.add_argument("-n", "--limit", type=int, default=20, help="Number of log entries to show")

    # manual-run command
    subparsers.add_parser("manual-run", help="Manually trigger the eviction job once")

    args = parser.parse_args()

    # Configure logging based on verbosity
    configure_logging(args)

    # Load environment variables
    load_environment_with_validation(args.env_file)

    # Only validate environment for actual commands, not help
    is_valid, message = validate_environment()
    if not is_valid:
        logger.error(f"Environment validation failed: {message}")
        sys.exit(1)

    try:
        table_prefix = get_table_prefix(args)
        target_database = get_cache_database_name()
        job_name = construct_eviction_job_name(target_database, table_prefix)
        conn = get_db_connection()

        if args.command == "setup":
            handle_setup(table_prefix, args.frequency, args.enable_after_setup, args.strategy, args.threshold)
        elif args.command == "remove":
            remove_all_eviction_objects(conn, table_prefix)
        elif args.command == "enable":
            set_processor_enabled(conn, table_prefix, job_name, True)
        elif args.command == "disable":
            set_processor_enabled(conn, table_prefix, job_name, False)
        elif args.command == "update-config":
            update_processor_config(conn, table_prefix, job_name, frequency_minutes=args.frequency, threshold=args.threshold, strategy=args.strategy)
        elif args.command == "status":
            status = get_processor_status(conn, table_prefix)
            print_status(status)
        elif args.command == "logs":
            view_logs(conn, table_prefix, args.limit)
        elif args.command == "manual-run":
            manual_run(table_prefix)

        conn.close()

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
