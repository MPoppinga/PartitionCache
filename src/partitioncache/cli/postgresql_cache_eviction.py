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

import dotenv
import psycopg
from psycopg import sql

logger = getLogger("PartitionCache.PostgreSQLCacheEviction")

# SQL file location
SQL_FILE = Path(__file__).parent.parent / "cache_handler" / "postgresql_cache_eviction.sql"


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


def check_pg_cron_installed(conn) -> bool:
    """Check if pg_cron extension is installed."""
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
        except Exception as e:
            logger.error(f"Failed to create pg_cron extension: {e}")
            return False
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
        return cur.fetchone() is not None


def setup_database_objects(conn, table_prefix: str):
    """Execute the SQL file to create all necessary database objects."""
    logger.info("Setting up database objects for cache eviction...")

    sql_content = SQL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql_content)
        cur.execute("SELECT partitioncache_initialize_eviction_tables(%s)", [table_prefix])
        cur.execute("SELECT partitioncache_create_eviction_config_trigger(%s)", [table_prefix])

    conn.commit()
    logger.info("PostgreSQL cache eviction database objects created successfully")


def insert_initial_config(conn, job_name: str, table_prefix: str, frequency: int, enabled: bool, strategy: str, threshold: int):
    """Insert the initial configuration row which will trigger the cron job creation."""
    logger.info(f"Inserting initial config for job '{job_name}'")
    config_table = f"{table_prefix}_eviction_config"

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, frequency_minutes, enabled, strategy, threshold)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO NOTHING
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, frequency, enabled, strategy, threshold),
        )
    conn.commit()
    logger.info("Initial configuration inserted. Cron job should be synced.")


def remove_all_eviction_objects(conn, table_prefix: str):
    """Remove all eviction-related tables, cron job and functions."""
    logger.info("Removing all PostgreSQL cache eviction objects and functions...")
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_remove_eviction_objects(%s)", [table_prefix])
    conn.commit()
    logger.info("All cache eviction objects and functions removed.")


def set_processor_enabled(conn, table_prefix: str, job_name: str, enabled: bool):
    """Enable or disable the eviction job."""
    status = "Enabling" if enabled else "Disabling"
    logger.info(f"{status} cache eviction job...")
    config_table = f"{table_prefix}_eviction_config"
    with conn.cursor() as cur:
        cur.execute(sql.SQL("UPDATE {} SET enabled = %s WHERE job_name = %s").format(sql.Identifier(config_table)), (enabled, job_name))
    conn.commit()
    logger.info(f"Cache eviction job {status.lower()}d.")


def update_processor_config(conn, table_prefix: str, job_name: str, **kwargs):
    """Update processor configuration."""
    config_table = f"{table_prefix}_eviction_config"
    updates = {k: v for k, v in kwargs.items() if v is not None}

    if not updates:
        logger.warning("No configuration options provided to update.")
        return

    set_clauses = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Literal(v)) for k, v in updates.items())

    logger.info(f"Updating eviction processor configuration for job '{job_name}'...")
    with conn.cursor() as cur:
        query = sql.SQL("UPDATE {} SET {} WHERE job_name = {}").format(sql.Identifier(config_table), set_clauses, sql.Literal(job_name))
        cur.execute(query)

    conn.commit()
    logger.info("Processor configuration updated successfully.")


def handle_setup(conn, table_prefix: str, frequency: int, enabled: bool, strategy: str, threshold: int):
    """Handle the main setup logic."""
    logger.info("Starting PostgreSQL cache eviction setup...")
    if not check_pg_cron_installed(conn):
        logger.error("pg_cron extension is not installed. Please install it first.")
        sys.exit(1)

    setup_database_objects(conn, table_prefix)

    job_name = f"partitioncache_evict_{table_prefix}"
    insert_initial_config(conn, job_name, table_prefix, frequency, enabled, strategy, threshold)

    logger.info("Setup complete. The eviction job is created and configured via the config table.")


def get_processor_status(conn, table_prefix: str):
    """Get basic processor status."""
    logger.info("Fetching eviction processor status...")
    config_table = f"{table_prefix}_eviction_config"
    job_name = f"partitioncache_evict_{table_prefix}"

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


def manual_run(conn, table_prefix):
    """Manually run the eviction job."""
    job_name = f"partitioncache_evict_{table_prefix}"
    logger.info(f"Manually running eviction job '{job_name}'...")
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_run_eviction_job(%s, %s)", (job_name, table_prefix))
    conn.commit()
    logger.info("Manual run complete. Check logs for details.")


def main():
    """Main function to handle CLI commands."""
    parser = argparse.ArgumentParser(description="Manage PostgreSQL cache eviction for PartitionCache.", formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--env", default=".env", help="Path to the .env file to load.")
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

    # Load .env file
    dotenv.load_dotenv(dotenv_path=args.env)

    # Only validate environment for actual commands, not help
    is_valid, message = validate_environment()
    if not is_valid:
        logger.error(f"Environment validation failed: {message}")
        sys.exit(1)

    try:
        table_prefix = get_table_prefix(args)
        job_name = f"partitioncache_evict_{table_prefix}"
        conn = get_db_connection()

        if args.command == "setup":
            handle_setup(conn, table_prefix, args.frequency, args.enable_after_setup, args.strategy, args.threshold)
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
            manual_run(conn, table_prefix)

        conn.close()

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
