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

The new commands provide information about:
- Which partition keys are being observed in the queue
- Cache table architecture (bit vs array)
- Cache table existence and column types
- Queue item counts per partition
- Last cache update timestamps
- Partition data types and bit sizes (for bit caches)
"""

import argparse
import os
import sys
from pathlib import Path
from logging import getLogger
import psycopg
from psycopg import sql
import dotenv

logger = getLogger("PartitionCache.PostgreSQLQueueProcessor")

# SQL file location
SQL_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor.sql"
SQL_INFO_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_queue_processor_info.sql"


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
            f"Cache: {cache_config['host']}:{cache_config['port']}/{cache_config['db']}"
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


def setup_database_objects(conn):
    """Execute the SQL file to create all necessary database objects."""
    logger.info("Setting up database objects...")

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

    # Read and execute SQL file for PostgreSQL queue processor specific objects
    sql_content = SQL_FILE.read_text()

    with conn.cursor() as cur:
        cur.execute(sql_content)

    sql_info_content = SQL_INFO_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(sql_info_content)

    # Initialize processor tables with correct queue prefix
    logger.info("Initializing processor tables with queue prefix...")
    queue_prefix = get_queue_table_prefix_from_env()
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_initialize_processor_tables(%s)", [queue_prefix])
        cur.execute("SELECT partitioncache_create_config_trigger(%s)", [queue_prefix])

    conn.commit()
    logger.info("PostgreSQL queue processor database objects created successfully")


def insert_initial_config(conn, job_name: str, table_prefix: str, queue_prefix: str, cache_backend: str, frequency: int, enabled: bool, timeout: int):
    """Insert the initial configuration row which will trigger the cron job creation."""
    logger.info(f"Inserting initial config for job '{job_name}'")
    config_table = f"{queue_prefix}_processor_config"

    with conn.cursor() as cur:
        # This will either insert a new config or do nothing if it already exists.
        # The trigger will handle creating/updating the cron.job.
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} (job_name, table_prefix, queue_prefix, cache_backend, frequency_seconds, enabled, timeout_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_name) DO NOTHING
                """
            ).format(sql.Identifier(config_table)),
            (job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout),
        )
    conn.commit()
    logger.info("Initial configuration inserted successfully. Cron job should be synced.")


def remove_all_processor_objects(conn, queue_prefix: str):
    """Remove all processor-related tables, cron job and PartitionCache functions."""
    logger.info("Removing all PostgreSQL queue processor objects and functions...")
    config_table = f"{queue_prefix}_processor_config"
    log_table = f"{queue_prefix}_processor_log"
    active_jobs_table = f"{queue_prefix}_active_jobs"

    with conn.cursor() as cur:
        # Drop processor-specific tables first. The DELETE trigger on the config
        # table will unschedule the cron job automatically.
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(config_table)))
        logger.info(f"Dropped table '{config_table}' (cron job should be removed by trigger if present).")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(log_table)))
        logger.info(f"Dropped table '{log_table}'")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(active_jobs_table)))
        logger.info(f"Dropped table '{active_jobs_table}'")

        # Explicitly remove any remaining cron.job entry (in case trigger wasn't present)
        cur.execute("DELETE FROM cron.job WHERE jobname = %s", ["partitioncache_process_queue"])  # best-effort

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


def enable_processor(conn, queue_prefix: str):
    """Enable the queue processor job."""
    logger.info("Enabling queue processor...")
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_set_processor_enabled(true, %s, 'partitioncache_process_queue')", [queue_prefix])
    conn.commit()
    logger.info("Queue processor enabled.")


def disable_processor(conn, queue_prefix: str):
    """Disable the queue processor job."""
    logger.info("Disabling queue processor...")
    with conn.cursor() as cur:
        cur.execute("SELECT partitioncache_set_processor_enabled(false, %s, 'partitioncache_process_queue')", [queue_prefix])
    conn.commit()
    logger.info("Queue processor disabled.")


def update_processor_config(
    conn,
    enabled: bool | None = None,
    max_parallel_jobs: int | None = None,
    frequency: int | None = None,
    timeout: int | None = None,
    table_prefix: str | None = None,
    queue_prefix: str | None = None,
):
    """Update processor configuration."""
    if all(arg is None for arg in [enabled, max_parallel_jobs, frequency, timeout, table_prefix]):
        logger.warning("No configuration options provided to update.")
        return

    if queue_prefix is None:
        queue_prefix = get_queue_table_prefix_from_env()

    job_name = "partitioncache_process_queue"
    logger.info(f"Updating processor configuration for job '{job_name}'...")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partitioncache_update_processor_config(%s, %s, %s, %s, %s, %s, %s)",
            (job_name, enabled, max_parallel_jobs, frequency, timeout, table_prefix, queue_prefix),
        )
    conn.commit()
    logger.info("Processor configuration updated successfully.")


def handle_setup(conn, table_prefix: str, queue_prefix: str, cache_backend: str, frequency: int, enabled: bool, timeout: int):
    """Handle the main setup logic."""
    logger.info("Starting PostgreSQL queue processor setup...")
    if not check_pg_cron_installed(conn):
        logger.error("pg_cron extension is not installed. Please install it first.")
        sys.exit(1)

    setup_database_objects(conn)

    job_name = "partitioncache_process_queue"
    insert_initial_config(conn, job_name, table_prefix, queue_prefix, cache_backend, frequency, enabled, timeout)

    logger.info("Setup complete. The processor job is created and configured via the config table.")


def get_processor_status(conn, queue_prefix: str):
    """Get basic processor status."""
    logger.info("Fetching processor status...")
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_processor_status(%s, %s)", [queue_prefix, "partitioncache_process_queue"])
        status = cur.fetchone()
        if status:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, status))
    return None


def get_processor_status_detailed(conn, table_prefix: str, queue_prefix: str):
    """Get detailed processor status."""
    logger.info("Fetching detailed processor status...")
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_processor_status_detailed(%s, %s, %s)", [table_prefix, queue_prefix, "partitioncache_process_queue"])
        status = cur.fetchone()
        if status:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, status))
    return None


def get_queue_and_cache_info(conn, table_prefix: str, queue_prefix: str):
    """Get detailed queue and cache info for all partitions."""
    logger.info("Fetching queue and cache info...")
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_queue_and_cache_info(%s, %s)", [table_prefix, queue_prefix])
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in results]


def print_status(status):
    """Print basic processor status."""
    if not status:
        print("Processor status not available. Is it set up?")
        return

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

    print("\n[Cache Architecture Summary]")
    print(f"  Total Partitions:      {status.get('total_partitions', 'N/A')}")
    print(f"  Created Caches:        {status.get('partitions_with_cache', 'N/A')}")
    print(f"    - RoaringBit:        {status.get('roaringbit_partitions', 'N/A')}")
    print(f"    - Bit:               {status.get('bit_cache_partitions', 'N/A')}")
    print(f"    - Array:             {status.get('array_cache_partitions', 'N/A')}")
    print(f"  Uncreated Caches:      {status.get('uncreated_cache_partitions', 'N/A')}")

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
        params = []
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
                log_dict = dict(zip(columns, log))
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
    # A parent parser for global options like --env that we need to process early
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env", default=".env", help="Path to the .env file to load.")

    # Parse the --env argument first, ignoring other args
    env_args, _ = env_parser.parse_known_args()

    # Load .env file if specified and it exists
    env_path = Path(env_args.env)
    if env_path.is_file():
        logger.info(f"Loading environment variables from {env_path}")
        dotenv.load_dotenv(dotenv_path=env_path)
    elif env_args.env != ".env":
        # Only warn if a non-default path was given and not found
        logger.warning(f"Specified environment file not found: {env_path}")

    # Validate environment after loading .env
    is_valid, message = validate_environment()
    if not is_valid:
        logger.error(f"Environment validation failed: {message}")
        sys.exit(1)

    # Main parser that inherits the --env option for help messages
    parser = argparse.ArgumentParser(
        description="Manage PostgreSQL queue processor for PartitionCache.",
        parents=[env_parser],
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

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
    setup_parser.set_defaults(
        func=lambda args: handle_setup(
            get_db_connection(),
            args.table_prefix or get_table_prefix_from_env(),
            get_queue_table_prefix_from_env(),
            get_cache_backend_from_env(),
            args.frequency,
            args.enable_after_setup,
            args.timeout,
        )
    )

    # remove command
    parser_remove = subparsers.add_parser("remove", help="Remove the pg_cron job and all processor tables")
    parser_remove.set_defaults(func=lambda args: remove_all_processor_objects(get_db_connection(), get_queue_table_prefix_from_env()))

    # enable command
    parser_enable = subparsers.add_parser("enable", help="Enable the queue processor job")
    parser_enable.set_defaults(func=lambda args: enable_processor(get_db_connection(), get_queue_table_prefix_from_env()))

    # disable command
    parser_disable = subparsers.add_parser("disable", help="Disable the queue processor job")
    parser_disable.set_defaults(func=lambda args: disable_processor(get_db_connection(), get_queue_table_prefix_from_env()))

    # update-config command
    parser_update = subparsers.add_parser("update-config", help="Update processor configuration")
    parser_update.add_argument("--enable", action=argparse.BooleanOptionalAction, help="Enable or disable the processor")
    parser_update.add_argument("--max-parallel-jobs", type=int, help="New max parallel jobs limit")
    parser_update.add_argument("--frequency", type=int, help="New frequency in seconds for the processor job")
    parser_update.add_argument("--timeout", type=int, help="New timeout in seconds for processing jobs")
    parser_update.add_argument("--table-prefix", type=str, help="New table prefix for the cache tables")
    parser_update.set_defaults(
        func=lambda args: update_processor_config(
            get_db_connection(),
            enabled=args.enable,
            max_parallel_jobs=args.max_parallel_jobs,
            frequency=args.frequency,
            timeout=args.timeout,
            table_prefix=args.table_prefix,
            queue_prefix=get_queue_table_prefix_from_env(),
        )
    )

    # status command
    parser_status = subparsers.add_parser("status", help="Show basic processor status")
    parser_status.set_defaults(
        func=lambda args: print_status(
            get_processor_status(
                get_db_connection(),
                get_queue_table_prefix_from_env(),
            )
        )
    )

    # status-detailed command
    parser_status_detailed = subparsers.add_parser("status-detailed", help="Show detailed processor status")
    parser_status_detailed.set_defaults(
        func=lambda args: print_detailed_status(
            get_processor_status_detailed(
                get_db_connection(),
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
                get_db_connection(),
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
        if hasattr(args, "func"):
            args.func(args)
        else:
            parser.print_help()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
