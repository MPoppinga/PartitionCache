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


def get_table_prefix_from_env() -> str:
    """
    Automatically determine the table prefix from environment variables.

    Returns the table prefix that matches what the cache handler actually uses internally.
    """
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")

    if cache_backend == "postgresql_array":
        table_name = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX")
        if table_name:
            return table_name
    elif cache_backend == "postgresql_bit":
        table_name = os.getenv("PG_BIT_CACHE_TABLE_PREFIX")
        if table_name:
            return table_name

    # Fallback to default
    return "partitioncache"


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

    # Check cache type
    cache_type = os.getenv("CACHE_BACKEND", "postgresql_array")
    if cache_type not in ["postgresql_array", "postgresql_bit"]:
        return False, f"PostgreSQL queue processor only supports postgresql_array or postgresql_bit cache backends, got: {cache_type}"

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
        cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
        logger.info(f"Initializing cache handler ({cache_backend}) to ensure metadata tables exist...")
        cache_handler = get_cache_handler(cache_backend)
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

    conn.commit()
    logger.info("PostgreSQL queue processor database objects created successfully")


def setup_pg_cron_job(conn, table_prefix: str, queue_prefix: str, frequency: int = 1):
    """Set up pg_cron job to process the queue."""
    logger.info(f"Setting up pg_cron job with frequency: every {frequency} second(s)")

    with conn.cursor() as cur:
        # Remove existing job if any
        cur.execute("""
            DELETE FROM cron.job 
            WHERE jobname = 'partitioncache_process_queue'
        """)

        # Use modern pg_cron syntax for second-level scheduling
        if frequency < 60:
            # Use the new "X seconds" syntax for frequencies under 60 seconds
            schedule = f"{frequency} seconds"
        else:
            # For 60+ seconds, use traditional cron format (minutes)
            schedule = f"*/{frequency // 60} * * * *"

        cur.execute(
            """
            INSERT INTO cron.job (jobname, schedule, command)
            VALUES (
                'partitioncache_process_queue',
                %s,
                %s
            )
        """,
            (schedule, f"SELECT partitioncache_process_queue('{table_prefix}', '{queue_prefix}');"),
        )

        conn.commit()
        logger.info(f"pg_cron job created successfully with schedule: {schedule}")


def remove_pg_cron_job(conn):
    """Remove the pg_cron job."""
    logger.info("Removing pg_cron job...")

    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM cron.job 
            WHERE jobname = 'partitioncache_process_queue'
        """)

        rows_deleted = cur.rowcount
        conn.commit()

        if rows_deleted > 0:
            logger.info("pg_cron job removed successfully")
        else:
            logger.info("No pg_cron job found to remove")


def get_processor_status(conn, table_prefix: str, queue_prefix: str):
    """Get the current status of the processor."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_processor_status(%s, %s)", [table_prefix, queue_prefix])
        result = cur.fetchone()

        if result:
            return {
                "enabled": result[0],
                "max_parallel_jobs": result[1],
                "frequency_seconds": result[2],
                "active_jobs": result[3],
                "queue_length": result[4],
                "recent_successes": result[5],
                "recent_failures": result[6],
            }
        return None


def get_processor_status_detailed(conn, table_prefix: str, queue_prefix: str):
    """Get detailed processor status including queue and cache information."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_processor_status_detailed(%s, %s)", [table_prefix, queue_prefix])
        result = cur.fetchone()

        if result:
            return {
                "enabled": result[0],
                "max_parallel_jobs": result[1],
                "frequency_seconds": result[2],
                "active_jobs": result[3],
                "queue_length": result[4],
                "recent_successes": result[5],
                "recent_failures": result[6],
                "total_partitions": result[7],
                "partitions_with_cache": result[8],
                "bit_cache_partitions": result[9],
                "array_cache_partitions": result[10],
                "uncreated_cache_partitions": result[11],
            }
        return None


def get_queue_and_cache_info(conn, table_prefix: str, queue_prefix: str):
    """Get detailed information about queues and cache architecture."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_get_queue_and_cache_info(%s, %s)", [table_prefix, queue_prefix])
        results = cur.fetchall()

        return [
            {
                "partition_key": row[0],
                "cache_table_name": row[1],
                "cache_architecture": row[2],
                "cache_column_type": row[3],
                "bitsize": row[4],
                "partition_datatype": row[5],
                "queue_items": row[6],
                "last_cache_update": row[7],
                "cache_exists": row[8],
            }
            for row in results
        ]


def print_status(status):
    """Print PostgreSQL queue processor status in a readable format."""
    if not status:
        print("Processor not initialized")
        return

    print("\n=== PartitionCache PostgreSQL Queue Processor Status ===")
    print(f"Enabled: {'Yes' if status['enabled'] else 'No'}")
    print(f"Max Parallel Jobs: {status['max_parallel_jobs']}")
    print(f"Frequency: Every {status['frequency_seconds']} second(s)")
    print("\nCurrent State:")
    print(f"  Active Jobs: {status['active_jobs']}")
    print(f"  Queue Length: {status['queue_length']}")
    print(f"  Recent Successes (5 min): {status['recent_successes']}")
    print(f"  Recent Failures (5 min): {status['recent_failures']}")


def print_detailed_status(status):
    """Print detailed PostgreSQL queue processor status including queue and cache information."""
    if not status:
        print("Processor not initialized")
        return

    print("\n=== PartitionCache PostgreSQL Queue Processor - Detailed Status ===")
    print(f"Enabled: {'Yes' if status['enabled'] else 'No'}")
    print(f"Max Parallel Jobs: {status['max_parallel_jobs']}")
    print(f"Frequency: Every {status['frequency_seconds']} second(s)")

    print("\nCurrent Processing State:")
    print(f"  Active Jobs: {status['active_jobs']}")
    print(f"  Queue Length: {status['queue_length']}")
    print(f"  Recent Successes (5 min): {status['recent_successes']}")
    print(f"  Recent Failures (5 min): {status['recent_failures']}")

    print("\nCache Architecture Overview:")
    print(f"  Total Partitions: {status['total_partitions']}")
    print(f"  Partitions with Cache: {status['partitions_with_cache']}")
    print(f"  Bit Cache Partitions: {status['bit_cache_partitions']}")
    print(f"  Array Cache Partitions: {status['array_cache_partitions']}")
    print(f"  Uncreated Cache Partitions: {status['uncreated_cache_partitions']}")


def print_queue_and_cache_info(queue_info):
    """Print detailed queue and cache information."""
    if not queue_info:
        print("No partition information found")
        return

    print("\n=== Queue and Cache Architecture Details ===")
    print(f"{'Partition Key':<20} {'Architecture':<12} {'Queue Items':<12} {'Cache Table':<25} {'Last Update':<20}")
    print("-" * 100)

    for info in queue_info:
        partition_key = info["partition_key"] or "unknown"
        architecture = info["cache_architecture"]
        queue_items = info["queue_items"]
        cache_table = info["cache_table_name"]
        last_update = info["last_cache_update"].strftime("%Y-%m-%d %H:%M:%S") if info["last_cache_update"] else "never"

        # Color coding for architecture
        arch_display = architecture
        if architecture == "bit":
            arch_display = f"bit({info['bitsize'] or 'unknown'})"
        elif architecture == "not_created":
            arch_display = "not created"

        print(f"{partition_key:<20} {arch_display:<12} {queue_items:<12} {cache_table:<25} {last_update:<20}")

    # Summary by architecture type
    print("\n=== Architecture Summary ===")
    arch_counts = {}
    total_queue_items = 0

    for info in queue_info:
        arch = info["cache_architecture"]
        arch_counts[arch] = arch_counts.get(arch, 0) + 1
        total_queue_items += info["queue_items"]

    for arch, count in arch_counts.items():
        print(f"{arch.replace('_', ' ').title()}: {count} partition(s)")

    print(f"Total Queue Items: {total_queue_items}")

    # Show partition datatypes
    datatypes = set(info["partition_datatype"] for info in queue_info if info["partition_datatype"])
    if datatypes:
        print(f"Partition Data Types: {', '.join(sorted(datatypes))}")


def view_logs(conn, limit: int = 20, status_filter: str | None = None, queue_prefix: str | None = None):
    """View recent processor logs."""
    if queue_prefix is None:
        queue_prefix = get_queue_table_prefix_from_env()

    log_table = f"{queue_prefix}_processor_log"

    query = sql.SQL("""
        SELECT job_id, query_hash, partition_key, status, 
               error_message, rows_affected, execution_time_ms, created_at
        FROM {}
        {}
        ORDER BY created_at DESC
        LIMIT %s
    """)

    where_clause = sql.SQL("")
    params = [limit]

    if status_filter:
        where_clause = sql.SQL("WHERE status = %s")
        params = [status_filter] + params

    with conn.cursor() as cur:
        cur.execute(query.format(sql.Identifier(log_table), where_clause), params)
        logs = cur.fetchall()

        if not logs:
            print("No logs found")
            return

        print(f"\n=== Recent Processor Logs (limit: {limit}) ===")
        for log in logs:
            job_id, query_hash, partition_key, status, error_msg, rows, exec_time, created_at = log
            print(f"\n[{created_at}] Job: {job_id}")
            print(f"  Query Hash: {query_hash}, Partition: {partition_key}")
            print(f"  Status: {status}")
            if exec_time is not None:
                print(f"  Execution Time: {exec_time}ms")
            if rows is not None:
                print(f"  Rows Affected: {rows}")
            if error_msg:
                print(f"  Error: {error_msg}")


def main():
    parser = argparse.ArgumentParser(description="Setup and manage PostgreSQL queue processor")

    parser.add_argument("--env", type=str, help="Path to .env file", default=".env")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Initialize the PostgreSQL queue processor")
    setup_parser.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: Based on Env Vars)")
    setup_parser.add_argument("--skip-pg-cron", action="store_true", help="Skip pg_cron job creation (manual execution only)")

    # Enable/disable commands
    subparsers.add_parser("enable", help="Enable the processor")
    subparsers.add_parser("disable", help="Disable the processor")

    # Config command
    config_parser = subparsers.add_parser("config", help="Update processor configuration")
    config_parser.add_argument("--max-jobs", type=int, help="Maximum parallel jobs (1-20)")
    config_parser.add_argument("--frequency", type=int, help="Execution frequency in seconds")

    # Status command
    subparsers.add_parser("status", help="Show processor status")

    # Detailed status command
    detailed_status_parser = subparsers.add_parser("status-detailed", help="Show detailed processor status with cache architecture info")
    detailed_status_parser.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: Based on Env Vars)")

    # Queue info command
    queue_info_parser = subparsers.add_parser("queue-info", help="Show detailed queue and cache architecture information")
    queue_info_parser.add_argument("--table-prefix", default=None, help="Table prefix for cache tables (default: Based on Env Vars)")

    # Logs command
    logs_parser = subparsers.add_parser("logs", help="View processor logs")
    logs_parser.add_argument("--limit", type=int, default=20, help="Number of log entries to show")
    logs_parser.add_argument("--status", choices=["started", "success", "failed", "timeout"], help="Filter logs by status")

    # Remove command
    subparsers.add_parser("remove", help="Remove pg_cron job")

    # Test command
    test_parser = subparsers.add_parser("test", help="Test process a single item from queue")
    test_parser.add_argument("--table-prefix", default=None, help="Table prefix for cache tables")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Load environment
    dotenv.load_dotenv(args.env)

    # Automatically determine table prefix from environment if not explicitly provided
    auto_table_prefix = get_table_prefix_from_env()

    # Validate environment for all commands
    valid, message = validate_environment()
    if not valid:
        logger.error(f"Environment validation failed:\n{message}")
        sys.exit(1)

    # Get database connection
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    try:
        if args.command == "setup":
            # Use auto-detected table prefix if default was not changed
            table_prefix = auto_table_prefix if args.table_prefix is None else args.table_prefix

            print(f"Using table prefix: {table_prefix}")

            # Check pg_cron
            if not args.skip_pg_cron and not check_pg_cron_installed(conn):
                logger.error("pg_cron extension not installed. Install it or use --skip-pg-cron for manual execution.")
                sys.exit(1)

            # Setup database objects
            setup_database_objects(conn)

            # Setup pg_cron job
            if not args.skip_pg_cron:
                queue_prefix = get_queue_table_prefix_from_env()
                setup_pg_cron_job(conn, table_prefix, queue_prefix)

            print("\nPostgreSQL queue processor setup complete!")
            print("Use 'setup_postgresql_queue_processor.py enable' to start processing")

        elif args.command == "enable":
            queue_prefix = get_queue_table_prefix_from_env()
            with conn.cursor() as cur:
                cur.execute("SELECT partitioncache_set_processor_enabled(true, %s)", [queue_prefix])
                conn.commit()
            print("Processor enabled")

        elif args.command == "disable":
            queue_prefix = get_queue_table_prefix_from_env()
            with conn.cursor() as cur:
                cur.execute("SELECT partitioncache_set_processor_enabled(false, %s)", [queue_prefix])
                conn.commit()
            print("Processor disabled")

        elif args.command == "config":
            params = []
            values = []

            if args.max_jobs is not None:
                params.append("p_max_parallel_jobs => %s")
                values.append(args.max_jobs)

            if args.frequency is not None:
                params.append("p_frequency_seconds => %s")
                values.append(args.frequency)

                # Update pg_cron job if frequency changed
                if check_pg_cron_installed(conn):
                    # Use auto-detected prefixes for pg_cron job update
                    queue_prefix = get_queue_table_prefix_from_env()
                    setup_pg_cron_job(conn, auto_table_prefix, queue_prefix, args.frequency)

            if params:
                queue_prefix = get_queue_table_prefix_from_env()
                params.append("p_queue_prefix => %s")
                values.append(queue_prefix)

                with conn.cursor() as cur:
                    query = f"SELECT partitioncache_update_processor_config({', '.join(params)})"
                    cur.execute(query, values)
                    conn.commit()
                print("Configuration updated")
            else:
                print("No configuration changes specified")

        elif args.command == "status":
            queue_prefix = get_queue_table_prefix_from_env()
            status = get_processor_status(conn, auto_table_prefix, queue_prefix)
            print_status(status)

        elif args.command == "status-detailed":
            queue_prefix = get_queue_table_prefix_from_env()
            status = get_processor_status_detailed(conn, args.table_prefix or auto_table_prefix, queue_prefix)
            print_detailed_status(status)

        elif args.command == "queue-info":
            queue_prefix = get_queue_table_prefix_from_env()
            queue_info = get_queue_and_cache_info(conn, args.table_prefix or auto_table_prefix, queue_prefix)
            print_queue_and_cache_info(queue_info)

        elif args.command == "logs":
            queue_prefix = get_queue_table_prefix_from_env()
            view_logs(conn, args.limit, args.status, queue_prefix)

        elif args.command == "remove":
            remove_pg_cron_job(conn)

        elif args.command == "test":
            # Use auto-detected prefixes if not explicitly provided
            table_prefix = args.table_prefix or auto_table_prefix
            queue_prefix = get_queue_table_prefix_from_env()

            print(f"Testing PostgreSQL queue processor with table prefix: {table_prefix}, queue prefix: {queue_prefix}")
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT * FROM partitioncache_process_queue_item(%s, %s)"), [table_prefix, queue_prefix])
                result = cur.fetchone()
                if result:
                    processed, message = result
                    print(f"Processed: {processed}")
                    print(f"Message: {message}")
                else:
                    print("No result from test")
                conn.commit()

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
