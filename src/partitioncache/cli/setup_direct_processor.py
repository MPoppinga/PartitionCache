#!/usr/bin/env python3
"""
Setup and manage PostgreSQL direct queue processor for PartitionCache.

This script sets up the necessary database objects and pg_cron jobs to process
queries directly within PostgreSQL, eliminating the need for external observer scripts.
"""

import argparse
import os
import sys
from pathlib import Path
from logging import getLogger
import psycopg
from psycopg import sql
import dotenv

logger = getLogger("PartitionCache")

# SQL file location
SQL_FILE = Path(__file__).parent.parent / "queue_handler" / "postgresql_direct_processor.sql"


def get_table_prefix_from_env() -> str:
    """
    Automatically determine the table prefix from environment variables.
    
    Returns the table prefix based on CACHE_BACKEND and corresponding table env vars.
    """
    cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
    
    if cache_backend == "postgresql_array":
        table_name = os.getenv("PG_ARRAY_CACHE_TABLE")
        if table_name:
            return table_name
    elif cache_backend == "postgresql_bit":
        table_name = os.getenv("PG_BIT_CACHE_TABLE") 
        if table_name:
            return table_name
    
    # Fallback to default
    return "partitioncache"


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
        return False, f"Direct processor only supports postgresql_array or postgresql_bit cache backends, got: {cache_type}"
    
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
    logger.info("Setting up database objects...")
    
    # Read and execute SQL file
    sql_content = SQL_FILE.read_text()
    
    # Replace table prefix in SQL if needed
    if table_prefix != "partitioncache":
        sql_content = sql_content.replace("partitioncache_", f"{table_prefix}_")
    
    with conn.cursor() as cur:
        cur.execute(sql_content)
    
    conn.commit()
    logger.info("Database objects created successfully")


def setup_pg_cron_job(conn, table_prefix: str, frequency: int = 1):
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
            schedule = f"*/{frequency//60} * * * *"
        
        cur.execute("""
            INSERT INTO cron.job (jobname, schedule, command)
            VALUES (
                'partitioncache_process_queue',
                %s,
                %s
            )
        """, (
            schedule,
            f"SELECT partitioncache_process_queue('{table_prefix}');"
        ))
        
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


def get_processor_status(conn):
    """Get the current status of the processor."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM get_processor_status()")
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


def print_status(status):
    """Print processor status in a readable format."""
    if not status:
        print("Processor not initialized")
        return
    
    print("\n=== PartitionCache Direct Processor Status ===")
    print(f"Enabled: {'Yes' if status['enabled'] else 'No'}")
    print(f"Max Parallel Jobs: {status['max_parallel_jobs']}")
    print(f"Frequency: Every {status['frequency_seconds']} second(s)")
    print("\nCurrent State:")
    print(f"  Active Jobs: {status['active_jobs']}")
    print(f"  Queue Length: {status['queue_length']}")
    print(f"  Recent Successes (5 min): {status['recent_successes']}")
    print(f"  Recent Failures (5 min): {status['recent_failures']}")


def view_logs(conn, limit: int = 20, status_filter: str | None = None):
    """View recent processor logs."""
    query = sql.SQL("""
        SELECT job_id, query_hash, partition_key, status, 
               error_message, rows_affected, execution_time_ms, created_at
        FROM partitioncache_processor_log
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
        cur.execute(query.format(where_clause), params)
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
    parser = argparse.ArgumentParser(description="Setup and manage PostgreSQL direct queue processor")
    
    parser.add_argument("--env", type=str, help="Path to .env file", default=".env")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Initialize the direct processor")
    setup_parser.add_argument(
        "--table-prefix",
        default=None,
        help="Table prefix for cache tables (default: Based on Env Vars)"
    )
    setup_parser.add_argument(
        "--skip-pg-cron",
        action="store_true",
        help="Skip pg_cron job creation (manual execution only)"
    )
    
    # Enable/disable commands
    subparsers.add_parser("enable", help="Enable the processor")
    subparsers.add_parser("disable", help="Disable the processor")
    
    # Config command
    config_parser = subparsers.add_parser("config", help="Update processor configuration")
    config_parser.add_argument("--max-jobs", type=int, help="Maximum parallel jobs (1-20)")
    config_parser.add_argument("--frequency", type=int, help="Execution frequency in seconds")
    
    # Status command
    subparsers.add_parser("status", help="Show processor status")
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="View processor logs")
    logs_parser.add_argument("--limit", type=int, default=20, help="Number of log entries to show")
    logs_parser.add_argument(
        "--status",
        choices=["started", "success", "failed", "timeout"],
        help="Filter logs by status"
    )
    
    # Remove command
    subparsers.add_parser("remove", help="Remove pg_cron job")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Test process a single item from queue")
    test_parser.add_argument(
        "--table-prefix",
        default=None,
        help="Table prefix for cache tables"
    )
    
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
                logger.error(
                    "pg_cron extension not installed. Install it or use --skip-pg-cron for manual execution."
                )
                sys.exit(1)
            
            # Setup database objects
            setup_database_objects(conn, table_prefix)
            
            # Setup pg_cron job
            if not args.skip_pg_cron:
                setup_pg_cron_job(conn, table_prefix)
            
            print("\nDirect processor setup complete!")
            print("Use 'setup_direct_processor.py enable' to start processing")
            
        elif args.command == "enable":
            with conn.cursor() as cur:
                cur.execute("SELECT set_processor_enabled(true)")
                conn.commit()
            print("Processor enabled")
            
        elif args.command == "disable":
            with conn.cursor() as cur:
                cur.execute("SELECT set_processor_enabled(false)")
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
                    # Use auto-detected table prefix for pg_cron job update
                    setup_pg_cron_job(conn, auto_table_prefix, args.frequency)
            
            if params:
                with conn.cursor() as cur:
                    query = f"SELECT update_processor_config({', '.join(params)})"
                    cur.execute(query, values)
                    conn.commit()
                print("Configuration updated")
            else:
                print("No configuration changes specified")
            
        elif args.command == "status":
            status = get_processor_status(conn)
            print_status(status)
            
        elif args.command == "logs":
            view_logs(conn, args.limit, args.status)
            
        elif args.command == "remove":
            remove_pg_cron_job(conn)
            
        elif args.command == "test":
            # Use auto-detected table prefix if default was not changed
            table_prefix = auto_table_prefix if args.table_prefix == "partitioncache" else args.table_prefix
            
            print(f"Testing direct processor with table prefix: {table_prefix}")
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT * FROM process_queue_item(%s)"),
                    [table_prefix]
                )
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