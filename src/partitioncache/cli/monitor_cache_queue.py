"""
Monitor the cache queue and add queries to the cache as they are added to the queue.
"""

import argparse
import concurrent.futures
import datetime
import multiprocessing
import os
import threading
import time

import psycopg

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cli.common_args import (
    add_database_args,
    add_environment_args,
    get_database_connection_params,
    load_environment_with_validation,
    resolve_cache_backend,
)
from partitioncache.db_handler import get_db_handler
from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.queue import (
    get_queue_lengths,
    get_queue_provider_name,
    pop_from_original_query_queue,
    pop_from_original_query_queue_blocking,
    pop_from_query_fragment_queue,
    pop_from_query_fragment_queue_blocking,
    push_to_query_fragment_queue,
)

args: argparse.Namespace

# Initialize threading components
status_lock = threading.Lock()
active_futures: list[str] = []
pending_jobs: list[tuple[str, str, str, str]] = []  # (query, hash, partition_key, partition_datatype)
pool: concurrent.futures.ProcessPoolExecutor | None = None  # Initialize pool as None
exit_event = threading.Event()  # Create an event to signal exit
fragment_processor_exit = threading.Event()  # Exit signal for fragment processor

# Add logging control variables
last_status_log_time = 0.0
status_log_interval = 10  # Log status every 10 seconds when idle


def query_fragment_processor():
    """Thread function that processes original queries into fragments and pushes to query fragment queue."""
    print("Starting query fragment processor thread")

    while not exit_event.is_set():
        try:
            # Pop query from original query queue
            # Use blocking or non-blocking based on settings
            if not getattr(args, "disable_optimized_polling", False):
                # Use efficient blocking pop (with LISTEN/NOTIFY for PostgreSQL, native blocking for Redis)
                query_result = pop_from_original_query_queue_blocking(timeout=60)
            else:
                # Use regular pop when optimized polling is disabled
                query_result = pop_from_original_query_queue()

            if query_result is None:
                continue  # Timeout occurred, check exit event and try again

            query, partition_key, partition_datatype = query_result
            print(f"Processing original query into fragments for partition_key: {partition_key} (datatype: {partition_datatype})")

            # Process the query into fragments using the partition_key from queue
            query_hash_pairs = generate_all_query_hash_pairs(query, partition_key, 1, True, True)
            print(f"Generated {len(query_hash_pairs)} fragments from original query")

            # Push fragments to query fragment queue using the partition_key and datatype from queue
            success = push_to_query_fragment_queue(query_hash_pairs, partition_key, partition_datatype)
            if success:
                print(f"Pushed {len(query_hash_pairs)} fragments to query fragment queue")
            else:
                print("Error pushing fragments to query fragment queue")

        except Exception as e:
            print(f"Error in query fragment processor: {e}")
            time.sleep(1)  # Brief pause before retrying

    print("Query fragment processor thread exiting")


def run_and_store_query(query: str, query_hash: str, partition_key: str, partition_datatype: str | None = None):
    """Worker function to execute and store a query.

    Args:
        query: SQL query string to execute
        query_hash: Unique hash identifier for the query
        partition_key: Partition key for organizing cached results
        partition_datatype: Datatype of the partition key (default: None)
    """
    try:
        # Resolve cache backend
        cache_backend = resolve_cache_backend(args)
        cache_handler = get_cache_handler(cache_backend)

        # Get database connection parameters
        db_connection_params = get_database_connection_params(args)

        if args.db_backend == "postgresql":
            db_connection_params["timeout"] = args.long_running_query_timeout
            db_handler = get_db_handler("postgres", **db_connection_params)
        elif args.db_backend == "mysql":
            db_handler = get_db_handler("mysql", **db_connection_params)
        elif args.db_backend == "sqlite":
            db_handler = get_db_handler("sqlite", **db_connection_params)
        else:
            raise AssertionError("No db backend specified, querying not possible")

        try:
            t = time.perf_counter()
            result = set(db_handler.execute(query))

            # Apply limit if specified
            if args.limit is not None and result is not None and len(result) >= args.limit:
                print(f"Query {query_hash} limited to {args.limit} partition keys")
                cache_handler.set_null(f"_LIMIT_{query_hash}", partition_key)  # Set null termination bit
                return True

        except psycopg.OperationalError as e:
            if "statement timeout" in str(e):
                print(f"Query {query_hash} is a long running query")
                cache_handler.set_null(f"_TIMEOUT_{query_hash}", partition_key)  # Set null termination bit
                return True
            else:
                raise e

        print(f"Running query {query_hash} for partition_key: {partition_key} (datatype: {partition_datatype})")
        print(f"Query {query_hash} returned {len(result)} results")
        print(f"Query {query_hash} took {time.perf_counter() - t} seconds")
        db_handler.close()

        if partition_datatype is not None:
            cache_handler.register_partition_key(partition_key, partition_datatype)

        cache_handler.set_set(query_hash, result, partition_key)
        cache_handler.set_query(query_hash, query, partition_key)
        print(f"Stored {query_hash} in cache")
        return True
    except Exception as e:
        print(f"Error processing query {query_hash}: {str(e)}")
        with multiprocessing.Lock():
            with open("tmp/error_sql_file.sql", "a") as f:
                f.write(f"Error from run on {datetime.datetime.now()}\n")
                f.write(query + "\n")
        return False


def print_status(active, pending, original_query_queue=0, query_fragment_queue=0):
    print(f"Active processes: {active}, Pending jobs: {pending}, Original query queue: {original_query_queue}, Query fragment queue: {query_fragment_queue}")


def process_completed_future(future, query_hash):
    """Process a completed future and update job status."""
    global pool  # Ensure pool is accessible
    if pool is None:
        raise AssertionError("No pool set up")

    with status_lock:
        if query_hash in active_futures:
            active_futures.remove(query_hash)
            print(f"Completed query {query_hash}")
        else:
            print(f"Warning: Completed hash {query_hash} not found in active_futures")

        # Start a new job from pending if available
        if pending_jobs:
            next_query, next_hash, next_partition_key, next_partition_datatype = pending_jobs.pop(0)
            new_future = pool.submit(run_and_store_query, next_query, next_hash, next_partition_key, next_partition_datatype)
            active_futures.append(next_hash)
            new_future.add_done_callback(lambda f, h=next_hash: process_completed_future(f, h))  # type: ignore[misc]
            print(f"Started query {next_hash} from pending queue")

        # Check queue lengths for status using generic function
        try:
            lengths = get_queue_lengths()
            print_status(len(active_futures), len(pending_jobs), lengths["original_query_queue"], lengths["query_fragment_queue"])
        except Exception as e:
            print(f"Error getting queue lengths: {e}")
            print_status(len(active_futures), len(pending_jobs))

        if args.close and len(active_futures) == 0 and len(pending_jobs) == 0:
            print("Closing cache at ", datetime.datetime.now())
            exit_event.set()  # Signal to exit


def fragment_executor():
    """Thread pool function that processes fragments from the fragment queue."""
    global pool, last_status_log_time

    # Initialize cache handler for the main process
    main_cache_handler = get_cache_handler(args.cache_backend)

    print("Starting fragment executor")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_processes) as pool:
        while not exit_event.is_set():
            try:
                current_time = time.time()

                with status_lock:
                    active = len(active_futures)
                    pending = len(pending_jobs)

                # Get queue lengths for status
                try:
                    lengths = get_queue_lengths()
                    incoming_count = lengths["original_query_queue"]
                    fragment_count = lengths["query_fragment_queue"]
                except Exception as e:
                    print(f"Error getting queue lengths: {e}")
                    incoming_count = fragment_count = 0

                # Determine if we should log status
                should_log_status = False
                if active > 0 or pending > 0 or incoming_count > 0 or fragment_count > 0:
                    # Always log when there's activity
                    should_log_status = True
                    last_status_log_time = current_time
                elif current_time - last_status_log_time >= args.status_log_interval:
                    # Log periodically when idle
                    should_log_status = True
                    last_status_log_time = current_time

                if should_log_status:
                    print("Waiting for fragment from fragment queue")
                    print_status(active, pending, incoming_count, fragment_count)

                # Pop fragment from fragment queue
                # Use blocking or non-blocking based on settings
                if not args.disable_optimized_polling:
                    # Use efficient blocking pop (with LISTEN/NOTIFY for PostgreSQL, native blocking for Redis)
                    queue_provider = get_queue_provider_name()
                    print(f"Using {queue_provider} blocking pop")
                    fragment_result = pop_from_query_fragment_queue_blocking(timeout=60)
                else:
                    # Use regular pop when optimized polling is disabled
                    queue_provider = get_queue_provider_name()
                    print(f"Using regular pop for {queue_provider}")
                    fragment_result = pop_from_query_fragment_queue()

                if fragment_result is None:
                    continue  # Timeout occurred, check exit event and try again
                query, hash_value, partition_key, partition_datatype = fragment_result
                print(f"Found fragment in fragment queue: {hash_value} for partition_key: {partition_key} (datatype: {partition_datatype})")

                # Check if already in cache or being processed
                if main_cache_handler.exists(hash_value, partition_key):
                    print(f"Query {hash_value} already in cache")
                    main_cache_handler.set_query(hash_value, query, partition_key)  # update the query last_seen
                    continue

                # Check for existing termination bits before submitting
                if main_cache_handler.exists(f"_LIMIT_{hash_value}", partition_key):
                    print(f"Query {hash_value} previously hit limit, skipping")
                    continue
                if main_cache_handler.exists(f"_TIMEOUT_{hash_value}", partition_key):
                    print(f"Query {hash_value} previously timed out, skipping")
                    continue

                if hash_value in active_futures or any(job[1] == hash_value for job in pending_jobs):
                    print(f"Query {hash_value} already in process")
                    continue

                with status_lock:
                    if len(active_futures) < args.max_processes:
                        future = pool.submit(run_and_store_query, query, hash_value, partition_key, partition_datatype)
                        active_futures.append(hash_value)
                        future.add_done_callback(lambda f, h=hash_value: process_completed_future(f, h))  # type: ignore
                        print(f"Started query {hash_value}")
                    else:
                        pending_jobs.append((query, hash_value, partition_key, partition_datatype))
                        print(f"Queued query {hash_value}")

            except KeyboardInterrupt:
                print("Exiting fragment executor")
                exit_event.set()  # Ensure the exit event is set
                pool.shutdown(wait=True)  # Wait for all processes to finish
                break
            except Exception as e:
                print(f"Error in fragment executor: {e}")
                time.sleep(1)  # Brief pause before retrying

    print("Fragment executor exiting")


def validate_queue_configuration():
    """Validate that the queue configuration is properly set up."""
    from partitioncache.queue_handler import validate_queue_configuration

    return validate_queue_configuration()


def main():
    parser = argparse.ArgumentParser(description="Monitor cache queue and process queries")

    # Processing configuration
    processing_group = parser.add_argument_group("processing options")
    processing_group.add_argument("--close", action="store_true", default=False, help="Close the cache after operation")
    processing_group.add_argument("--max-processes", type=int, default=12, help="Max number of processes to use")
    processing_group.add_argument("--long-running-query-timeout", type=str, default="0", help="Timeout for long running queries")
    processing_group.add_argument("--limit", type=int, default=None, help="Limit the number of returned partition keys")
    processing_group.add_argument("--status-log-interval", type=int, default=10, help="Interval in seconds for logging status when queues are empty (default: 10)")
    processing_group.add_argument("--disable-optimized-polling", action="store_true", help="Disable optimized polling and use simple polling")

    # Add common argument groups
    add_database_args(parser)
    add_environment_args(parser)

    # Cache backend argument (not using add_cache_args because this tool doesn't need partition args)
    parser.add_argument("--cache-backend", type=str, help="Cache backend to use (if not specified, uses CACHE_BACKEND environment variable)")

    global args
    args = parser.parse_args()

    print("Starting main, Current Time: ", datetime.datetime.now())
    if args.db_backend is None:
        raise ValueError("db_backend is required")

    # Load environment variables
    load_environment_with_validation(args.env_file)

    # Set cache backend if not specified
    if args.cache_backend is None:
        args.cache_backend = os.getenv("CACHE_BACKEND", "postgresql_bit")

    # Validate queue configuration (supports both PostgreSQL and Redis)
    try:
        is_valid = validate_queue_configuration()
        if not is_valid:
            raise ValueError("Invalid queue configuration")
        provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")
        print(f"Using queue provider: {provider}")
    except ValueError as e:
        print(f"Queue configuration error: {e}")
        print("For PostgreSQL queues, set: PG_QUEUE_HOST, PG_QUEUE_PORT, PG_QUEUE_USER, PG_QUEUE_PASSWORD, PG_QUEUE_DB")
        print("For Redis queues, set: REDIS_HOST, REDIS_PORT, QUERY_QUEUE_REDIS_DB, QUERY_QUEUE_REDIS_QUEUE_KEY")
        print("Set QUERY_QUEUE_PROVIDER to 'redis' to use Redis instead of PostgreSQL")
        raise

    print("Starting two-threaded queue monitoring system:")
    print("- Thread 1: Process original queries into fragments")
    print("- Thread 2: Execute fragments from query fragment queue")
    print("- Partition keys are now read from the queue instead of command line arguments")

    # Start the query fragment processor thread
    fragment_processor_thread = threading.Thread(target=query_fragment_processor, daemon=True)
    fragment_processor_thread.start()

    # Start the fragment executor in the main thread
    try:
        fragment_executor()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
        exit_event.set()

    # Wait for fragment processor thread to finish
    fragment_processor_thread.join(timeout=5)
    print("Monitor cache queue shutting down")


if __name__ == "__main__":
    main()
