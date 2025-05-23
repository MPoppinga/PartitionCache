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
import dotenv
import psycopg

from partitioncache.cache_handler import get_cache_handler
from partitioncache.db_handler import get_db_handler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.queue import pop_from_incoming_queue, pop_from_outgoing_queue, push_to_outgoing_queue, get_queue_lengths

args: argparse.Namespace

# Initialize threading components
status_lock = threading.Lock()
active_futures: list[str] = []
pending_jobs: list[tuple[str, str]] = []
pool: concurrent.futures.ProcessPoolExecutor | None = None  # Initialize pool as None
exit_event = threading.Event()  # Create an event to signal exit
fragment_processor_exit = threading.Event()  # Exit signal for fragment processor


def query_fragment_processor():
    """Thread function that processes original queries into fragments and pushes to query fragment queue."""
    print("Starting query fragment processor thread")

    while not exit_event.is_set():
        try:
            # Pop query from original query queue
            query = pop_from_incoming_queue()
            if query is None:
                continue  # Timeout occurred, check exit event and try again

            print("Processing original query into fragments")

            # Process the query into fragments
            query_hash_pairs = generate_all_query_hash_pairs(query, args.partition_key, 1, True, True)
            print(f"Generated {len(query_hash_pairs)} fragments from original query")

            # Push fragments to query fragment queue using the generic function
            success = push_fragments_to_outgoing_queue(query_hash_pairs)
            if success:
                print(f"Pushed {len(query_hash_pairs)} fragments to query fragment queue")
            else:
                print("Error pushing fragments to query fragment queue")

        except Exception as e:
            print(f"Error in query fragment processor: {e}")
            time.sleep(1)  # Brief pause before retrying

    print("Query fragment processor thread exiting")


def push_fragments_to_outgoing_queue(query_hash_pairs):
    """
    Push query fragments to the query fragment queue.

    This is a wrapper function around push_to_outgoing_queue for backward compatibility
    with tests and existing code.

    Args:
        query_hash_pairs: List of (query, hash) tuples to push to query fragment queue.

    Returns:
        bool: True if all fragments were pushed successfully, False otherwise.
    """
    try:
        return push_to_outgoing_queue(query_hash_pairs)
    except Exception as e:
        print(f"Error pushing fragments to query fragment queue: {e}")
        return False


def run_and_store_query(query: str, hash: str):
    """Worker function to execute and store a query."""
    try:
        cache_handler = get_cache_handler(args.cache_backend)

        db_handler: AbstractDBHandler
        if args.db_backend == "postgresql":
            if args.db_env_file is None:
                raise ValueError("db_env_file is required")

            db_handler = get_db_handler(
                "postgres",
                host=os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", 5432))),
                user=os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
                password=os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
                dbname=args.db_name,
                timeout=args.long_running_query_timeout,
            )
        elif args.db_backend == "mysql":
            db_handler = get_db_handler(
                "mysql",
                host=os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", 3306))),
                user=os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
                password=os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
                dbname=args.db_name,
            )
        elif args.db_backend == "sqlite":
            db_handler = get_db_handler("sqlite", db_path=args.db_dir)
        else:
            raise AssertionError("No db backend specified, querying not possible")

        try:
            t = time.perf_counter()
            result = set(db_handler.execute(query))

            # Apply limit if specified
            if args.limit is not None and result is not None and len(result) >= args.limit:
                print(f"Query {hash} limited to {args.limit} partition keys")
                cache_handler.set_null(f"_LIMIT_{hash}")  # Set null termination bit
                return True

        except psycopg.OperationalError as e:
            if "statement timeout" in str(e):
                print(f"Query {hash} is a long running query")
                cache_handler.set_null(f"_TIMEOUT_{hash}")  # Set null termination bit
                return True
            else:
                raise e

        print(f"Running query {hash}")
        print(f"Query {hash} returned {len(result)} results")
        print(f"Query {hash} took {time.perf_counter() - t} seconds")
        db_handler.close()

        cache_handler.set_set(hash, result)
        print(f"Stored {hash} in cache")
        return True
    except Exception as e:
        print(f"Error processing query {hash}: {str(e)}")
        with multiprocessing.Lock():
            with open("tmp/error_sql_file.sql", "a") as f:
                f.write(f"Error from run on {datetime.datetime.now()}\n")
                f.write(query + "\n")
        return False


def print_status(active, pending, original_query_queue=0, query_fragment_queue=0):
    print(
        f"Active processes: {active}, Pending jobs: {pending}, "
        f"Original query queue: {original_query_queue}, Query fragment queue: {query_fragment_queue}"
    )


def process_completed_future(future, hash):
    """Process a completed future and update job status."""
    global pool  # Ensure pool is accessible
    if pool is None:
        raise AssertionError("No pool set up")

    with status_lock:
        if hash in active_futures:
            active_futures.remove(hash)
            print(f"Completed query {hash}")
        else:
            print(f"Warning: Completed hash {hash} not found in active_futures")

        # Start a new job from pending if available
        if pending_jobs:
            next_query, next_hash = pending_jobs.pop(0)
            new_future = pool.submit(run_and_store_query, next_query, next_hash)
            active_futures.append(next_hash)
            new_future.add_done_callback(lambda f: process_completed_future(f, next_hash))
            print(f"Started query {next_hash} from pending queue")

        # Check queue lengths for status using generic function
        try:
            lengths = get_queue_lengths()
            print_status(len(active_futures), len(pending_jobs), lengths["incoming"], lengths["outgoing"])
        except Exception as e:
            print(f"Error getting queue lengths: {e}")
            print_status(len(active_futures), len(pending_jobs))

        if args.close and len(active_futures) == 0 and len(pending_jobs) == 0:
            print("Closing cache at ", datetime.datetime.now())
            exit_event.set()  # Signal to exit


def fragment_executor():
    """Thread pool function that processes fragments from the outgoing queue."""
    global pool

    # Initialize cache handler for the main process
    main_cache_handler = get_cache_handler(args.cache_backend)

    print("Starting fragment executor")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_processes) as pool:
        while not exit_event.is_set():
            try:
                print("Waiting for fragment from outgoing queue")
                with status_lock:
                    active = len(active_futures)
                    pending = len(pending_jobs)

                # Get queue lengths for status using generic function
                try:
                    lengths = get_queue_lengths()
                    print_status(active, pending, lengths["incoming"], lengths["outgoing"])
                except Exception as e:
                    print(f"Error getting queue lengths: {e}")
                    print_status(active, pending)

                # Pop fragment from outgoing queue
                fragment_result = pop_from_outgoing_queue()
                if fragment_result is None:
                    continue  # Timeout occurred, check exit event and try again

                query, hash_value = fragment_result
                print(f"Found fragment in outgoing queue: {hash_value}")

                # Check if already in cache or being processed
                if main_cache_handler.exists(hash_value):
                    print(f"Query {hash_value} already in cache")
                    continue

                # Check for existing termination bits before submitting
                if main_cache_handler.exists(f"_LIMIT_{hash_value}"):
                    print(f"Query {hash_value} previously hit limit, skipping")
                    continue
                if main_cache_handler.exists(f"_TIMEOUT_{hash_value}"):
                    print(f"Query {hash_value} previously timed out, skipping")
                    continue

                if hash_value in active_futures or any(job[1] == hash_value for job in pending_jobs):
                    print(f"Query {hash_value} already in process")
                    continue

                with status_lock:
                    if len(active_futures) < args.max_processes:
                        future = pool.submit(run_and_store_query, query, hash_value)
                        active_futures.append(hash_value)
                        future.add_done_callback(lambda f, h=hash_value: process_completed_future(f, h))  # type: ignore
                        print(f"Started query {hash_value}")
                    else:
                        pending_jobs.append((query, hash_value))
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
    parser = argparse.ArgumentParser()

    parser.add_argument("--db_backend", type=str, default="sqlite", help="database backend", choices=["postgresql", "mysql", "sqlite"])
    parser.add_argument("--cache_backend", type=str, default="rocksdb", help="cache backend")
    parser.add_argument("--db_dir", type=str, default="data/test_db.sqlite", help="database directory")
    parser.add_argument("--db_env_file", type=str, help="database environment file")
    parser.add_argument("--partition_key", type=str, default="partition_key", help="search space identifier")
    parser.add_argument("--close", action="store_true", default=False, help="Close the cache after operation")

    parser.add_argument("--max_processes", type=int, default=12, help="max number of processes to use")
    # DB Info
    parser.add_argument(
        "--db-name",
        dest="db_name",
        action="store",
        type=str,
        help="database name",
    )

    parser.add_argument("--long_running_query_timeout", type=str, default="0", help="timeout for long running queries")

    parser.add_argument("--limit", type=int, default=None, help="limit the number of returned partition keys")

    global args
    args = parser.parse_args()

    print("Starting main, Current Time: ", datetime.datetime.now())
    if args.db_backend is None:
        raise ValueError("db_backend is required")

    # Load dotenv
    dotenv.load_dotenv(args.db_env_file)

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
