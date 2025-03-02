"""
Monitor the cache queue and add queries to the cache as they are added to the queue.
"""

import argparse
import concurrent.futures
import datetime
import multiprocessing
import os
import signal
import threading
import time
import dotenv
import psycopg
import redis
from logging import getLogger

from partitioncache.cache_handler import get_cache_handler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.db_handler.mysql import MySQLDBHandler
from partitioncache.db_handler.postgres import PostgresDBHandler
from partitioncache.db_handler.sqlite import SQLiteDBHandler
from partitioncache.query_processor import generate_all_query_hash_pairs

# Initialize threading components
status_lock = threading.Lock()
active_futures: list[str] = []
pending_jobs: list[tuple[str, str]] = []
pool: concurrent.futures.ProcessPoolExecutor | None = None  # Initialize pool as None
exit_event = threading.Event()  # Create an event to signal exit
args = None  # Global args variable
logger = getLogger("PartitionCache")

# Global variables for cleanup
redis_connection: redis.Redis | None = None
main_cache_handler = None

def signal_handler(signum: int, frame) -> None:
    """Handle termination signals gracefully."""
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    exit_event.set()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_backend", type=str, default="sqlite", help="database backend", choices=["postgresql", "mysql", "sqlite"])
    parser.add_argument("--cache_backend", type=str, default="rocksdb", help="cache backend")
    parser.add_argument("--db_dir", type=str, default="data/test_db.sqlite", help="database directory")
    parser.add_argument("--db_env_file", type=str, help="database environment file")
    parser.add_argument("--partition_key", type=str, default="partition_key", help="search space identifier")
    parser.add_argument("--close", action="store_true", default=False, help="Close the cache after operation")
    parser.add_argument("--max_processes", type=int, default=12, help="max number of processes to use")
    parser.add_argument(
        "--db-name",
        dest="db_name",
        action="store",
        type=str,
        help="database name",
    )
    parser.add_argument("--long_running_query_timeout", type=str, default="0", help="timeout for long running queries")
    parser.add_argument("--limit", type=int, default=None, help="limit the number of returned partition keys")
    return parser.parse_args()

def pop_redis(r: redis.Redis):
    result = r.blpop([os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY", "query_queue")])
    return result

def run_and_store_query(query: str, hash: str):
    """Worker function to execute and store a query."""
    assert args is not None
    try:
        cache_handler = get_cache_handler(args.cache_backend)

        db_handler: AbstractDBHandler
        if args.db_backend == "postgresql":
            if args.db_env_file is None:
                raise ValueError("db_env_file is required")

            db_handler = PostgresDBHandler(
                host=os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", 5432))),
                user=os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
                password=os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
                dbname=args.db_name,
                timeout=args.long_running_query_timeout,
            )
        elif args.db_backend == "mysql":
            db_handler = MySQLDBHandler(
                host=os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", 3306))),
                user=os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
                password=os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
                dbname=args.db_name,
            )
        elif args.db_backend == "sqlite":
            db_handler = SQLiteDBHandler(args.db_dir)
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

def print_status(active, pending):
    print(f"Active processes: {active}, Pending jobs: {pending}")

def process_completed_future(future, hash):
    """Process a completed future and update job status."""
    global pool  # Ensure pool is accessible
    assert args is not None
    
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

        print_status(len(active_futures), len(pending_jobs))
        if args.close and len(active_futures) == 0 and len(pending_jobs) == 0:
            print("Closing cache at ", datetime.datetime.now())
            exit_event.set()  # Signal to exit

def cleanup_resources() -> None:
    """Clean up all resources before exit."""
    global pool, redis_connection, main_cache_handler
    
    logger.info("Cleaning up resources...")
    
    # Shutdown process pool if it exists
    if pool is not None:
        logger.info("Shutting down process pool...")
        pool.shutdown(wait=True)
        pool = None
    
    # Close Redis connection if it exists
    if redis_connection is not None:
        logger.info("Closing Redis connection...")
        redis_connection.close()
        redis_connection = None
    
    # Close cache handler if it exists
    if main_cache_handler is not None:
        logger.info("Closing cache handler...")
        main_cache_handler.close()
        main_cache_handler = None

def main():
    global args, redis_connection, main_cache_handler, pool
    print("Starting main, Current Time: ", datetime.datetime.now())
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Parse command line arguments
        args = parse_args()
        
        if args.db_backend is None:
            raise ValueError("db_backend is required")

        # Load dotenv
        dotenv.load_dotenv(args.db_env_file)

        # Initialize Redis connection for query queue
        if os.environ.get("QUERY_QUEUE_PROVIDER", None) == "redis":
            if os.getenv("REDIS_HOST") is None:
                raise ValueError("REDIS_HOST not set")
            if os.getenv("REDIS_PORT") is None:
                raise ValueError("REDIS_PORT not set")
            if os.getenv("QUERY_QUEUE_REDIS_DB") is None:
                raise ValueError("QUERY_QUEUE_REDIS_DB not set")
            if os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY") is None:
                raise ValueError("QUERY_QUEUE_REDIS_QUEUE_KEY not set")

            redis_connection = redis.Redis(
                host=os.getenv("REDIS_HOST", ""),
                port=int(os.getenv("REDIS_PORT", 0)),
                db=int(os.getenv("QUERY_QUEUE_REDIS_DB", 1)),
            )
        else:
            raise AssertionError("No query queue provider specified, monitoring not possible")

        # Initialize cache handler for the main process
        main_cache_handler = get_cache_handler(args.cache_backend)

        # Function to continually fetch and submit jobs
        def job_fetcher():
            global pool  # Make pool accessible in process_completed_future
            assert args is not None
            assert redis_connection is not None
            assert main_cache_handler is not None
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_processes) as pool:
                while not exit_event.is_set():
                    try:
                        print("Waiting for query")
                        with status_lock:
                            active = len(active_futures)
                            pending = len(pending_jobs)
                            print_status(active, pending)
                        
                        # Use a timeout for blpop to allow checking exit_event
                        rr = redis_connection.blpop([os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY", "query_queue")], timeout=1)
                        if rr is None:
                            continue
                            
                        print("Found query in queue")
                        query = rr[1].decode("utf-8")  # type: ignore

                        # Process the query
                        query_hash_pair_list = generate_all_query_hash_pairs(query, args.partition_key, 1, True, True)

                        print(f"Found {len(query_hash_pair_list)} subqueries in query to store in cache")

                        # Order by length of query
                        query_hash_pair_list.sort(key=lambda x: len(x[0]), reverse=True)

                        for query, hash in query_hash_pair_list:
                            if main_cache_handler.exists(hash):
                                print(f"Query {hash} already in cache")
                                continue

                            # Check for existing termination bits before submitting
                            if main_cache_handler.exists(f"_LIMIT_{hash}"):
                                print(f"Query {hash} previously hit limit, skipping")
                                continue
                            if main_cache_handler.exists(f"_TIMEOUT_{hash}"):
                                print(f"Query {hash} previously timed out, skipping")
                                continue

                            if hash in active_futures or any(job[1] == hash for job in pending_jobs):
                                print(f"Query {hash} already in process")
                                continue

                            with status_lock:
                                if len(active_futures) < args.max_processes:
                                    future = pool.submit(run_and_store_query, query, hash)
                                    active_futures.append(hash)
                                    future.add_done_callback(lambda f, h=hash: process_completed_future(f, h))
                                    print(f"Started query {hash}")
                                else:
                                    pending_jobs.append((query, hash))
                                    print(f"Queued query {hash}")

                    except redis.ConnectionError as e:
                        logger.error(f"Redis connection error: {e}")
                        time.sleep(5)  # Wait before retrying
                        continue
                    except Exception as e:
                        logger.error(f"Error in job_fetcher: {e}")
                        if not exit_event.is_set():
                            time.sleep(1)  # Brief pause before continuing if not exiting
                            continue
                        break

                # Wait for active jobs to complete when exiting
                logger.info("Waiting for active jobs to complete...")
                while active_futures and not exit_event.wait(timeout=1):
                    with status_lock:
                        logger.info(f"Remaining active jobs: {len(active_futures)}")

        # Start the job fetcher in the main thread
        job_fetcher()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        exit_event.set()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        exit_event.set()
        raise
    finally:
        cleanup_resources()

if __name__ == "__main__":
    main()
