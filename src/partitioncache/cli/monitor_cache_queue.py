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
import redis

from partitioncache.cache_handler import get_cache_handler
from partitioncache.db_handler import get_db_handler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.query_processor import generate_all_query_hash_pairs

parser = argparse.ArgumentParser()

parser.add_argument("--db_backend", type=str, default="sqlite", help="database backend", choices=["postgresql", "mysql", "sqlite"])
parser.add_argument("--cache_backend", type=str, default="rocksdb", help="cache backend")
parser.add_argument("--db_dir", type=str, default="data/test_db.sqlite", help="database directory")
parser.add_argument("--db_env_file", type=str, help="database environment file")
parser.add_argument("--partition_key", type=str, default="partition_key", help="search space identifier")
parser.add_argument("--close", action="store_true", default=False, help="Close the cache after operation")

parser.add_argument("--max_processes", type=int, default=12, help="max number of processes to use")
## DB Info
parser.add_argument(
    "--db-name",
    dest="db_name",
    action="store",
    type=str,
    help="database name",
)

parser.add_argument("--long_running_query_timeout", type=str, default="0", help="timeout for long running queries")

parser.add_argument("--limit", type=int, default=None, help="limit the number of returned partition keys")

args = parser.parse_args()
# Initialize threading components
status_lock = threading.Lock()
active_futures: list[str] = []
pending_jobs: list[tuple[str, str]] = []
# pool: concurrent.futures.ProcessPoolExecutor | None = None  # This global variable is no longer used.
exit_event = threading.Event()  # Create an event to signal exit
outgoing_fragment_queue = multiprocessing.Queue()


def pop_redis(r: redis.Redis):
    result = r.blpop([os.getenv("QUERY_QUEUE_REDIS_QUEUE_KEY", "query_queue")])
    return result


def run_and_store_query(query: str, hash: str):
    """Worker function to execute and store a query."""
    try:
        cache_handler = get_cache_handler(args.cache_backend)

        db_handler: AbstractDBHandler
        if args.db_backend == "postgresql":
            if args.db_env_file is None:
                raise ValueError("db_env_file is required")

            db_handler = get_db_handler('postgres', 
                host=os.getenv("PG_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("PG_DB_PORT", os.getenv("DB_PORT", 5432))),
                user=os.getenv("PG_DB_USER", os.getenv("DB_USER", "postgres")),
                password=os.getenv("PG_DB_PASSWORD", os.getenv("DB_PASSWORD", "postgres")),
                dbname=args.db_name,
                timeout=args.long_running_query_timeout,
            )
        elif args.db_backend == "mysql":
            db_handler = get_db_handler('mysql',
                host=os.getenv("MY_DB_HOST", os.getenv("DB_HOST", "localhost")),
                port=int(os.getenv("MY_DB_PORT", os.getenv("DB_PORT", 3306))),
                user=os.getenv("MY_DB_USER", os.getenv("DB_USER", "root")),
                password=os.getenv("MY_DB_PASSWORD", os.getenv("DB_PASSWORD", "root")),
                dbname=args.db_name,
            )
        elif args.db_backend == "sqlite":
            db_handler = get_db_handler('sqlite', db_path=args.db_dir)
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


def process_completed_future(future, hash, pool_executor: concurrent.futures.ProcessPoolExecutor):
    """Process a completed future and update job status."""
    # global pool  # No longer needed, pool_executor is passed as an argument
    if pool_executor is None: # Should not happen if called correctly
        raise AssertionError("No pool_executor provided to process_completed_future")

    with status_lock:
        if hash in active_futures:
            active_futures.remove(hash)
            print(f"Completed query {hash}")
        else:
            print(f"Warning: Completed hash {hash} not found in active_futures")

        # Start a new job from pending if available
        if pending_jobs:
            next_query, next_hash = pending_jobs.pop(0)
            new_future = pool_executor.submit(run_and_store_query, next_query, next_hash)
            active_futures.append(next_hash)
            new_future.add_done_callback(lambda f: process_completed_future(f, next_hash, pool_executor))
            print(f"Started query {next_hash} from pending queue")

        print_status(len(active_futures), len(pending_jobs))
        if args.close and len(active_futures) == 0 and len(pending_jobs) == 0:
            print("Closing cache at ", datetime.datetime.now())
            exit_event.set()  # Signal to exit
            


def main():
    print("Starting main, Current Time: ", datetime.datetime.now())
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

        r = redis.Redis(
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
        # global pool  # Make pool accessible in process_completed_future # No longer needed here
        # with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_processes) as pool: # pool is managed in main
        while not exit_event.is_set():
            try:
                print("Waiting for query")
                # with status_lock: # Not needed here anymore, status is managed by worker controller
                #     active = len(active_futures)
                #     pending = len(pending_jobs)
                #     print_status(active, pending)
                rr: list[bytes]

                rr = pop_redis(r)  # type: ignore
                print("Found query in queue")

                query = rr[1].decode("utf-8")

                # Process the query
                query_hash_pair_list = generate_all_query_hash_pairs(query, args.partition_key, 1, True, True)

                print(f"Found {len(query_hash_pair_list)} subqueries in query to store in cache")

                # Order by length of query
                query_hash_pair_list.sort(key=lambda x: len(x[0]), reverse=True)

                for query, hash in query_hash_pair_list:
                    # The following checks are now responsibility of the worker process or a pre-processing step if needed.
                    # if main_cache_handler.exists(hash):
                    #     print(f"Query {hash} already in cache")
                    #     continue
                    #
                    # # Check for existing termination bits before submitting
                    # if main_cache_handler.exists(f"_LIMIT_{hash}"):
                    #     print(f"Query {hash} previously hit limit, skipping")
                    #     continue
                    # if main_cache_handler.exists(f"_TIMEOUT_{hash}"):
                    #     print(f"Query {hash} previously timed out, skipping")
                    #     continue
                    #
                    # if hash in active_futures or any(job[1] == hash for job in pending_jobs):
                    #     print(f"Query {hash} already in process")
                    #     continue

                    outgoing_fragment_queue.put((query, hash))
                    print(f"Added fragment {hash} to outgoing queue")

            except KeyboardInterrupt:
                print("Exiting")
                exit_event.set()  # Ensure the exit event is set
                # pool.shutdown(wait=True)  # pool is managed in main
                break
            except Exception as e:
                print(f"Error in job_fetcher: {str(e)}")
                # Potentially add more robust error handling or signaling here
                # For now, just print and continue, or break if it's critical
                if exit_event.is_set(): # If already exiting, don't try to recover
                    break
                time.sleep(1) # Avoid busy-looping on persistent errors


    # Start the job fetcher in a separate thread (this will be handled in the next step)
    # For now, setting up the main processing loop that consumes from outgoing_fragment_queue
    
    fetcher_thread = threading.Thread(target=job_fetcher, daemon=True) # daemon=True allows main program to exit even if thread is running
    fetcher_thread.start()

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.max_processes) as pool_executor:
        while not exit_event.is_set():
            try:
                query, hash = outgoing_fragment_queue.get(timeout=1)
                print(f"Retrieved fragment {hash} from outgoing_fragment_queue")

                if main_cache_handler.exists(hash):
                    print(f"Query {hash} already in cache, skipping submission.")
                    continue
                if main_cache_handler.exists(f"_LIMIT_{hash}"):
                    print(f"Query {hash} previously hit limit, skipping submission.")
                    continue
                if main_cache_handler.exists(f"_TIMEOUT_{hash}"):
                    print(f"Query {hash} previously timed out, skipping submission.")
                    continue
                
                with status_lock:
                    if hash in active_futures or any(job[1] == hash for job in pending_jobs):
                        print(f"Query {hash} already in process, skipping submission.")
                        continue

                    if len(active_futures) < args.max_processes:
                        future = pool_executor.submit(run_and_store_query, query, hash)
                        active_futures.append(hash)
                        # Pass pool_executor to the callback
                        future.add_done_callback(lambda f, h=hash: process_completed_future(f, h, pool_executor))
                        print(f"Started query {hash} from outgoing_fragment_queue")
                    else:
                        pending_jobs.append((query, hash))
                        print(f"Queued query {hash} from outgoing_fragment_queue (pool full)")
                    
                    print_status(len(active_futures), len(pending_jobs))

            except multiprocessing.queues.Empty: # Corrected exception type
                # print("outgoing_fragment_queue is empty, continuing...")
                if args.close and len(active_futures) == 0 and len(pending_jobs) == 0 and outgoing_fragment_queue.empty():
                    # If close is signaled, no active/pending jobs, and queue is empty, then break
                    # This check is added here to ensure that if job_fetcher finishes and signals close,
                    # and all processing is done, the main loop can also terminate.
                    print("All jobs processed and queue is empty after close signal. Exiting main processing loop.")
                    exit_event.set() # Ensure other parts of the system know to exit
                    break
                time.sleep(0.1)  # Prevent busy-waiting
                continue
            except KeyboardInterrupt:
                print("Main loop interrupted. Shutting down...")
                exit_event.set()
                break
            except Exception as e:
                print(f"Error in main processing loop: {str(e)}")
                exit_event.set() # Signal exit on unexpected error
                break
        
        print("Main processing loop finished. Waiting for fetcher thread to join...")
        fetcher_thread.join(timeout=5) # Wait for fetcher to finish, with a timeout
        print("Fetcher thread joined. Shutting down ProcessPoolExecutor...")
        # The 'with' statement handles pool_executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
