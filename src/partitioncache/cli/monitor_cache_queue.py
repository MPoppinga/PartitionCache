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
from logging import getLogger

import psycopg

from partitioncache.apply_cache import (
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
    get_partition_keys,
    get_partition_keys_lazy,
)
from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from partitioncache.cli.common_args import (
    add_database_args,
    add_environment_args,
    add_variant_generation_args,
    add_verbosity_args,
    configure_logging,
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

logger = getLogger("PartitionCache")

args: argparse.Namespace

# Initialize threading components
status_lock = threading.Lock()
active_futures: list[str] = []
pending_jobs: list[tuple[str, str, str, str]] = []  # (query, hash, partition_key, partition_datatype)
pool: concurrent.futures.ThreadPoolExecutor | None = None  # Initialize pool as None
exit_event = threading.Event()  # Create an event to signal exit
fragment_processor_exit = threading.Event()  # Exit signal for fragment processor

# Add logging control variables
last_status_log_time = 0.0
status_log_interval = 10  # Log status every 10 seconds when idle


def query_fragment_processor():
    """Thread function that processes original queries into fragments and pushes to query fragment queue."""
    logger.info("Starting query fragment processor thread")

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
            logger.debug(f"Processing original query into fragments for partition_key: {partition_key} (datatype: {partition_datatype})")

            # Process the query into fragments using the partition_key from queue
            query_hash_pairs = generate_all_query_hash_pairs(
                query,
                partition_key,
                min_component_size=args.min_component_size,
                follow_graph=args.follow_graph,
                keep_all_attributes=True,
                auto_detect_star_join=not args.no_auto_detect_star_join,
                max_component_size=args.max_component_size,
                star_join_table=args.star_join_table,
                warn_no_partition_key=not args.no_warn_partition_key,
            )
            logger.debug(f"Generated {len(query_hash_pairs)} fragments from original query")

            # Push fragments to query fragment queue using the partition_key and datatype from queue
            success = push_to_query_fragment_queue(query_hash_pairs, partition_key, partition_datatype)
            if success:
                logger.debug(f"Pushed {len(query_hash_pairs)} fragments to query fragment queue")
            else:
                logger.error("Error pushing fragments to query fragment queue")

        except Exception as e:
            logger.error(f"Error in query fragment processor: {e}")
            time.sleep(1)  # Brief pause before retrying

    logger.info("Query fragment processor thread exiting")


def apply_cache_optimization(query: str, query_hash: str, partition_key: str, partition_datatype: str | None, cache_handler, args) -> tuple[str, bool, dict]:
    """
    Apply cache-aware optimization to a query before execution.

    Args:
        query: SQL query to potentially optimize
        query_hash: Hash of the original query
        partition_key: Partition key for the query
        partition_datatype: Datatype of the partition key
        cache_handler: Cache handler instance
        args: Command line arguments

    Returns:
        tuple: (optimized_query, was_optimized, stats)
            - optimized_query: Either the original or optimized query
            - was_optimized: Whether optimization was applied
            - stats: Dictionary with optimization statistics
    """
    stats = {"total_hashes": 0, "cache_hits": 0, "method_used": None}

    if not args.enable_cache_optimization:
        return query, False, stats

    try:
        # Check if we should use lazy optimization
        use_lazy = args.prefer_lazy_optimization and isinstance(cache_handler, AbstractCacheHandler_Lazy)

        if use_lazy:
            # Use lazy intersection - no min-hits check needed
            lazy_subquery, total_hashes, hits = get_partition_keys_lazy(
                query=query,
                cache_handler=cache_handler,
                partition_key=partition_key,
                min_component_size=args.min_component_size,
                canonicalize_queries=False,
                follow_graph=args.follow_graph,
            )

            stats["total_hashes"] = total_hashes
            stats["cache_hits"] = hits

            if hits > 0 and lazy_subquery:
                # Map method names for lazy optimization
                lazy_method_map = {
                    "IN": "IN_SUBQUERY",
                    "IN_SUBQUERY": "IN_SUBQUERY",
                    "TMP_TABLE_IN": "TMP_TABLE_IN",
                    "TMP_TABLE_JOIN": "TMP_TABLE_JOIN",
                }
                method = lazy_method_map.get(args.cache_optimization_method, "IN_SUBQUERY")
                stats["method_used"] = method

                optimized_query = extend_query_with_partition_keys_lazy(
                    query=query,
                    lazy_subquery=lazy_subquery,
                    partition_key=partition_key,
                    method=method,  # type: ignore[arg-type]
                    analyze_tmp_table=True,
                )

                logger.info(f"Applied lazy cache optimization to {query_hash}: {hits}/{total_hashes} cache hits, method={method}")
                return optimized_query, True, stats
        else:
            # Use standard intersection with min-hits check
            partition_keys, total_hashes, hits = get_partition_keys(
                query=query,
                cache_handler=cache_handler,
                partition_key=partition_key,
                min_component_size=args.min_component_size,
                canonicalize_queries=False,
            )

            stats["total_hashes"] = total_hashes
            stats["cache_hits"] = hits

            if hits >= args.min_cache_hits and partition_keys:
                method = args.cache_optimization_method
                stats["method_used"] = method

                optimized_query = extend_query_with_partition_keys(
                    query=query,
                    partition_keys=partition_keys,
                    partition_key=partition_key,
                    method=method,
                    analyze_tmp_table=True,
                )

                logger.info(f"Applied cache optimization to {query_hash}: {hits}/{total_hashes} cache hits, {len(partition_keys)} keys, method={method}")
                return optimized_query, True, stats

        # No optimization applied
        if stats["cache_hits"] > 0:
            logger.debug(f"Cache hits below threshold for {query_hash}: {stats['cache_hits']}/{stats['total_hashes']} (min={args.min_cache_hits})")

    except Exception as e:
        logger.warning(f"Failed to apply cache optimization to {query_hash}: {e}")

    return query, False, stats


def run_and_store_query(query: str, query_hash: str, partition_key: str, partition_datatype: str | None = None):
    """Worker function to execute and store a query.

    Args:
        query: SQL query string to execute
        query_hash: Unique hash identifier for the query
        partition_key: Partition key for organizing cached results
        partition_datatype: Datatype of the partition key (default: None)
    """
    original_query = query  # Store the original query for cache storage
    original_hash = query_hash  # Preserve the original hash

    try:
        # Resolve cache backend
        cache_backend = resolve_cache_backend(args)
        cache_handler = get_cache_handler(cache_backend, singleton=True)

        # Apply cache optimization if enabled
        query_to_execute = query
        optimization_applied = False
        optimization_stats = {}

        if args.enable_cache_optimization:
            query_to_execute, optimization_applied, optimization_stats = apply_cache_optimization(
                query, query_hash, partition_key, partition_datatype, cache_handler, args
            )

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
            result = set(db_handler.execute(query_to_execute))

            # Apply limit if specified
            if args.limit is not None and result is not None and len(result) >= args.limit:
                logger.info(f"Query {query_hash} limited to {args.limit} partition keys")
                cache_handler.set_query_status(query_hash, partition_key, "failed")  # Set failed status for limit exceeded
                return True

        except psycopg.OperationalError as e:
            if "statement timeout" in str(e):
                logger.info(f"Query {query_hash} is a long running query")
                cache_handler.set_query_status(query_hash, partition_key, "timeout")  # Set timeout status
                return True
            else:
                raise e

        logger.debug(f"Running query {query_hash} for partition_key: {partition_key} (datatype: {partition_datatype})")
        logger.debug(f"Query {query_hash} returned {len(result)} results")
        logger.debug(f"Query {query_hash} took {time.perf_counter() - t} seconds")
        db_handler.close()

        if partition_datatype is not None:
            cache_handler.register_partition_key(partition_key, partition_datatype)

        # Always store with original hash and query (not the optimized version)
        cache_handler.set_set(original_hash, result, partition_key)
        cache_handler.set_query(original_hash, original_query, partition_key)
        logger.debug(f"Stored {original_hash} in cache")

        if optimization_applied:
            logger.info(
                f"Query {original_hash} executed with cache optimization: {optimization_stats['cache_hits']} hits, method={optimization_stats['method_used']}"
            )
        return True
    except Exception as e:
        logger.error(f"Error processing query {query_hash}: {str(e)}")
        with multiprocessing.Lock():
            with open("tmp/error_sql_file.sql", "a") as f:
                f.write(f"Error from run on {datetime.datetime.now()}\n")
                f.write(query + "\n")
        return False


def print_status(active, pending, original_query_queue=0, query_fragment_queue=0):
    logger.info(
        f"Active processes: {active}, Pending jobs: {pending}, Original query queue: {original_query_queue}, Query fragment queue: {query_fragment_queue}"
    )


def print_enhanced_status(active, pending, queue_lengths, waiting_reason=None):
    """Enhanced status logging with queue sizes and waiting context."""
    original_queue = queue_lengths.get("original_query_queue", 0)
    fragment_queue = queue_lengths.get("query_fragment_queue", 0)

    status_msg = f"Active: {active}, Pending: {pending}, Fragment Queue: {fragment_queue}, Original Queue: {original_queue}"

    if waiting_reason:
        status_msg += f" - {waiting_reason}"

    logger.info(status_msg)


def get_timeout_for_state(can_consume, exit_event_set, error_count=0):
    """Determine optimal timeout based on current system state."""
    if exit_event_set:
        return 0.5  # Quick exit during shutdown
    elif error_count > 0:
        return min(0.5 * (2**error_count), 5.0)  # Exponential backoff for errors
    elif can_consume:
        return 60.0  # Long blocking for efficiency when we can process
    else:
        return 1.0  # Short wait when waiting for capacity


def process_pending_job_if_available():
    """Process a pending job if threads are available. Returns True if job was processed."""
    global pool
    if pool is None:
        return False

    with status_lock:
        if pending_jobs and len(active_futures) < args.max_processes:
            next_query, next_hash, next_partition_key, next_partition_datatype = pending_jobs.pop(0)
            future = pool.submit(run_and_store_query, next_query, next_hash, next_partition_key, next_partition_datatype)
            active_futures.append(next_hash)
            future.add_done_callback(lambda f, h=next_hash: process_completed_future(f, h))  # type: ignore[misc]
            logger.debug(f"Started pending query {next_hash}")
            return True
    return False


def process_completed_future(future, query_hash):
    """Process a completed future and update job status."""
    global pool  # Ensure pool is accessible
    if pool is None:
        raise AssertionError("No pool set up")

    with status_lock:
        if query_hash in active_futures:
            active_futures.remove(query_hash)
            logger.debug(f"Completed query {query_hash}")
        else:
            logger.warning(f"Warning: Completed hash {query_hash} not found in active_futures")

        # Start a new job from pending if available
        if pending_jobs:
            next_query, next_hash, next_partition_key, next_partition_datatype = pending_jobs.pop(0)
            new_future = pool.submit(run_and_store_query, next_query, next_hash, next_partition_key, next_partition_datatype)
            active_futures.append(next_hash)
            new_future.add_done_callback(lambda f, h=next_hash: process_completed_future(f, h))  # type: ignore[misc]
            logger.debug(f"Started query {next_hash} from pending queue")

        # Check queue lengths for status using enhanced logging
        try:
            lengths = get_queue_lengths()
            print_enhanced_status(len(active_futures), len(pending_jobs), lengths, "Job completed")
        except Exception as e:
            logger.error(f"Error getting queue lengths: {e}")
            # Fallback to basic status if queue length check fails
            lengths = {"original_query_queue": 0, "query_fragment_queue": 0}
            print_enhanced_status(len(active_futures), len(pending_jobs), lengths, "Job completed (queue lengths unavailable)")

        if args.close and len(active_futures) == 0 and len(pending_jobs) == 0:
            logger.info(f"Closing cache at {datetime.datetime.now()}")
            exit_event.set()  # Signal to exit


def fragment_executor():
    """Thread pool function that processes fragments from the fragment queue."""
    global pool, last_status_log_time

    # Initialize cache handler for the main process
    main_cache_handler = get_cache_handler(args.cache_backend, singleton=True)

    # Error tracking for retry logic
    consecutive_errors = 0
    max_consecutive_errors = 5

    logger.info("Starting fragment executor")
    logger.info(f"Configuration: max_processes={args.max_processes}, max_pending_jobs={args.max_pending_jobs}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_processes) as pool:
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
                    logger.error(f"Error getting queue lengths: {e}")
                    incoming_count = fragment_count = 0
                    lengths = {"original_query_queue": 0, "query_fragment_queue": 0}

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

                # Priority 1: Process pending jobs first if threads are available
                if process_pending_job_if_available():
                    if should_log_status:
                        print_enhanced_status(active, pending, lengths, "Processing pending jobs")
                    continue

                # Determine if we can consume from queue
                can_consume = (active < args.max_processes) and (pending < args.max_pending_jobs)

                # Determine waiting reason for logging
                waiting_reason = None
                if active >= args.max_processes:
                    waiting_reason = "Waiting for thread capacity"
                elif pending >= args.max_pending_jobs:
                    waiting_reason = "Pending buffer full"
                elif fragment_count == 0:
                    waiting_reason = "Waiting for work"
                else:
                    waiting_reason = "Normal processing"

                if should_log_status:
                    print_enhanced_status(active, pending, lengths, waiting_reason)

                # Priority 2: Consume from queue only when we have capacity
                fragment_result = None
                if can_consume:
                    # Get dynamic timeout based on state and error count
                    timeout = get_timeout_for_state(can_consume, exit_event.is_set(), consecutive_errors)

                    # Pop fragment from fragment queue with appropriate timeout
                    if not args.disable_optimized_polling:
                        # Use efficient blocking pop (with LISTEN/NOTIFY for PostgreSQL, native blocking for Redis)
                        queue_provider = get_queue_provider_name()
                        logger.debug(f"Using {queue_provider} blocking pop with timeout {timeout}s")
                        fragment_result = pop_from_query_fragment_queue_blocking(timeout=int(timeout))
                    else:
                        # Use regular pop when optimized polling is disabled
                        queue_provider = get_queue_provider_name()
                        logger.debug(f"Using regular pop for {queue_provider}")
                        fragment_result = pop_from_query_fragment_queue()
                else:
                    # Cannot consume - wait briefly for capacity
                    timeout = get_timeout_for_state(False, exit_event.is_set(), consecutive_errors)
                    logger.debug(f"Cannot consume (active: {active}/{args.max_processes}, pending: {pending}/{args.max_pending_jobs}), waiting {timeout}s")
                    time.sleep(timeout)
                    continue

                # Reset error counter on successful queue operation
                consecutive_errors = 0

                if fragment_result is None:
                    continue  # Timeout occurred, check exit event and try again

                query, hash_value, partition_key, partition_datatype = fragment_result
                logger.debug(f"Found fragment in fragment queue: {hash_value} for partition_key: {partition_key} (datatype: {partition_datatype})")

                # Check if already in cache with valid status (unified approach)
                if main_cache_handler.exists(hash_value, partition_key, check_invalid_too=False):
                    logger.debug(f"Query {hash_value} already in cache with valid status")
                    main_cache_handler.set_query(hash_value, query, partition_key)  # update the query last_seen
                    continue

                if hash_value in active_futures or any(job[1] == hash_value for job in pending_jobs):
                    logger.debug(f"Query {hash_value} already in process")
                    continue

                # Submit job with buffer limit protection
                with status_lock:
                    if len(active_futures) < args.max_processes:
                        future = pool.submit(run_and_store_query, query, hash_value, partition_key, partition_datatype)
                        active_futures.append(hash_value)
                        future.add_done_callback(lambda f, h=hash_value: process_completed_future(f, h))  # type: ignore
                        logger.debug(f"Started query {hash_value}")
                    elif len(pending_jobs) < args.max_pending_jobs:
                        pending_jobs.append((query, hash_value, partition_key, partition_datatype))
                        logger.debug(f"Queued query {hash_value} (pending: {len(pending_jobs)}/{args.max_pending_jobs})")
                    else:
                        # This should not happen due to our consumption logic, but safety check
                        logger.warning(f"Cannot queue {hash_value}: pending buffer full ({len(pending_jobs)}/{args.max_pending_jobs})")
                        # Re-add to queue (this is a fallback - shouldn't normally happen)
                        logger.warning("This indicates a logic error in consumption control")

            except KeyboardInterrupt:
                logger.info("Received interrupt signal - initiating graceful shutdown")
                exit_event.set()  # Ensure the exit event is set

                # Graceful shutdown: wait for active threads to complete
                with status_lock:
                    active_count = len(active_futures)
                    pending_count = len(pending_jobs)

                if active_count > 0:
                    logger.info(f"Waiting for {active_count} active threads to complete...")
                    pool.shutdown(wait=True)  # Wait for all processes to finish
                    logger.info("All active threads completed")

                if pending_count > 0:
                    logger.warning(f"Shutdown with {pending_count} pending jobs - these will be lost")

                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in fragment executor (consecutive errors: {consecutive_errors}): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(f"Too many consecutive errors ({consecutive_errors}), shutting down")
                    exit_event.set()
                    break

                # Use exponential backoff for error recovery
                error_sleep = get_timeout_for_state(False, False, consecutive_errors)
                logger.info(f"Retrying in {error_sleep}s...")
                time.sleep(error_sleep)

    logger.info("Fragment executor exiting")


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
    processing_group.add_argument(
        "--status-log-interval", type=int, default=10, help="Interval in seconds for logging status when queues are empty (default: 10)"
    )
    processing_group.add_argument("--disable-optimized-polling", action="store_true", help="Disable optimized polling and use simple polling")
    processing_group.add_argument("--max-pending-jobs", type=int, help="Maximum number of jobs to keep in pending buffer (default: 2 * max_processes)")

    # Cache optimization configuration
    optimization_group = parser.add_argument_group("cache optimization options")
    optimization_group.add_argument(
        "--enable-cache-optimization", action="store_true", default=True, help="Enable cache-aware optimization for fragment queries"
    )
    optimization_group.add_argument(
        "--cache-optimization-method",
        type=str,
        default="IN",
        choices=["IN", "VALUES", "IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"],
        help="Method for applying cache restrictions (default: IN)",
    )
    optimization_group.add_argument("--min-cache-hits", type=int, default=1, help="Minimum cache hits required for non-lazy optimization (default: 1)")
    optimization_group.add_argument("--prefer-lazy-optimization", action="store_true", default=True, help="Prefer lazy methods when available (default: True)")
    optimization_group.add_argument(
        "--no-prefer-lazy-optimization", dest="prefer_lazy_optimization", action="store_false", help="Disable preference for lazy optimization"
    )

    # Add common argument groups
    add_database_args(parser)
    add_environment_args(parser)
    add_variant_generation_args(parser)
    add_verbosity_args(parser)

    # Cache backend argument (not using add_cache_args because this tool doesn't need partition args)
    parser.add_argument("--cache-backend", type=str, help="Cache backend to use (if not specified, uses CACHE_BACKEND environment variable)")

    global args
    args = parser.parse_args()

    # Configure logging based on verbosity
    configure_logging(args)

    logger.info(f"Starting main, Current Time: {datetime.datetime.now()}")
    if args.db_backend is None:
        raise ValueError("db_backend is required")

    # Load environment variables
    load_environment_with_validation(args.env_file)

    # Set cache backend if not specified
    if args.cache_backend is None:
        args.cache_backend = os.getenv("CACHE_BACKEND", "postgresql_bit")

    # Validate and set max_pending_jobs
    if args.max_pending_jobs is None:
        args.max_pending_jobs = 2 * args.max_processes
        logger.debug(f"Setting max_pending_jobs to default: {args.max_pending_jobs} (2 * max_processes)")
    elif args.max_pending_jobs < 1:
        raise ValueError(f"max_pending_jobs must be >= 1, got: {args.max_pending_jobs}")
    else:
        logger.debug(f"Using max_pending_jobs: {args.max_pending_jobs}")

    if args.max_pending_jobs > 10 * args.max_processes:
        logger.warning(
            f"max_pending_jobs ({args.max_pending_jobs}) is very large compared to max_processes ({args.max_processes}). Consider reducing for better memory efficiency."
        )

    # Validate queue configuration (supports both PostgreSQL and Redis)
    try:
        is_valid = validate_queue_configuration()
        if not is_valid:
            raise ValueError("Invalid queue configuration")
        provider = os.environ.get("QUERY_QUEUE_PROVIDER", "postgresql")
        logger.info(f"Using queue provider: {provider}")
    except ValueError as e:
        logger.error(f"Queue configuration error: {e}")
        logger.error("For PostgreSQL queues, set: PG_QUEUE_HOST, PG_QUEUE_PORT, PG_QUEUE_USER, PG_QUEUE_PASSWORD, PG_QUEUE_DB")
        logger.error("For Redis queues, set: REDIS_HOST, REDIS_PORT, QUERY_QUEUE_REDIS_DB, QUERY_QUEUE_REDIS_QUEUE_KEY")
        logger.error("Set QUERY_QUEUE_PROVIDER to 'redis' to use Redis instead of PostgreSQL")
        raise

    logger.info("Starting two-threaded queue monitoring system:")
    logger.info("- Thread 1: Process original queries into fragments")
    logger.info("- Thread 2: Execute fragments from query fragment queue with enhanced consumption control")
    logger.info("- Partition keys are read from the queue instead of command line arguments")
    logger.info(f"- Configuration: max_processes={args.max_processes}, max_pending_jobs={args.max_pending_jobs}")
    logger.info(f"- Queue provider: {provider}, Cache backend: {args.cache_backend}")
    logger.info(f"- Status logging interval: {args.status_log_interval}s")
    if args.disable_optimized_polling:
        logger.info("- Using regular polling (optimized polling disabled)")
    else:
        logger.info("- Using optimized polling with blocking queue operations")

    if args.enable_cache_optimization:
        logger.info("- Cache optimization ENABLED:")
        logger.info(f"  - Method: {args.cache_optimization_method}")
        logger.info(f"  - Min cache hits: {args.min_cache_hits}")
        logger.info(f"  - Prefer lazy: {args.prefer_lazy_optimization}")

    # Start the query fragment processor thread
    fragment_processor_thread = threading.Thread(target=query_fragment_processor, daemon=True)
    fragment_processor_thread.start()

    # Start the fragment executor in the main thread
    try:
        fragment_executor()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        exit_event.set()

    # Wait for fragment processor thread to finish
    fragment_processor_thread.join(timeout=5)
    logger.info("Monitor cache queue shutting down")


if __name__ == "__main__":
    main()
