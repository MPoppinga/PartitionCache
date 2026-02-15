"""
Monitor the cache queue and add queries to the cache as they are added to the queue.
"""

import argparse
import concurrent.futures
import datetime
import os
import threading
import time

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
    get_database_connection_params,
    load_environment_with_validation,
    parse_variant_generation_json_args,
    resolve_cache_backend,
)
from partitioncache.db_handler import get_db_handler
from partitioncache.logging_utils import configure_enhanced_logging, get_thread_aware_logger
from partitioncache.query_accelerator import create_query_accelerator
from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.queue import (
    get_queue_lengths,
    pop_from_original_query_queue,
    pop_from_original_query_queue_blocking,
    pop_from_query_fragment_queue,
    pop_from_query_fragment_queue_blocking,
    push_to_query_fragment_queue,
)

logger = get_thread_aware_logger("PartitionCache")

args: argparse.Namespace

# Global query accelerator instance
query_accelerator = None

# Initialize threading components
status_lock = threading.Lock()
active_futures: dict[concurrent.futures.Future, str] = {}  # Map of Future to query_hash
pool: concurrent.futures.ThreadPoolExecutor | None = None  # Initialize pool as None

# Query time logging lock and file handle
query_time_lock = threading.Lock()
query_time_file = None

# Phase 1 Optimization: Remove semaphore bottleneck to enable true 12-thread parallelism
# Note: Database connection pooling should be used instead of artificial threading limits


def initialize_query_time_logging():
    """Initialize query time logging file handle."""
    global query_time_file

    if hasattr(args, "log_query_times"):
        if args.log_query_times:
            try:
                # Open file in unbuffered append mode
                query_time_file = open(args.log_query_times, "a", encoding="utf-8", buffering=1)
            except Exception as e:
                logger.error(f"Failed to initialize query time logging: {e}")
                query_time_file = None
        else:
            logger.info("CSV logging disabled")
    else:
        logger.info("CSV logging missing")


def close_query_time_logging():
    """Close query time logging file handle."""
    global query_time_file
    if query_time_file:
        try:
            query_time_file.close()
            query_time_file = None
            logger.debug("Query time logging file closed")
        except Exception as e:
            logger.warning(f"Error closing query time logging file: {e}")


def log_query_time(query_hash: str, execution_time: float) -> None:
    """
    Log query hash and execution time to the specified log file.

    Args:
        query_hash: The hash identifier of the query
        execution_time: Execution time in seconds
    """

    if not query_time_file:
        logger.warning(f"No file handle (query_hash={query_hash})")
        return

    try:
        with query_time_lock:
            line = f"{query_hash},{execution_time:.6f}\n"
            query_time_file.write(line)
            query_time_file.flush()  # Force immediate write
            # Force OS-level flush for immediate disk write
            os.fsync(query_time_file.fileno())
            logger.info(f"{query_hash}, {execution_time:.6f}s written to CSV")
    except Exception as e:
        logger.error(f"Failed to log {query_hash}: {e}")


exit_event = threading.Event()  # Create an event to signal exit
fragment_processor_exit = threading.Event()  # Exit signal for fragment processor

# Add logging control variables
last_status_log_time = 0.0
status_log_interval = 10  # Log status every 10 seconds when idle


def query_fragment_processor(args, constraint_args):
    """Thread function that processes original queries into fragments and pushes to query fragment queue.

    Args:
        args: Command line arguments
        constraint_args: Tuple of (add_constraints, remove_constraints_all, remove_constraints_add)
    """
    add_constraints, remove_constraints_all, remove_constraints_add = constraint_args
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

            # Detect spatial mode and resolve geometry column from handler
            is_spatial = partition_datatype == "geometry"
            geometry_column = None
            if is_spatial:
                try:
                    spatial_cache_handler = get_cache_handler(resolve_cache_backend(args), singleton=True)
                    geometry_column = getattr(spatial_cache_handler, "geometry_column", "geom")
                except Exception:
                    geometry_column = "geom"

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
                bucket_steps=args.bucket_steps,
                add_constraints=add_constraints,
                remove_constraints_all=remove_constraints_all,
                remove_constraints_add=remove_constraints_add,
                skip_partition_key_joins=is_spatial,
                geometry_column=geometry_column,
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

    if not args.enable_cache_optimization or args.disable_cache_optimization:
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
    """Worker function to execute and store a query."""
    logger.info(f"WORKER THREAD START: Processing query {query_hash}")

    original_query = query
    original_hash = query_hash
    cache_handler = None
    db_handler = None
    success = False

    try:
        cache_backend = resolve_cache_backend(args)
        cache_handler = get_cache_handler(cache_backend, singleton=True)
        logger.info(f"Got cache handler for {query_hash}")

        query_to_execute = query
        optimization_applied = False
        optimization_stats = {}

        if args.enable_cache_optimization and not args.disable_cache_optimization:
            query_to_execute, optimization_applied, optimization_stats = apply_cache_optimization(
                query, query_hash, partition_key, partition_datatype, cache_handler, args
            )

        if partition_datatype is not None:
            logger.info(f"REGISTERING PARTITION KEY: Starting for {query_hash}, key={partition_key}, datatype={partition_datatype}")
            kwargs = {}
            if hasattr(cache_handler, "_get_partition_bitsize"):
                existing_bitsize = cache_handler._get_partition_bitsize(partition_key)  # type: ignore[attr-defined]
                if args.bitsize is not None:
                    kwargs["bitsize"] = args.bitsize
                    if existing_bitsize is not None and existing_bitsize != args.bitsize:
                        logger.info(f"Updating bitsize from {existing_bitsize} to {args.bitsize} for partition '{partition_key}'")
                        cache_handler._set_partition_bitsize(partition_key, args.bitsize)  # type: ignore[attr-defined]
                elif existing_bitsize is not None:
                    # Use existing
                    kwargs["bitsize"] = existing_bitsize
                # else: use handler default
            elif hasattr(args, "bitsize") and args.bitsize is not None:
                kwargs["bitsize"] = args.bitsize
            cache_handler.register_partition_key(partition_key, partition_datatype, **kwargs)
            logger.info(f"PARTITION KEY REGISTERED: Successfully registered for {query_hash}")

        lazy_insertion_available = hasattr(cache_handler, "set_cache_lazy") and hasattr(cache_handler, "set_entry_lazy")
        use_lazy_insertion = (
            lazy_insertion_available and not args.force_recalculate and args.long_running_query_timeout == "0" and not args.disable_lazy_insertion
        )

        if use_lazy_insertion:
            logger.info(f"STARTING LAZY INSERTION: Beginning lazy insertion for query {query_hash}")

            logger.info(f"EXECUTING LAZY INSERTION: Calling set_entry_lazy for {query_hash}")
            t = time.perf_counter()
            try:
                if isinstance(cache_handler, AbstractCacheHandler_Lazy):
                    lazy_success = cache_handler.set_entry_lazy(original_hash, query_to_execute, original_query, partition_key)
                    execution_time = time.perf_counter() - t
                    if lazy_success:
                        logger.info(f"✅ LAZY INSERTION SUCCESS: Lazily stored {original_hash} in cache in {execution_time:.3f}s")
                        log_query_time(query_hash, execution_time)
                        success = True
                    else:
                        logger.warning(f"⚠️ LAZY INSERTION FAILED: Lazy insertion failed for {original_hash}, falling back to traditional method")
                        log_query_time(f"{query_hash}_lazy_failed", execution_time)
            except psycopg.OperationalError as e:
                execution_time = time.perf_counter() - t
                if "statement timeout" in str(e) or "query_canceled" in str(e):
                    logger.info(f"Query {query_hash} is a long running query (detected during lazy insertion)")
                    log_query_time(f"{query_hash}_timeout", execution_time)
                    cache_handler.set_query_status(query_hash, partition_key, "timeout")
                    success = True
                else:
                    logger.error(f"Lazy insertion operational error for {original_hash}: {e}, falling back to traditional method")
                    log_query_time(f"{query_hash}_lazy_exception", execution_time)
            except Exception as lazy_insert_error:
                execution_time = time.perf_counter() - t
                logger.error(f"Lazy insertion exception for {original_hash}: {lazy_insert_error}, falling back to traditional method")
                log_query_time(f"{query_hash}_lazy_exception", execution_time)

        if not use_lazy_insertion or not success:
            # Spatial datatypes require lazy insertion - cannot fall back to execute + set_cache
            if not success and partition_datatype == "geometry":
                logger.error(f"Spatial datatype '{partition_datatype}' requires lazy insertion for query {query_hash}. "
                             "Ensure lazy insertion is enabled and the cache handler supports it.")
                return False

            logger.info(f"Beginning calculation for cache population for query {query_hash}")
            db_connection_params = get_database_connection_params(args)
            if args.db_backend == "postgresql":
                db_connection_params["timeout"] = args.long_running_query_timeout
                db_handler = get_db_handler("postgres", **db_connection_params)
                # TODO query_accelerator duckdb
            elif args.db_backend == "mysql":
                db_handler = get_db_handler("mysql", **db_connection_params)
            elif args.db_backend == "sqlite":
                db_handler = get_db_handler("sqlite", **db_connection_params)
            else:
                raise AssertionError("No db backend specified, querying not possible")

            execution_start = time.perf_counter()
            try:
                # Use DuckDB acceleration when available and enabled
                global query_accelerator
                if query_accelerator and args.enable_duckdb_acceleration and args.db_backend == "postgresql":
                    logger.info(f"Executing query via DuckDB acceleration with timeout={args.long_running_query_timeout}s")
                    result = query_accelerator.execute_query(query_to_execute)
                    execution_time = time.perf_counter() - execution_start
                    logger.info(f"DuckDB acceleration result: {len(result)} rows in {execution_time:.3f}s")
                else:
                    logger.info(f"Executing query via standard database handler with timeout={args.long_running_query_timeout}s")
                    result = set(db_handler.execute(query_to_execute))
                    execution_time = time.perf_counter() - execution_start
                    logger.info(f"Query {query_hash} returned {len(result)} results in {execution_time:.3f}s")

                log_query_time(query_hash, execution_time)

                if args.limit is not None and result is not None and len(result) >= args.limit:
                    logger.info(f"Query {query_hash} limited to {args.limit} partition keys")
                    cache_handler.set_query_status(query_hash, partition_key, "failed")
                else:
                    success_cache = cache_handler.set_cache(original_hash, result, partition_key)
                    success_query = cache_handler.set_query(original_hash, original_query, partition_key)
                    if success_cache and success_query:
                        logger.debug(f"Successfully stored {original_hash} in cache with {len(result)} results")
                    else:
                        logger.error(f"Failed to store {original_hash} in cache (cache_success={success_cache}, query_success={success_query})")
                        cache_handler.set_query_status(original_hash, partition_key, "failed")
                success = True

            except psycopg.OperationalError as e:
                execution_time = time.perf_counter() - execution_start if "execution_start" in locals() else 0
                error_msg = str(e).lower()
                if "timeout" in error_msg or "cancel" in error_msg:
                    logger.warning(f"QUERY TIMEOUT: Query {query_hash} was cancelled due to timeout after {execution_time:.2f}s")
                    log_query_time(f"{query_hash}_timeout", execution_time)
                    cache_handler.set_query_status(query_hash, partition_key, "timeout")
                    success = True
                else:
                    logger.error(f"QUERY EXECUTION ERROR: Failed to execute query for {query_hash} after {execution_time:.2f}s: {type(e).__name__}: {e}")
                    log_query_time(f"{query_hash}_error", execution_time)
                    cache_handler.set_query_status(query_hash, partition_key, "failed")
            finally:
                if db_handler:
                    db_handler.close()
                    db_handler = None

    except Exception as e:
        logger.error(f"WORKER THREAD EXCEPTION: Error processing query {query_hash}: {str(e)}", exc_info=True)
        if cache_handler and "current transaction is aborted" in str(e):
            try:
                cache_handler._connection.rollback()
            except Exception as rollback_error:
                logger.warning(f"Failed to rollback transaction for {query_hash}: {rollback_error}")

    return success


def print_status(active, pending, original_query_queue=0, query_fragment_queue=0):
    logger.info(
        f"Active processes: {active}, Pending jobs: {pending}, Original query queue: {original_query_queue}, Query fragment queue: {query_fragment_queue}"
    )


def print_enhanced_status(active, queue_lengths, waiting_reason=None):
    """Enhanced status logging with queue sizes and waiting context."""
    original_queue = queue_lengths.get("original_query_queue", 0)
    fragment_queue = queue_lengths.get("query_fragment_queue", 0)

    status_msg = f"Active: {active}, Fragment Queue: {fragment_queue}, Original Queue: {original_queue}"

    if waiting_reason:
        status_msg += f" - {waiting_reason}"

    logger.info(status_msg)


def get_timeout_for_state(can_consume, exit_event_set, error_count=0):
    """Determine optimal timeout based on current system state."""
    if exit_event_set:
        return 0.1  # Quick exit during shutdown
    elif error_count > 0:
        return min(0.5 * (2**error_count), 5.0)  # Exponential backoff for errors
    elif can_consume:
        return 10.0  # Shorter blocking to allow periodic status checks
    else:
        return 0.1  # Short wait when waiting for capacity


def fragment_executor():
    """Thread pool function that processes fragments from the fragment queue."""
    global pool, last_status_log_time

    # Initialize cache handler for the main process
    main_cache_handler = get_cache_handler(args.cache_backend, singleton=True)

    # Error tracking for retry logic
    consecutive_errors = 0
    max_consecutive_errors = 5

    # Track previous fragment count to detect when queue becomes empty
    previous_fragment_count = None

    logger.info(f"Starting fragment executor threadpool -- Configuration: max_processes={args.max_processes}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_processes) as pool:
        while not exit_event.is_set():
            try:
                current_time = time.time()

                # Check for completed futures and clean them up
                with status_lock:
                    completed_futures = []
                    for future, query_hash in list(active_futures.items()):
                        if future.done():
                            completed_futures.append((future, query_hash))

                    # Process completed futures
                    for future, query_hash in completed_futures:
                        try:
                            result = future.result()
                            if result:
                                logger.info(f"Query {query_hash} completed successfully")
                            else:
                                logger.warning(f"Query {query_hash} completed with failure")
                        except Exception as e:
                            logger.error(f"Query {query_hash} failed with exception: {e}")

                        # Remove from active futures
                        del active_futures[future]
                        logger.info(f"Removed {query_hash} from active futures (remaining: {len(active_futures)})")

                    active = len(active_futures)

                # Get queue lengths for status
                try:
                    lengths = get_queue_lengths()
                    fragment_count = lengths["query_fragment_queue"]
                except Exception as e:
                    logger.error(f"Error getting queue lengths: {e}")
                    fragment_count = 0
                    lengths = {"original_query_queue": 0, "query_fragment_queue": 0}

                # Detect when queue becomes empty
                if previous_fragment_count is not None and previous_fragment_count > 0 and fragment_count == 0:
                    logger.info("Fragment queue has become empty - waiting for work")
                previous_fragment_count = fragment_count

                # Determine if we should log status
                should_log_status = False
                time_since_last_log = current_time - last_status_log_time

                # Log periodically based on interval
                if time_since_last_log >= args.status_log_interval:
                    should_log_status = True
                    last_status_log_time = current_time
                # Also log when significant changes occur
                elif active == 0 and fragment_count == 0 and previous_fragment_count > 0:
                    # Just became idle - log immediately (don't reset periodic timer)
                    should_log_status = True

                # Check if we should close
                if args.close and active == 0 and fragment_count == 0:
                    logger.info(f"Closing cache at {datetime.datetime.now()}")
                    exit_event.set()
                    continue

                # Determine waiting reason for logging
                waiting_reason = None
                if active >= args.max_processes:
                    waiting_reason = "Waiting for thread capacity"
                elif fragment_count == 0:
                    waiting_reason = "Waiting for work"
                else:
                    waiting_reason = "Normal processing"

                if should_log_status:
                    print_enhanced_status(active, lengths, waiting_reason)

                # Submit new jobs if we have capacity
                while len(active_futures) < args.max_processes:
                    logger.debug(f"Attempting to submit job (active: {len(active_futures)}/{args.max_processes})")

                    # Get dynamic timeout based on state
                    can_consume = len(active_futures) < args.max_processes
                    timeout = get_timeout_for_state(can_consume, exit_event.is_set(), consecutive_errors)

                    # Try to get a job from the queue
                    fragment_result = None
                    if not args.disable_optimized_polling and can_consume:
                        # Check if we should use blocking vs non-blocking pop
                        # Use blocking pop only when we have active jobs (efficient while processing)
                        # Use non-blocking pop when idle to ensure responsive status logging
                        if len(active_futures) > 0:
                            # Use efficient blocking pop with shorter timeout
                            timeout = min(timeout, 2.0)
                            logger.debug(f"Attempting blocking pop with timeout={timeout:.1f}s (active: {len(active_futures)}/{args.max_processes})")
                            fragment_result = pop_from_query_fragment_queue_blocking(timeout=int(timeout))
                        else:
                            # When idle (no active jobs), use non-blocking pop to avoid blocking status logs
                            logger.debug("Attempting non-blocking pop (idle state for responsive logging)")
                            fragment_result = pop_from_query_fragment_queue()
                    elif can_consume:
                        # Use regular pop
                        fragment_result = pop_from_query_fragment_queue()
                    else:
                        # Can't consume right now
                        # Break from inner loop to check for completed futures
                        break

                    if fragment_result is None:
                        # No more jobs available or timeout
                        break

                    query, hash_value, partition_key, partition_datatype = fragment_result
                    logger.debug(f"Found fragment in fragment queue: {hash_value}")

                    # Check if already in cache (unless force-recalculate)
                    if not args.force_recalculate and main_cache_handler.exists(hash_value, partition_key, check_query=False):
                        logger.debug(f"Query {hash_value} already in cache")
                        main_cache_handler.set_query(hash_value, query, partition_key)
                        log_query_time(f"{hash_value}_cache_hit", 0.0)
                        continue
                    elif args.force_recalculate and main_cache_handler.exists(hash_value, partition_key, check_query=False):
                        logger.info(f"Query {hash_value} exists in cache but force-recalculate is enabled")

                    # Check if not already being processed
                    if any(h == hash_value for h in active_futures.values()):
                        logger.debug(f"Query {hash_value} already in process")
                        continue

                    # Submit the job
                    with status_lock:
                        future = pool.submit(run_and_store_query, query, hash_value, partition_key, partition_datatype)
                        active_futures[future] = hash_value
                        logger.info(f"Submitted to threadpool: {hash_value} (active: {len(active_futures)})")

                # Reset error counter on successful iteration
                consecutive_errors = 0

                # Small sleep to prevent CPU spinning in tight loop
                time.sleep(0.05)  # 50ms - allows quick detection of completed futures

            except KeyboardInterrupt:
                logger.info("Received interrupt signal - initiating graceful shutdown")
                exit_event.set()  # Ensure the exit event is set

                # Graceful shutdown: wait for active threads to complete
                with status_lock:
                    active_count = len(active_futures)

                if active_count > 0:
                    logger.info(f"Waiting for {active_count} active threads to complete...")
                    pool.shutdown(wait=True)  # Wait for all processes to finish
                    logger.info("All active threads completed")

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
    processing_group.add_argument(
        "--force-recalculate", action="store_true", default=False, help="Force recalculation of queries even if they already exist in cache"
    )
    processing_group.add_argument("--log-query-times", type=str, help="Log query hash and execution time to specified file in CSV format: <hash>,<seconds>")
    processing_group.add_argument(
        "--disable-lazy-insertion", action="store_true", default=False, help="Disable lazy insertion and always use traditional query execution"
    )

    # Cache optimization configuration
    optimization_group = parser.add_argument_group("cache optimization options")
    optimization_group.add_argument(
        "--enable-cache-optimization", action="store_true", default=True, help="Enable cache-aware optimization for fragment queries"
    )
    optimization_group.add_argument(
        "--disable-cache-optimization", action="store_true", default=False, help="Disable cache-aware optimization for fragment queries"
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

    # Cache backend argument with bitsize support
    cache_group = parser.add_argument_group("cache configuration")
    cache_group.add_argument("--cache-backend", type=str, help="Cache backend to use (if not specified, uses CACHE_BACKEND environment variable)")
    cache_group.add_argument("--bitsize", type=int, help="Bitsize for bit cache handlers (default: uses handler default or environment variable)")

    # DuckDB query acceleration options
    acceleration_group = parser.add_argument_group("query acceleration options")
    acceleration_group.add_argument(
        "--enable-duckdb-acceleration", action="store_true", default=False, help="Enable DuckDB in-memory query acceleration for PostgreSQL queries"
    )
    acceleration_group.add_argument("--preload-tables", type=str, help="Comma-separated list of PostgreSQL tables to preload into DuckDB for faster queries")
    acceleration_group.add_argument("--duckdb-memory-limit", type=str, default="2GB", help="Memory limit for DuckDB instance (default: 2GB)")
    acceleration_group.add_argument("--duckdb-threads", type=int, default=4, help="Number of threads for DuckDB processing (default: 4)")
    acceleration_group.add_argument(
        "--disable-acceleration-stats", action="store_true", default=False, help="Disable DuckDB acceleration performance statistics logging"
    )
    acceleration_group.add_argument(
        "--duckdb-database-path",
        type=str,
        default="/tmp/partitioncache_accel.duckdb",
        help="Path to DuckDB database file (default: /tmp/partitioncache_accel.duckdb, use ':memory:' for in-memory)",
    )
    acceleration_group.add_argument(
        "--force-reload-tables", action="store_true", default=False, help="Force reload tables from PostgreSQL even if they exist in DuckDB"
    )

    global args
    args = parser.parse_args()

    # Configure enhanced logging with thread-aware formatting
    configure_enhanced_logging(verbose=getattr(args, "verbose", False), quiet=getattr(args, "quiet", False), logger_name="PartitionCache")

    logger.info(f"Starting main, Current Time: {datetime.datetime.now()}")
    if args.db_backend is None:
        raise ValueError("db_backend is required")

    # Load environment variables
    load_environment_with_validation(args.env_file)

    # Parse JSON arguments for constraint modifications
    add_constraints, remove_constraints_all, remove_constraints_add = parse_variant_generation_json_args(args)
    constraint_args = (add_constraints, remove_constraints_all, remove_constraints_add)

    # Set cache backend if not specified
    if args.cache_backend is None:
        args.cache_backend = os.getenv("CACHE_BACKEND", "postgresql_bit")

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

    # Initialize DuckDB query accelerator if enabled
    global query_accelerator
    preload_tables = []
    if args.enable_duckdb_acceleration:
        if args.db_backend != "postgresql":
            logger.warning("DuckDB acceleration only supports PostgreSQL backend, disabling acceleration")
            args.enable_duckdb_acceleration = False
        else:
            logger.info("Initializing DuckDB query accelerator...")

            # Parse preload tables
            if args.preload_tables:
                preload_tables = [table.strip() for table in args.preload_tables.split(",") if table.strip()]
                logger.info(f"Will preload {len(preload_tables)} tables: {preload_tables}")

            # Get database connection parameters for accelerator
            db_connection_params = get_database_connection_params(args)

            try:
                query_accelerator = create_query_accelerator(
                    postgresql_connection_params=db_connection_params,
                    preload_tables=preload_tables,
                    duckdb_memory_limit=args.duckdb_memory_limit,
                    duckdb_threads=args.duckdb_threads,
                    enable_statistics=not args.disable_acceleration_stats,
                    duckdb_database_path=args.duckdb_database_path,
                    force_reload_tables=args.force_reload_tables,
                    query_timeout=float(args.long_running_query_timeout),
                )

                if query_accelerator:
                    # Preload tables if specified
                    if preload_tables:
                        success = query_accelerator.preload_tables()
                        if not success:
                            logger.warning("Failed to preload some tables, acceleration may be suboptimal")

                    logger.info("DuckDB query accelerator initialized successfully")
                else:
                    logger.warning("Failed to initialize DuckDB query accelerator, disabling acceleration")
                    args.enable_duckdb_acceleration = False

            except Exception as e:
                logger.error(f"Failed to initialize DuckDB query accelerator: {e}")
                logger.warning("Disabling DuckDB acceleration due to initialization failure")
                args.enable_duckdb_acceleration = False
                query_accelerator = None

    logger.info("Starting two-threaded queue monitoring system:")
    logger.info("- Thread 1: Process original queries into fragments")
    logger.info("- Thread 2: Execute fragments from query fragment queue with enhanced consumption control")
    logger.info("- Partition keys are read from the queue instead of command line arguments")
    logger.info(f"- Configuration: max_processes={args.max_processes}")
    logger.info(f"- Queue provider: {provider}, Cache backend: {args.cache_backend}")
    logger.info(f"- Status logging interval: {args.status_log_interval}s")
    if args.disable_optimized_polling:
        logger.info("- Using regular polling (optimized polling disabled)")
    else:
        logger.info("- Using optimized polling with blocking queue operations")

    # Log DuckDB acceleration status
    if args.enable_duckdb_acceleration and query_accelerator:
        logger.info("- DuckDB query acceleration ENABLED:")
        logger.info(f"  - Memory limit: {args.duckdb_memory_limit}")
        logger.info(f"  - Threads: {args.duckdb_threads}")
        if preload_tables:
            logger.info(f"  - Preloaded tables: {len(preload_tables)} ({', '.join(preload_tables)})")
        else:
            logger.info("  - No tables preloaded")
        logger.info(f"  - Statistics: {'disabled' if args.disable_acceleration_stats else 'enabled'}")
    else:
        logger.info("- DuckDB query acceleration DISABLED")

    if args.enable_cache_optimization and not args.disable_cache_optimization:
        logger.info("- Cache optimization ENABLED:")
        logger.info(f"  - Method: {args.cache_optimization_method}")
        logger.info(f"  - Min cache hits: {args.min_cache_hits}")
        logger.info(f"  - Prefer lazy: {args.prefer_lazy_optimization}")

    # Log lazy insertion configuration
    logger.info(f"- Lazy insertion: {'DISABLED' if args.disable_lazy_insertion else 'ENABLED (when conditions met)'}")
    if not args.disable_lazy_insertion:
        logger.info("  - Conditions: timeout=0, no force-recalculate, handler supports lazy methods")

    # Initialize query time logging
    initialize_query_time_logging()

    # Start the query fragment processor thread
    fragment_processor_thread = threading.Thread(target=query_fragment_processor, args=(args, constraint_args), daemon=True)
    fragment_processor_thread.start()

    # Start the fragment executor in the main thread
    try:
        fragment_executor()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        exit_event.set()
    except Exception as e:
        logger.error(f"Fatal error in fragment executor: {e}")
        exit_event.set()
        raise
    finally:
        # Ensure cleanup happens even if there's an error
        try:
            # Wait for fragment processor thread to finish
            fragment_processor_thread.join(timeout=5)

            # Clean up query accelerator
            if query_accelerator:
                logger.info("Shutting down DuckDB query accelerator...")
                query_accelerator.close()

            # Close query time logging
            close_query_time_logging()
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")

    logger.info("Monitor cache queue shutting down")


if __name__ == "__main__":
    main()
